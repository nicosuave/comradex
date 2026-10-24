//! Native controls own a physical socket, including after a parent terminal.
//! We relay the backend's acknowledgements, never reconstruct control history or
//! retry an ambiguously delivered control. The backend owns application semantics.

use super::*;
use serde_json::Value;
use std::collections::HashSet;

const MAX_RESPONSES: usize = 128;
const MAX_CONTROLS: usize = 32;
const MAX_CONTROL_BYTES: usize = 8 * 1024 * 1024;
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(90);
const CHAIN_TIMEOUT: Duration = Duration::from_secs(30 * 60);

pub(super) fn is_control(kind: Option<&str>) -> bool {
    matches!(kind, Some("response.steer" | "response.inject"))
}

pub(super) fn interrupt_response_id(value: &Value) -> std::result::Result<&str, &'static str> {
    if value["mode"] != "discard_partial_items" {
        return Err("unsupported response.interrupt mode");
    }
    value["response_id"]
        .as_str()
        .filter(|id| !id.is_empty() && id.len() <= 512)
        .ok_or("response.interrupt requires a response identity")
}

pub(super) struct Candidate {
    response_id: String,
    settings: Value,
    soft_keys: Vec<crate::routing::ThreadKey>,
    calls: HashSet<String>,
    tracking_overflow: bool,
}

impl Candidate {
    pub(super) fn response_id(&self) -> &str {
        &self.response_id
    }

    pub(super) fn is_result_continuation(&self, value: &Value) -> bool {
        self.settings.pointer("/multi_agent/enabled") == Some(&Value::Bool(true))
            && value["type"] == "response.create"
            && value["input"].as_array().is_some_and(|input| {
                input.iter().any(|item| {
                    matches!(
                        item["type"].as_str(),
                        Some(
                            "function_call_output"
                                | "custom_tool_call_output"
                                | "mcp_approval_response"
                        )
                    )
                })
            })
    }

    pub(super) fn new(
        event: &Value,
        create: &Value,
        soft_keys: Vec<crate::routing::ThreadKey>,
    ) -> Option<Self> {
        let response_id = event.pointer("/response/id")?.as_str()?;
        if response_id.is_empty() || response_id.len() > 512 {
            return None;
        }
        Some(Self {
            response_id: response_id.into(),
            settings: settings(create),
            soft_keys,
            calls: HashSet::new(),
            tracking_overflow: false,
        })
    }

    pub(super) fn observe(&mut self, event: &Value) {
        let response = websocket_protocol::response_id(event);
        if response.is_some_and(|id| id != self.response_id) {
            return;
        }
        if event["type"] == "response.output_item.done" {
            self.remember_call(&event["item"]);
        }
        if let Some(output) = event.pointer("/response/output").and_then(Value::as_array) {
            for item in output {
                self.remember_call(item);
            }
        }
    }

    fn remember_call(&mut self, item: &Value) {
        let id = match item["type"].as_str() {
            Some("function_call" | "custom_tool_call") => item["call_id"].as_str(),
            Some("mcp_approval_request") => item["id"].as_str(),
            _ => None,
        };
        if let Some(id) = id {
            if id.len() > 512 || self.calls.len() >= 1024 {
                self.tracking_overflow = true;
            } else {
                self.calls.insert(id.into());
            }
        }
    }

    pub(super) fn validate_control(&self, value: &Value) -> std::result::Result<(), &'static str> {
        let injection = value["type"] == "response.inject";
        let id = if injection {
            &value["response_id"]
        } else {
            &value["previous_response_id"]
        };
        if id.as_str() != Some(self.response_id.as_str()) {
            return Err("control response does not belong to the active socket owner");
        }
        if value["type"] == "response.create" {
            return Ok(());
        }
        if injection {
            if self.settings.pointer("/multi_agent/enabled") != Some(&Value::Bool(true)) {
                return Err("response.inject requires an explicitly enabled multi_agent create");
            }
            let Some(input) = value["input"].as_array().filter(|input| !input.is_empty()) else {
                return Err("response.inject requires nonempty function results");
            };
            if self.tracking_overflow {
                return Err("native call identity limit exceeded");
            }
            for item in input {
                if item["type"] != "function_call_output"
                    || !item["output"].is_string()
                    || !item["call_id"]
                        .as_str()
                        .is_some_and(|id| self.calls.contains(id))
                {
                    return Err(
                        "injection requires a string result for a call advertised by this response; use a same-parent create for rich results",
                    );
                }
            }
        } else if self.settings.pointer("/multi_agent/enabled") == Some(&Value::Bool(true)) {
            return Err("steering and multi-agent injection cannot share a native chain");
        }
        Ok(())
    }
}

fn settings(value: &Value) -> Value {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        for key in ["type", "input", "previous_response_id"] {
            object.remove(key);
        }
    }
    value
}

struct OwnedChain {
    candidate: Candidate,
    responses: HashSet<String>,
    submitted_calls: HashSet<String>,
    injection: Option<(tokio::time::Instant, Vec<String>, Value)>,
    steers: VecDeque<tokio::time::Instant>,
    accepted_steers: HashSet<String>,
    successor_deadline: Option<tokio::time::Instant>,
    continuation_pending: bool,
    controls: usize,
    bytes: usize,
    expires: tokio::time::Instant,
}

impl OwnedChain {
    fn new(mut candidate: Candidate) -> Self {
        // The original fresh create could replace soft aliases. Once control
        // state owns this socket, later successors are continuations: preserve
        // any newer cohort/thread placement while binding their response IDs
        // separately to the physical socket's account.
        candidate.soft_keys = candidate
            .soft_keys
            .into_iter()
            .map(crate::routing::ThreadKey::preserve_existing)
            .collect();
        Self {
            responses: HashSet::from([candidate.response_id.clone()]),
            candidate,
            submitted_calls: HashSet::new(),
            injection: None,
            steers: VecDeque::new(),
            accepted_steers: HashSet::new(),
            successor_deadline: None,
            continuation_pending: false,
            controls: 0,
            bytes: 0,
            expires: tokio::time::Instant::now() + CHAIN_TIMEOUT,
        }
    }

    fn deadline(&self) -> tokio::time::Instant {
        self.steers
            .front()
            .copied()
            .into_iter()
            .chain(self.injection.as_ref().map(|(deadline, _, _)| *deadline))
            .chain(self.successor_deadline)
            .chain([self.expires])
            .min()
            .unwrap()
    }

    fn write_deadline(&self) -> tokio::time::Instant {
        self.deadline()
            .min(tokio::time::Instant::now() + BRIDGE_WRITE_STALL_TIMEOUT)
    }

    fn submit(&mut self, value: &Value, bytes: usize) -> std::result::Result<(), &'static str> {
        if self.controls >= MAX_CONTROLS || bytes > MAX_CONTROL_BYTES.saturating_sub(self.bytes) {
            return Err("native chain control count or byte limit exceeded; finish on this socket");
        }
        let now = tokio::time::Instant::now();
        match value["type"].as_str() {
            Some("response.interrupt") => {
                if !self.responses.contains(interrupt_response_id(value)?) {
                    return Err("interrupted response does not belong to the owned socket");
                }
                // Cancel and Drain use the same wire frame. The client decides
                // whether to require an ACK; do not invent a successor or clear calls.
            }
            Some("response.steer" | "response.inject") => {
                self.candidate.validate_control(value)?;
                if value["type"] == "response.inject" {
                    // The wire acknowledgement has no injection ID. One outstanding frame
                    // makes correlation unambiguous without rewriting or replaying frames.
                    if self.injection.is_some() {
                        return Err(
                            "an injection acknowledgement is pending; wait before submitting another frame",
                        );
                    }
                    let calls: Vec<String> = value["input"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|item| item["call_id"].as_str().unwrap().to_owned())
                        .collect();
                    let unique: HashSet<_> = calls.iter().collect();
                    if unique.len() != calls.len()
                        || calls.iter().any(|id| self.submitted_calls.contains(id))
                    {
                        return Err("a tool result has already been submitted");
                    }
                    self.submitted_calls.extend(calls.iter().cloned());
                    self.injection = Some((now + CONFIRM_TIMEOUT, calls, value["input"].clone()));
                } else {
                    self.steers.push_back(now + CONFIRM_TIMEOUT);
                }
            }
            Some("response.create") => {
                if value["previous_response_id"].as_str()
                    != Some(self.candidate.response_id.as_str())
                {
                    return Err(
                        "native continuation must name the current response on the owned socket; open a new connection for ordinary routing",
                    );
                }
                if self.injection.is_some() || self.continuation_pending {
                    return Err("native continuation is waiting for upstream acceptance");
                }
                if settings(value) != self.candidate.settings {
                    return Err("native continuation must preserve the original request settings");
                }
                let Some(input) = value["input"].as_array() else {
                    return Err("native continuation input must be an array");
                };
                let mut submitted = HashSet::new();
                for item in input {
                    let id = match item["type"].as_str() {
                        Some("function_call_output" | "custom_tool_call_output") => {
                            item["call_id"].as_str()
                        }
                        Some("mcp_approval_response") => item["approval_request_id"].as_str(),
                        _ => continue,
                    }
                    .ok_or("native result has no call identity")?;
                    if self.candidate.tracking_overflow
                        || !self.candidate.calls.contains(id)
                        || self.submitted_calls.contains(id)
                        || !submitted.insert(id.to_owned())
                    {
                        return Err("native continuation result is foreign or already submitted");
                    }
                }
                self.submitted_calls.extend(submitted);
                self.continuation_pending = true;
                self.successor_deadline = Some(now + CONFIRM_TIMEOUT);
            }
            // Needs no validation, but still falls through to the chain budget below so it
            // cannot be relayed without limit.
            Some("response.processed") => {}
            _ => {
                return Err(
                    "unsupported frame in owned native chain; use raw mode for opaque protocols",
                );
            }
        }
        self.controls += 1;
        self.bytes += bytes;
        Ok(())
    }

    fn observe(&mut self, value: &Value) -> std::result::Result<(), &'static str> {
        match value["type"].as_str() {
            Some("response.inject.created" | "response.inject.failed") => {
                if value["response_id"].as_str() != Some(self.candidate.response_id.as_str())
                    || self.injection.is_none()
                {
                    return Err("unmatched native injection acknowledgement");
                }
                if value["type"] == "response.inject.failed"
                    && self
                        .injection
                        .as_ref()
                        .is_some_and(|(_, _, input)| input != &value["input"])
                {
                    return Err("native injection failure does not match the submitted input");
                }
                let (_, calls, _) = self.injection.take().unwrap();
                if value["type"] == "response.inject.failed" {
                    for call in calls {
                        self.submitted_calls.remove(&call);
                    }
                }
            }
            Some("response.steer.accepted" | "response.steer.failed") => {
                if !value
                    .pointer("/steer/previous_response_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| self.responses.contains(id))
                {
                    return Err("unmatched native steering acknowledgement");
                }
                let id = value
                    .pointer("/steer/id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty() && id.len() <= 512)
                    .ok_or("native steering acknowledgement has no identity")?;
                let already_accepted = self.accepted_steers.contains(id);
                if !already_accepted && self.steers.pop_front().is_none() {
                    return Err("unsolicited native steering acknowledgement");
                }
                if value["type"] == "response.steer.accepted" {
                    if already_accepted {
                        return Err("duplicate native steering acknowledgement");
                    }
                    self.accepted_steers.insert(id.into());
                    self.successor_deadline
                        .get_or_insert(tokio::time::Instant::now() + CONFIRM_TIMEOUT);
                } else {
                    self.accepted_steers.remove(id);
                    if self.accepted_steers.is_empty() && !self.continuation_pending {
                        self.successor_deadline = None;
                    }
                }
            }
            Some("response.steer.pending") => {
                if !value
                    .pointer("/steer/id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| self.accepted_steers.contains(id))
                {
                    return Err("unsolicited native steering input request");
                }
                self.successor_deadline = Some(self.expires);
            }
            Some("response.created") => {
                let Some(id) = value.pointer("/response/id").and_then(Value::as_str) else {
                    return Err("native successor has no response identity");
                };
                if !self.continuation_pending && self.successor_deadline.is_none() {
                    return Err("native successor has no owning control");
                }
                if id.len() > 512
                    || self.responses.len() >= MAX_RESPONSES
                    || !self.responses.insert(id.into())
                {
                    return Err("invalid or excessive native successors");
                }
                self.candidate.response_id = id.into();
                self.candidate.calls.clear();
                self.candidate.tracking_overflow = false;
                self.submitted_calls.clear();
                self.successor_deadline = None;
                self.continuation_pending = false;
            }
            _ => {}
        }
        self.candidate.observe(value);
        Ok(())
    }
}

async fn send_native_error(
    client: &mut UpgradedWebSocket,
    kind: &str,
    message: &str,
) -> Result<()> {
    tokio::time::timeout(
        BRIDGE_WRITE_STALL_TIMEOUT,
        send_direct_error(client, kind, message),
    )
    .await?
}

impl App {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn run_native_control_socket(
        &self,
        client: &mut UpgradedWebSocket,
        upstream: &mut UpgradedWebSocket,
        credentials: &Credentials,
        account: &str,
        candidate: Candidate,
        initial: Message,
    ) -> Result<()> {
        let mut chain = OwnedChain::new(candidate);
        let mut initial = Some(initial);
        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(chain.deadline()) => {
                    send_native_error(client, "native_control_timeout", "native control confirmation or socket lifetime expired; delivery may be unknown and was not replayed").await?;
                    break;
                }
                message = async { match initial.take() { Some(message) => Some(Ok(message)), None => client.next().await } } => {
                    let Some(message) = message else { break; };
                    let message = message?;
                    if let Message::Text(text) = &message {
                        if text.len() > self.config.proxy.max_request_bytes {
                            send_native_error(client, "native_control_limit", "native frame exceeds configured request limit").await?;
                            continue;
                        }
                        let parsed = serde_json::from_str::<Value>(text);
                        let validation = parsed.as_ref().map_err(|_| "invalid native JSON frame")
                            .and_then(|value| chain.submit(value, text.len()));
                        if let Err(reason) = validation {
                            send_native_error(client, "native_control_invalid", reason).await?;
                            continue;
                        }
                    } else if matches!(message, Message::Binary(_)) {
                        send_native_error(client, "native_control_invalid", "native controls require JSON text; use raw mode for binary frames").await?;
                        continue;
                    }
                    if self.auth.ensure_bearer_usable(&self.config.accounts[account], credentials).is_err() {
                        send_native_error(client, "native_control_auth_unavailable", "the native socket credential is unavailable; controls cannot move to another connection").await?;
                        break;
                    }
                    let closes = matches!(message, Message::Close(_));
                    tokio::time::timeout_at(chain.write_deadline(), upstream.send(message)).await??;
                    if closes { break; }
                }
                message = upstream.next() => {
                    let message = match message {
                        Some(Ok(message)) => message,
                        _ => {
                            send_native_error(client, "native_control_disconnected", "native upstream disconnected; control delivery may be unknown and was not replayed").await?;
                            break;
                        }
                    };
                    let closes = matches!(message, Message::Close(_));
                    let mut terminal_failure = false;
                    if let Message::Text(text) = &message
                        && let Ok(value) = serde_json::from_str::<Value>(text) {
                            if let Err(reason) = chain.observe(&value) {
                                send_native_error(client, "native_control_protocol_error", reason).await?;
                                break;
                            }
                            if value["type"] == "response.created" {
                                let key = self.router.affinity.key(&format!("previous-response:{}", chain.candidate.response_id));
                                self.router.bind(key, account).await;
                                for key in &chain.candidate.soft_keys { self.router.bind(key.clone(), account).await; }
                            }
                            let failure = classify_terminal_event(&value).kind;
                            let failure_envelope = matches!(value["type"].as_str(), Some("error" | "response.failed" | "response.incomplete"));
                            // A steered or ordinary length-limited parent may still own
                            // a successor/result continuation. A classified incomplete
                            // failure ends the chain after relaying the real terminal.
                            terminal_failure = matches!(value["type"].as_str(), Some("error" | "response.failed"))
                                || (value["type"] == "response.incomplete" && matches!(failure,
                                    FailureKind::Quota | FailureKind::ScopedQuota | FailureKind::Capacity
                                    | FailureKind::Authentication { .. } | FailureKind::Transient));
                            if failure_envelope {
                                match failure {
                                    FailureKind::Quota => self.router.quota_failure_for_owner(account, &hyper::HeaderMap::new(), &credentials.quota_owner()).await,
                                    FailureKind::Capacity => self.router.capacity_failure(account).await,
                                    FailureKind::Authentication { .. } => self.reject_account_bearer(account, credentials).await,
                                    FailureKind::Transient => self.router.soft_failure(account).await,
                                    _ => {},
                                }
                            }
                    }
                    tokio::time::timeout_at(chain.write_deadline(), client.send(message)).await??;
                    if closes { return Ok(()); }
                    if terminal_failure { break; }
                }
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), upstream.close(None)).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), client.close(None)).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn processed_frames_are_charged_to_the_chain_budget() {
        // `response.processed` needs no validation, but an uncharged relay would let a client
        // stream unlimited frames onto an owned socket bounded only by CHAIN_TIMEOUT.
        let mut chain = chain(false);
        let processed = json!({"type":"response.processed"});
        for _ in 0..MAX_CONTROLS {
            assert_eq!(chain.submit(&processed, 1), Ok(()));
        }
        assert_eq!(
            chain.submit(&processed, 1),
            Err("native chain control count or byte limit exceeded; finish on this socket")
        );
    }

    fn chain(injection: bool) -> OwnedChain {
        let create = if injection {
            json!({"type":"response.create","multi_agent":{"enabled":true},"input":[]})
        } else {
            json!({"type":"response.create","input":[]})
        };
        let mut candidate =
            Candidate::new(&json!({"response":{"id":"parent"}}), &create, vec![]).unwrap();
        candidate.observe(&json!({"type":"response.output_item.done","response_id":"parent","item":{"type":"function_call","call_id":"call_1"}}));
        OwnedChain::new(candidate)
    }

    #[tokio::test]
    async fn rejected_steer_does_not_clear_dispatched_continuation_deadline() {
        let mut chain = chain(false);
        chain
            .submit(
                &json!({"type":"response.steer","previous_response_id":"parent","input":"hi"}),
                20,
            )
            .unwrap();
        chain.observe(&json!({"type":"response.steer.accepted","steer":{"id":"steer_1","previous_response_id":"parent"}})).unwrap();
        chain.submit(&json!({"type":"response.create","previous_response_id":"parent","input":[{"type":"function_call_output","call_id":"call_1","output":"ok"}]}), 20).unwrap();
        let deadline = chain.deadline();
        chain.observe(&json!({"type":"response.steer.failed","steer":{"id":"steer_1","previous_response_id":"parent"}})).unwrap();
        assert!(chain.continuation_pending);
        assert_eq!(chain.deadline(), deadline);
    }

    #[tokio::test]
    async fn injection_bounds_identity_and_at_most_once_results() {
        let mut chain = chain(true);
        let mut inject = json!({"type":"response.inject","response_id":"foreign","input":[{"type":"function_call_output","call_id":"call_1","output":"saved"}]});
        assert!(chain.submit(&inject, 20).is_err());
        inject["response_id"] = json!("parent");
        chain.submit(&inject, 20).unwrap();
        assert!(chain.submit(&inject, 20).is_err());
        let deadline = chain.deadline();
        chain
            .observe(&json!({"type":"response.completed","response":{"id":"parent"}}))
            .unwrap();
        assert_eq!(chain.deadline(), deadline);
        chain
            .observe(&json!({"type":"response.inject.created","response_id":"parent"}))
            .unwrap();
        assert!(chain.submit(&inject, 20).is_err());
    }

    #[tokio::test]
    async fn native_write_stall_is_bounded_independently_of_socket_lifetime() {
        let chain = chain(false);
        let before = tokio::time::Instant::now();
        let deadline = chain.write_deadline();
        assert!(deadline < chain.expires);
        assert!(deadline >= before + BRIDGE_WRITE_STALL_TIMEOUT);
        assert!(deadline <= tokio::time::Instant::now() + BRIDGE_WRITE_STALL_TIMEOUT);
    }

    #[tokio::test]
    async fn native_control_byte_and_count_limits_are_absolute() {
        let mut chain = chain(false);
        let steer = json!({"type":"response.steer","previous_response_id":"parent","input":"hi"});
        assert!(chain.submit(&steer, MAX_CONTROL_BYTES + 1).is_err());
        for _ in 0..MAX_CONTROLS {
            chain.submit(&steer, 1).unwrap();
        }
        assert!(chain.submit(&steer, 1).is_err());
        assert!(chain.deadline() < chain.expires);
    }
}
