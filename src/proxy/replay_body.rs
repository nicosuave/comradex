use std::{
    fs::File as StdFile,
    io::Read,
    sync::{Arc, atomic::Ordering},
};

use anyhow::{Context, Result};
use async_compression::{codecs::zstd::params::DParameter, tokio::bufread::ZstdDecoder};
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::{
    Error as HyperError,
    body::{Body, Frame, Incoming},
};
use tempfile::NamedTempFile;
use tokio_util::io::{ReaderStream, StreamReader};

use crate::state::Stats;

const REQUEST_BODY_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_MODEL_BYTES: usize = 512;

pub type ProxyBody = BoxBody<Bytes, std::io::Error>;

#[derive(Debug)]
pub enum ReplayLimit {
    Request,
    Global,
}

impl std::fmt::Display for ReplayLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Request => "request body exceeds configured limit",
            Self::Global => "global replay spool limit exceeded",
        })
    }
}

impl std::error::Error for ReplayLimit {}

#[derive(Debug)]
pub struct DecodeError(std::io::Error);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "decode zstd request body: {}", self.0)
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

pub struct ReplayBody {
    storage: Storage,
    len: usize,
    stats: Arc<Stats>,
    model: Option<String>,
    cyber_access_program: Option<String>,
    model_scan_complete: bool,
    thread_id: Option<String>,
    context_session_id: Option<String>,
    context_envelope: bool,
    previous_response_id: Option<String>,
    prompt_cache_key: Option<String>,
    file_ids: Vec<String>,
    file_ids_overflow: bool,
    nonportable_state: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    Model,
    AccessPrograms,
    CyberAccessProgram,
    ClientMetadata,
    ThreadId,
    SessionId,
    PreviousResponseId,
    PromptCacheKey,
    FileId,
    Type,
    Input,
    Other,
}

struct Container {
    object: bool,
    expect_key: bool,
    file_id: Option<String>,
    account_scoped_file: bool,
    input_array: bool,
    native_input_item: bool,
}

#[derive(Default)]
struct MetadataScanner {
    containers: Vec<Container>,
    client_depth: Option<usize>,
    access_programs_depth: Option<usize>,
    in_string: bool,
    escape: bool,
    string_is_key: bool,
    capture_value: Option<KeyKind>,
    token: Vec<u8>,
    token_overflow: bool,
    routing_token: Vec<u8>,
    routing_token_overflow: bool,
    key: Option<KeyKind>,
    pending_value: Option<KeyKind>,
    model: Option<String>,
    cyber_access_program: Option<String>,
    model_scan_incomplete: bool,
    thread_id: Option<String>,
    context_session_id: Option<String>,
    context_envelope: bool,
    previous_response_id: Option<String>,
    prompt_cache_key: Option<String>,
    file_ids: Vec<String>,
    file_ids_overflow: bool,
    nonportable_state: bool,
    disabled: bool,
}

impl MetadataScanner {
    fn finish(&mut self) {
        // Do not relax ownership for a truncated or malformed request.
        self.nonportable_state |= self.in_string || !self.containers.is_empty();
        self.model_scan_incomplete |= self.in_string || !self.containers.is_empty();
    }

    fn feed(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if self.disabled {
                return;
            }
            if self.in_string {
                // Decode both model selectors and root property names so escaped
                // spellings route exactly like the upstream JSON parser.
                if ((self.string_is_key
                    && (self.containers.len() == 1
                        || self.access_programs_depth == Some(self.containers.len())))
                    || matches!(
                        self.capture_value,
                        Some(KeyKind::Model | KeyKind::CyberAccessProgram)
                    ))
                    && !self.routing_token_overflow
                    && (byte != b'"' || self.escape)
                {
                    // Each ASCII character can occupy six bytes as a JSON escape.
                    // The longest routing key is `access_programs`.
                    let limit = if self.string_is_key {
                        b"access_programs".len() * 6
                    } else {
                        MAX_MODEL_BYTES * 6
                    };
                    if self.routing_token.len() < limit {
                        self.routing_token.push(byte);
                    } else {
                        self.routing_token_overflow = true;
                    }
                }
                if self.escape {
                    self.escape = false;
                    self.token_overflow = true;
                } else if byte == b'\\' {
                    self.escape = true;
                    if self.string_is_key || self.capture_value == Some(KeyKind::Type) {
                        // The upstream JSON parser decodes escaped property names and item types.
                        // This bounded scanner intentionally does not, so an escaped routing-
                        // relevant token must fail closed instead of silently looking portable.
                        self.nonportable_state = true;
                    }
                    self.token_overflow = true;
                } else if byte == b'"' {
                    self.finish_string();
                } else if !self.token_overflow {
                    let limit = if self.capture_value.is_some() {
                        512
                    } else {
                        64
                    };
                    if self.token.len() < limit {
                        self.token.push(byte);
                    } else {
                        self.token_overflow = true;
                    }
                }
                continue;
            }
            match byte {
                b' ' | b'\t' | b'\r' | b'\n' => {}
                b'"' => {
                    self.in_string = true;
                    self.token.clear();
                    self.token_overflow = false;
                    self.routing_token.clear();
                    self.routing_token_overflow = false;
                    self.capture_value = self.pending_value.take().filter(|kind| {
                        matches!(
                            kind,
                            KeyKind::Model
                                | KeyKind::CyberAccessProgram
                                | KeyKind::ThreadId
                                | KeyKind::SessionId
                                | KeyKind::PreviousResponseId
                                | KeyKind::PromptCacheKey
                                | KeyKind::FileId
                                | KeyKind::Type
                        )
                    });
                    self.string_is_key = self.capture_value.is_none()
                        && self
                            .containers
                            .last()
                            .is_some_and(|v| v.object && v.expect_key);
                }
                b':' => {
                    self.pending_value = self.key.take();
                }
                b'{' => {
                    if self.containers.len() >= 128 {
                        self.nonportable_state = true;
                        self.disabled = true;
                        continue;
                    }
                    let is_client = self.pending_value == Some(KeyKind::ClientMetadata);
                    let is_access_programs = self.pending_value == Some(KeyKind::AccessPrograms);
                    let native_input_item = self.containers.last().is_some_and(|v| v.input_array);
                    self.pending_value = None;
                    self.containers.push(Container {
                        object: true,
                        expect_key: true,
                        file_id: None,
                        account_scoped_file: false,
                        input_array: false,
                        native_input_item,
                    });
                    if is_client {
                        self.client_depth = Some(self.containers.len());
                    }
                    if is_access_programs {
                        self.access_programs_depth = Some(self.containers.len());
                    }
                }
                b'[' => {
                    if self.containers.len() >= 128 {
                        self.nonportable_state = true;
                        self.disabled = true;
                        continue;
                    }
                    let input_array =
                        self.containers.len() == 1 && self.pending_value == Some(KeyKind::Input);
                    self.pending_value = None;
                    self.containers.push(Container {
                        object: false,
                        expect_key: false,
                        file_id: None,
                        account_scoped_file: false,
                        input_array,
                        native_input_item: false,
                    });
                }
                b'}' | b']' => {
                    if self.client_depth == Some(self.containers.len()) {
                        self.client_depth = None;
                    }
                    if self.access_programs_depth == Some(self.containers.len()) {
                        self.access_programs_depth = None;
                    }
                    if let Some(container) = self.containers.pop()
                        && container.account_scoped_file
                        && let Some(file_id) = container.file_id
                        && !self.file_ids.contains(&file_id)
                    {
                        if self.file_ids.len() < 32 {
                            self.file_ids.push(file_id);
                        } else {
                            self.file_ids_overflow = true;
                            self.disabled = true;
                        }
                    }
                    self.pending_value = None;
                    self.key = None;
                }
                b',' => {
                    if let Some(container) = self.containers.last_mut()
                        && container.object
                    {
                        container.expect_key = true;
                    }
                    self.pending_value = None;
                    self.key = None;
                }
                _ => {
                    if self.pending_value.is_some() {
                        self.pending_value = None;
                    }
                }
            }
        }
    }

    fn finish_string(&mut self) {
        self.in_string = false;
        self.context_envelope |=
            !self.string_is_key && self.token.starts_with(b"comradex-context-v1:");
        // Also inspect an all-turns request without session metadata so it fails closed.
        self.context_envelope |= !self.string_is_key && self.token == b"all_turns";
        if let Some(kind) = self.capture_value {
            if matches!(kind, KeyKind::Model | KeyKind::CyberAccessProgram) {
                let value = self
                    .decoded_routing_token()
                    .filter(|value| !value.is_empty() && value.len() <= MAX_MODEL_BYTES);
                match kind {
                    KeyKind::Model => self.model = value,
                    KeyKind::CyberAccessProgram => self.cyber_access_program = value,
                    _ => unreachable!(),
                }
                self.model_scan_incomplete |= self.routing_token_overflow;
            }
            if !self.token_overflow && !self.token.is_empty() {
                let value = String::from_utf8(self.token.clone()).ok();
                match kind {
                    KeyKind::ThreadId => self.thread_id = value,
                    KeyKind::SessionId => self.context_session_id = value,
                    KeyKind::PreviousResponseId => self.previous_response_id = value,
                    KeyKind::PromptCacheKey => self.prompt_cache_key = value,
                    KeyKind::FileId => {
                        if let Some(container) = self.containers.last_mut() {
                            container.file_id = value;
                        }
                    }
                    KeyKind::Type => {
                        if let (Some(container), Some(value)) = (self.containers.last_mut(), value)
                        {
                            container.account_scoped_file =
                                matches!(value.as_str(), "input_file" | "input_image");
                            self.nonportable_state |= matches!(
                                value.as_str(),
                                "reasoning"
                                    | "compaction"
                                    | "item_reference"
                                    | "code_interpreter_call"
                                    | "computer_call"
                                    | "computer_call_output"
                                    | "file_search_call"
                                    | "image_generation_call"
                                    | "tool_search_call"
                                    | "tool_search_output"
                                    | "web_search_call"
                            );
                        }
                    }
                    KeyKind::Model
                    | KeyKind::AccessPrograms
                    | KeyKind::CyberAccessProgram
                    | KeyKind::ClientMetadata
                    | KeyKind::Other
                    | KeyKind::Input => {}
                }
            }
        } else if self.string_is_key {
            let in_client = self.client_depth == Some(self.containers.len());
            let at_root = self.containers.len() == 1;
            if !self.token_overflow
                && (matches!(
                    self.token.as_slice(),
                    b"encrypted_content"
                        | b"operation_id"
                        | b"codex_operation_id"
                        | b"internal_chat_message_metadata_passthrough"
                ) || (self.containers.last().is_some_and(|v| v.native_input_item)
                    && self.token == b"id")
                    || (at_root
                        && matches!(
                            self.token.as_slice(),
                            b"conversation" | b"prompt" | b"turn_state"
                        )))
            {
                self.nonportable_state = true;
            }
            self.key = if at_root && self.decoded_routing_token().as_deref() == Some("model") {
                // A later non-string or oversized model must not leave an earlier
                // model active when the upstream parser uses the final property.
                self.model = None;
                Some(KeyKind::Model)
            } else if at_root && self.decoded_routing_token().as_deref() == Some("access_programs")
            {
                self.cyber_access_program = None;
                Some(KeyKind::AccessPrograms)
            } else if self.access_programs_depth == Some(self.containers.len())
                && self.decoded_routing_token().as_deref() == Some("cyber")
            {
                self.cyber_access_program = None;
                Some(KeyKind::CyberAccessProgram)
            } else if at_root && !self.token_overflow && self.token == b"client_metadata" {
                Some(KeyKind::ClientMetadata)
            } else if in_client && !self.token_overflow && self.token == b"thread_id" {
                Some(KeyKind::ThreadId)
            } else if in_client && !self.token_overflow && self.token == b"session_id" {
                Some(KeyKind::SessionId)
            } else if at_root && !self.token_overflow && self.token == b"previous_response_id" {
                Some(KeyKind::PreviousResponseId)
            } else if at_root && !self.token_overflow && self.token == b"prompt_cache_key" {
                Some(KeyKind::PromptCacheKey)
            } else if !self.token_overflow && self.token == b"file_id" {
                Some(KeyKind::FileId)
            } else if !self.token_overflow && self.token == b"type" {
                Some(KeyKind::Type)
            } else if at_root && !self.token_overflow && self.token == b"input" {
                Some(KeyKind::Input)
            } else {
                Some(KeyKind::Other)
            };
            if let Some(container) = self.containers.last_mut() {
                container.expect_key = false;
            }
        }
        self.capture_value = None;
        self.string_is_key = false;
    }

    fn decoded_routing_token(&self) -> Option<String> {
        if self.routing_token_overflow {
            return None;
        }
        if !self.routing_token.contains(&b'\\') {
            return String::from_utf8(self.routing_token.clone()).ok();
        }
        let mut quoted = Vec::with_capacity(self.routing_token.len() + 2);
        quoted.push(b'"');
        quoted.extend_from_slice(&self.routing_token);
        quoted.push(b'"');
        serde_json::from_slice(&quoted).ok()
    }
}

struct Reservation {
    stats: Arc<Stats>,
    bytes: usize,
}

impl Reservation {
    fn add(&mut self, amount: usize, limit: usize) -> Result<()> {
        let previous = self
            .stats
            .active_spool_bytes
            .fetch_add(amount, Ordering::AcqRel);
        if previous.saturating_add(amount) > limit {
            self.stats
                .active_spool_bytes
                .fetch_sub(amount, Ordering::AcqRel);
            return Err(ReplayLimit::Global.into());
        }
        self.bytes += amount;
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.stats
            .active_spool_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

enum Storage {
    Memory(Bytes),
    Files(Vec<Option<StdFile>>),
}

impl ReplayBody {
    pub fn from_bytes(
        bytes: Bytes,
        hard_limit: usize,
        global_limit: usize,
        stats: Arc<Stats>,
    ) -> Result<Self> {
        if bytes.len() > hard_limit {
            return Err(ReplayLimit::Request.into());
        }
        let previous = stats
            .active_spool_bytes
            .fetch_add(bytes.len(), Ordering::AcqRel);
        if previous.saturating_add(bytes.len()) > global_limit {
            stats
                .active_spool_bytes
                .fetch_sub(bytes.len(), Ordering::AcqRel);
            return Err(ReplayLimit::Global.into());
        }
        let mut metadata = MetadataScanner::default();
        metadata.feed(&bytes);
        metadata.finish();
        Ok(Self {
            len: bytes.len(),
            storage: Storage::Memory(bytes),
            stats,
            model: metadata.model,
            cyber_access_program: metadata.cyber_access_program,
            model_scan_complete: !metadata.disabled && !metadata.model_scan_incomplete,
            thread_id: metadata.thread_id,
            context_session_id: metadata.context_session_id,
            context_envelope: metadata.context_envelope,
            previous_response_id: metadata.previous_response_id,
            prompt_cache_key: metadata.prompt_cache_key,
            file_ids: metadata.file_ids,
            file_ids_overflow: metadata.file_ids_overflow,
            nonportable_state: metadata.nonportable_state,
        })
    }

    pub async fn read_encoded<B>(
        incoming: B,
        zstd: bool,
        memory_limit: usize,
        hard_limit: usize,
        global_limit: usize,
        stats: Arc<Stats>,
    ) -> Result<Self>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        if !zstd {
            return Self::read(incoming, memory_limit, hard_limit, global_limit, stats).await;
        }
        // Bound the wire representation as well as the decoded body. Decode before
        // scanning: binary bytes cannot establish JSON routing or context ownership.
        let mut wire_len = 0usize;
        let stream = incoming
            .into_data_stream()
            .map_err(std::io::Error::other)
            .and_then(move |data| {
                wire_len = wire_len.saturating_add(data.len());
                let result = if wire_len > hard_limit {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::FileTooLarge,
                        ReplayLimit::Request,
                    ))
                } else {
                    Ok(data)
                };
                std::future::ready(result)
            });
        // zstd windows are independent of decoded output size. Keep the decoder's
        // allocation bounded even when an input declares an enormous window.
        let mut decoder =
            ZstdDecoder::with_params(StreamReader::new(stream), &[DParameter::window_log_max(27)]);
        // Consume every frame and reject trailing garbage rather than silently
        // forwarding only the first frame of a request.
        decoder.multiple_members(true);
        let decoded = StreamBody::new(
            ReaderStream::new(decoder)
                .map_ok(Frame::data)
                .map_err(DecodeError),
        );
        Self::read(decoded, memory_limit, hard_limit, global_limit, stats).await
    }

    async fn read<B>(
        incoming: B,
        memory_limit: usize,
        hard_limit: usize,
        global_limit: usize,
        stats: Arc<Stats>,
    ) -> Result<Self>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut incoming = incoming;
        let mut memory = Vec::new();
        let mut temp: Option<(tokio::fs::File, tempfile::TempPath)> = None;
        let mut len = 0usize;
        let mut reservation = Reservation {
            stats: stats.clone(),
            bytes: 0,
        };
        let mut metadata = MetadataScanner::default();
        loop {
            let Some(frame) = tokio::time::timeout(REQUEST_BODY_IDLE_TIMEOUT, incoming.frame())
                .await
                .context("request body idle timeout")?
            else {
                break;
            };
            let frame = frame.context("read request body")?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            len = len
                .checked_add(data.len())
                .context("request length overflow")?;
            if len > hard_limit {
                return Err(ReplayLimit::Request.into());
            }
            reservation.add(data.len(), global_limit)?;
            metadata.feed(&data);
            if temp.is_none() && len <= memory_limit {
                memory.extend_from_slice(&data);
            } else {
                if temp.is_none() {
                    let file = tokio::task::spawn_blocking(NamedTempFile::new)
                        .await
                        .context("join replay spool creation")?
                        .context("create replay spool")?;
                    let (file, path) = file.into_parts();
                    let mut file = tokio::fs::File::from_std(file);
                    tokio::io::AsyncWriteExt::write_all(&mut file, &memory).await?;
                    memory.clear();
                    temp = Some((file, path));
                }
                tokio::io::AsyncWriteExt::write_all(&mut temp.as_mut().expect("created").0, &data)
                    .await?;
            }
        }
        metadata.finish();
        let storage = if let Some((file, path)) = temp {
            file.sync_data().await?;
            drop(file);
            let one = tokio::fs::File::open(&path).await?.into_std().await;
            let two = tokio::fs::File::open(&path).await?.into_std().await;
            drop(path);
            Storage::Files(vec![Some(one), Some(two)])
        } else {
            Storage::Memory(Bytes::from(memory))
        };
        reservation.bytes = 0;
        Ok(Self {
            storage,
            len,
            stats,
            model: metadata.model,
            cyber_access_program: metadata.cyber_access_program,
            model_scan_complete: !metadata.disabled && !metadata.model_scan_incomplete,
            thread_id: metadata.thread_id,
            context_session_id: metadata.context_session_id,
            context_envelope: metadata.context_envelope,
            previous_response_id: metadata.previous_response_id,
            prompt_cache_key: metadata.prompt_cache_key,
            file_ids: metadata.file_ids,
            file_ids_overflow: metadata.file_ids_overflow,
            nonportable_state: metadata.nonportable_state,
        })
    }

    pub fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    /// Newer Codex clients request Daybreak as a treatment on a base model.
    /// Keep the existing Daybreak account pins effective for both wire formats.
    pub fn routing_model(&self) -> Option<&str> {
        match self.cyber_access_program.as_deref() {
            Some("daybreak_blue") => Some("gpt-daybreak-blue-latest"),
            Some("daybreak_red") => Some("gpt-daybreak-red-latest"),
            _ => self.model(),
        }
    }

    pub fn model_scan_complete(&self) -> bool {
        self.model_scan_complete
    }

    pub fn thread_id(&self) -> Option<&str> {
        self.thread_id.as_deref()
    }

    pub fn context_session_id(&self) -> Option<&str> {
        self.context_session_id.as_deref()
    }

    pub fn has_context_envelope(&self) -> bool {
        self.context_envelope
    }

    pub fn use_context_routing_metadata(&mut self, view: &Self) {
        self.nonportable_state = view.nonportable_state;
    }

    pub fn previous_response_id(&self) -> Option<&str> {
        self.previous_response_id.as_deref()
    }

    pub fn prompt_cache_key(&self) -> Option<&str> {
        self.prompt_cache_key.as_deref()
    }

    pub fn file_ids(&self) -> &[String] {
        &self.file_ids
    }

    pub fn file_ids_overflow(&self) -> bool {
        self.file_ids_overflow
    }

    pub fn has_nonportable_state(&self) -> bool {
        self.nonportable_state
    }

    /// Materialize a replay body for protocol translation while releasing its
    /// spool reservation. Callers can construct a replacement `ReplayBody`
    /// without temporarily double-counting the request against the global
    /// spool limit.
    pub async fn into_bytes(mut self) -> Result<Bytes> {
        let storage = std::mem::replace(&mut self.storage, Storage::Memory(Bytes::new()));
        let bytes = match storage {
            Storage::Memory(bytes) => bytes,
            Storage::Files(mut files) => {
                let mut file = files
                    .get_mut(0)
                    .and_then(Option::take)
                    .context("replay body has no materializable spool")?;
                let expected = self.len;
                Bytes::from(
                    tokio::task::spawn_blocking(move || {
                        let mut bytes = Vec::with_capacity(expected);
                        file.read_to_end(&mut bytes)?;
                        Ok::<_, std::io::Error>(bytes)
                    })
                    .await
                    .context("join replay materialization")??,
                )
            }
        };
        self.stats
            .active_spool_bytes
            .fetch_sub(self.len, Ordering::AcqRel);
        self.len = 0;
        Ok(bytes)
    }

    pub fn body(&mut self, attempt: usize) -> Result<ProxyBody> {
        match &mut self.storage {
            Storage::Memory(bytes) => Ok(Full::new(bytes.clone()).map_err(never_to_io).boxed()),
            Storage::Files(files) => {
                let file = files
                    .get_mut(attempt)
                    .and_then(Option::take)
                    .context("replay attempt already consumed")?;
                let stream = ReaderStream::new(tokio::fs::File::from_std(file)).map_ok(Frame::data);
                Ok(BodyExt::boxed(StreamBody::new(stream)))
            }
        }
    }
}

impl Drop for ReplayBody {
    fn drop(&mut self) {
        self.stats
            .active_spool_bytes
            .fetch_sub(self.len, Ordering::Relaxed);
    }
}

pub fn incoming_body(body: Incoming) -> ProxyBody {
    body.map_err(|e: HyperError| std::io::Error::other(e))
        .boxed()
}

pub fn empty_body() -> ProxyBody {
    Full::new(Bytes::new()).map_err(never_to_io).boxed()
}

pub fn json_body(value: serde_json::Value) -> ProxyBody {
    Full::new(Bytes::from(value.to_string()))
        .map_err(never_to_io)
        .boxed()
}

pub fn bytes_body(bytes: Bytes) -> ProxyBody {
    Full::new(bytes).map_err(never_to_io).boxed()
}

fn never_to_io(never: std::convert::Infallible) -> std::io::Error {
    match never {}
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn zstd_bytes(bytes: &[u8]) -> Bytes {
        let mut encoder = async_compression::tokio::bufread::ZstdEncoder::new(bytes);
        let mut encoded = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut encoder, &mut encoded)
            .await
            .unwrap();
        encoded.into()
    }

    #[tokio::test]
    async fn zstd_metadata_and_replay_survive_chunking_and_disk_spooling() {
        let payload = Bytes::from_static(br#"{"model":"gpt-5","access_programs":{"cyber":"daybreak_blue"},"client_metadata":{"thread_id":"thread-1","session_id":"session-1"},"previous_response_id":"resp-1","prompt_cache_key":"cache-1","input":[{"type":"input_file","file_id":"file-1"},{"type":"compaction","encrypted_content":"owned"}]}"#);
        let wire = zstd_bytes(&payload).await;
        for memory_limit in [1, 4096] {
            let stats = Arc::new(Stats::default());
            let chunks = wire
                .iter()
                .map(|byte| Ok::<_, std::io::Error>(Frame::data(Bytes::copy_from_slice(&[*byte]))))
                .collect::<Vec<_>>();
            let body = StreamBody::new(futures_util::stream::iter(chunks));
            let mut replay =
                ReplayBody::read_encoded(body, true, memory_limit, 4096, 4096, stats.clone())
                    .await
                    .unwrap();
            assert_eq!(replay.model(), Some("gpt-5"));
            assert_eq!(replay.routing_model(), Some("gpt-daybreak-blue-latest"));
            assert!(replay.model_scan_complete());
            assert_eq!(replay.thread_id(), Some("thread-1"));
            assert_eq!(replay.context_session_id(), Some("session-1"));
            assert_eq!(replay.previous_response_id(), Some("resp-1"));
            assert_eq!(replay.prompt_cache_key(), Some("cache-1"));
            assert_eq!(replay.file_ids(), &["file-1"]);
            assert!(replay.has_nonportable_state());
            assert_eq!(
                stats.active_spool_bytes.load(Ordering::Relaxed),
                payload.len()
            );
            for attempt in 0..2 {
                assert_eq!(
                    replay
                        .body(attempt)
                        .unwrap()
                        .collect()
                        .await
                        .unwrap()
                        .to_bytes(),
                    payload
                );
            }
            drop(replay);
            assert_eq!(stats.active_spool_bytes.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn zstd_replay_limits_release_partial_reservations() {
        let stats = Arc::new(Stats::default());
        let payload = vec![b'x'; 50_000];
        let wire = zstd_bytes(&payload).await;
        for (hard, global, expected) in [
            (40_000, 100_000, "request body"),
            (100_000, 40_000, "global replay"),
        ] {
            let error = ReplayBody::read_encoded(
                Full::new(wire.clone()),
                true,
                1,
                hard,
                global,
                stats.clone(),
            )
            .await
            .err()
            .unwrap();
            assert!(error.to_string().contains(expected), "{error:#}");
            assert_eq!(stats.active_spool_bytes.load(Ordering::Relaxed), 0);
        }
        // An existing request's reservation must count toward decoded admission,
        // and remain intact when a second request exceeds the shared limit.
        let existing = ReplayBody::from_bytes(
            Bytes::from(vec![b'x'; 30_000]),
            100_000,
            60_000,
            stats.clone(),
        )
        .unwrap();
        let error =
            ReplayBody::read_encoded(Full::new(wire), true, 1, 100_000, 60_000, stats.clone())
                .await
                .err()
                .unwrap();
        assert!(matches!(
            error.downcast_ref::<ReplayLimit>(),
            Some(ReplayLimit::Global)
        ));
        assert_eq!(stats.active_spool_bytes.load(Ordering::Relaxed), 30_000);
        drop(existing);
        assert_eq!(stats.active_spool_bytes.load(Ordering::Relaxed), 0);

        let wire = zstd_bytes(&payload).await;
        let truncated = wire.slice(..wire.len() - 1);
        let error = ReplayBody::read_encoded(
            Full::new(truncated),
            true,
            1,
            100_000,
            100_000,
            stats.clone(),
        )
        .await
        .err()
        .unwrap();
        assert!(error.downcast_ref::<DecodeError>().is_some(), "{error:#}");
        assert_eq!(stats.active_spool_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn extracts_only_top_level_model_across_chunks() {
        for (payload, expected) in [
            (
                r#"{"model":"gpt-5","input":{"model":"nested"}}"#,
                Some("gpt-5"),
            ),
            (
                r#"{"input":[{"model":"nested"}],"model":"gpt-5"}"#,
                Some("gpt-5"),
            ),
            (r#"{"input":{"model":"nested"}}"#, None),
            (r#"[{"model":"nested"}]"#, None),
            (r#"{"model":null}"#, None),
            (r#"{"model":""}"#, None),
            (r#"{"model":"first","model":"last"}"#, Some("last")),
            (r#"{"model":"first","model":null}"#, None),
            (r#"{"model":"first","model":{"model":"nested"}}"#, None),
            (r#"{"mo\u0064el":"gpt-\u0035"}"#, Some("gpt-5")),
            (r#"{"model":"quoted\"model"}"#, Some("quoted\"model")),
        ] {
            for size in [1, 3, payload.len()] {
                let mut scanner = MetadataScanner::default();
                for chunk in payload.as_bytes().chunks(size) {
                    scanner.feed(chunk);
                }
                scanner.finish();
                assert_eq!(scanner.model.as_deref(), expected, "{payload}");
            }
        }

        let mut scanner = MetadataScanner::default();
        let oversized = format!(r#"{{"model":"first","model":"{}"}}"#, "x".repeat(3073));
        scanner.feed(oversized.as_bytes());
        scanner.finish();
        assert_eq!(scanner.model, None);
        assert!(scanner.model_scan_incomplete);
    }

    #[test]
    fn access_program_routing_uses_only_the_final_root_property_across_chunks() {
        for (properties, expected) in [
            (
                r#""access_programs":{"cyber":"daybreak_blue"}"#,
                "gpt-daybreak-blue-latest",
            ),
            (
                r#""access_programs":{"cyber":"daybreak_red"}"#,
                "gpt-daybreak-red-latest",
            ),
            (r#""access_programs":{"cyber":"standard"}"#, "gpt-6-sol"),
            (
                r#""input":[{"access_programs":{"cyber":"daybreak_blue"}}]"#,
                "gpt-6-sol",
            ),
            (
                r#""access_programs":{"cyber":"daybreak_blue"},"access_programs":null"#,
                "gpt-6-sol",
            ),
            (
                r#""access_programs":{"cyber":"daybreak_blue","cyber":"standard"}"#,
                "gpt-6-sol",
            ),
            (
                r#""access_programs":{"cyber":"daybreak_blue","cyber":null}"#,
                "gpt-6-sol",
            ),
            (
                r#""access_progr\u0061ms":{"cy\u0062er":"daybreak_\u0062lue"}"#,
                "gpt-daybreak-blue-latest",
            ),
            (
                r#""access_programs":{"nested":{"cyber":"daybreak_blue"}}"#,
                "gpt-6-sol",
            ),
            (
                r#""access_programs":{"cyber":"daybreak_blue"},"access_programs":{}"#,
                "gpt-6-sol",
            ),
            (
                r#""access_programs":{"cyber":"daybreak_blue"},"access_programs":{"cyber":"daybreak_red"}"#,
                "gpt-daybreak-red-latest",
            ),
        ] {
            let payload = format!(r#"{{"model":"gpt-6-sol",{properties}}}"#);
            for size in [1, 3, payload.len()] {
                let mut scanner = MetadataScanner::default();
                for chunk in payload.as_bytes().chunks(size) {
                    scanner.feed(chunk);
                }
                scanner.finish();
                let replay = ReplayBody::from_bytes(
                    Bytes::from(payload.clone()),
                    4096,
                    4096,
                    Arc::new(Stats::default()),
                )
                .unwrap();
                assert_eq!(scanner.cyber_access_program, replay.cyber_access_program);
                assert_eq!(replay.model(), Some("gpt-6-sol"));
                assert_eq!(replay.routing_model(), Some(expected), "{payload}");
                assert!(replay.model_scan_complete());
            }
        }
    }

    #[test]
    fn model_scan_reports_limits_and_incomplete_requests() {
        let nested = format!("{}null{}", "[".repeat(129), "]".repeat(129));
        for payload in [
            format!(r#"{{"input":{nested},"model":"gpt-5"}}"#),
            format!(r#"{{"model":"first","input":{nested},"model":"last"}}"#),
            format!(r#"{{"model":"{}"}}"#, "x".repeat(MAX_MODEL_BYTES * 6 + 1)),
            r#"{"model":"unfinished"#.to_owned(),
            r#"{"model":"gpt-5""#.to_owned(),
            format!(
                r#"{{"access_programs":{{"cyber":"{}"}}}}"#,
                "x".repeat(MAX_MODEL_BYTES * 6 + 1)
            ),
            r#"{"model":"gpt-6-sol","access_programs":{"cyber":"daybreak_blue"#.to_owned(),
        ] {
            let replay = ReplayBody::from_bytes(
                Bytes::from(payload),
                10_000,
                10_000,
                Arc::new(Stats::default()),
            )
            .unwrap();
            assert!(!replay.model_scan_complete());
        }

        // All JSON spellings of a model within the configured byte limit fit.
        let payload = format!(r#"{{"model":"{}"}}"#, r"\u0078".repeat(MAX_MODEL_BYTES));
        let replay = ReplayBody::from_bytes(
            Bytes::from(payload),
            10_000,
            10_000,
            Arc::new(Stats::default()),
        )
        .unwrap();
        assert_eq!(replay.model(), Some("x".repeat(MAX_MODEL_BYTES).as_str()));
        assert!(replay.model_scan_complete());
    }

    #[test]
    fn context_routing_view_preserves_request_model() {
        let stats = Arc::new(Stats::default());
        let mut request = ReplayBody::from_bytes(
            Bytes::from_static(br#"{"model":"gpt-5"}"#),
            4096,
            4096,
            stats.clone(),
        )
        .unwrap();
        let context =
            ReplayBody::from_bytes(Bytes::from_static(br#"{"input":[]}"#), 4096, 4096, stats)
                .unwrap();
        request.use_context_routing_metadata(&context);
        assert_eq!(request.model(), Some("gpt-5"));
        assert!(request.model_scan_complete());
    }

    #[test]
    fn nesting_limit_does_not_hide_later_account_owned_state() {
        for opening in *b"[{" {
            let mut scanner = MetadataScanner::default();
            let mut nested = "null".to_owned();
            for _ in 0..129 {
                nested = if opening == b'[' {
                    format!("[{nested}]")
                } else {
                    format!("{{\"padding\":{nested}}}")
                };
            }
            let request = format!(
                "{{\"padding\":{nested},\"input\":[{{\"encrypted_content\":\"opaque\"}}]}}"
            );
            scanner.feed(request.as_bytes());
            assert!(scanner.nonportable_state);
        }
    }

    #[test]
    fn finds_body_thread_id_across_chunks_without_retaining_other_strings() {
        let mut scanner = MetadataScanner::default();
        scanner.feed(br#"{"input":""#);
        scanner.feed(&vec![b'x'; 50_000]);
        scanner.feed(br#"","client_meta"#);
        scanner.feed(br#"data":{"other":1,"thread_"#);
        scanner.feed(br#"id":"thread-from-large-body"},"tail":true}"#);
        assert_eq!(scanner.thread_id.as_deref(), Some("thread-from-large-body"));
        let mut scanner = MetadataScanner::default();
        scanner.feed(br#"{"previous_response_id":"resp_123","prompt_cache_key":"cache_456"}"#);
        assert_eq!(scanner.previous_response_id.as_deref(), Some("resp_123"));
        assert_eq!(scanner.prompt_cache_key.as_deref(), Some("cache_456"));
        let mut scanner = MetadataScanner::default();
        scanner.feed(
            br#"{"input":[{"type":"input_file","file_id":"file_1"},{"file_id":"file_2","type":"input_image"},{"type":"function_call_output","output":{"file_id":"not_an_upload"}}]}"#,
        );
        assert_eq!(scanner.file_ids, ["file_1", "file_2"]);
    }

    #[test]
    fn detects_account_bound_response_state_across_chunks() {
        let mut scanner = MetadataScanner::default();
        scanner.feed(br#"{"input":[{"type":"reas"#);
        scanner.feed(br#"oning","encrypted_con"#);
        scanner.feed(br#"tent":"ciphertext"}]}"#);
        assert!(scanner.nonportable_state);

        let mut operation = MetadataScanner::default();
        operation
            .feed(br#"{"internal_chat_message_metadata_passthrough":{"operation_id":"op_1"}}"#);
        assert!(operation.nonportable_state);

        for body in [
            br#"{"input":[{"type":"reas\u006fning"}]}"#.as_slice(),
            br#"{"input":[{"encrypted_\u0063ontent":"ciphertext"}]}"#.as_slice(),
        ] {
            let mut escaped = MetadataScanner::default();
            escaped.feed(body);
            assert!(escaped.nonportable_state);
        }

        let mut portable = MetadataScanner::default();
        portable.feed(
            br#"{"input":[{"type":"message","role":"user","content":"hello\nworld"}],"reasoning":{"effort":"high"}}"#,
        );
        assert!(!portable.nonportable_state);
    }

    #[test]
    fn native_reasoning_is_account_bound_and_chunk_independent() {
        let native = serde_json::json!({"type":"reasoning","id":"rs_native","summary":[{"type":"summary_text","text":"synthetic summary"}],"encrypted_content":"x".repeat(2000)});
        for body in [
            serde_json::json!({"input":[native.clone()]}),
            serde_json::json!({"input":[{"encrypted_content":"opaque","summary":[],"type":"reasoning","id":"rs_last"}]}),
        ] {
            let bytes = serde_json::to_vec(&body).unwrap();
            for size in [1, 7, bytes.len()] {
                let mut scanner = MetadataScanner::default();
                for chunk in bytes.chunks(size) {
                    scanner.feed(chunk);
                }
                assert!(scanner.nonportable_state);
            }
        }
        for body in [
            serde_json::json!({"input":[{"type":"compaction","encrypted_content":"opaque"}]}),
            serde_json::json!({"input":[{"type":"reasoning","id":"rs_incomplete"}]}),
            serde_json::json!({"input":[{"type":"reasoning","encrypted_content":null}]}),
            serde_json::json!({"input":[{"type":"reasoning","encrypted_content":""}]}),
            serde_json::json!({"input":[{"type":"function_call_output","output":[native.clone()]}]}),
            serde_json::json!({"input":[{"type":"reasoning","encrypted_content":"opaque","extra":{"encrypted_content":"owned"}}]}),
            serde_json::json!({"input":[{"type":"reasoning","encrypted_content":"opaque","operation_id":"op"}]}),
            serde_json::json!({"input":[[native.clone()]]}),
            serde_json::json!({"extra":native}),
        ] {
            let mut scanner = MetadataScanner::default();
            for chunk in serde_json::to_vec(&body).unwrap().chunks(3) {
                scanner.feed(chunk);
            }
            assert!(scanner.nonportable_state, "{body}");
        }
        for body in [
            br#"{"input":[{"type":"compaction","type":"reasoning","encrypted_content":"opaque"}]}"#.as_slice(),
            br#"{"input":[{"type":"reasoning","encrypted_content":"first","encrypted_content":"last"}]}"#,
        ] {
            let mut scanner = MetadataScanner::default();
            scanner.feed(body);
            assert!(scanner.nonportable_state);
        }
        let mut unfinished = MetadataScanner::default();
        unfinished.feed(br#"{"input":[{"type":"reasoning","encrypted_content":"opaque""#);
        unfinished.finish();
        assert!(unfinished.nonportable_state);
    }

    #[test]
    fn responses_item_ids_are_account_bound_but_tool_call_ids_are_not() {
        for item in [
            serde_json::json!({"type":"message","id":"msg_owned","role":"assistant","content":[]}),
            serde_json::json!({"id":"fc_owned","type":"function_call","call_id":"call_1","name":"f","arguments":"{}"}),
        ] {
            let bytes = serde_json::to_vec(&serde_json::json!({"input":[item]})).unwrap();
            for size in [1, 7, bytes.len()] {
                let mut scanner = MetadataScanner::default();
                for chunk in bytes.chunks(size) {
                    scanner.feed(chunk);
                }
                scanner.finish();
                assert!(scanner.nonportable_state);
            }
        }
        let mut scanner = MetadataScanner::default();
        scanner.feed(br#"{"input":[{"type":"function_call","call_id":"call_1","name":"f","arguments":"{}"},{"type":"function_call_output","call_id":"call_1","output":{"id":"application_record"}}]}"#);
        scanner.finish();
        assert!(!scanner.nonportable_state);
    }
}
