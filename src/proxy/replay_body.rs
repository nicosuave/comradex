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
    ClientMetadata,
    ThreadId,
    SessionId,
    PreviousResponseId,
    PromptCacheKey,
    FileId,
    Type,
    Input,
    EncryptedContent,
    Other,
}

struct Container {
    object: bool,
    expect_key: bool,
    file_id: Option<String>,
    account_scoped_file: bool,
    input_array: bool,
    native_input_item: bool,
    reasoning: bool,
    type_keys: usize,
    encrypted_content: bool,
    encrypted_content_string: bool,
}

#[derive(Default)]
struct MetadataScanner {
    containers: Vec<Container>,
    client_depth: Option<usize>,
    in_string: bool,
    escape: bool,
    string_is_key: bool,
    capture_value: Option<KeyKind>,
    token: Vec<u8>,
    token_overflow: bool,
    key: Option<KeyKind>,
    pending_value: Option<KeyKind>,
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
        // An unfinished item has not established that its encrypted payload is native
        // reasoning; do not relax ownership for a truncated or malformed request.
        self.nonportable_state |= self.in_string || !self.containers.is_empty();
    }

    fn feed(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if self.disabled {
                return;
            }
            if self.in_string {
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
                    self.capture_value = self.pending_value.take().filter(|kind| {
                        matches!(
                            kind,
                            KeyKind::ThreadId
                                | KeyKind::SessionId
                                | KeyKind::PreviousResponseId
                                | KeyKind::PromptCacheKey
                                | KeyKind::FileId
                                | KeyKind::Type
                                | KeyKind::EncryptedContent
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
                    let native_input_item = self.containers.last().is_some_and(|v| v.input_array);
                    self.pending_value = None;
                    self.containers.push(Container {
                        object: true,
                        expect_key: true,
                        file_id: None,
                        account_scoped_file: false,
                        input_array: false,
                        native_input_item,
                        reasoning: false,
                        type_keys: 0,
                        encrypted_content: false,
                        encrypted_content_string: false,
                    });
                    if is_client {
                        self.client_depth = Some(self.containers.len());
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
                        reasoning: false,
                        type_keys: 0,
                        encrypted_content: false,
                        encrypted_content_string: false,
                    });
                }
                b'}' | b']' => {
                    if self.client_depth == Some(self.containers.len()) {
                        self.client_depth = None;
                    }
                    if let Some(container) = self.containers.pop() {
                        // Only a self-contained native reasoning item directly in input may
                        // carry portable ciphertext. Other encrypted payloads retain ownership.
                        if container.reasoning || container.encrypted_content {
                            self.nonportable_state |= !(container.native_input_item
                                && container.reasoning
                                && container.type_keys == 1
                                && container.encrypted_content_string);
                        }
                        if container.account_scoped_file
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
            if kind == KeyKind::EncryptedContent
                && !self.token.is_empty()
                && let Some(container) = self.containers.last_mut()
            {
                container.encrypted_content_string = true;
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
                            container.reasoning = value == "reasoning";
                            self.nonportable_state |= matches!(
                                value.as_str(),
                                "compaction"
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
                    KeyKind::ClientMetadata
                    | KeyKind::Other
                    | KeyKind::Input
                    | KeyKind::EncryptedContent => {}
                }
            }
        } else if self.string_is_key {
            let in_client = self.client_depth == Some(self.containers.len());
            let at_root = self.containers.len() == 1;
            if !self.token_overflow
                && (matches!(
                    self.token.as_slice(),
                    b"operation_id"
                        | b"codex_operation_id"
                        | b"internal_chat_message_metadata_passthrough"
                ) || (at_root
                    && matches!(
                        self.token.as_slice(),
                        b"conversation" | b"prompt" | b"turn_state"
                    )))
            {
                self.nonportable_state = true;
            }
            self.key = if at_root && !self.token_overflow && self.token == b"client_metadata" {
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
                if let Some(container) = self.containers.last_mut() {
                    container.type_keys += 1;
                }
                Some(KeyKind::Type)
            } else if at_root && !self.token_overflow && self.token == b"input" {
                Some(KeyKind::Input)
            } else if !self.token_overflow && self.token == b"encrypted_content" {
                if let Some(container) = self.containers.last_mut() {
                    // Duplicate ciphertext properties are ambiguous to downstream parsers.
                    self.nonportable_state |= container.encrypted_content;
                    container.encrypted_content = true;
                }
                Some(KeyKind::EncryptedContent)
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
        let payload = Bytes::from_static(br#"{"client_metadata":{"thread_id":"thread-1","session_id":"session-1"},"previous_response_id":"resp-1","prompt_cache_key":"cache-1","input":[{"type":"input_file","file_id":"file-1"},{"type":"compaction","encrypted_content":"owned"}]}"#);
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
        assert!(!scanner.nonportable_state);

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
    fn native_reasoning_exception_is_item_scoped_and_chunk_independent() {
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
                assert!(!scanner.nonportable_state);
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
}
