use std::{
    fs::File as StdFile,
    io::Write,
    sync::{Arc, atomic::Ordering},
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::{
    Error as HyperError,
    body::{Frame, Incoming},
};
use tempfile::NamedTempFile;
use tokio_util::io::ReaderStream;

use crate::state::Stats;

pub type ProxyBody = BoxBody<Bytes, std::io::Error>;

pub struct ReplayBody {
    storage: Storage,
    len: usize,
    stats: Arc<Stats>,
    thread_id: Option<String>,
    previous_response_id: Option<String>,
    prompt_cache_key: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    ClientMetadata,
    ThreadId,
    PreviousResponseId,
    PromptCacheKey,
    Other,
}

#[derive(Clone, Copy)]
struct Container {
    object: bool,
    expect_key: bool,
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
    previous_response_id: Option<String>,
    prompt_cache_key: Option<String>,
    disabled: bool,
}

impl MetadataScanner {
    fn feed(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if self.disabled
                || (self.thread_id.is_some()
                    && self.previous_response_id.is_some()
                    && self.prompt_cache_key.is_some())
            {
                return;
            }
            if self.in_string {
                if self.escape {
                    self.escape = false;
                    self.token_overflow = true;
                } else if byte == b'\\' {
                    self.escape = true;
                    self.token_overflow = true;
                } else if byte == b'"' {
                    self.finish_string();
                } else if !self.token_overflow {
                    let limit = if self.capture_value.is_some() {
                        512
                    } else {
                        32
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
                                | KeyKind::PreviousResponseId
                                | KeyKind::PromptCacheKey
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
                        self.disabled = true;
                        continue;
                    }
                    let is_client = self.pending_value == Some(KeyKind::ClientMetadata);
                    self.pending_value = None;
                    self.containers.push(Container {
                        object: true,
                        expect_key: true,
                    });
                    if is_client {
                        self.client_depth = Some(self.containers.len());
                    }
                }
                b'[' => {
                    if self.containers.len() >= 128 {
                        self.disabled = true;
                        continue;
                    }
                    self.pending_value = None;
                    self.containers.push(Container {
                        object: false,
                        expect_key: false,
                    });
                }
                b'}' | b']' => {
                    if self.client_depth == Some(self.containers.len()) {
                        self.client_depth = None;
                    }
                    self.containers.pop();
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
        if let Some(kind) = self.capture_value {
            if !self.token_overflow && !self.token.is_empty() {
                let value = String::from_utf8(self.token.clone()).ok();
                match kind {
                    KeyKind::ThreadId => self.thread_id = value,
                    KeyKind::PreviousResponseId => self.previous_response_id = value,
                    KeyKind::PromptCacheKey => self.prompt_cache_key = value,
                    KeyKind::ClientMetadata | KeyKind::Other => {}
                }
            }
        } else if self.string_is_key {
            let in_client = self.client_depth == Some(self.containers.len());
            let at_root = self.containers.len() == 1;
            self.key = if at_root && !self.token_overflow && self.token == b"client_metadata" {
                Some(KeyKind::ClientMetadata)
            } else if in_client && !self.token_overflow && self.token == b"thread_id" {
                Some(KeyKind::ThreadId)
            } else if at_root && !self.token_overflow && self.token == b"previous_response_id" {
                Some(KeyKind::PreviousResponseId)
            } else if at_root && !self.token_overflow && self.token == b"prompt_cache_key" {
                Some(KeyKind::PromptCacheKey)
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
            bail!("global replay spool limit exceeded")
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
    pub async fn read(
        incoming: Incoming,
        memory_limit: usize,
        hard_limit: usize,
        global_limit: usize,
        stats: Arc<Stats>,
    ) -> Result<Self> {
        let mut incoming = incoming;
        let mut memory = Vec::new();
        let mut temp: Option<NamedTempFile> = None;
        let mut len = 0usize;
        let mut reservation = Reservation {
            stats: stats.clone(),
            bytes: 0,
        };
        let mut metadata = MetadataScanner::default();
        while let Some(frame) = incoming.frame().await {
            let frame = frame.context("read request body")?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            len = len
                .checked_add(data.len())
                .context("request length overflow")?;
            if len > hard_limit {
                bail!("request body exceeds configured limit")
            }
            reservation.add(data.len(), global_limit)?;
            metadata.feed(&data);
            if temp.is_none() && len <= memory_limit {
                memory.extend_from_slice(&data);
            } else {
                if temp.is_none() {
                    let mut file = NamedTempFile::new().context("create replay spool")?;
                    file.write_all(&memory)?;
                    memory.clear();
                    temp = Some(file);
                }
                temp.as_mut().expect("created").write_all(&data)?;
            }
        }
        let storage = if let Some(file) = temp {
            file.as_file().sync_data()?;
            let one = StdFile::open(file.path())?;
            let two = StdFile::open(file.path())?;
            drop(file);
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
            previous_response_id: metadata.previous_response_id,
            prompt_cache_key: metadata.prompt_cache_key,
        })
    }

    pub fn thread_id(&self) -> Option<&str> {
        self.thread_id.as_deref()
    }

    pub fn previous_response_id(&self) -> Option<&str> {
        self.previous_response_id.as_deref()
    }

    pub fn prompt_cache_key(&self) -> Option<&str> {
        self.prompt_cache_key.as_deref()
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

fn never_to_io(never: std::convert::Infallible) -> std::io::Error {
    match never {}
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
