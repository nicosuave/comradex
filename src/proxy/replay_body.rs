use std::{
    fs::File as StdFile,
    io::{Read, Write},
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_util::TryStreamExt;
use http_body_util::{BodyExt, Full, StreamBody, combinators::BoxBody};
use hyper::{
    Error as HyperError,
    body::{Body, Frame, Incoming},
};
use tempfile::NamedTempFile;
use tokio::{sync::mpsc, task::JoinHandle};
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
    file_ids: Vec<String>,
    file_ids_overflow: bool,
    nonportable_state: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyKind {
    ClientMetadata,
    ThreadId,
    PreviousResponseId,
    PromptCacheKey,
    FileId,
    Type,
    Other,
}

struct Container {
    object: bool,
    expect_key: bool,
    file_id: Option<String>,
    account_scoped_file: bool,
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
    file_ids: Vec<String>,
    file_ids_overflow: bool,
    nonportable_state: bool,
    disabled: bool,
}

impl MetadataScanner {
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
                        self.disabled = true;
                        continue;
                    }
                    let is_client = self.pending_value == Some(KeyKind::ClientMetadata);
                    self.pending_value = None;
                    self.containers.push(Container {
                        object: true,
                        expect_key: true,
                        file_id: None,
                        account_scoped_file: false,
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
                        file_id: None,
                        account_scoped_file: false,
                    });
                }
                b'}' | b']' => {
                    if self.client_depth == Some(self.containers.len()) {
                        self.client_depth = None;
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
        if let Some(kind) = self.capture_value {
            if !self.token_overflow && !self.token.is_empty() {
                let value = String::from_utf8(self.token.clone()).ok();
                match kind {
                    KeyKind::ThreadId => self.thread_id = value,
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
                    KeyKind::ClientMetadata | KeyKind::Other => {}
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
            } else if at_root && !self.token_overflow && self.token == b"previous_response_id" {
                Some(KeyKind::PreviousResponseId)
            } else if at_root && !self.token_overflow && self.token == b"prompt_cache_key" {
                Some(KeyKind::PromptCacheKey)
            } else if !self.token_overflow && self.token == b"file_id" {
                Some(KeyKind::FileId)
            } else if !self.token_overflow && self.token == b"type" {
                Some(KeyKind::Type)
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
        let reserved = self
            .bytes
            .checked_add(amount)
            .context("replay reservation overflow")?;
        self.stats
            .active_spool_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(amount)
                    .filter(|&updated| updated <= limit)
            })
            .map_err(|_| anyhow::anyhow!("global replay spool limit exceeded"))?;
        self.bytes = reserved;
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

/// A bounded handoff to a blocking spool writer.
///
/// Dropping this value closes the channel. That makes cancellation release the
/// worker and its `NamedTempFile` without doing filesystem work on a Tokio
/// executor thread.
struct SpoolWriter {
    sender: Option<mpsc::Sender<SpoolCommand>>,
    task: JoinHandle<Result<Storage>>,
}

enum SpoolCommand {
    Write(Bytes),
    Finish,
}

impl SpoolWriter {
    fn start(initial: Vec<u8>) -> Self {
        // A small queue bounds memory growth while still allowing the reader and
        // filesystem to make progress independently.
        let (sender, mut receiver) = mpsc::channel::<SpoolCommand>(2);
        let task = tokio::task::spawn_blocking(move || {
            let mut file = NamedTempFile::new().context("create replay spool")?;
            file.write_all(&initial).context("seed replay spool")?;
            loop {
                match receiver.blocking_recv() {
                    Some(SpoolCommand::Write(chunk)) => {
                        file.write_all(&chunk).context("write replay spool")?;
                    }
                    Some(SpoolCommand::Finish) => break,
                    None => bail!("replay spool cancelled"),
                }
            }
            file.as_file().sync_data().context("sync replay spool")?;
            let one = file.reopen().context("open first replay spool reader")?;
            let two = file.reopen().context("open second replay spool reader")?;
            drop(file);
            Ok(Storage::Files(vec![Some(one), Some(two)]))
        });
        Self {
            sender: Some(sender),
            task,
        }
    }

    async fn write(&self, bytes: Bytes) -> Result<()> {
        self.sender
            .as_ref()
            .context("replay spool already finalized")?
            .send(SpoolCommand::Write(bytes))
            .await
            .map_err(|_| anyhow::anyhow!("replay spool writer stopped"))
    }

    async fn finish(mut self) -> Result<Storage> {
        self.sender
            .as_ref()
            .context("replay spool already finalized")?
            .send(SpoolCommand::Finish)
            .await
            .map_err(|_| anyhow::anyhow!("replay spool writer stopped"))?;
        self.sender.take();
        self.task.await.context("join replay spool writer")?
    }
}

impl ReplayBody {
    pub fn from_bytes(
        bytes: Bytes,
        hard_limit: usize,
        global_limit: usize,
        stats: Arc<Stats>,
    ) -> Result<Self> {
        if bytes.len() > hard_limit {
            bail!("request body exceeds configured limit")
        }
        let mut reservation = Reservation {
            stats: stats.clone(),
            bytes: 0,
        };
        reservation.add(bytes.len(), global_limit)?;
        let mut metadata = MetadataScanner::default();
        metadata.feed(&bytes);
        reservation.bytes = 0;
        Ok(Self {
            len: bytes.len(),
            storage: Storage::Memory(bytes),
            stats,
            thread_id: metadata.thread_id,
            previous_response_id: metadata.previous_response_id,
            prompt_cache_key: metadata.prompt_cache_key,
            file_ids: metadata.file_ids,
            file_ids_overflow: metadata.file_ids_overflow,
            nonportable_state: metadata.nonportable_state,
        })
    }

    /// Read a replayable request body with both an idle timeout between frames
    /// and a total ingress deadline.
    pub async fn read_with_timeouts(
        incoming: Incoming,
        memory_limit: usize,
        hard_limit: usize,
        global_limit: usize,
        stats: Arc<Stats>,
        idle_timeout: Duration,
        total_timeout: Duration,
    ) -> Result<Self> {
        Self::read_body_with_timeouts(
            incoming,
            memory_limit,
            hard_limit,
            global_limit,
            stats,
            idle_timeout,
            total_timeout,
        )
        .await
    }

    async fn read_body_with_timeouts<B>(
        incoming: B,
        memory_limit: usize,
        hard_limit: usize,
        global_limit: usize,
        stats: Arc<Stats>,
        idle_timeout: Duration,
        total_timeout: Duration,
    ) -> Result<Self>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        tokio::time::timeout(
            total_timeout,
            Self::read_body(
                incoming,
                memory_limit,
                hard_limit,
                global_limit,
                stats,
                Some(idle_timeout),
            ),
        )
        .await
        .context("request body total ingress timeout")?
    }

    async fn read_body<B>(
        mut incoming: B,
        memory_limit: usize,
        hard_limit: usize,
        global_limit: usize,
        stats: Arc<Stats>,
        idle_timeout: Option<Duration>,
    ) -> Result<Self>
    where
        B: Body<Data = Bytes> + Unpin,
        B::Error: std::error::Error + Send + Sync + 'static,
    {
        let mut memory = Vec::new();
        let mut spool: Option<SpoolWriter> = None;
        let mut len = 0usize;
        let mut reservation = Reservation {
            stats: stats.clone(),
            bytes: 0,
        };
        let mut metadata = MetadataScanner::default();
        loop {
            let next = if let Some(idle_timeout) = idle_timeout {
                tokio::time::timeout(idle_timeout, incoming.frame())
                    .await
                    .context("request body idle timeout")?
            } else {
                incoming.frame().await
            };
            let Some(frame) = next else {
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
                bail!("request body exceeds configured limit")
            }
            reservation.add(data.len(), global_limit)?;
            metadata.feed(&data);
            if spool.is_none() && len <= memory_limit {
                memory.extend_from_slice(&data);
            } else {
                if spool.is_none() {
                    spool = Some(SpoolWriter::start(std::mem::take(&mut memory)));
                }
                spool.as_ref().expect("created").write(data).await?;
            }
        }
        let storage = if let Some(spool) = spool {
            spool.finish().await?
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
            file_ids: metadata.file_ids,
            file_ids_overflow: metadata.file_ids_overflow,
            nonportable_state: metadata.nonportable_state,
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
    use std::{io, sync::atomic::Ordering, time::Duration};

    use futures_util::{StreamExt, stream};

    use super::*;

    fn data_frame(bytes: &'static [u8]) -> Result<Frame<Bytes>, io::Error> {
        Ok(Frame::data(Bytes::from_static(bytes)))
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

    #[tokio::test]
    async fn spills_without_blocking_and_preserves_two_replays() {
        let stats = Arc::new(Stats::default());
        let incoming = StreamBody::new(stream::iter([
            data_frame(br#"{"client_metadata":{"thread_id":"thread-spilled"},"input":""#),
            data_frame(b"large-body"),
            data_frame(br#""}"#),
        ]));
        let mut replay = ReplayBody::read_body(incoming, 8, 1_024, 1_024, stats.clone(), None)
            .await
            .expect("spool request body");

        let expected = Bytes::from_static(
            br#"{"client_metadata":{"thread_id":"thread-spilled"},"input":"large-body"}"#,
        );
        assert!(matches!(&replay.storage, Storage::Files(_)));
        assert_eq!(replay.thread_id(), Some("thread-spilled"));
        assert_eq!(
            stats.active_spool_bytes.load(Ordering::Acquire),
            expected.len()
        );

        let first = replay
            .body(0)
            .expect("first replay")
            .collect()
            .await
            .expect("read first replay")
            .to_bytes();
        let second = replay
            .body(1)
            .expect("second replay")
            .collect()
            .await
            .expect("read second replay")
            .to_bytes();
        assert_eq!(first, second);
        assert_eq!(first, expected);
        assert!(replay.body(0).is_err());

        drop(replay);
        assert_eq!(stats.active_spool_bytes.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn cancellation_releases_spool_reservation() {
        let stats = Arc::new(Stats::default());
        let incoming = StreamBody::new(
            stream::iter([data_frame(b"spill-me")])
                .chain(stream::pending::<Result<Frame<Bytes>, io::Error>>()),
        );
        let task_stats = stats.clone();
        let task = tokio::spawn(async move {
            ReplayBody::read_body(incoming, 1, 1_024, 1_024, task_stats, None).await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while stats.active_spool_bytes.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("body reader reserved spool bytes");
        task.abort();
        let join_error = match task.await {
            Err(error) => error,
            Ok(_) => panic!("reader completed instead of being cancelled"),
        };
        assert!(join_error.is_cancelled());
        assert_eq!(stats.active_spool_bytes.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn idle_and_total_timeouts_release_reservations() {
        let stats = Arc::new(Stats::default());
        let stalled = StreamBody::new(
            stream::iter([data_frame(b"started")])
                .chain(stream::pending::<Result<Frame<Bytes>, io::Error>>()),
        );
        let error = ReplayBody::read_body_with_timeouts(
            stalled,
            1,
            1_024,
            1_024,
            stats.clone(),
            Duration::from_millis(20),
            Duration::from_secs(1),
        )
        .await
        .err()
        .expect("idle input should time out");
        assert!(error.to_string().contains("idle timeout"), "{error:#}");
        assert_eq!(stats.active_spool_bytes.load(Ordering::Acquire), 0);

        let never_started = StreamBody::new(stream::pending::<Result<Frame<Bytes>, io::Error>>());
        let error = ReplayBody::read_body_with_timeouts(
            never_started,
            1,
            1_024,
            1_024,
            stats.clone(),
            Duration::from_secs(1),
            Duration::from_millis(20),
        )
        .await
        .err()
        .expect("total input deadline should expire");
        assert!(
            error.to_string().contains("total ingress timeout"),
            "{error:#}"
        );
        assert_eq!(stats.active_spool_bytes.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn hard_and_global_limits_do_not_leak_reservations() {
        let hard_stats = Arc::new(Stats::default());
        let too_large = StreamBody::new(stream::iter([data_frame(b"12345")]));
        let error = ReplayBody::read_body(too_large, 1, 4, 100, hard_stats.clone(), None)
            .await
            .err()
            .expect("hard limit should reject body");
        assert!(error.to_string().contains("configured limit"));
        assert_eq!(hard_stats.active_spool_bytes.load(Ordering::Acquire), 0);

        let global_stats = Arc::new(Stats::default());
        let globally_too_large = StreamBody::new(stream::iter([data_frame(b"12345")]));
        let error =
            ReplayBody::read_body(globally_too_large, 1, 100, 4, global_stats.clone(), None)
                .await
                .err()
                .expect("global limit should reject body");
        assert!(error.to_string().contains("global replay spool limit"));
        assert_eq!(global_stats.active_spool_bytes.load(Ordering::Acquire), 0);
    }
}
