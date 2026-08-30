//! Durable lifecycle tracking for response operations with ambiguous transport outcomes.
//!
//! The ledger deliberately does not provide a replay API. An operation that may have been
//! dispatched is either recoverable on its owning account (after an upstream acknowledgement)
//! or is abandoned fail-closed. Moving an operation to another account is never safe.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt, fs,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, mpsc},
    thread,
    time::Duration,
};

use serde::{Deserialize, Serialize};

#[cfg(test)]
use std::sync::Arc;

const SNAPSHOT_VERSION: u8 = 1;
const MAX_IDENTIFIER_BYTES: usize = 512;
const MAX_REASON_BYTES: usize = 2_048;

/// Disk and retention limits for an operation ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerLimits {
    /// Maximum number of retained active and terminal records.
    pub max_records: usize,
    /// Maximum serialized snapshot size.
    pub max_bytes: usize,
    /// Age after which terminal records may be pruned during the next mutation.
    pub terminal_retention_ms: u64,
}

impl Default for LedgerLimits {
    fn default() -> Self {
        Self {
            max_records: 4_096,
            max_bytes: 4 * 1024 * 1024,
            terminal_retention_ms: 24 * 60 * 60 * 1_000,
        }
    }
}

/// Persisted lifecycle state for one response operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    /// The request is about to be written, or may already have been written, upstream.
    Dispatching,
    /// Upstream emitted `response.created` on the owning account.
    Acknowledged,
    Completed,
    Failed,
    /// The outcome is ambiguous or cannot be recovered safely.
    Abandoned,
}

impl OperationState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Abandoned)
    }
}

/// A durable operation record. `generation` is a compare-and-swap token for transitions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OperationRecord {
    pub operation_id: String,
    pub account_id: String,
    pub state: OperationState,
    pub generation: u64,
    pub created_unix_ms: u64,
    pub updated_unix_ms: u64,
    pub lease_expires_unix_ms: u64,
    pub upstream_response_id: Option<String>,
    pub terminal_reason: Option<String>,
}

/// The only nonterminal recovery indicated by startup reconciliation.
///
/// The owning account is part of the result by design. Callers must reconnect/query through that
/// exact account, and must abandon the operation if same-account recovery is unavailable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SameAccountRecovery {
    pub operation_id: String,
    pub account_id: String,
    pub upstream_response_id: String,
    pub expected_generation: u64,
    pub lease_expires_unix_ms: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StartupReconciliation {
    pub abandoned_operation_ids: Vec<String>,
    pub same_account_recoveries: Vec<SameAccountRecovery>,
}

#[derive(Debug)]
pub enum LedgerError {
    Io(std::io::Error),
    Corrupt(String),
    InvalidInput(String),
    NotFound(String),
    Conflict {
        operation_id: String,
        expected_generation: u64,
        actual_generation: u64,
    },
    InvalidTransition {
        operation_id: String,
        from: OperationState,
        to: OperationState,
    },
    WrongAccount {
        operation_id: String,
        owner: String,
        attempted: String,
    },
    Capacity {
        max_records: usize,
        max_bytes: usize,
    },
    AlreadyOpen(PathBuf),
    Worker(String),
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "operation ledger I/O failed: {error}"),
            Self::Corrupt(message) => write!(f, "operation ledger is corrupt: {message}"),
            Self::InvalidInput(message) => write!(f, "invalid operation ledger input: {message}"),
            Self::NotFound(id) => write!(f, "operation {id:?} is not in the ledger"),
            Self::Conflict {
                operation_id,
                expected_generation,
                actual_generation,
            } => write!(
                f,
                "operation {operation_id:?} generation conflict: expected {expected_generation}, found {actual_generation}"
            ),
            Self::InvalidTransition {
                operation_id,
                from,
                to,
            } => write!(
                f,
                "operation {operation_id:?} cannot transition from {from:?} to {to:?}"
            ),
            Self::WrongAccount {
                operation_id,
                owner,
                attempted,
            } => write!(
                f,
                "operation {operation_id:?} belongs to account {owner:?}, not {attempted:?}"
            ),
            Self::Capacity {
                max_records,
                max_bytes,
            } => write!(
                f,
                "operation ledger capacity exhausted ({max_records} records, {max_bytes} bytes)"
            ),
            Self::AlreadyOpen(path) => {
                write!(f, "operation ledger {} is already open", path.display())
            }
            Self::Worker(message) => write!(f, "operation ledger worker failed: {message}"),
        }
    }
}

impl Error for LedgerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for LedgerError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub type Result<T> = std::result::Result<T, LedgerError>;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Snapshot {
    version: u8,
    snapshot_generation: u64,
    #[serde(default)]
    wal_sequence: u64,
    records: BTreeMap<String, OperationRecord>,
}

impl Snapshot {
    fn empty() -> Self {
        Self {
            version: SNAPSHOT_VERSION,
            snapshot_generation: 0,
            wal_sequence: 0,
            records: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WalTransaction {
    version: u8,
    sequence: u64,
    changes: Vec<WalChange>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct WalChange {
    operation_id: String,
    record: Option<OperationRecord>,
}

#[derive(Debug, Deserialize, Serialize)]
struct WalEnvelope {
    transaction: WalTransaction,
    checksum: String,
}

struct LedgerState {
    snapshot: Snapshot,
    record_bytes: usize,
    wal_entries: usize,
    wal_bytes: usize,
    poisoned: Option<String>,
}

struct WalWrite {
    encoded: Vec<u8>,
    compact_snapshot: Option<Snapshot>,
    completed: mpsc::SyncSender<std::result::Result<(), String>>,
}

struct PreparedWrite {
    encoded: Vec<u8>,
    compact_snapshot: Option<Snapshot>,
}

struct WalWriter {
    sender: Mutex<Option<mpsc::Sender<WalWrite>>>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
    #[cfg(test)]
    stats: Arc<WalWriterStats>,
}

#[cfg(test)]
#[derive(Default)]
struct WalWriterStats {
    fsyncs: std::sync::atomic::AtomicUsize,
    max_batch: std::sync::atomic::AtomicUsize,
}

/// An in-process operation ledger backed by a checksummed WAL and periodic atomic snapshots.
///
/// Construct exactly one instance for a given path. Mutations are ordered in-process and group
/// committed by a dedicated writer. A successful return means the corresponding WAL transaction
/// has been synced. Async callers must await `begin_dispatch` before writing upstream bytes.
pub struct OperationLedger {
    limits: LedgerLimits,
    state: Mutex<LedgerState>,
    writer: WalWriter,
    _lock: LedgerLock,
}

/// Tokio-safe facade that moves every blocking ledger operation onto the blocking pool.
#[derive(Clone)]
pub struct AsyncOperationLedger {
    inner: std::sync::Arc<OperationLedger>,
}

impl AsyncOperationLedger {
    /// Opens and reconciles a ledger without blocking the async executor. Since Comradex cannot
    /// resume an acknowledged response stream after restart, all recovery candidates are
    /// durably abandoned on their original account before this returns.
    pub async fn open_fail_closed(
        path: impl Into<PathBuf>,
        limits: LedgerLimits,
        now_unix_ms: u64,
    ) -> Result<Self> {
        let path = path.into();
        tokio::task::spawn_blocking(move || {
            let (ledger, reconciliation) = OperationLedger::open(path, limits, now_unix_ms)?;
            for recovery in reconciliation.same_account_recoveries {
                ledger.abandon_after_transport_loss(
                    &recovery.operation_id,
                    recovery.expected_generation,
                    &recovery.account_id,
                    now_unix_ms,
                    "startup cannot resume an acknowledged response stream",
                )?;
            }
            Ok(Self {
                inner: std::sync::Arc::new(ledger),
            })
        })
        .await
        .map_err(|error| LedgerError::Worker(error.to_string()))?
    }

    pub async fn begin_dispatch(
        &self,
        operation_id: String,
        account_id: String,
        now_unix_ms: u64,
        lease_expires_unix_ms: u64,
    ) -> Result<OperationRecord> {
        let ledger = self.inner.clone();
        run_blocking(move || {
            ledger.begin_dispatch(operation_id, account_id, now_unix_ms, lease_expires_unix_ms)
        })
        .await
    }

    pub async fn acknowledge(
        &self,
        operation_id: String,
        expected_generation: u64,
        account_id: String,
        upstream_response_id: String,
        now_unix_ms: u64,
        lease_expires_unix_ms: u64,
    ) -> Result<OperationRecord> {
        let ledger = self.inner.clone();
        run_blocking(move || {
            ledger.acknowledge(
                &operation_id,
                expected_generation,
                &account_id,
                upstream_response_id,
                now_unix_ms,
                lease_expires_unix_ms,
            )
        })
        .await
    }

    pub async fn complete(
        &self,
        operation_id: String,
        expected_generation: u64,
        account_id: String,
        now_unix_ms: u64,
    ) -> Result<OperationRecord> {
        let ledger = self.inner.clone();
        run_blocking(move || {
            ledger.complete(&operation_id, expected_generation, &account_id, now_unix_ms)
        })
        .await
    }

    pub async fn complete_from_terminal(
        &self,
        operation_id: String,
        expected_generation: u64,
        account_id: String,
        upstream_response_id: String,
        now_unix_ms: u64,
    ) -> Result<OperationRecord> {
        let ledger = self.inner.clone();
        run_blocking(move || {
            ledger.complete_from_terminal(
                &operation_id,
                expected_generation,
                &account_id,
                upstream_response_id,
                now_unix_ms,
            )
        })
        .await
    }

    pub async fn fail(
        &self,
        operation_id: String,
        expected_generation: u64,
        account_id: String,
        now_unix_ms: u64,
        reason: String,
    ) -> Result<OperationRecord> {
        let ledger = self.inner.clone();
        run_blocking(move || {
            ledger.fail(
                &operation_id,
                expected_generation,
                &account_id,
                now_unix_ms,
                reason,
            )
        })
        .await
    }

    pub async fn abandon_after_transport_loss(
        &self,
        operation_id: String,
        expected_generation: u64,
        account_id: String,
        now_unix_ms: u64,
        reason: String,
    ) -> Result<OperationRecord> {
        let ledger = self.inner.clone();
        run_blocking(move || {
            ledger.abandon_after_transport_loss(
                &operation_id,
                expected_generation,
                &account_id,
                now_unix_ms,
                reason,
            )
        })
        .await
    }

    #[cfg(test)]
    pub async fn get(&self, operation_id: String) -> Result<Option<OperationRecord>> {
        let ledger = self.inner.clone();
        run_blocking(move || Ok(ledger.get(&operation_id))).await
    }

    /// Creates a cancellation-independent lifecycle owner before any blocking work begins.
    /// Dropping the last handle while its operation is active schedules a durable abandonment.
    pub fn track(
        &self,
        operation_id: impl Into<String>,
        account_id: impl Into<String>,
    ) -> AsyncOperationHandle {
        AsyncOperationHandle {
            inner: std::sync::Arc::new(AsyncHandleInner {
                ledger: self.inner.clone(),
                operation_id: operation_id.into(),
                account_id: account_id.into(),
                record: Mutex::new(None),
            }),
        }
    }
}

/// Shared operation state updated inside the blocking mutation, not by the awaiting future.
#[derive(Clone)]
pub struct AsyncOperationHandle {
    inner: std::sync::Arc<AsyncHandleInner>,
}

struct AsyncHandleInner {
    ledger: std::sync::Arc<OperationLedger>,
    operation_id: String,
    account_id: String,
    record: Mutex<Option<OperationRecord>>,
}

impl AsyncOperationHandle {
    pub fn operation_id(&self) -> &str {
        &self.inner.operation_id
    }

    pub fn account_id(&self) -> &str {
        &self.inner.account_id
    }

    pub fn snapshot(&self) -> Option<OperationRecord> {
        self.inner
            .record
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub async fn begin(
        &self,
        now_unix_ms: u64,
        lease_expires_unix_ms: u64,
    ) -> Result<OperationRecord> {
        run_handle_mutation(
            self.inner.clone(),
            move |ledger, inner, _| {
                ledger.begin_dispatch(
                    inner.operation_id.clone(),
                    inner.account_id.clone(),
                    now_unix_ms,
                    lease_expires_unix_ms,
                )
            },
            |record| record.state == OperationState::Dispatching,
        )
        .await
    }

    pub async fn acknowledge(
        &self,
        upstream_response_id: impl Into<String>,
        now_unix_ms: u64,
        lease_expires_unix_ms: u64,
    ) -> Result<OperationRecord> {
        let response_id = upstream_response_id.into();
        let expected_response_id = response_id.clone();
        run_handle_mutation(
            self.inner.clone(),
            move |ledger, inner, current| {
                let current =
                    current.ok_or_else(|| LedgerError::NotFound(inner.operation_id.clone()))?;
                ledger.acknowledge(
                    &inner.operation_id,
                    current.generation,
                    &inner.account_id,
                    response_id,
                    now_unix_ms,
                    lease_expires_unix_ms,
                )
            },
            move |record| {
                record.state == OperationState::Acknowledged
                    && record.upstream_response_id.as_deref() == Some(expected_response_id.as_str())
            },
        )
        .await
    }

    pub async fn complete(&self, now_unix_ms: u64) -> Result<OperationRecord> {
        run_handle_mutation(
            self.inner.clone(),
            move |ledger, inner, current| {
                let current =
                    current.ok_or_else(|| LedgerError::NotFound(inner.operation_id.clone()))?;
                ledger.complete(
                    &inner.operation_id,
                    current.generation,
                    &inner.account_id,
                    now_unix_ms,
                )
            },
            |record| record.state == OperationState::Completed,
        )
        .await
    }

    pub async fn complete_from_terminal(
        &self,
        upstream_response_id: impl Into<String>,
        now_unix_ms: u64,
    ) -> Result<OperationRecord> {
        let response_id = upstream_response_id.into();
        let expected_response_id = response_id.clone();
        run_handle_mutation(
            self.inner.clone(),
            move |ledger, inner, current| {
                let current =
                    current.ok_or_else(|| LedgerError::NotFound(inner.operation_id.clone()))?;
                ledger.complete_from_terminal(
                    &inner.operation_id,
                    current.generation,
                    &inner.account_id,
                    response_id,
                    now_unix_ms,
                )
            },
            move |record| {
                record.state == OperationState::Completed
                    && record.upstream_response_id.as_deref() == Some(expected_response_id.as_str())
            },
        )
        .await
    }

    pub async fn fail(
        &self,
        now_unix_ms: u64,
        reason: impl Into<String>,
    ) -> Result<OperationRecord> {
        let reason = reason.into();
        run_handle_mutation(
            self.inner.clone(),
            move |ledger, inner, current| {
                let current =
                    current.ok_or_else(|| LedgerError::NotFound(inner.operation_id.clone()))?;
                ledger.fail(
                    &inner.operation_id,
                    current.generation,
                    &inner.account_id,
                    now_unix_ms,
                    reason,
                )
            },
            |record| record.state == OperationState::Failed,
        )
        .await
    }

    pub async fn abandon(
        &self,
        now_unix_ms: u64,
        reason: impl Into<String>,
    ) -> Result<OperationRecord> {
        let reason = reason.into();
        run_handle_mutation(
            self.inner.clone(),
            move |ledger, inner, current| {
                let current =
                    current.ok_or_else(|| LedgerError::NotFound(inner.operation_id.clone()))?;
                ledger.abandon_after_transport_loss(
                    &inner.operation_id,
                    current.generation,
                    &inner.account_id,
                    now_unix_ms,
                    reason,
                )
            },
            |record| record.state == OperationState::Abandoned,
        )
        .await
    }
}

async fn run_handle_mutation<F, P>(
    inner: std::sync::Arc<AsyncHandleInner>,
    mutation: F,
    satisfied: P,
) -> Result<OperationRecord>
where
    F: FnOnce(
            &OperationLedger,
            &AsyncHandleInner,
            Option<&OperationRecord>,
        ) -> Result<OperationRecord>
        + Send
        + 'static,
    P: Fn(&OperationRecord) -> bool + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let current = inner
            .record
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        match mutation(&inner.ledger, &inner, current.as_ref()) {
            Ok(record) => {
                *inner
                    .record
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(record.clone());
                Ok(record)
            }
            Err(error) => {
                let Some(record) = inner.ledger.get(&inner.operation_id) else {
                    return Err(error);
                };
                if record.account_id != inner.account_id {
                    return Err(LedgerError::WrongAccount {
                        operation_id: inner.operation_id.clone(),
                        owner: record.account_id,
                        attempted: inner.account_id.clone(),
                    });
                }
                *inner
                    .record
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(record.clone());
                if satisfied(&record) {
                    Ok(record)
                } else {
                    Err(error)
                }
            }
        }
    })
    .await
    .map_err(|error| LedgerError::Worker(error.to_string()))?
}

impl Drop for AsyncHandleInner {
    fn drop(&mut self) {
        let Some(record) = self
            .record
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        else {
            return;
        };
        if record.state.is_terminal() {
            return;
        }
        let ledger = self.ledger.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn_blocking(move || {
                let _ = ledger.abandon_after_transport_loss(
                    &record.operation_id,
                    record.generation,
                    &record.account_id,
                    current_unix_ms(),
                    "async operation handle dropped before terminal state",
                );
            });
        }
    }
}

async fn run_blocking<T, F>(operation: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| LedgerError::Worker(error.to_string()))?
}

fn current_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

impl OperationLedger {
    /// Opens the ledger and performs fail-closed startup reconciliation.
    ///
    /// Every persisted `dispatching` record is abandoned because the previous process may have
    /// sent it without observing an acknowledgement. Expired acknowledgements are also abandoned.
    /// Unexpired acknowledged records are returned only as same-account recovery candidates.
    pub fn open(
        path: impl Into<PathBuf>,
        limits: LedgerLimits,
        now_unix_ms: u64,
    ) -> Result<(Self, StartupReconciliation)> {
        validate_limits(limits)?;
        let path = path.into();
        let lock = LedgerLock::acquire(&path)?;
        let mut snapshot = load_snapshot(&path, limits.max_bytes)?;
        validate_snapshot(&snapshot)?;
        let (wal_entries, wal_bytes) = replay_wal(&wal_path(&path), &mut snapshot, limits)?;
        validate_snapshot(&snapshot)?;
        let record_bytes = records_serialized_bytes(&snapshot.records)?;
        let writer = WalWriter::start(path.clone())?;
        let ledger = Self {
            limits,
            state: Mutex::new(LedgerState {
                snapshot,
                record_bytes,
                wal_entries,
                wal_bytes,
                poisoned: None,
            }),
            writer,
            _lock: lock,
        };
        let reconciliation = ledger.reconcile_startup(now_unix_ms)?;
        Ok((ledger, reconciliation))
    }

    /// Durably records intent before the caller sends any request bytes upstream.
    pub fn begin_dispatch(
        &self,
        operation_id: impl Into<String>,
        account_id: impl Into<String>,
        now_unix_ms: u64,
        lease_expires_unix_ms: u64,
    ) -> Result<OperationRecord> {
        let operation_id = operation_id.into();
        let account_id = account_id.into();
        validate_identifier("operation_id", &operation_id)?;
        validate_identifier("account_id", &account_id)?;
        validate_lease(now_unix_ms, lease_expires_unix_ms)?;

        self.mutate_record(&operation_id, now_unix_ms, |existing| {
            if existing.is_some() {
                return Err(LedgerError::InvalidInput(format!(
                    "operation_id {operation_id:?} already exists"
                )));
            }
            let record = OperationRecord {
                operation_id: operation_id.clone(),
                account_id,
                state: OperationState::Dispatching,
                generation: 1,
                created_unix_ms: now_unix_ms,
                updated_unix_ms: now_unix_ms,
                lease_expires_unix_ms,
                upstream_response_id: None,
                terminal_reason: None,
            };
            Ok((record.clone(), record))
        })
    }

    /// Records `response.created`. Ownership and generation are checked before transition.
    pub fn acknowledge(
        &self,
        operation_id: &str,
        expected_generation: u64,
        account_id: &str,
        upstream_response_id: impl Into<String>,
        now_unix_ms: u64,
        lease_expires_unix_ms: u64,
    ) -> Result<OperationRecord> {
        let upstream_response_id = upstream_response_id.into();
        validate_identifier("upstream_response_id", &upstream_response_id)?;
        validate_lease(now_unix_ms, lease_expires_unix_ms)?;
        self.transition(
            operation_id,
            expected_generation,
            account_id,
            OperationState::Acknowledged,
            now_unix_ms,
            |record| {
                record.lease_expires_unix_ms = lease_expires_unix_ms;
                record.upstream_response_id = Some(upstream_response_id);
                record.terminal_reason = None;
            },
        )
    }

    /// Extends the lease for an acknowledged operation on its owning account.
    pub fn renew_acknowledged_lease(
        &self,
        operation_id: &str,
        expected_generation: u64,
        account_id: &str,
        now_unix_ms: u64,
        lease_expires_unix_ms: u64,
    ) -> Result<OperationRecord> {
        validate_lease(now_unix_ms, lease_expires_unix_ms)?;
        self.transition(
            operation_id,
            expected_generation,
            account_id,
            OperationState::Acknowledged,
            now_unix_ms,
            |record| record.lease_expires_unix_ms = lease_expires_unix_ms,
        )
    }

    pub fn complete(
        &self,
        operation_id: &str,
        expected_generation: u64,
        account_id: &str,
        now_unix_ms: u64,
    ) -> Result<OperationRecord> {
        self.transition(
            operation_id,
            expected_generation,
            account_id,
            OperationState::Completed,
            now_unix_ms,
            |_| {},
        )
    }

    /// Atomically records a completed terminal that arrived without a preceding created event.
    /// The terminal's response id is definitive acknowledgement by the owning account.
    pub fn complete_from_terminal(
        &self,
        operation_id: &str,
        expected_generation: u64,
        account_id: &str,
        upstream_response_id: impl Into<String>,
        now_unix_ms: u64,
    ) -> Result<OperationRecord> {
        let upstream_response_id = upstream_response_id.into();
        validate_identifier("upstream_response_id", &upstream_response_id)?;
        validate_identifier("operation_id", operation_id)?;
        validate_identifier("account_id", account_id)?;
        self.mutate_record(operation_id, now_unix_ms, |record| {
            let mut record = record
                .cloned()
                .ok_or_else(|| LedgerError::NotFound(operation_id.to_owned()))?;
            if record.generation != expected_generation {
                return Err(LedgerError::Conflict {
                    operation_id: operation_id.to_owned(),
                    expected_generation,
                    actual_generation: record.generation,
                });
            }
            if record.account_id != account_id {
                return Err(LedgerError::WrongAccount {
                    operation_id: operation_id.to_owned(),
                    owner: record.account_id.clone(),
                    attempted: account_id.to_owned(),
                });
            }
            if record.state != OperationState::Dispatching {
                return Err(LedgerError::InvalidTransition {
                    operation_id: operation_id.to_owned(),
                    from: record.state,
                    to: OperationState::Completed,
                });
            }
            record.state = OperationState::Completed;
            record.generation = next_generation(&record)?;
            record.updated_unix_ms = record.updated_unix_ms.max(now_unix_ms);
            record.upstream_response_id = Some(upstream_response_id);
            Ok((record.clone(), record))
        })
    }

    /// Records a known failure. This is valid both before and after acknowledgement.
    pub fn fail(
        &self,
        operation_id: &str,
        expected_generation: u64,
        account_id: &str,
        now_unix_ms: u64,
        reason: impl Into<String>,
    ) -> Result<OperationRecord> {
        let reason = reason.into();
        validate_reason(&reason)?;
        self.transition(
            operation_id,
            expected_generation,
            account_id,
            OperationState::Failed,
            now_unix_ms,
            |record| record.terminal_reason = Some(reason),
        )
    }

    /// Fail-closes an operation whose transport ended without a definitive terminal event.
    pub fn abandon_after_transport_loss(
        &self,
        operation_id: &str,
        expected_generation: u64,
        account_id: &str,
        now_unix_ms: u64,
        reason: impl Into<String>,
    ) -> Result<OperationRecord> {
        let reason = reason.into();
        validate_reason(&reason)?;
        self.transition(
            operation_id,
            expected_generation,
            account_id,
            OperationState::Abandoned,
            now_unix_ms,
            |record| record.terminal_reason = Some(reason),
        )
    }

    /// Abandons active operations whose leases expired. No replay is attempted.
    pub fn expire_leases(&self, now_unix_ms: u64) -> Result<Vec<OperationRecord>> {
        self.mutate_bulk(now_unix_ms, |snapshot| {
            let mut expired = Vec::new();
            for record in snapshot.records.values_mut() {
                if !record.state.is_terminal() && record.lease_expires_unix_ms <= now_unix_ms {
                    record.state = OperationState::Abandoned;
                    record.generation = next_generation(record)?;
                    record.updated_unix_ms = record.updated_unix_ms.max(now_unix_ms);
                    record.terminal_reason = Some("operation lease expired".to_owned());
                    expired.push(record.clone());
                }
            }
            Ok(expired)
        })
    }

    pub fn get(&self, operation_id: &str) -> Option<OperationRecord> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .snapshot
            .records
            .get(operation_id)
            .cloned()
    }

    pub fn records(&self) -> Vec<OperationRecord> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .snapshot
            .records
            .values()
            .cloned()
            .collect()
    }

    fn reconcile_startup(&self, now_unix_ms: u64) -> Result<StartupReconciliation> {
        self.mutate_bulk(now_unix_ms, |snapshot| {
            let mut result = StartupReconciliation::default();
            for record in snapshot.records.values_mut() {
                let reason = match record.state {
                    OperationState::Dispatching => {
                        Some("startup reconciliation found an ambiguous dispatch")
                    }
                    OperationState::Acknowledged if record.lease_expires_unix_ms <= now_unix_ms => {
                        Some("startup reconciliation found an expired acknowledgement")
                    }
                    OperationState::Acknowledged => {
                        let response_id = record.upstream_response_id.clone().ok_or_else(|| {
                            LedgerError::Corrupt(format!(
                                "acknowledged operation {:?} has no upstream response id",
                                record.operation_id
                            ))
                        })?;
                        result.same_account_recoveries.push(SameAccountRecovery {
                            operation_id: record.operation_id.clone(),
                            account_id: record.account_id.clone(),
                            upstream_response_id: response_id,
                            expected_generation: record.generation,
                            lease_expires_unix_ms: record.lease_expires_unix_ms,
                        });
                        None
                    }
                    OperationState::Completed
                    | OperationState::Failed
                    | OperationState::Abandoned => None,
                };
                if let Some(reason) = reason {
                    record.state = OperationState::Abandoned;
                    record.generation = next_generation(record)?;
                    record.updated_unix_ms = record.updated_unix_ms.max(now_unix_ms);
                    record.terminal_reason = Some(reason.to_owned());
                    result
                        .abandoned_operation_ids
                        .push(record.operation_id.clone());
                }
            }
            Ok(result)
        })
    }

    fn transition<F>(
        &self,
        operation_id: &str,
        expected_generation: u64,
        account_id: &str,
        target: OperationState,
        now_unix_ms: u64,
        update: F,
    ) -> Result<OperationRecord>
    where
        F: FnOnce(&mut OperationRecord),
    {
        validate_identifier("operation_id", operation_id)?;
        validate_identifier("account_id", account_id)?;
        self.mutate_record(operation_id, now_unix_ms, |record| {
            let mut record = record
                .cloned()
                .ok_or_else(|| LedgerError::NotFound(operation_id.to_owned()))?;
            if record.generation != expected_generation {
                return Err(LedgerError::Conflict {
                    operation_id: operation_id.to_owned(),
                    expected_generation,
                    actual_generation: record.generation,
                });
            }
            if record.account_id != account_id {
                return Err(LedgerError::WrongAccount {
                    operation_id: operation_id.to_owned(),
                    owner: record.account_id.clone(),
                    attempted: account_id.to_owned(),
                });
            }
            if !allowed_transition(record.state, target) {
                return Err(LedgerError::InvalidTransition {
                    operation_id: operation_id.to_owned(),
                    from: record.state,
                    to: target,
                });
            }
            record.state = target;
            record.generation = next_generation(&record)?;
            record.updated_unix_ms = record.updated_unix_ms.max(now_unix_ms);
            update(&mut record);
            Ok((record.clone(), record))
        })
    }

    fn mutate_record<T, F>(&self, operation_id: &str, now_unix_ms: u64, mutation: F) -> Result<T>
    where
        F: FnOnce(Option<&OperationRecord>) -> Result<(OperationRecord, T)>,
    {
        let (completed, result) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            check_poisoned(&state)?;
            let old = state.snapshot.records.get(operation_id).cloned();
            let (new, result) = mutation(old.as_ref())?;
            if new.operation_id != operation_id {
                return Err(LedgerError::Corrupt(
                    "operation mutation changed its identifier".to_owned(),
                ));
            }
            let old_bytes = old
                .as_ref()
                .map(|record| record_serialized_bytes(operation_id, record))
                .transpose()?
                .unwrap_or(0);
            let new_bytes = record_serialized_bytes(operation_id, &new)?;
            state.snapshot.records.insert(operation_id.to_owned(), new);
            state.record_bytes = state
                .record_bytes
                .saturating_sub(old_bytes)
                .saturating_add(new_bytes);
            advance_snapshot_generation(&mut state.snapshot)?;

            let removed = match prune_state_to_limits(&mut state, self.limits, now_unix_ms) {
                Ok(removed) => removed,
                Err(error) => {
                    if let Some(old) = old {
                        state.snapshot.records.insert(operation_id.to_owned(), old);
                    } else {
                        state.snapshot.records.remove(operation_id);
                    }
                    state.snapshot.snapshot_generation -= 1;
                    state.snapshot.wal_sequence -= 1;
                    state.record_bytes = records_serialized_bytes(&state.snapshot.records)?;
                    return Err(error);
                }
            };
            let mut changes: Vec<_> = removed
                .into_iter()
                .filter(|(id, _)| id != operation_id)
                .map(|(operation_id, _)| WalChange {
                    operation_id,
                    record: None,
                })
                .collect();
            changes.push(WalChange {
                operation_id: operation_id.to_owned(),
                record: state.snapshot.records.get(operation_id).cloned(),
            });
            let encoded = encode_wal_transaction(WalTransaction {
                version: SNAPSHOT_VERSION,
                sequence: state.snapshot.wal_sequence,
                changes,
            })?;
            let request = prepare_write(&mut state, self.limits, encoded);
            let completed = self.writer.enqueue(request)?;
            (completed, result)
        };
        await_durable(&self.state, completed)?;
        Ok(result)
    }

    fn mutate_bulk<T, F>(&self, now_unix_ms: u64, mutation: F) -> Result<T>
    where
        F: FnOnce(&mut Snapshot) -> Result<T>,
    {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        check_poisoned(&state)?;
        let mut candidate = state.snapshot.clone();
        let result = mutation(&mut candidate)?;
        prune_to_limits(&mut candidate, self.limits, now_unix_ms)?;
        if candidate.records == state.snapshot.records {
            return Ok(result);
        }
        advance_snapshot_generation(&mut candidate)?;
        validate_snapshot(&candidate)?;
        let transaction = WalTransaction {
            version: SNAPSHOT_VERSION,
            sequence: candidate.wal_sequence,
            changes: diff_records(&state.snapshot.records, &candidate.records),
        };
        let encoded = encode_wal_transaction(transaction)?;
        state.snapshot = candidate;
        state.record_bytes = records_serialized_bytes(&state.snapshot.records)?;
        let request = prepare_write(&mut state, self.limits, encoded);
        let completed = self.writer.enqueue(request)?;
        drop(state);
        await_durable(&self.state, completed)?;
        Ok(result)
    }
}

impl WalWriter {
    fn start(path: PathBuf) -> Result<Self> {
        let (sender, receiver) = mpsc::channel();
        #[cfg(test)]
        let stats = Arc::new(WalWriterStats::default());
        #[cfg(test)]
        let thread_stats = stats.clone();
        let thread = thread::Builder::new()
            .name("comradex-operation-wal".to_owned())
            .spawn(move || {
                wal_writer_loop(
                    path,
                    receiver,
                    #[cfg(test)]
                    thread_stats,
                )
            })?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            thread: Mutex::new(Some(thread)),
            #[cfg(test)]
            stats,
        })
    }

    fn enqueue(
        &self,
        prepared: PreparedWrite,
    ) -> Result<mpsc::Receiver<std::result::Result<(), String>>> {
        let (completed, receiver) = mpsc::sync_channel(1);
        self.sender
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .ok_or_else(|| LedgerError::Worker("operation WAL writer stopped".to_owned()))?
            .send(WalWrite {
                encoded: prepared.encoded,
                compact_snapshot: prepared.compact_snapshot,
                completed,
            })
            .map_err(|_| LedgerError::Worker("operation WAL writer stopped".to_owned()))?;
        Ok(receiver)
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        self.sender
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(handle) = self
            .thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

fn wal_writer_loop(
    path: PathBuf,
    receiver: mpsc::Receiver<WalWrite>,
    #[cfg(test)] stats: Arc<WalWriterStats>,
) {
    let mut poisoned: Option<String> = None;
    while let Ok(first) = receiver.recv() {
        if let Some(error) = &poisoned {
            let _ = first.completed.send(Err(error.clone()));
            continue;
        }
        let mut batch = vec![first];
        if batch[0].compact_snapshot.is_none()
            && let Ok(next) = receiver.recv_timeout(Duration::from_millis(2))
        {
            let compact = next.compact_snapshot.is_some();
            batch.push(next);
            while !compact
                && batch
                    .last()
                    .is_some_and(|entry| entry.compact_snapshot.is_none())
            {
                match receiver.try_recv() {
                    Ok(next) => {
                        let stop = next.compact_snapshot.is_some();
                        batch.push(next);
                        if stop {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        #[cfg(test)]
        {
            use std::sync::atomic::Ordering;
            stats.max_batch.fetch_max(batch.len(), Ordering::Relaxed);
        }

        let outcome = write_wal_batch(&path, &batch);
        #[cfg(test)]
        if outcome.is_ok() && batch.iter().any(|entry| !entry.encoded.is_empty()) {
            use std::sync::atomic::Ordering;
            stats.fsyncs.fetch_add(1, Ordering::Relaxed);
        }
        if let Err(error) = &outcome {
            poisoned = Some(error.clone());
        }
        for entry in batch {
            let _ = entry.completed.send(outcome.clone());
        }
    }
}

fn write_wal_batch(snapshot_path: &Path, batch: &[WalWrite]) -> std::result::Result<(), String> {
    let path = wal_path(snapshot_path);
    let has_appends = batch.iter().any(|entry| !entry.encoded.is_empty());
    if has_appends {
        append_wal_batch(&path, batch).map_err(|error| error.to_string())?;
    }
    if let Some(compaction) = batch.iter().find_map(|entry| {
        entry
            .compact_snapshot
            .as_ref()
            .map(|snapshot| (snapshot, entry.encoded.is_empty()))
    }) && let Err(error) =
        persist_snapshot(snapshot_path, compaction.0).and_then(|()| replace_with_empty_file(&path))
    {
        if compaction.1 {
            return Err(error.to_string());
        }
        tracing::warn!(%error, "operation WAL compaction deferred");
    }
    Ok(())
}

fn check_poisoned(state: &LedgerState) -> Result<()> {
    if let Some(message) = &state.poisoned {
        return Err(LedgerError::Corrupt(format!(
            "operation WAL is poisoned after a failed append: {message}"
        )));
    }
    Ok(())
}

fn advance_snapshot_generation(snapshot: &mut Snapshot) -> Result<()> {
    snapshot.snapshot_generation = snapshot
        .snapshot_generation
        .checked_add(1)
        .ok_or_else(|| LedgerError::Corrupt("snapshot generation overflow".to_owned()))?;
    snapshot.wal_sequence = snapshot
        .wal_sequence
        .checked_add(1)
        .ok_or_else(|| LedgerError::Corrupt("WAL sequence overflow".to_owned()))?;
    Ok(())
}

fn prepare_write(
    state: &mut LedgerState,
    limits: LedgerLimits,
    mut encoded: Vec<u8>,
) -> PreparedWrite {
    let projected_bytes = state.wal_bytes.saturating_add(encoded.len());
    let direct_compaction = encoded.len() > limits.max_bytes || projected_bytes > limits.max_bytes;
    if direct_compaction {
        encoded.clear();
    } else {
        state.wal_entries = state.wal_entries.saturating_add(1);
        state.wal_bytes = projected_bytes;
    }
    let compact_snapshot = if direct_compaction || should_compact(state, limits) {
        state.wal_entries = 0;
        state.wal_bytes = 0;
        Some(state.snapshot.clone())
    } else {
        None
    };
    PreparedWrite {
        encoded,
        compact_snapshot,
    }
}

fn await_durable(
    state: &Mutex<LedgerState>,
    completed: mpsc::Receiver<std::result::Result<(), String>>,
) -> Result<()> {
    match completed.recv() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .poisoned = Some(error.clone());
            Err(LedgerError::Worker(error))
        }
        Err(_) => {
            let error = "operation WAL writer stopped before durability confirmation".to_owned();
            state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .poisoned = Some(error.clone());
            Err(LedgerError::Worker(error))
        }
    }
}

struct LedgerLock {
    file: File,
}

impl LedgerLock {
    fn acquire(ledger_path: &Path) -> Result<Self> {
        let parent = ledger_parent(ledger_path)?;
        fs::create_dir_all(parent)?;
        let mut lock_name = ledger_path
            .file_name()
            .ok_or_else(|| {
                LedgerError::InvalidInput(format!("ledger path {:?} has no file name", ledger_path))
            })?
            .to_os_string();
        lock_name.push(".lock");
        let lock_path = parent.join(lock_name);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&lock_path)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    return Err(LedgerError::AlreadyOpen(ledger_path.to_owned()));
                }
                return Err(error.into());
            }
        }
        Ok(Self { file })
    }
}

impl Drop for LedgerLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            unsafe {
                libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

fn allowed_transition(from: OperationState, to: OperationState) -> bool {
    matches!(
        (from, to),
        (OperationState::Dispatching, OperationState::Acknowledged)
            | (OperationState::Dispatching, OperationState::Failed)
            | (OperationState::Dispatching, OperationState::Abandoned)
            | (OperationState::Acknowledged, OperationState::Acknowledged)
            | (OperationState::Acknowledged, OperationState::Completed)
            | (OperationState::Acknowledged, OperationState::Failed)
            | (OperationState::Acknowledged, OperationState::Abandoned)
    )
}

fn next_generation(record: &OperationRecord) -> Result<u64> {
    record.generation.checked_add(1).ok_or_else(|| {
        LedgerError::Corrupt(format!(
            "operation {:?} generation overflow",
            record.operation_id
        ))
    })
}

fn validate_limits(limits: LedgerLimits) -> Result<()> {
    if limits.max_records == 0 {
        return Err(LedgerError::InvalidInput(
            "max_records must be greater than zero".to_owned(),
        ));
    }
    if limits.terminal_retention_ms == 0 {
        return Err(LedgerError::InvalidInput(
            "terminal_retention_ms must be greater than zero".to_owned(),
        ));
    }
    let empty_size = serialize_snapshot(&Snapshot::empty())?.len();
    if limits.max_bytes < empty_size {
        return Err(LedgerError::InvalidInput(format!(
            "max_bytes must fit an empty snapshot ({empty_size} bytes)"
        )));
    }
    Ok(())
}

fn validate_identifier(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(LedgerError::InvalidInput(format!(
            "{name} must not be empty"
        )));
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(LedgerError::InvalidInput(format!(
            "{name} exceeds {MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_reason(reason: &str) -> Result<()> {
    if reason.is_empty() || reason.len() > MAX_REASON_BYTES {
        return Err(LedgerError::InvalidInput(format!(
            "reason must contain 1..={MAX_REASON_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_lease(now_unix_ms: u64, lease_expires_unix_ms: u64) -> Result<()> {
    if lease_expires_unix_ms <= now_unix_ms {
        return Err(LedgerError::InvalidInput(
            "lease expiry must be later than the mutation timestamp".to_owned(),
        ));
    }
    Ok(())
}

fn validate_snapshot(snapshot: &Snapshot) -> Result<()> {
    if snapshot.version != SNAPSHOT_VERSION {
        return Err(LedgerError::Corrupt(format!(
            "unsupported snapshot version {}",
            snapshot.version
        )));
    }
    if snapshot.snapshot_generation == u64::MAX || snapshot.wal_sequence == u64::MAX {
        return Err(LedgerError::Corrupt(
            "operation ledger generation overflow".to_owned(),
        ));
    }
    for (key, record) in &snapshot.records {
        validate_identifier("operation_id", &record.operation_id)
            .map_err(|error| LedgerError::Corrupt(error.to_string()))?;
        validate_identifier("account_id", &record.account_id)
            .map_err(|error| LedgerError::Corrupt(error.to_string()))?;
        if key != &record.operation_id {
            return Err(LedgerError::Corrupt(format!(
                "record key {key:?} does not match operation id {:?}",
                record.operation_id
            )));
        }
        if record.generation == 0 {
            return Err(LedgerError::Corrupt(format!(
                "operation {key:?} has generation zero"
            )));
        }
        if record.updated_unix_ms < record.created_unix_ms {
            return Err(LedgerError::Corrupt(format!(
                "operation {key:?} was updated before it was created"
            )));
        }
        if record.state == OperationState::Acknowledged
            && record
                .upstream_response_id
                .as_deref()
                .unwrap_or_default()
                .is_empty()
        {
            return Err(LedgerError::Corrupt(format!(
                "acknowledged operation {key:?} has no upstream response id"
            )));
        }
        if matches!(
            record.state,
            OperationState::Acknowledged | OperationState::Completed
        ) && record.upstream_response_id.is_none()
        {
            return Err(LedgerError::Corrupt(format!(
                "operation {key:?} reached {:?} without an upstream response id",
                record.state
            )));
        }
        if matches!(
            record.state,
            OperationState::Failed | OperationState::Abandoned
        ) && record.terminal_reason.is_none()
        {
            return Err(LedgerError::Corrupt(format!(
                "terminal operation {key:?} has no reason"
            )));
        }
        if !matches!(
            record.state,
            OperationState::Failed | OperationState::Abandoned
        ) && record.terminal_reason.is_some()
        {
            return Err(LedgerError::Corrupt(format!(
                "operation {key:?} has a terminal reason while in {:?}",
                record.state
            )));
        }
        if !record.state.is_terminal() && record.lease_expires_unix_ms <= record.updated_unix_ms {
            return Err(LedgerError::Corrupt(format!(
                "active operation {key:?} has an expired lease"
            )));
        }
        if let Some(response_id) = &record.upstream_response_id {
            validate_identifier("upstream_response_id", response_id)
                .map_err(|error| LedgerError::Corrupt(error.to_string()))?;
        }
        if let Some(reason) = &record.terminal_reason {
            validate_reason(reason).map_err(|error| LedgerError::Corrupt(error.to_string()))?;
        }
    }
    Ok(())
}

fn load_snapshot(path: &Path, max_bytes: usize) -> Result<Snapshot> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.len() > max_bytes as u64 => {
            return Err(LedgerError::Corrupt(format!(
                "snapshot is {} bytes, exceeding the configured {max_bytes}-byte bound",
                metadata.len()
            )));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Snapshot::empty());
        }
        Err(error) => return Err(error.into()),
    }
    let bytes = fs::read(path)?;
    if bytes.len() > max_bytes {
        return Err(LedgerError::Corrupt(format!(
            "snapshot is {} bytes, exceeding the configured {max_bytes}-byte bound",
            bytes.len()
        )));
    }
    serde_json::from_slice(&bytes).map_err(|error| LedgerError::Corrupt(error.to_string()))
}

fn wal_path(snapshot_path: &Path) -> PathBuf {
    let mut name = snapshot_path.file_name().unwrap_or_default().to_os_string();
    name.push(".wal");
    snapshot_path.with_file_name(name)
}

fn replay_wal(
    path: &Path,
    snapshot: &mut Snapshot,
    limits: LedgerLimits,
) -> Result<(usize, usize)> {
    let mut bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(error) => return Err(error.into()),
    };
    let valid_len = bytes
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    let had_torn_tail = valid_len != bytes.len();
    if valid_len > limits.max_bytes {
        return Err(LedgerError::Corrupt(format!(
            "operation WAL is {} bytes, exceeding the configured {}-byte bound",
            valid_len, limits.max_bytes
        )));
    }
    if had_torn_tail {
        bytes.truncate(valid_len);
    }
    let mut entries = 0usize;
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let envelope: WalEnvelope = serde_json::from_slice(line).map_err(|error| {
            LedgerError::Corrupt(format!("operation WAL entry {}: {error}", index + 1))
        })?;
        verify_wal_checksum(&envelope)?;
        let transaction = envelope.transaction;
        if transaction.version != SNAPSHOT_VERSION {
            return Err(LedgerError::Corrupt(format!(
                "unsupported operation WAL version {}",
                transaction.version
            )));
        }
        if transaction.sequence <= snapshot.wal_sequence {
            continue;
        }
        let expected = snapshot
            .wal_sequence
            .checked_add(1)
            .ok_or_else(|| LedgerError::Corrupt("WAL sequence overflow".to_owned()))?;
        if transaction.sequence != expected {
            return Err(LedgerError::Corrupt(format!(
                "operation WAL sequence gap: expected {expected}, found {}",
                transaction.sequence
            )));
        }
        for change in transaction.changes {
            match change.record {
                Some(record) => {
                    if record.operation_id != change.operation_id {
                        return Err(LedgerError::Corrupt(
                            "operation WAL record id mismatch".to_owned(),
                        ));
                    }
                    snapshot.records.insert(change.operation_id, record);
                }
                None => {
                    snapshot.records.remove(&change.operation_id);
                }
            }
        }
        snapshot.wal_sequence = transaction.sequence;
        snapshot.snapshot_generation = snapshot
            .snapshot_generation
            .checked_add(1)
            .ok_or_else(|| LedgerError::Corrupt("snapshot generation overflow".to_owned()))?;
        entries = entries.saturating_add(1);
    }
    if had_torn_tail {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(valid_len as u64)?;
        file.sync_all()?;
    }
    Ok((entries, bytes.len()))
}

fn diff_records(
    before: &BTreeMap<String, OperationRecord>,
    after: &BTreeMap<String, OperationRecord>,
) -> Vec<WalChange> {
    let mut changes = Vec::new();
    for (id, record) in after {
        if before.get(id) != Some(record) {
            changes.push(WalChange {
                operation_id: id.clone(),
                record: Some(record.clone()),
            });
        }
    }
    for id in before.keys() {
        if !after.contains_key(id) {
            changes.push(WalChange {
                operation_id: id.clone(),
                record: None,
            });
        }
    }
    changes
}

fn encode_wal_transaction(transaction: WalTransaction) -> Result<Vec<u8>> {
    let payload = serde_json::to_vec(&transaction)
        .map_err(|error| LedgerError::Corrupt(error.to_string()))?;
    let envelope = WalEnvelope {
        checksum: blake3::hash(&payload).to_hex().to_string(),
        transaction,
    };
    let mut encoded =
        serde_json::to_vec(&envelope).map_err(|error| LedgerError::Corrupt(error.to_string()))?;
    encoded.push(b'\n');
    Ok(encoded)
}

fn verify_wal_checksum(envelope: &WalEnvelope) -> Result<()> {
    let payload = serde_json::to_vec(&envelope.transaction)
        .map_err(|error| LedgerError::Corrupt(error.to_string()))?;
    let actual = blake3::hash(&payload).to_hex().to_string();
    if actual != envelope.checksum {
        return Err(LedgerError::Corrupt(
            "operation WAL checksum mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn append_wal_batch(path: &Path, batch: &[WalWrite]) -> Result<()> {
    let parent = ledger_parent(path)?;
    fs::create_dir_all(parent)?;
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    for entry in batch {
        if !entry.encoded.is_empty() {
            file.write_all(&entry.encoded)?;
        }
    }
    file.sync_all()?;
    Ok(())
}

fn should_compact(state: &LedgerState, limits: LedgerLimits) -> bool {
    let byte_threshold = (limits.max_bytes / 4).clamp(16 * 1024, 1024 * 1024);
    state.wal_entries >= 256 || state.wal_bytes >= byte_threshold
}

fn replace_with_empty_file(path: &Path) -> Result<()> {
    let parent = ledger_parent(path)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    temp.as_file_mut().sync_all()?;
    temp.persist(path).map_err(|error| error.error)?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn prune_to_limits(snapshot: &mut Snapshot, limits: LedgerLimits, now_unix_ms: u64) -> Result<()> {
    let retention_cutoff = now_unix_ms.saturating_sub(limits.terminal_retention_ms);
    snapshot.records.retain(|_, record| {
        !record.state.is_terminal() || record.updated_unix_ms > retention_cutoff
    });

    while snapshot.records.len() > limits.max_records {
        if !remove_oldest_terminal(snapshot) {
            return Err(capacity(limits));
        }
    }

    loop {
        let bytes = serialize_snapshot(snapshot)?;
        if bytes.len() <= limits.max_bytes {
            return Ok(());
        }
        if !remove_oldest_terminal(snapshot) {
            return Err(capacity(limits));
        }
    }
}

fn record_serialized_bytes(operation_id: &str, record: &OperationRecord) -> Result<usize> {
    let key = serde_json::to_vec(operation_id)
        .map_err(|error| LedgerError::Corrupt(error.to_string()))?;
    let value =
        serde_json::to_vec(record).map_err(|error| LedgerError::Corrupt(error.to_string()))?;
    Ok(key.len().saturating_add(1).saturating_add(value.len()))
}

fn records_serialized_bytes(records: &BTreeMap<String, OperationRecord>) -> Result<usize> {
    records.iter().try_fold(0usize, |total, (id, record)| {
        Ok(total.saturating_add(record_serialized_bytes(id, record)?))
    })
}

fn estimated_snapshot_bytes(snapshot: &Snapshot, record_bytes: usize) -> Result<usize> {
    let mut empty = snapshot.clone();
    empty.records.clear();
    let base = serialize_snapshot(&empty)?.len();
    Ok(base
        .saturating_sub(2)
        .saturating_add(record_bytes)
        .saturating_add(snapshot.records.len().saturating_sub(1))
        .saturating_add(2))
}

fn prune_state_to_limits(
    state: &mut LedgerState,
    limits: LedgerLimits,
    now_unix_ms: u64,
) -> Result<Vec<(String, OperationRecord)>> {
    let mut removed = Vec::new();
    let over_limit = |state: &LedgerState| -> Result<bool> {
        Ok(state.snapshot.records.len() > limits.max_records
            || estimated_snapshot_bytes(&state.snapshot, state.record_bytes)? > limits.max_bytes)
    };
    if !over_limit(state)? {
        return Ok(removed);
    }

    let retention_cutoff = now_unix_ms.saturating_sub(limits.terminal_retention_ms);
    let expired: Vec<_> = state
        .snapshot
        .records
        .iter()
        .filter(|(_, record)| {
            record.state.is_terminal() && record.updated_unix_ms <= retention_cutoff
        })
        .map(|(id, _)| id.clone())
        .collect();
    for id in expired {
        if let Some(record) = state.snapshot.records.remove(&id) {
            state.record_bytes = state
                .record_bytes
                .saturating_sub(record_serialized_bytes(&id, &record)?);
            removed.push((id, record));
        }
    }
    while over_limit(state)? {
        let oldest = state
            .snapshot
            .records
            .iter()
            .filter(|(_, record)| record.state.is_terminal())
            .min_by_key(|(_, record)| (record.updated_unix_ms, record.created_unix_ms))
            .map(|(id, _)| id.clone());
        let Some(id) = oldest else {
            for (id, record) in removed.drain(..) {
                state.record_bytes = state
                    .record_bytes
                    .saturating_add(record_serialized_bytes(&id, &record)?);
                state.snapshot.records.insert(id, record);
            }
            return Err(capacity(limits));
        };
        let record = state.snapshot.records.remove(&id).expect("record exists");
        state.record_bytes = state
            .record_bytes
            .saturating_sub(record_serialized_bytes(&id, &record)?);
        removed.push((id, record));
    }
    Ok(removed)
}

fn remove_oldest_terminal(snapshot: &mut Snapshot) -> bool {
    let oldest = snapshot
        .records
        .iter()
        .filter(|(_, record)| record.state.is_terminal())
        .min_by_key(|(_, record)| (record.updated_unix_ms, record.created_unix_ms))
        .map(|(id, _)| id.clone());
    oldest.and_then(|id| snapshot.records.remove(&id)).is_some()
}

fn capacity(limits: LedgerLimits) -> LedgerError {
    LedgerError::Capacity {
        max_records: limits.max_records,
        max_bytes: limits.max_bytes,
    }
}

fn serialize_snapshot(snapshot: &Snapshot) -> Result<Vec<u8>> {
    serde_json::to_vec(snapshot).map_err(|error| LedgerError::Corrupt(error.to_string()))
}

fn persist_snapshot(path: &Path, snapshot: &Snapshot) -> Result<()> {
    let parent = ledger_parent(path)?;
    fs::create_dir_all(parent)?;
    let bytes = serialize_snapshot(snapshot)?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    temp.write_all(&bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|error| error.error)?;
    #[cfg(unix)]
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn ledger_parent(path: &Path) -> Result<&Path> {
    if path.file_name().is_none() {
        return Err(LedgerError::InvalidInput(format!(
            "ledger path {:?} has no file name",
            path
        )));
    }
    Ok(path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new(".")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(max_records: usize) -> LedgerLimits {
        LedgerLimits {
            max_records,
            max_bytes: 64 * 1024,
            terminal_retention_ms: 1_000,
        }
    }

    fn open_at(path: &Path, now: u64) -> (OperationLedger, StartupReconciliation) {
        OperationLedger::open(path, limits(8), now).unwrap()
    }

    #[test]
    fn lifecycle_is_durable_and_generation_checked() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, reconciliation) = open_at(&path, 100);
        assert_eq!(reconciliation, StartupReconciliation::default());

        let dispatch = ledger
            .begin_dispatch("op-1", "account-a", 100, 1_000)
            .unwrap();
        assert_eq!(dispatch.generation, 1);
        assert_eq!(dispatch.state, OperationState::Dispatching);

        let acknowledged = ledger
            .acknowledge("op-1", 1, "account-a", "resp-1", 110, 2_000)
            .unwrap();
        assert_eq!(acknowledged.generation, 2);
        assert_eq!(acknowledged.state, OperationState::Acknowledged);

        assert!(matches!(
            ledger.complete("op-1", 1, "account-a", 120),
            Err(LedgerError::Conflict {
                expected_generation: 1,
                actual_generation: 2,
                ..
            })
        ));

        let completed = ledger.complete("op-1", 2, "account-a", 120).unwrap();
        assert_eq!(completed.state, OperationState::Completed);
        assert_eq!(completed.generation, 3);
        drop(ledger);

        let (reopened, reconciliation) = open_at(&path, 121);
        assert_eq!(reconciliation, StartupReconciliation::default());
        assert_eq!(reopened.get("op-1"), Some(completed));
    }

    #[test]
    fn startup_abandons_ambiguous_dispatch_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger
            .begin_dispatch("ambiguous", "account-a", 100, 10_000)
            .unwrap();
        drop(ledger);

        let (reopened, reconciliation) = open_at(&path, 200);
        assert_eq!(reconciliation.abandoned_operation_ids, vec!["ambiguous"]);
        assert!(reconciliation.same_account_recoveries.is_empty());
        let record = reopened.get("ambiguous").unwrap();
        assert_eq!(record.state, OperationState::Abandoned);
        assert_eq!(record.generation, 2);
        assert!(
            record
                .terminal_reason
                .unwrap()
                .contains("ambiguous dispatch")
        );
    }

    #[test]
    fn startup_exposes_only_same_account_acknowledged_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger
            .begin_dispatch("recoverable", "account-a", 100, 2_000)
            .unwrap();
        ledger
            .acknowledge("recoverable", 1, "account-a", "resp-1", 110, 2_000)
            .unwrap();
        drop(ledger);

        let (reopened, reconciliation) = open_at(&path, 200);
        assert!(reconciliation.abandoned_operation_ids.is_empty());
        assert_eq!(
            reconciliation.same_account_recoveries,
            vec![SameAccountRecovery {
                operation_id: "recoverable".to_owned(),
                account_id: "account-a".to_owned(),
                upstream_response_id: "resp-1".to_owned(),
                expected_generation: 2,
                lease_expires_unix_ms: 2_000,
            }]
        );
        assert_eq!(
            reopened.get("recoverable").unwrap().state,
            OperationState::Acknowledged
        );
    }

    #[test]
    fn startup_abandons_expired_acknowledgement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger
            .begin_dispatch("expired", "account-a", 100, 150)
            .unwrap();
        ledger
            .acknowledge("expired", 1, "account-a", "resp-1", 110, 150)
            .unwrap();
        drop(ledger);

        let (reopened, reconciliation) = open_at(&path, 200);
        assert_eq!(reconciliation.abandoned_operation_ids, vec!["expired"]);
        assert!(reconciliation.same_account_recoveries.is_empty());
        assert_eq!(
            reopened.get("expired").unwrap().state,
            OperationState::Abandoned
        );
    }

    #[test]
    fn account_ownership_is_enforced_for_every_transition() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger
            .begin_dispatch("op-1", "account-a", 100, 1_000)
            .unwrap();

        assert!(matches!(
            ledger.acknowledge("op-1", 1, "account-b", "resp-1", 110, 1_000),
            Err(LedgerError::WrongAccount { owner, attempted, .. })
                if owner == "account-a" && attempted == "account-b"
        ));
        assert_eq!(ledger.get("op-1").unwrap().generation, 1);
    }

    #[test]
    fn transport_loss_is_terminal_and_cannot_be_replayed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger
            .begin_dispatch("op-1", "account-a", 100, 1_000)
            .unwrap();
        let abandoned = ledger
            .abandon_after_transport_loss(
                "op-1",
                1,
                "account-a",
                110,
                "eventless upstream disconnect",
            )
            .unwrap();
        assert_eq!(abandoned.state, OperationState::Abandoned);
        assert!(matches!(
            ledger.acknowledge("op-1", 2, "account-a", "resp-1", 120, 1_000),
            Err(LedgerError::InvalidTransition { .. })
        ));
    }

    #[test]
    fn lease_expiry_abandons_dispatching_and_acknowledged_records() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger.begin_dispatch("a", "acct", 100, 150).unwrap();
        ledger.begin_dispatch("b", "acct", 100, 150).unwrap();
        ledger
            .acknowledge("b", 1, "acct", "resp-b", 110, 150)
            .unwrap();
        ledger.begin_dispatch("c", "acct", 100, 500).unwrap();

        let expired = ledger.expire_leases(150).unwrap();
        assert_eq!(expired.len(), 2);
        assert!(
            expired
                .iter()
                .all(|record| record.state == OperationState::Abandoned)
        );
        assert_eq!(ledger.get("c").unwrap().state, OperationState::Dispatching);
    }

    #[test]
    fn active_capacity_exhaustion_rolls_back_memory_and_disk() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = OperationLedger::open(&path, limits(1), 100).unwrap();
        ledger.begin_dispatch("a", "acct", 100, 1_000).unwrap();
        assert!(matches!(
            ledger.begin_dispatch("b", "acct", 110, 1_000),
            Err(LedgerError::Capacity { .. })
        ));
        assert!(ledger.get("b").is_none());
        drop(ledger);

        let (reopened, _) = OperationLedger::open(&path, limits(1), 120).unwrap();
        assert!(reopened.get("b").is_none());
    }

    #[test]
    fn old_terminal_records_are_pruned_before_rejecting_new_work() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = OperationLedger::open(&path, limits(1), 100).unwrap();
        ledger.begin_dispatch("old", "acct", 100, 500).unwrap();
        ledger.fail("old", 1, "acct", 110, "known failure").unwrap();
        ledger.begin_dispatch("new", "acct", 2_000, 3_000).unwrap();
        assert!(ledger.get("old").is_none());
        assert!(ledger.get("new").is_some());
    }

    #[test]
    fn byte_bound_rejects_an_active_record_that_cannot_fit() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let empty_size = serialize_snapshot(&Snapshot::empty()).unwrap().len();
        let tiny = LedgerLimits {
            max_records: 8,
            max_bytes: empty_size + 8,
            terminal_retention_ms: 1_000,
        };
        let (ledger, _) = OperationLedger::open(&path, tiny, 100).unwrap();
        assert!(matches!(
            ledger.begin_dispatch("a", "acct", 100, 1_000),
            Err(LedgerError::Capacity { .. })
        ));
        assert!(ledger.records().is_empty());
    }

    #[test]
    fn malformed_snapshot_fails_closed_instead_of_being_replaced() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        fs::write(&path, b"not json").unwrap();
        assert!(matches!(
            OperationLedger::open(&path, limits(8), 100),
            Err(LedgerError::Corrupt(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), b"not json");
    }

    #[cfg(unix)]
    #[test]
    fn a_second_writer_is_rejected_until_the_ledger_closes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (first, _) = open_at(&path, 100);
        assert!(matches!(
            OperationLedger::open(&path, limits(8), 100),
            Err(LedgerError::AlreadyOpen(open_path)) if open_path == path
        ));
        drop(first);
        assert!(OperationLedger::open(&path, limits(8), 100).is_ok());
    }

    #[test]
    fn generation_overflow_fails_closed_without_replacing_the_snapshot() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let snapshot = Snapshot {
            version: SNAPSHOT_VERSION,
            snapshot_generation: u64::MAX,
            wal_sequence: 0,
            records: BTreeMap::new(),
        };
        persist_snapshot(&path, &snapshot).unwrap();
        let original = fs::read(&path).unwrap();
        assert!(matches!(
            OperationLedger::open(&path, limits(8), 100),
            Err(LedgerError::Corrupt(message)) if message.contains("generation overflow")
        ));
        assert_eq!(fs::read(path).unwrap(), original);
    }

    #[test]
    fn invalid_transition_does_not_change_persisted_generation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger.begin_dispatch("op", "acct", 100, 1_000).unwrap();
        assert!(matches!(
            ledger.complete("op", 1, "acct", 110),
            Err(LedgerError::InvalidTransition {
                from: OperationState::Dispatching,
                to: OperationState::Completed,
                ..
            })
        ));
        assert_eq!(ledger.get("op").unwrap().generation, 1);
    }

    #[test]
    fn id_bearing_terminal_can_atomically_complete_before_created() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger.begin_dispatch("op", "acct", 100, 1_000).unwrap();

        let completed = ledger
            .complete_from_terminal("op", 1, "acct", "resp-terminal", 110)
            .unwrap();
        assert_eq!(completed.state, OperationState::Completed);
        assert_eq!(completed.generation, 2);
        assert_eq!(
            completed.upstream_response_id.as_deref(),
            Some("resp-terminal")
        );
        drop(ledger);

        let (reopened, reconciliation) = open_at(&path, 120);
        assert_eq!(reconciliation, StartupReconciliation::default());
        assert_eq!(reopened.get("op"), Some(completed));
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_is_private_on_unix() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger.begin_dispatch("op", "acct", 100, 1_000).unwrap();
        assert_eq!(
            fs::metadata(wal_path(&path)).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn wal_recovers_records_and_repairs_a_torn_tail() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger.begin_dispatch("op", "acct", 100, 1_000).unwrap();
        ledger
            .acknowledge("op", 1, "acct", "resp", 110, 1_000)
            .unwrap();
        drop(ledger);

        let (reopened, recovery) = open_at(&path, 120);
        assert_eq!(recovery.same_account_recoveries.len(), 1);
        assert_eq!(
            reopened.get("op").unwrap().state,
            OperationState::Acknowledged
        );
        drop(reopened);

        let wal = wal_path(&path);
        OpenOptions::new()
            .append(true)
            .open(&wal)
            .unwrap()
            .write_all(b"{\"torn\":")
            .unwrap();
        let torn_len = fs::metadata(&wal).unwrap().len();
        let (repaired, _) = OperationLedger::open(&path, limits(8), 130).unwrap();
        assert!(fs::metadata(&wal).unwrap().len() < torn_len);
        assert!(fs::read(&wal).unwrap().ends_with(b"\n"));
        assert!(repaired.get("op").is_some());
    }

    #[test]
    fn wal_rejects_corrupt_complete_entry() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        ledger.begin_dispatch("op", "acct", 100, 1_000).unwrap();
        drop(ledger);
        let wal = wal_path(&path);
        let mut bytes = fs::read(&wal).unwrap();
        let marker = b"\"checksum\":\"";
        let position = bytes
            .windows(marker.len())
            .position(|window| window == marker)
            .unwrap()
            + marker.len();
        bytes[position] = if bytes[position] == b'0' { b'1' } else { b'0' };
        fs::write(&wal, bytes).unwrap();
        assert!(matches!(
            OperationLedger::open(&path, limits(8), 130),
            Err(LedgerError::Corrupt(message)) if message.contains("checksum")
        ));
    }

    #[test]
    fn concurrent_mutations_are_group_committed() {
        use std::sync::{Barrier, atomic::Ordering};

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let (ledger, _) = open_at(&path, 100);
        let ledger = Arc::new(ledger);
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();
        for index in 0..8 {
            let ledger = ledger.clone();
            let barrier = barrier.clone();
            threads.push(thread::spawn(move || {
                barrier.wait();
                ledger
                    .begin_dispatch(format!("op-{index}"), "acct", 100, 1_000)
                    .unwrap();
            }));
        }
        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }
        assert!(ledger.writer.stats.max_batch.load(Ordering::Relaxed) > 1);
        assert!(ledger.writer.stats.fsyncs.load(Ordering::Relaxed) < 8);
    }

    #[test]
    fn wal_compaction_is_bounded_and_recoverable() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("operations.json");
        let limits = LedgerLimits {
            max_records: 300,
            max_bytes: 128 * 1024,
            terminal_retention_ms: 60_000,
        };
        let (ledger, _) = OperationLedger::open(&path, limits, 100).unwrap();
        for index in 0..270 {
            let id = format!("op-{index}");
            ledger.begin_dispatch(&id, "acct", 100, 10_000).unwrap();
            ledger.fail(&id, 1, "acct", 110, "done").unwrap();
        }
        assert!(
            fs::metadata(&path).is_ok(),
            "compaction must create a snapshot"
        );
        assert!(fs::metadata(wal_path(&path)).unwrap().len() < limits.max_bytes as u64);
        drop(ledger);

        let (reopened, _) = OperationLedger::open(&path, limits, 120).unwrap();
        assert_eq!(reopened.records().len(), 270);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)] // The test deliberately stalls each blocking transition before canceling its waiter.
    async fn canceled_waiters_cannot_leave_handle_mutations_nonterminal() {
        async fn wait_for_state(ledger: &AsyncOperationLedger, id: &str, state: OperationState) {
            for _ in 0..200 {
                if ledger
                    .get(id.to_owned())
                    .await
                    .unwrap()
                    .is_some_and(|record| record.state == state)
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
            panic!("operation {id} did not reach {state:?}");
        }

        let directory = tempfile::tempdir().unwrap();
        let ledger = AsyncOperationLedger::open_fail_closed(
            directory.path().join("operations.json"),
            limits(16),
            100,
        )
        .await
        .unwrap();

        let begin = ledger.track("begin", "acct");
        let guard = ledger.inner.state.lock().unwrap();
        let task_handle = begin.clone();
        let task = tokio::spawn(async move { task_handle.begin(100, 1_000).await });
        tokio::task::yield_now().await;
        task.abort();
        drop(guard);
        while begin.snapshot().is_none() {
            tokio::task::yield_now().await;
        }
        drop(begin);
        wait_for_state(&ledger, "begin", OperationState::Abandoned).await;

        let ack = ledger.track("ack", "acct");
        ack.begin(100, 1_000).await.unwrap();
        let guard = ledger.inner.state.lock().unwrap();
        let task_handle = ack.clone();
        let task = tokio::spawn(async move { task_handle.acknowledge("resp", 110, 1_000).await });
        tokio::task::yield_now().await;
        task.abort();
        drop(guard);
        while ack.snapshot().unwrap().state == OperationState::Dispatching {
            tokio::task::yield_now().await;
        }
        drop(ack);
        wait_for_state(&ledger, "ack", OperationState::Abandoned).await;

        let complete = ledger.track("complete", "acct");
        complete.begin(100, 1_000).await.unwrap();
        complete.acknowledge("resp-c", 110, 1_000).await.unwrap();
        let guard = ledger.inner.state.lock().unwrap();
        let task_handle = complete.clone();
        let task = tokio::spawn(async move { task_handle.complete(120).await });
        tokio::task::yield_now().await;
        task.abort();
        drop(guard);
        wait_for_state(&ledger, "complete", OperationState::Completed).await;

        let fail = ledger.track("fail", "acct");
        fail.begin(100, 1_000).await.unwrap();
        let guard = ledger.inner.state.lock().unwrap();
        let task_handle = fail.clone();
        let task = tokio::spawn(async move { task_handle.fail(120, "failed").await });
        tokio::task::yield_now().await;
        task.abort();
        drop(guard);
        wait_for_state(&ledger, "fail", OperationState::Failed).await;
    }
}
