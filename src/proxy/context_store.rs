use std::{
    fs,
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    time::Duration,
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use tokio::sync::oneshot;

const DATABASE_VERSION: i64 = 1;
const MAX_PARTICIPANTS: i64 = 32;

type Job = Box<dyn FnOnce(&mut Connection) + Send + 'static>;

struct DatabaseWorker {
    sender: mpsc::Sender<Job>,
}

impl DatabaseWorker {
    fn start(path: PathBuf) -> Result<Self> {
        let (sender, receiver) = mpsc::channel::<Job>();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("comradex-context-db".into())
            .spawn(move || {
                let mut connection = match open_database(&path) {
                    Ok(connection) => connection,
                    Err(error) => {
                        let _ = ready_sender.send(Err(error));
                        return;
                    }
                };
                if ready_sender.send(Ok(())).is_err() {
                    return;
                }
                for job in receiver {
                    job(&mut connection);
                }
            })
            .context("spawn context database worker")?;
        ready_receiver
            .recv()
            .context("context database worker stopped during startup")??;
        Ok(Self { sender })
    }

    async fn call<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (completed, result) = oneshot::channel();
        self.sender
            .send(Box::new(move |connection| {
                let _ = completed.send(operation(connection));
            }))
            .map_err(|_| anyhow::anyhow!("context database worker stopped"))?;
        result
            .await
            .context("context database worker dropped response")?
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextAccount {
    pub alias: String,
    pub physical_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSession {
    pub owner: ContextAccount,
    pub participants: Vec<ContextAccount>,
}

#[derive(Clone)]
pub struct ContextStore {
    database: Arc<DatabaseWorker>,
    key: [u8; 32],
}

impl ContextStore {
    pub fn open(path: &Path, key: &str) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create context state directory {}", parent.display()))?;
        }
        Ok(Self {
            database: Arc::new(DatabaseWorker::start(path.to_path_buf())?),
            key: *blake3::hash(key.as_bytes()).as_bytes(),
        })
    }

    pub fn physical_key(&self, physical_id: &str) -> String {
        hash_component(&self.key, b"physical-id", physical_id)
    }

    pub async fn record_dispatch(
        &self,
        scope: &str,
        session: &str,
        account_alias: &str,
        physical_id: &str,
    ) -> Result<()> {
        let scope_key = hash_component(&self.key, b"scope", scope);
        let session_key = hash_component(&self.key, b"session", session);
        let alias = account_alias.to_owned();
        let physical_key = self.physical_key(physical_id);
        self.database
            .call(move |connection| {
                let transaction =
                    connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let exists = transaction
                    .query_row(
                        "select 1 from context_sessions where scope_key = ?1 and session_key = ?2",
                        params![scope_key, session_key],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();

                if !exists {
                    transaction.execute(
                        "insert into context_sessions (scope_key, session_key, owner_alias, owner_physical_key) values (?1, ?2, ?3, ?4)",
                        params![scope_key, session_key, alias, physical_key],
                    )?;
                    transaction.execute(
                        "insert into context_participants (scope_key, session_key, ordinal, alias, physical_key) values (?1, ?2, 0, ?3, ?4)",
                        params![scope_key, session_key, alias, physical_key],
                    )?;
                    transaction.commit()?;
                    return Ok(());
                }

                let already_recorded = transaction
                    .query_row(
                        "select 1 from context_participants where scope_key = ?1 and session_key = ?2 and physical_key = ?3",
                        params![scope_key, session_key, physical_key],
                        |_| Ok(()),
                    )
                    .optional()?
                    .is_some();
                if !already_recorded {
                    let next_ordinal = transaction.query_row(
                        "select count(*) from context_participants where scope_key = ?1 and session_key = ?2",
                        params![scope_key, session_key],
                        |row| row.get::<_, i64>(0),
                    )?;
                    anyhow::ensure!(
                        next_ordinal < MAX_PARTICIPANTS,
                        "context participant limit of {MAX_PARTICIPANTS} reached"
                    );
                    transaction.execute(
                        "insert into context_participants (scope_key, session_key, ordinal, alias, physical_key) values (?1, ?2, ?3, ?4, ?5)",
                        params![scope_key, session_key, next_ordinal, alias, physical_key],
                    )?;
                }
                transaction.commit()?;
                Ok(())
            })
            .await
    }

    pub async fn lookup(&self, scope: &str, session: &str) -> Result<Option<ContextSession>> {
        let scope_key = hash_component(&self.key, b"scope", scope);
        let session_key = hash_component(&self.key, b"session", session);
        self.database
            .call(move |connection| {
                let owner = connection
                    .query_row(
                        "select owner_alias, owner_physical_key from context_sessions where scope_key = ?1 and session_key = ?2",
                        params![scope_key, session_key],
                        |row| {
                            Ok(ContextAccount {
                                alias: row.get(0)?,
                                physical_id: row.get(1)?,
                            })
                        },
                    )
                    .optional()?;
                let Some(owner) = owner else {
                    return Ok(None);
                };

                let mut statement = connection.prepare(
                    "select alias, physical_key from context_participants where scope_key = ?1 and session_key = ?2 order by ordinal",
                )?;
                let participants = statement
                    .query_map(params![scope_key, session_key], |row| {
                        Ok(ContextAccount {
                            alias: row.get(0)?,
                            physical_id: row.get(1)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(Some(ContextSession {
                    owner,
                    participants,
                }))
            })
            .await
    }
}

fn hash_component(key: &[u8; 32], domain: &[u8], raw: &str) -> String {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(b"comradex-context-store\0");
    hasher.update(domain);
    hasher.update(b"\0");
    hasher.update(raw.as_bytes());
    hasher.finalize().to_hex().to_string()
}

fn open_database(path: &Path) -> Result<Connection> {
    let existed = path.exists();
    let mut connection = Connection::open(path)
        .with_context(|| format!("open context database {}", path.display()))?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "journal_mode", "wal")?;
    connection.pragma_update(None, "synchronous", "full")?;
    connection.pragma_update(None, "foreign_keys", true)?;

    if existed {
        let version =
            connection.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?;
        anyhow::ensure!(
            version == DATABASE_VERSION,
            "unsupported context database version {version}"
        );
        return Ok(connection);
    }

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        "
        create table context_sessions (
            scope_key text not null,
            session_key text not null,
            owner_alias text not null,
            owner_physical_key text not null,
            primary key (scope_key, session_key)
        ) without rowid;
        create table context_participants (
            scope_key text not null,
            session_key text not null,
            ordinal integer not null check (ordinal >= 0 and ordinal < 32),
            alias text not null,
            physical_key text not null,
            primary key (scope_key, session_key, ordinal),
            unique (scope_key, session_key, physical_key),
            foreign key (scope_key, session_key)
                references context_sessions (scope_key, session_key)
                on delete cascade
        ) without rowid;
        pragma user_version = 1;
        ",
    )?;
    transaction.commit()?;
    connection.execute_batch("pragma wal_checkpoint(full);")?;
    connection
        .close()
        .map_err(|(_, error)| error)
        .context("close newly created context database")?;
    fs::File::open(path)
        .with_context(|| format!("open context database for sync {}", path.display()))?
        .sync_all()
        .with_context(|| format!("sync context database {}", path.display()))?;
    sync_parent_directory(path)?;

    let connection = Connection::open(path)
        .with_context(|| format!("reopen context database {}", path.display()))?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "journal_mode", "wal")?;
    connection.pragma_update(None, "synchronous", "full")?;
    connection.pragma_update(None, "foreign_keys", true)?;
    Ok(connection)
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<()> {
    fs::File::open(path.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    #[tokio::test]
    async fn dispatches_persist_without_storing_raw_context_identifiers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contexts.sqlite3");
        let expected_physical_key;
        {
            let store = ContextStore::open(&path, KEY).unwrap();
            expected_physical_key = store.physical_key("physical-secret-one");
            store
                .record_dispatch(
                    "scope-secret",
                    "session-secret",
                    "primary",
                    "physical-secret-one",
                )
                .await
                .unwrap();
            store
                .record_dispatch(
                    "scope-secret",
                    "session-secret",
                    "secondary",
                    "physical-secret-two",
                )
                .await
                .unwrap();
        }

        let restored = ContextStore::open(&path, KEY).unwrap();
        let context = restored
            .lookup("scope-secret", "session-secret")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.owner.alias, "primary");
        assert_eq!(context.owner.physical_id, expected_physical_key);
        assert_eq!(context.participants.len(), 2);
        let bytes = fs::read(path).unwrap();
        for raw in [
            "scope-secret",
            "session-secret",
            "physical-secret-one",
            "physical-secret-two",
        ] {
            assert!(
                !bytes
                    .windows(raw.len())
                    .any(|window| window == raw.as_bytes())
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_first_dispatch_selects_one_owner_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            Arc::new(ContextStore::open(&dir.path().join("contexts.sqlite3"), KEY).unwrap());
        let mut tasks = Vec::new();
        for index in 0..16 {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move {
                let alias = format!("account-{index}");
                let physical = format!("physical-{index}");
                store
                    .record_dispatch("scope", "session", &alias, &physical)
                    .await
                    .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        let context = store.lookup("scope", "session").await.unwrap().unwrap();
        assert_eq!(context.participants.len(), 16);
        assert!(context.participants.contains(&context.owner));
    }

    #[tokio::test]
    async fn duplicate_physical_account_retains_first_alias() {
        let dir = tempfile::tempdir().unwrap();
        let store = ContextStore::open(&dir.path().join("contexts.sqlite3"), KEY).unwrap();
        store
            .record_dispatch("scope", "session", "first-alias", "same-physical")
            .await
            .unwrap();
        store
            .record_dispatch("scope", "session", "renamed-alias", "same-physical")
            .await
            .unwrap();

        let context = store.lookup("scope", "session").await.unwrap().unwrap();
        assert_eq!(context.owner.alias, "first-alias");
        assert_eq!(context.participants.len(), 1);
        assert_eq!(context.participants[0].alias, "first-alias");
    }

    #[tokio::test]
    async fn same_alias_with_new_physical_id_records_a_distinct_participant() {
        let dir = tempfile::tempdir().unwrap();
        let store = ContextStore::open(&dir.path().join("contexts.sqlite3"), KEY).unwrap();
        store
            .record_dispatch("scope", "session", "shared-alias", "physical-one")
            .await
            .unwrap();
        store
            .record_dispatch("scope", "session", "shared-alias", "physical-two")
            .await
            .unwrap();

        let context = store.lookup("scope", "session").await.unwrap().unwrap();
        assert_eq!(context.owner.alias, "shared-alias");
        assert_eq!(
            context.owner.physical_id,
            store.physical_key("physical-one")
        );
        assert_eq!(context.participants.len(), 2);
        assert_ne!(
            context.participants[0].physical_id,
            context.participants[1].physical_id
        );
    }

    #[tokio::test]
    async fn participant_limit_rejects_dispatch_and_remains_durable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contexts.sqlite3");
        let store = ContextStore::open(&path, KEY).unwrap();
        for index in 0..32 {
            store
                .record_dispatch(
                    "scope",
                    "session",
                    &format!("account-{index}"),
                    &format!("physical-{index}"),
                )
                .await
                .unwrap();
        }
        let error = store
            .record_dispatch("scope", "session", "account-32", "physical-32")
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("participant limit of 32 reached"));
        drop(store);

        let restored = ContextStore::open(&path, KEY).unwrap();
        let context = restored.lookup("scope", "session").await.unwrap().unwrap();
        assert_eq!(context.owner.alias, "account-0");
        assert_eq!(context.participants.len(), 32);
        assert_eq!(context.participants.last().unwrap().alias, "account-31");
        assert!(
            !context
                .participants
                .iter()
                .any(|participant| participant.alias == "account-32")
        );
    }

    #[tokio::test]
    async fn database_errors_are_returned_and_failed_creation_rolls_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = ContextStore::open(&dir.path().join("contexts.sqlite3"), KEY).unwrap();
        store
            .record_dispatch("scope", "existing", "account", "physical")
            .await
            .unwrap();
        store
            .database
            .call(|connection| {
                connection.execute("drop table context_participants", [])?;
                Ok(())
            })
            .await
            .unwrap();

        assert!(store.lookup("scope", "existing").await.is_err());
        assert!(
            store
                .record_dispatch("scope", "failed", "other", "other-physical")
                .await
                .is_err()
        );
        let session_count = store
            .database
            .call(|connection| {
                Ok(
                    connection.query_row("select count(*) from context_sessions", [], |row| {
                        row.get::<_, i64>(0)
                    })?,
                )
            })
            .await
            .unwrap();
        assert_eq!(session_count, 1);
    }
}
