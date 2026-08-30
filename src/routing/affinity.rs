use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tracing::error;

const DATABASE_VERSION: i64 = 1;
const MAX_TOUCH_PERSIST_INTERVAL_SECONDS: u64 = 15 * 60;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ThreadKey(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Binding {
    pub account_id: String,
    pub last_seen: u64,
    pub account_generation: u64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct LegacySnapshot {
    version: u8,
    bindings: HashMap<String, Binding>,
    #[serde(default)]
    account_epochs: HashMap<String, u64>,
}

type Job = Box<dyn FnOnce(&mut Connection) + Send + 'static>;

struct DatabaseWorker {
    sender: mpsc::Sender<Job>,
    path: PathBuf,
}

impl DatabaseWorker {
    fn start(path: PathBuf, legacy_path: PathBuf, idle: Duration) -> Result<Self> {
        let (sender, receiver) = mpsc::channel::<Job>();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let worker_path = path.clone();
        std::thread::Builder::new()
            .name("comradex-affinity-db".into())
            .spawn(move || {
                let mut connection = match open_database(&worker_path, &legacy_path, idle) {
                    Ok(connection) => connection,
                    Err(error) => {
                        let _ = ready_sender.send(Err(error));
                        return;
                    }
                };
                if ready_sender.send(Ok(())).is_err() {
                    return;
                }
                let cleanup_interval = cleanup_interval(idle);
                let mut next_cleanup = Instant::now() + cleanup_interval;
                loop {
                    let wait = next_cleanup.saturating_duration_since(Instant::now());
                    match receiver.recv_timeout(wait) {
                        Ok(job) => job(&mut connection),
                        Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    let current = Instant::now();
                    if current >= next_cleanup {
                        if let Err(error) = expire_stale_bindings(&connection, idle) {
                            error!(%error, "periodic affinity expiration failed");
                        }
                        next_cleanup = current + cleanup_interval;
                    }
                }
            })
            .context("spawn affinity database worker")?;
        ready_receiver
            .recv()
            .context("affinity database worker stopped during startup")??;
        Ok(Self { sender, path })
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
            .map_err(|_| anyhow::anyhow!("affinity database worker stopped"))?;
        result
            .await
            .context("affinity database worker dropped response")?
    }
}

pub struct AffinityStore {
    database: Arc<DatabaseWorker>,
    key: [u8; 32],
    idle: Duration,
}

impl AffinityStore {
    pub fn load(legacy_path: PathBuf, key_text: &str, idle: Duration) -> Result<Self> {
        let database_path = legacy_path.with_extension("sqlite3");
        if let Some(parent) = database_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create affinity state directory {}", parent.display()))?;
        }
        Ok(Self {
            database: Arc::new(DatabaseWorker::start(database_path, legacy_path, idle)?),
            key: *blake3::hash(key_text.as_bytes()).as_bytes(),
            idle,
        })
    }

    pub fn key(&self, raw: &str) -> ThreadKey {
        ThreadKey(
            blake3::keyed_hash(&self.key, raw.as_bytes())
                .to_hex()
                .to_string(),
        )
    }

    pub async fn get(&self, key: &ThreadKey) -> Option<Binding> {
        let key = key.0.clone();
        let now = now();
        let idle_seconds = self.idle.as_secs();
        let touch_interval = idle_seconds
            .saturating_div(4)
            .clamp(1, MAX_TOUCH_PERSIST_INTERVAL_SECONDS);
        match self
            .database
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let binding = transaction
                    .query_row(
                        "select account_id, last_seen, account_generation from bindings where key = ?1",
                        [&key],
                        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
                    )
                    .optional()?;
                let Some((account_id, last_seen, account_generation)) = binding else {
                    transaction.commit()?;
                    return Ok(None);
                };
                let mut binding = Binding {
                    account_id,
                    last_seen: from_sql_u64(last_seen, "binding last_seen")?,
                    account_generation: from_sql_u64(
                        account_generation,
                        "binding account_generation",
                    )?,
                };
                if now.saturating_sub(binding.last_seen) > idle_seconds {
                    transaction.execute("delete from bindings where key = ?1", [&key])?;
                    transaction.commit()?;
                    return Ok(None);
                }
                if now.saturating_sub(binding.last_seen) >= touch_interval {
                    transaction.execute(
                        "update bindings set last_seen = ?2 where key = ?1",
                        params![key, to_sql_u64(now, "binding last_seen")?],
                    )?;
                    binding.last_seen = now;
                }
                transaction.commit()?;
                Ok(Some(binding))
            })
            .await
        {
            Ok(binding) => binding,
            Err(error) => {
                error!(%error, "affinity lookup failed");
                None
            }
        }
    }

    pub async fn put(&self, key: ThreadKey, account_id: String, generation: u64) -> bool {
        let now = now();
        match self
            .database
            .call(move |connection| {
                connection.execute(
                    "insert into bindings (key, account_id, last_seen, account_generation) values (?1, ?2, ?3, ?4) on conflict(key) do update set account_id = excluded.account_id, last_seen = excluded.last_seen, account_generation = excluded.account_generation",
                    params![
                        key.0,
                        account_id,
                        to_sql_u64(now, "binding last_seen")?,
                        to_sql_u64(generation, "binding account_generation")?
                    ],
                )?;
                Ok(())
            })
            .await
        {
            Ok(()) => true,
            Err(error) => {
                error!(%error, "affinity write failed");
                false
            }
        }
    }

    pub async fn account_epoch(&self, account: &str) -> u64 {
        let account = account.to_owned();
        match self
            .database
            .call(move |connection| {
                let generation = connection
                    .query_row(
                        "select generation from account_epochs where account_id = ?1",
                        [account],
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?
                    .unwrap_or(0);
                from_sql_u64(generation, "account generation")
            })
            .await
        {
            Ok(epoch) => epoch,
            Err(error) => {
                error!(%error, "affinity account epoch lookup failed");
                u64::MAX
            }
        }
    }

    pub async fn remove(&self, key: &ThreadKey) -> bool {
        let key = key.0.clone();
        match self
            .database
            .call(move |connection| {
                connection.execute("delete from bindings where key = ?1", [key])?;
                Ok(())
            })
            .await
        {
            Ok(()) => true,
            Err(error) => {
                error!(%error, "affinity removal failed");
                false
            }
        }
    }

    pub async fn invalidate_account(&self, account: &str) -> bool {
        let account = account.to_owned();
        let logged_account = account.clone();
        match self
            .database
            .call(move |connection| {
                let transaction = connection.transaction()?;
                transaction.execute("delete from bindings where account_id = ?1", [&account])?;
                transaction.execute(
                    "insert into account_epochs (account_id, generation) values (?1, 1) on conflict(account_id) do update set generation = generation + 1",
                    [&account],
                )?;
                transaction.commit()?;
                Ok(())
            })
            .await
        {
            Ok(()) => true,
            Err(error) => {
                error!(%error, account = logged_account, "affinity account invalidation failed");
                false
            }
        }
    }

    pub async fn len_and_bytes(&self) -> (usize, usize) {
        let entries = match self
            .database
            .call(|connection| {
                let entries = connection.query_row("select count(*) from bindings", [], |row| {
                    row.get::<_, i64>(0)
                })?;
                usize::try_from(entries).context("negative affinity binding count")
            })
            .await
        {
            Ok(entries) => entries,
            Err(error) => {
                error!(%error, "affinity statistics query failed");
                0
            }
        };
        let database_bytes = file_len(&self.database.path)
            .saturating_add(file_len(&self.database.path.with_extension("sqlite3-wal")));
        (entries, database_bytes)
    }
}

fn open_database(database_path: &Path, legacy_path: &Path, idle: Duration) -> Result<Connection> {
    if database_path.exists() {
        let connection = Connection::open(database_path)
            .with_context(|| format!("open affinity database {}", database_path.display()))?;
        anyhow::ensure!(
            legacy_migration_complete(&connection)?,
            "affinity database exists without a completed legacy migration marker"
        );
        configure_database(&connection)?;
        create_schema(&connection)?;
        expire_stale_bindings(&connection, idle)?;
        return Ok(connection);
    }

    let migration_path = sqlite_path_with_suffix(database_path, ".migrating");
    remove_incomplete_database(&migration_path);
    let migration = (|| {
        let mut connection = Connection::open(&migration_path).with_context(|| {
            format!(
                "open affinity migration database {}",
                migration_path.display()
            )
        })?;
        configure_migration_database(&connection)?;
        create_schema(&connection)?;
        migrate_legacy_snapshot(&mut connection, legacy_path, idle)?;
        expire_stale_bindings(&connection, idle)?;
        drop(connection);
        fs::rename(&migration_path, database_path).with_context(|| {
            format!(
                "publish affinity database {} as {}",
                migration_path.display(),
                database_path.display()
            )
        })?;
        sync_parent_directory(database_path)?;
        Ok(())
    })();
    if let Err(error) = migration {
        remove_incomplete_database(&migration_path);
        return Err(error);
    }

    let connection = Connection::open(database_path)
        .with_context(|| format!("open affinity database {}", database_path.display()))?;
    configure_database(&connection)?;
    Ok(connection)
}

fn legacy_migration_complete(connection: &Connection) -> Result<bool> {
    let has_metadata = connection.query_row(
        "select exists(select 1 from sqlite_master where type = 'table' and name = 'metadata')",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    if !has_metadata {
        return Ok(false);
    }
    Ok(connection
        .query_row(
            "select value from metadata where key = 'legacy_import_complete'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .is_some())
}

fn remove_incomplete_database(database_path: &Path) {
    for path in [
        database_path.to_path_buf(),
        sqlite_path_with_suffix(database_path, "-wal"),
        sqlite_path_with_suffix(database_path, "-shm"),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                error!(%error, path = %path.display(), "failed to remove incomplete affinity database")
            }
        }
    }
}

fn sqlite_path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_directory(_path: &Path) -> Result<()> {
    Ok(())
}

fn configure_migration_database(connection: &Connection) -> Result<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "synchronous", "full")?;
    connection.pragma_update(None, "foreign_keys", true)?;
    Ok(())
}

fn configure_database(connection: &Connection) -> Result<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "journal_mode", "wal")?;
    connection.pragma_update(None, "synchronous", "normal")?;
    connection.pragma_update(None, "foreign_keys", true)?;
    Ok(())
}

fn create_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "
        create table if not exists metadata (
            key text primary key,
            value text not null
        );
        create table if not exists bindings (
            key text primary key,
            account_id text not null,
            last_seen integer not null,
            account_generation integer not null
        );
        create index if not exists bindings_last_seen on bindings(last_seen);
        create index if not exists bindings_account on bindings(account_id);
        create table if not exists account_epochs (
            account_id text primary key,
            generation integer not null
        );
        ",
    )?;
    connection.pragma_update(None, "user_version", DATABASE_VERSION)?;
    Ok(())
}

fn migrate_legacy_snapshot(
    connection: &mut Connection,
    legacy_path: &Path,
    idle: Duration,
) -> Result<()> {
    let imported = connection
        .query_row(
            "select value from metadata where key = 'legacy_import_complete'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .is_some();
    if imported {
        return Ok(());
    }
    let snapshot = match fs::read(legacy_path) {
        Ok(bytes) => Some(
            serde_json::from_slice::<LegacySnapshot>(&bytes).with_context(|| {
                format!("parse legacy affinity snapshot {}", legacy_path.display())
            })?,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(error).with_context(|| {
                format!("read legacy affinity snapshot {}", legacy_path.display())
            });
        }
    };
    let cutoff = now().saturating_sub(idle.as_secs());
    let transaction = connection.transaction()?;
    let mut imported_bindings = 0usize;
    let mut imported_epochs = 0usize;
    if let Some(snapshot) = snapshot {
        let _legacy_version = snapshot.version;
        {
            let mut insert = transaction.prepare(
                "insert or replace into bindings (key, account_id, last_seen, account_generation) values (?1, ?2, ?3, ?4)",
            )?;
            for (key, binding) in snapshot.bindings {
                if binding.last_seen < cutoff {
                    continue;
                }
                insert.execute(params![
                    key,
                    binding.account_id,
                    to_sql_u64(binding.last_seen, "legacy binding last_seen")?,
                    to_sql_u64(
                        binding.account_generation,
                        "legacy binding account_generation",
                    )?
                ])?;
                imported_bindings += 1;
            }
        }
        {
            let mut insert = transaction.prepare(
                "insert or replace into account_epochs (account_id, generation) values (?1, ?2)",
            )?;
            for (account, generation) in snapshot.account_epochs {
                insert.execute(params![
                    account,
                    to_sql_u64(generation, "legacy account generation")?
                ])?;
                imported_epochs += 1;
            }
        }
    }
    let stored_bindings = transaction.query_row("select count(*) from bindings", [], |row| {
        row.get::<_, i64>(0)
    })?;
    let stored_bindings = usize::try_from(stored_bindings).context("negative binding count")?;
    anyhow::ensure!(
        stored_bindings == imported_bindings,
        "legacy affinity migration count mismatch: imported {imported_bindings}, stored {stored_bindings}"
    );
    let stored_epochs =
        transaction.query_row("select count(*) from account_epochs", [], |row| {
            row.get::<_, i64>(0)
        })?;
    let stored_epochs = usize::try_from(stored_epochs).context("negative account epoch count")?;
    anyhow::ensure!(
        stored_epochs == imported_epochs,
        "legacy account epoch migration count mismatch: imported {imported_epochs}, stored {stored_epochs}"
    );
    transaction.execute(
        "insert into metadata (key, value) values ('legacy_import_complete', '1')",
        [],
    )?;
    transaction.commit()?;
    Ok(())
}

fn expire_stale_bindings(connection: &Connection, idle: Duration) -> Result<()> {
    let cutoff = now().saturating_sub(idle.as_secs());
    connection.execute(
        "delete from bindings where last_seen < ?1",
        [to_sql_u64(cutoff, "binding expiration cutoff")?],
    )?;
    Ok(())
}

fn cleanup_interval(idle: Duration) -> Duration {
    Duration::from_secs(idle.as_secs().saturating_div(4).clamp(1, 60 * 60))
}

fn to_sql_u64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).with_context(|| format!("{field} exceeds SQLite integer range"))
}

fn from_sql_u64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).with_context(|| format!("{field} is negative"))
}

fn file_len(path: &Path) -> usize {
    fs::metadata(path)
        .ok()
        .and_then(|metadata| usize::try_from(metadata.len()).ok())
        .unwrap_or(0)
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "0123456789abcdef0123456789abcdef";

    fn legacy_binding(account: &str) -> Binding {
        Binding {
            account_id: account.into(),
            last_seen: now(),
            account_generation: 0,
        }
    }

    #[tokio::test]
    async fn migrates_legacy_snapshot_without_rewriting_it() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("affinity.json");
        let snapshot = LegacySnapshot {
            version: 0,
            bindings: HashMap::from([
                ("hashed-one".into(), legacy_binding("a")),
                ("hashed-two".into(), legacy_binding("b")),
            ]),
            account_epochs: HashMap::from([("a".into(), 3)]),
        };
        let legacy_bytes = serde_json::to_vec(&snapshot).unwrap();
        fs::write(&legacy_path, &legacy_bytes).unwrap();

        let store = AffinityStore::load(legacy_path.clone(), KEY, Duration::from_secs(60)).unwrap();
        assert_eq!(store.len_and_bytes().await.0, 2);
        assert_eq!(store.account_epoch("a").await, 3);
        assert_eq!(
            store
                .get(&ThreadKey("hashed-one".into()))
                .await
                .unwrap()
                .account_id,
            "a"
        );
        assert!(
            store
                .put(ThreadKey("hashed-three".into()), "c".into(), 0)
                .await
        );
        assert_eq!(fs::read(legacy_path).unwrap(), legacy_bytes);
    }

    #[test]
    fn existing_database_never_rereads_legacy_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("affinity.json");
        fs::write(
            &legacy_path,
            serde_json::to_vec(&LegacySnapshot::default()).unwrap(),
        )
        .unwrap();
        drop(AffinityStore::load(
            legacy_path.clone(),
            KEY,
            Duration::from_secs(60),
        ));
        fs::write(&legacy_path, b"now-malformed").unwrap();

        assert!(
            AffinityStore::load(legacy_path, KEY, Duration::from_secs(60)).is_ok(),
            "an existing completed database must not reread legacy JSON"
        );
    }

    #[test]
    fn unmarked_existing_database_fails_without_importing_legacy_state() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("affinity.json");
        fs::write(
            &legacy_path,
            serde_json::to_vec(&LegacySnapshot::default()).unwrap(),
        )
        .unwrap();
        let database_path = dir.path().join("affinity.sqlite3");
        drop(Connection::open(&database_path).unwrap());

        let error = AffinityStore::load(legacy_path, KEY, Duration::from_secs(60))
            .err()
            .expect("unmarked existing database must fail closed");
        assert!(format!("{error:#}").contains("without a completed legacy migration marker"));
        let connection = Connection::open(database_path).unwrap();
        assert!(!legacy_migration_complete(&connection).unwrap());
    }

    #[tokio::test]
    async fn interrupted_first_migration_is_retried_from_legacy_state() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("affinity.json");
        fs::write(
            &legacy_path,
            serde_json::to_vec(&LegacySnapshot {
                version: 0,
                bindings: HashMap::from([("expected".into(), legacy_binding("a"))]),
                account_epochs: HashMap::new(),
            })
            .unwrap(),
        )
        .unwrap();
        let database_path = dir.path().join("affinity.sqlite3");
        let migration_path = sqlite_path_with_suffix(&database_path, ".migrating");
        let connection = Connection::open(&migration_path).unwrap();
        configure_migration_database(&connection).unwrap();
        create_schema(&connection).unwrap();
        connection
            .execute(
                "insert into bindings (key, account_id, last_seen, account_generation) values ('partial', 'wrong', 0, 0)",
                [],
            )
            .unwrap();
        drop(connection);

        let store = AffinityStore::load(legacy_path, KEY, Duration::from_secs(60)).unwrap();
        assert_eq!(store.len_and_bytes().await.0, 1);
        assert!(store.get(&ThreadKey("partial".into())).await.is_none());
        assert_eq!(
            store
                .get(&ThreadKey("expected".into()))
                .await
                .unwrap()
                .account_id,
            "a"
        );
        assert!(!migration_path.exists());
    }

    #[tokio::test]
    async fn inserts_past_the_old_hundred_thousand_record_cap() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("affinity.json");
        let mut bindings = HashMap::with_capacity(100_000);
        for index in 0..100_000 {
            bindings.insert(format!("key-{index:06}"), legacy_binding("account"));
        }
        fs::write(
            &legacy_path,
            serde_json::to_vec(&LegacySnapshot {
                version: 0,
                bindings,
                account_epochs: HashMap::new(),
            })
            .unwrap(),
        )
        .unwrap();

        let store = AffinityStore::load(legacy_path, KEY, Duration::from_secs(60)).unwrap();
        assert_eq!(store.len_and_bytes().await.0, 100_000);
        assert!(
            store
                .put(store.key("record-100001"), "account".into(), 0)
                .await
        );
        assert_eq!(store.len_and_bytes().await.0, 100_001);
    }

    #[tokio::test]
    async fn individual_mutations_persist_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("affinity.json");
        let key;
        {
            let store =
                AffinityStore::load(legacy_path.clone(), KEY, Duration::from_secs(60)).unwrap();
            key = store.key("thread");
            assert!(store.put(key.clone(), "a".into(), 0).await);
        }
        let restored = AffinityStore::load(legacy_path, KEY, Duration::from_secs(60)).unwrap();
        assert_eq!(restored.get(&key).await.unwrap().account_id, "a");
    }

    #[tokio::test]
    async fn account_invalidation_is_atomic_and_durable() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("affinity.json");
        let store = AffinityStore::load(legacy_path.clone(), KEY, Duration::from_secs(60)).unwrap();
        let old = store.key("old");
        assert!(store.put(old.clone(), "a".into(), 0).await);
        assert!(store.invalidate_account("a").await);
        assert!(store.get(&old).await.is_none());
        assert_eq!(store.account_epoch("a").await, 1);
        drop(store);

        let restored = AffinityStore::load(legacy_path, KEY, Duration::from_secs(60)).unwrap();
        assert!(restored.get(&old).await.is_none());
        assert_eq!(restored.account_epoch("a").await, 1);
    }

    #[tokio::test]
    async fn stale_bindings_expire_while_store_remains_running() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("affinity.json");
        let store = AffinityStore::load(legacy_path, KEY, Duration::from_secs(1)).unwrap();
        let key = store.key("abandoned");
        assert!(store.put(key.clone(), "a".into(), 0).await);
        store
            .database
            .call(move |connection| {
                connection.execute("update bindings set last_seen = 0 where key = ?1", [key.0])?;
                Ok(())
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if store.len_and_bytes().await.0 == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("periodic cleanup did not expire the stale binding");
    }

    #[test]
    fn malformed_legacy_snapshot_is_left_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("affinity.json");
        let malformed = b"not-json";
        fs::write(&legacy_path, malformed).unwrap();
        let error = AffinityStore::load(legacy_path.clone(), KEY, Duration::from_secs(60))
            .err()
            .expect("malformed migration must fail");
        assert!(format!("{error:#}").contains("parse legacy affinity snapshot"));
        assert_eq!(fs::read(legacy_path).unwrap(), malformed);
        assert!(!dir.path().join("affinity.sqlite3").exists());
        assert!(!dir.path().join("affinity.sqlite3.migrating").exists());
    }

    #[test]
    fn expiry_query_uses_last_seen_index() {
        let connection = Connection::open_in_memory().unwrap();
        create_schema(&connection).unwrap();
        let detail: String = connection
            .query_row(
                "explain query plan delete from bindings where last_seen < ?1",
                [0],
                |row| row.get(3),
            )
            .unwrap();
        assert!(
            detail.contains("bindings_last_seen"),
            "query plan: {detail}"
        );
    }
}
