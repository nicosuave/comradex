//! Operator session placement is separate from response/file ownership. Each edit
//! gets a new placement key so an older stream cannot restore the old placement.

use super::*;
use crate::{config::Config, routing::Router};
use anyhow::{bail, ensure};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SessionAction {
    Show,
    Move { account: String },
    Reset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionStatus {
    pub pool: String,
    pub provider: String,
    pub account: Option<String>,
    pub assigned_account: Option<String>,
    pub overridden: bool,
    pub last_seen: Option<u64>,
}

#[derive(Clone)]
struct Record {
    key: String,
    revision: i64,
    account: Option<String>,
    previous: Option<String>,
    assigned: Option<String>,
    overridden: bool,
    last_seen: u64,
}

/// Snapshot taken at admission. Observations from this request can update only
/// that revision; a later operator edit always wins over an in-flight request.
#[derive(Clone, Default)]
pub(crate) struct SessionRoute {
    records: Vec<Record>,
    pub placement: Option<ThreadKey>,
    pub assigned: Option<String>,
    pub previous: Option<String>,
}

impl SessionRoute {
    pub fn overridden(&self) -> bool {
        self.placement.is_some()
    }

    pub fn check_owner(&self, owner: Option<&str>) -> Result<()> {
        if let Some((assigned, owner)) = self.assigned.as_deref().zip(owner) {
            ensure!(
                assigned == owner,
                "session is assigned to {assigned}, but this request requires {owner}; start a fresh request with portable history or reset the session assignment"
            );
        }
        Ok(())
    }

    pub async fn observe(&self, store: &AffinityStore, account: &str) -> Result<()> {
        self.record(store, account, true).await
    }

    pub async fn observe_connection(&self, store: &AffinityStore, account: &str) -> Result<()> {
        self.record(store, account, false).await
    }

    /// Preserve pre-feature affinity evidence before a transport handshake can
    /// rebind those aliases. It is a conservative fallback, never a hard owner.
    pub async fn remember_previous(
        &mut self,
        store: &AffinityStore,
        account: String,
    ) -> Result<()> {
        let records = self.records.clone();
        let previous = account.clone();
        store.database.call(move |connection| {
            let transaction = connection.transaction()?;
            for record in records {
                transaction.execute(
                    "update session_routes set previous_account = ?1 where key = ?2 and revision = ?3 and previous_account is null",
                    params![previous, record.key, record.revision],
                )?;
            }
            transaction.commit()?;
            Ok(())
        }).await?;
        self.previous = Some(account);
        Ok(())
    }

    async fn record(
        &self,
        store: &AffinityStore,
        account: &str,
        content_dispatch: bool,
    ) -> Result<()> {
        let records = self.records.clone();
        if records.is_empty() {
            return Ok(());
        }
        let account = account.to_owned();
        store.database.call(move |connection| {
            let transaction = connection.transaction()?;
            for record in records {
                transaction.execute(
                    "update session_routes set account = ?1,
                         multiple_accounts = multiple_accounts or (?5 and previous_account is not null and previous_account != ?1),
                         previous_account = case when ?5 then ?1 else previous_account end,
                         last_seen = ?2 where key = ?3 and revision = ?4",
                    params![account, to_sql_u64(now(), "session last_seen")?, record.key, record.revision, content_dispatch],
                )?;
            }
            transaction.commit()?;
            Ok(())
        }).await
    }
}

pub(super) fn create_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "create table if not exists session_routes (
            key text primary key,
            revision integer not null default 0,
            account text,
            previous_account text,
            multiple_accounts integer not null default 0,
            assigned_account text,
            overridden integer not null default 0,
            last_seen integer not null
        );
        create index if not exists session_routes_last_seen on session_routes(last_seen);",
    )?;
    Ok(())
}

fn read_record(connection: &Connection, key: &str) -> Result<Option<Record>> {
    let record = connection.query_row(
        "select revision, account, assigned_account, overridden, last_seen, previous_account, multiple_accounts from session_routes where key = ?1",
        [key],
        |row| Ok(Record {
            key: key.to_owned(),
            revision: row.get(0)?,
            account: row.get(1)?,
            previous: row.get(5)?,
            assigned: row.get(2)?,
            overridden: row.get(3)?,
            last_seen: row.get::<_, i64>(4)?.max(0) as u64,
        }),
    ).optional()?;
    Ok(record)
}

impl AffinityStore {
    fn session_record_key(&self, provider: &str, pool: &str, id: &str) -> String {
        // Length-delimited components prevent a pool name from changing scope.
        self.key(&format!(
            "session-route:{}:{provider}:{}:{pool}:{id}",
            provider.len(),
            pool.len()
        ))
        .hash
    }

    /// IDs are ordered from the most specific thread to shared session/cache
    /// cohorts. A per-thread edit must not move an unrelated sibling thread.
    pub(crate) async fn session_route(
        &self,
        provider: &str,
        pool: &str,
        ids: &[String],
    ) -> Result<SessionRoute> {
        let mut keys = Vec::new();
        for id in ids {
            if valid_id(id) {
                let key = self.session_record_key(provider, pool, id);
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
        }
        if keys.is_empty() {
            return Ok(SessionRoute::default());
        }
        let records = self
            .database
            .call(move |connection| {
                let transaction = connection.transaction()?;
                let mut records = Vec::new();
                for key in keys {
                    transaction.execute(
                        "insert or ignore into session_routes (key, last_seen) values (?1, ?2)",
                        params![key, to_sql_u64(now(), "session last_seen")?],
                    )?;
                    records.push(
                        read_record(&transaction, &key)?.context("session record disappeared")?,
                    );
                }
                transaction.commit()?;
                Ok(records)
            })
            .await?;
        let directive = records.iter().find(|record| record.overridden);
        let previous = records.iter().find(|record| record.previous.is_some());
        Ok(SessionRoute {
            assigned: directive.and_then(|record| record.assigned.clone()),
            placement: directive.map(|record| {
                self.key(&format!(
                    "session-placement:{}:{}",
                    record.key, record.revision
                ))
            }),
            previous: previous.and_then(|record| record.previous.clone()),
            records,
        })
    }

    async fn session_record(&self, provider: &str, pool: &str, id: &str) -> Result<Option<Record>> {
        let key = self.session_record_key(provider, pool, id);
        self.database
            .call(move |connection| read_record(connection, &key))
            .await
    }

    async fn edit_session(
        &self,
        provider: &str,
        pool: &str,
        id: &str,
        assigned: Option<String>,
    ) -> Result<Record> {
        let key = self.session_record_key(provider, pool, id);
        self.database.call(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "insert into session_routes (key, revision, account, assigned_account, overridden, last_seen)
                 values (?1, 1, null, ?2, 1, ?3)
                 on conflict(key) do update set revision = revision + 1, account = null,
                     assigned_account = excluded.assigned_account, overridden = 1, last_seen = excluded.last_seen",
                params![key, assigned, to_sql_u64(now(), "session last_seen")?],
            )?;
            let record = read_record(&transaction, &key)?.context("edited session disappeared")?;
            transaction.commit()?;
            Ok(record)
        }).await
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 512 && !id.chars().any(char::is_whitespace)
}

fn provider(config: &Config, pool: &str) -> Result<&'static str> {
    let pool = config.pools.get(pool).context("unknown pool")?;
    let account = pool
        .members
        .first()
        .and_then(|name| config.accounts.get(name))
        .context("pool has no accounts")?;
    Ok(if account.is_claude() {
        "claude"
    } else {
        "codex"
    })
}

async fn legacy_account(
    store: &AffinityStore,
    provider: &str,
    pool: &str,
    id: &str,
) -> Option<String> {
    let keys = if provider == "claude" {
        vec![format!("claude:{pool}:{id}")]
    } else {
        vec![
            format!("prompt-cache:{id}"),
            format!("session:{id}"),
            format!("thread:{id}"),
        ]
    };
    for key in keys {
        if let Some(binding) = store.get(&store.key(&key)).await {
            return Some(binding.account_id);
        }
    }
    None
}

pub async fn manage_session(
    config: &Config,
    router: &Router,
    session_id: &str,
    pool: Option<&str>,
    action: SessionAction,
) -> Result<SessionStatus> {
    ensure!(
        valid_id(session_id),
        "session ID must be 1–512 bytes without whitespace"
    );
    if let Some(pool) = pool {
        ensure!(config.pools.contains_key(pool), "unknown pool {pool}");
    }
    let mut candidates = Vec::new();
    for name in config
        .pools
        .keys()
        .filter(|name| pool.is_none_or(|pool| pool == name.as_str()))
    {
        let provider = provider(config, name)?;
        let record = router
            .affinity
            .session_record(provider, name, session_id)
            .await?;
        let legacy = if record.is_none() {
            legacy_account(&router.affinity, provider, name, session_id)
                .await
                .filter(|account| config.pools[name].members.contains(account))
        } else {
            None
        };
        if pool.is_some() || record.is_some() || legacy.is_some() {
            candidates.push((name.clone(), provider, record, legacy));
        }
    }
    ensure!(
        !candidates.is_empty(),
        "unknown session; specify --pool to assign or reset a session before its next request"
    );
    ensure!(
        candidates.len() == 1,
        "session exists in several pools; specify --pool"
    );
    let (pool, provider, mut record, legacy) = candidates.pop().expect("one candidate");
    match action {
        SessionAction::Show => {}
        SessionAction::Move { account } => {
            let target = config
                .accounts
                .get(&account)
                .with_context(|| format!("unknown account {account}"))?;
            ensure!(
                target.is_claude() == (provider == "claude"),
                "account {account} belongs to a different provider"
            );
            ensure!(
                config.pools[&pool].members.contains(&account),
                "account {account} is not a member of pool {pool}"
            );
            if router
                .select_exact(&config.pools[&pool], &account)
                .await
                .is_none()
            {
                bail!("account {account} is unavailable");
            }
            record = Some(
                router
                    .affinity
                    .edit_session(provider, &pool, session_id, Some(account))
                    .await?,
            );
        }
        SessionAction::Reset => {
            record = Some(
                router
                    .affinity
                    .edit_session(provider, &pool, session_id, None)
                    .await?,
            );
        }
    }
    Ok(SessionStatus {
        pool,
        provider: provider.to_owned(),
        account: record
            .as_ref()
            .and_then(|record| record.account.clone())
            .or_else(|| {
                if record.as_ref().is_some_and(|record| record.overridden) {
                    None
                } else {
                    legacy
                }
            }),
        assigned_account: record.as_ref().and_then(|record| record.assigned.clone()),
        overridden: record.as_ref().is_some_and(|record| record.overridden),
        last_seen: record.map(|record| record.last_seen),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        toml::from_str(
            r#"
            [listeners.default]
            address = "127.0.0.1:0"
            pool = "default"
            [pools.default]
            members = ["a", "b"]
            [pools.claude]
            members = ["c", "d"]
            [accounts.a]
            kind = "inbound"
            [accounts.b]
            kind = "inbound"
            [accounts.c]
            kind = "claude_inbound"
            [accounts.d]
            kind = "claude_inbound"
        "#,
        )
        .unwrap()
    }

    fn store(dir: &Path) -> Arc<AffinityStore> {
        Arc::new(
            AffinityStore::load(
                dir.join("affinity.json"),
                "session-test-key",
                Duration::from_secs(3600),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn session_edits_survive_restart_and_old_requests_cannot_restore_placement() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let affinity = store(dir.path());
        let router = Router::new(&config, affinity.clone());
        let ids = vec!["private-session-id".to_owned()];
        let old_request = affinity
            .session_route("codex", "default", &ids)
            .await
            .unwrap();
        old_request.observe(&affinity, "a").await.unwrap();
        let shown = manage_session(&config, &router, &ids[0], None, SessionAction::Show)
            .await
            .unwrap();
        assert_eq!(shown.account.as_deref(), Some("a"));
        assert!(!shown.overridden);

        manage_session(
            &config,
            &router,
            &ids[0],
            None,
            SessionAction::Move {
                account: "b".into(),
            },
        )
        .await
        .unwrap();
        old_request.observe(&affinity, "a").await.unwrap();
        let moved = affinity
            .session_route("codex", "default", &ids)
            .await
            .unwrap();
        assert_eq!(moved.assigned.as_deref(), Some("b"));
        assert_eq!(
            moved.previous.as_deref(),
            Some("a"),
            "retain the owner of opaque context"
        );
        assert!(moved.check_owner(Some("a")).is_err());
        moved.observe(&affinity, "b").await.unwrap();

        let reset = manage_session(&config, &router, &ids[0], None, SessionAction::Reset)
            .await
            .unwrap();
        assert!(reset.overridden && reset.assigned_account.is_none() && reset.account.is_none());
        let reset_route = affinity
            .session_route("codex", "default", &ids)
            .await
            .unwrap();
        assert_ne!(reset_route.placement, moved.placement);
        moved.observe(&affinity, "b").await.unwrap();
        assert!(
            manage_session(&config, &router, &ids[0], None, SessionAction::Show)
                .await
                .unwrap()
                .account
                .is_none()
        );
        drop(router);
        drop(affinity);

        let restored = store(dir.path());
        let route = restored
            .session_route("codex", "default", &ids)
            .await
            .unwrap();
        assert_eq!(route.placement, reset_route.placement);
        assert_eq!(route.previous.as_deref(), Some("b"));
        assert!(route.assigned.is_none());
        // Raw IDs never enter persisted session records or placement bindings.
        for suffix in ["", "-wal"] {
            if let Ok(bytes) = fs::read(dir.path().join(format!("affinity.sqlite3{suffix}"))) {
                assert!(
                    !bytes
                        .windows(ids[0].len())
                        .any(|window| window == ids[0].as_bytes())
                );
            }
        }
    }

    #[tokio::test]
    async fn session_controls_are_scoped_and_validate_targets_without_changing_policy() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let affinity = store(dir.path());
        let router = Router::new(&config, affinity.clone());
        let ids = vec!["shared-id".to_owned()];
        for (provider, pool, account) in [("codex", "default", "a"), ("claude", "claude", "c")] {
            affinity
                .session_route(provider, pool, &ids)
                .await
                .unwrap()
                .observe(&affinity, account)
                .await
                .unwrap();
        }
        let ambiguous = manage_session(&config, &router, &ids[0], None, SessionAction::Show)
            .await
            .unwrap_err();
        assert!(ambiguous.to_string().contains("several pools"));
        for account in ["missing", "c"] {
            assert!(
                manage_session(
                    &config,
                    &router,
                    &ids[0],
                    Some("default"),
                    SessionAction::Move {
                        account: account.into()
                    }
                )
                .await
                .is_err()
            );
        }
        assert!(router.begin_login("b").await);
        assert!(
            manage_session(
                &config,
                &router,
                &ids[0],
                Some("default"),
                SessionAction::Move {
                    account: "b".into()
                }
            )
            .await
            .is_err()
        );
        let shown = manage_session(
            &config,
            &router,
            &ids[0],
            Some("default"),
            SessionAction::Show,
        )
        .await
        .unwrap();
        assert_eq!(shown.account.as_deref(), Some("a"));
        assert!(!shown.overridden);
        let claude = manage_session(
            &config,
            &router,
            &ids[0],
            Some("claude"),
            SessionAction::Move {
                account: "d".into(),
            },
        )
        .await
        .unwrap();
        assert_eq!(claude.provider, "claude");
        assert_eq!(claude.assigned_account.as_deref(), Some("d"));
        assert!(
            affinity
                .session_route("codex", "default", &ids)
                .await
                .unwrap()
                .assigned
                .is_none()
        );
    }

    #[tokio::test]
    async fn thread_edits_take_precedence_without_changing_sibling_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let config = config();
        let affinity = store(dir.path());
        let router = Router::new(&config, affinity.clone());
        manage_session(
            &config,
            &router,
            "tree",
            Some("default"),
            SessionAction::Move {
                account: "a".into(),
            },
        )
        .await
        .unwrap();
        manage_session(
            &config,
            &router,
            "child",
            Some("default"),
            SessionAction::Move {
                account: "b".into(),
            },
        )
        .await
        .unwrap();
        let child = affinity
            .session_route("codex", "default", &["child".into(), "tree".into()])
            .await
            .unwrap();
        let sibling = affinity
            .session_route("codex", "default", &["sibling".into(), "tree".into()])
            .await
            .unwrap();
        assert_eq!(child.assigned.as_deref(), Some("b"));
        assert_eq!(sibling.assigned.as_deref(), Some("a"));
    }
}
