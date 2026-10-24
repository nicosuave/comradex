//! Shared plumbing for the daemon's embedded SQLite stores.
//!
//! Both the affinity store and the context store are long-lived local databases opened once
//! at startup, written from a single serialized worker, and holding state the daemon can
//! rebuild. They were written independently and drifted: the affinity store recovers from a
//! database it cannot use, and the context store refused to start forever instead. Keeping
//! the open/discard/retain primitives here is what stops them diverging again.

use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use rusqlite::Connection;
use tracing::{error, warn};

/// Sidecar files SQLite keeps beside the database in WAL mode. Any operation that moves or
/// removes a database has to take these with it, or the next open reads a stale WAL.
pub(crate) fn sidecar_paths(path: &Path) -> [PathBuf; 3] {
    [
        path.to_path_buf(),
        path_with_suffix(path, "-wal"),
        path_with_suffix(path, "-shm"),
    ]
}

pub(crate) fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    value.into()
}

/// Remove a database that was never completely created, with its sidecars. Failures are
/// logged rather than propagated: the caller is on a recovery path and a leftover file must
/// not be what keeps the daemon from starting.
pub(crate) fn discard_database(path: &Path, what: &str) {
    for path in sidecar_paths(path) {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                error!(%error, path = %path.display(), "failed to remove incomplete {what} database")
            }
        }
    }
}

/// Move a database aside instead of deleting it, so a daemon that cannot interpret its
/// contents still starts and the old file stays available for inspection or rollback.
///
/// Used where discarding could lose state the daemon did not write itself, such as a
/// version it does not recognise after a downgrade.
pub(crate) fn quarantine_database(path: &Path, what: &str) -> Result<PathBuf> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_secs())
        .unwrap_or_default();
    let quarantined = path_with_suffix(path, &format!(".unsupported-{stamp}"));
    for source in sidecar_paths(path) {
        if !source.exists() {
            continue;
        }
        let suffix = source
            .as_os_str()
            .to_string_lossy()
            .strip_prefix(&*path.as_os_str().to_string_lossy())
            .unwrap_or_default()
            .to_owned();
        let target = path_with_suffix(&quarantined, &suffix);
        fs::rename(&source, &target).with_context(|| {
            format!(
                "move aside unsupported {what} database {}",
                source.display()
            )
        })?;
    }
    warn!(
        path = %quarantined.display(),
        "moved aside an unsupported {what} database and started with an empty one"
    );
    Ok(quarantined)
}

/// The pragma set both stores open with. `synchronous` differs by store: affinity bindings
/// are a routing cache that can be rebuilt, while context participants are needed to fan a
/// session's history back out, so that store pays for full durability.
pub(crate) fn configure(connection: &Connection, synchronous: &str) -> Result<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.pragma_update(None, "journal_mode", "wal")?;
    connection.pragma_update(None, "synchronous", synchronous)?;
    connection.pragma_update(None, "foreign_keys", true)?;
    Ok(())
}

/// Read `user_version`, which both stores use to decide whether they understand the schema.
pub(crate) fn user_version(connection: &Connection) -> Result<i64> {
    connection
        .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
        .context("read database user_version")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sidecars_cover_the_wal_pair_and_quarantine_moves_them_together() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.sqlite3");
        let [main, wal, shm] = sidecar_paths(&path);
        assert_eq!(main, path);
        assert!(wal.ends_with("store.sqlite3-wal"));
        assert!(shm.ends_with("store.sqlite3-shm"));

        for file in [&main, &wal, &shm] {
            fs::write(file, b"x").unwrap();
        }
        let quarantined = quarantine_database(&path, "test").unwrap();
        // Nothing is left behind to be read as a live database, and the WAL travelled with
        // its database rather than being stranded beside a fresh one.
        assert!(!main.exists() && !wal.exists() && !shm.exists());
        assert!(quarantined.exists());
        assert!(path_with_suffix(&quarantined, "-wal").exists());
        assert!(path_with_suffix(&quarantined, "-shm").exists());

        // Discarding is tolerant of files that are already gone.
        discard_database(&path, "test");
        discard_database(&path, "test");
    }
}
