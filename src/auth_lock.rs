use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

const LOCK_FILE_NAME: &str = ".comradex-auth.lock";

/// A process-wide advisory lock for a managed CODEX_HOME credential file.
///
/// Every Comradex credential writer uses this lock. The official `codex login`
/// process does not know about it, so the wrapper keeps this guard alive for
/// the child's entire lifetime.
#[derive(Debug)]
pub struct HomeAuthLock {
    file: File,
    path: PathBuf,
}

impl HomeAuthLock {
    /// Wait synchronously until this managed home is exclusively owned.
    pub fn acquire(home: &Path) -> Result<Self> {
        Self::open_and_lock(home, false)?.context("blocking auth lock unexpectedly unavailable")
    }

    /// Wait for the file lock without blocking a Tokio worker thread. Polling a
    /// nonblocking kernel lock also makes cancellation immediate: no detached
    /// blocking task can acquire the home after its caller has timed out.
    pub async fn acquire_async(home: &Path) -> Result<Self> {
        loop {
            if let Some(lock) = Self::try_acquire(home)? {
                return Ok(lock);
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    /// Attempt to own this managed home without waiting.
    pub fn try_acquire(home: &Path) -> Result<Option<Self>> {
        Self::open_and_lock(home, true)
    }

    fn open_and_lock(home: &Path, nonblocking: bool) -> Result<Option<Self>> {
        fs::create_dir_all(home)
            .with_context(|| format!("create managed codex_home {}", home.display()))?;
        let path = home.join(LOCK_FILE_NAME);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options
            .open(&path)
            .with_context(|| format!("open managed auth lock {}", path.display()))?;
        match lock_file(&file, nonblocking) {
            Ok(true) => Ok(Some(Self { file, path })),
            Ok(false) => Ok(None),
            Err(error) => {
                Err(error).with_context(|| format!("lock managed credentials {}", home.display()))
            }
        }
    }
}

impl Drop for HomeAuthLock {
    fn drop(&mut self) {
        if let Err(error) = unlock_file(&self.file) {
            tracing::warn!(
                path = %self.path.display(),
                %error,
                "failed to explicitly release managed auth lock"
            );
        }
    }
}

#[cfg(unix)]
fn lock_file(file: &File, nonblocking: bool) -> io::Result<bool> {
    use std::os::fd::AsRawFd;

    let operation = libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 };
    loop {
        if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        if nonblocking && error.kind() == io::ErrorKind::WouldBlock {
            return Ok(false);
        }
        return Err(error);
    }
}

#[cfg(unix)]
fn unlock_file(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(unix))]
fn lock_file(_file: &File, _nonblocking: bool) -> io::Result<bool> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "managed auth locking is currently supported on Unix only",
    ))
}

#[cfg(not(unix))]
fn unlock_file(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separately_opened_lock_is_exclusive_and_reusable() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("account");
        let first = HomeAuthLock::acquire(&home).unwrap();

        assert!(HomeAuthLock::try_acquire(&home).unwrap().is_none());
        drop(first);
        assert!(HomeAuthLock::try_acquire(&home).unwrap().is_some());
    }

    #[test]
    fn lock_file_is_private_on_unix() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("account");
        let _guard = HomeAuthLock::acquire(&home).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(home.join(LOCK_FILE_NAME))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
