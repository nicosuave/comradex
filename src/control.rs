//! Authenticated local control channel for routing changes that must not restart the daemon.

use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::UnixStream as StdUnixStream,
    },
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader},
    net::{UnixListener, UnixStream},
    sync::Mutex,
};
use tracing::warn;

use crate::{
    accounts,
    config::{self, Config},
    routing::{Router, RoutingSnapshot},
};

const SOCKET_NAME: &str = "control.sock";
const MAX_MESSAGE_BYTES: usize = 16 * 1024;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Request {
    SetPreferred {
        secret: String,
        pool: String,
        account: Option<String>,
    },
    RoutingStatus {
        secret: String,
    },
}

impl Request {
    fn secret(&self) -> &str {
        match self {
            Self::SetPreferred { secret, .. } | Self::RoutingStatus { secret } => secret,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Response {
    ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    routing: Option<RoutingSnapshot>,
}

struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            warn!(path = %self.0.display(), %error, "failed to remove control socket");
        }
    }
}

pub struct ControlServer {
    listener: UnixListener,
    _guard: SocketGuard,
    config_path: PathBuf,
    config: Arc<Config>,
    router: Arc<Router>,
    edit_lock: Arc<Mutex<()>>,
}

pub fn socket_path(state_dir: &Path) -> PathBuf {
    state_dir.join(SOCKET_NAME)
}

impl ControlServer {
    pub fn bind(
        state_dir: &Path,
        config_path: PathBuf,
        config: Arc<Config>,
        router: Arc<Router>,
    ) -> Result<Self> {
        let path = socket_path(state_dir);
        remove_stale_socket(&path)?;
        let listener = UnixListener::bind(&path)
            .with_context(|| format!("bind control socket {}", path.display()))?;
        let guard = SocketGuard(path.clone());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("secure control socket {}", path.display()))?;
        Ok(Self {
            listener,
            _guard: guard,
            config_path,
            config,
            router,
            edit_lock: Arc::new(Mutex::new(())),
        })
    }

    pub async fn run(self) -> Result<()> {
        loop {
            let (stream, _) = self.listener.accept().await?;
            let config_path = self.config_path.clone();
            let config = self.config.clone();
            let router = self.router.clone();
            let edit_lock = self.edit_lock.clone();
            tokio::spawn(async move {
                if let Err(error) = handle(stream, config_path, config, router, edit_lock).await {
                    warn!(%error, "control request failed");
                }
            });
        }
    }
}

fn remove_stale_socket(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
    };
    if !metadata.file_type().is_socket() {
        bail!(
            "refusing to replace non-socket control path {}",
            path.display()
        )
    }
    if StdUnixStream::connect(path).is_ok() {
        bail!(
            "another Comradex daemon is already listening at {}",
            path.display()
        )
    }
    fs::remove_file(path).with_context(|| format!("remove stale socket {}", path.display()))
}

async fn handle(
    stream: UnixStream,
    config_path: PathBuf,
    config: Arc<Config>,
    router: Arc<Router>,
    edit_lock: Arc<Mutex<()>>,
) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = AsyncBufReader::new(reader).take((MAX_MESSAGE_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    let read = tokio::time::timeout(CLIENT_TIMEOUT, reader.read_until(b'\n', &mut bytes))
        .await
        .context("control request timed out")?
        .context("read control request")?;
    let response = if read == 0 || bytes.len() > MAX_MESSAGE_BYTES || !bytes.ends_with(b"\n") {
        Response {
            ok: false,
            error: Some("invalid control request framing".into()),
            routing: None,
        }
    } else {
        match serde_json::from_slice::<Request>(&bytes) {
            Ok(request) => process(request, &config_path, &config, &router, &edit_lock).await,
            Err(error) => Response {
                ok: false,
                error: Some(format!("invalid control request: {error}")),
                routing: None,
            },
        }
    };
    let mut encoded = serde_json::to_vec(&response)?;
    encoded.push(b'\n');
    writer.write_all(&encoded).await?;
    writer.shutdown().await?;
    Ok(())
}

async fn process(
    request: Request,
    config_path: &Path,
    config: &Config,
    router: &Router,
    edit_lock: &Mutex<()>,
) -> Response {
    if !secrets_equal(request.secret(), &config.proxy.installation_secret) {
        return Response {
            ok: false,
            error: Some("unauthorized".into()),
            routing: None,
        };
    }
    let result: Result<RoutingSnapshot> = async {
        match request {
            Request::RoutingStatus { .. } => {}
            Request::SetPreferred { pool, account, .. } => {
                let pool_config = config
                    .pools
                    .get(&pool)
                    .with_context(|| format!("unknown pool {pool}"))?;
                if let Some(account) = &account
                    && !pool_config.members.contains(account)
                {
                    bail!("account {account} is not a member of pool {pool}")
                }
                let _guard = edit_lock.lock().await;
                let text = fs::read_to_string(config_path)
                    .with_context(|| format!("read {}", config_path.display()))?;
                let updated = accounts::set_preferred_account(&text, &pool, account.as_deref())?;
                config::write_validated(config_path, &updated)?;
                router.set_preferred(&pool, account).await;
            }
        }
        Ok(router.routing_snapshot().await)
    }
    .await;
    match result {
        Ok(routing) => Response {
            ok: true,
            error: None,
            routing: Some(routing),
        },
        Err(error) => Response {
            ok: false,
            error: Some(format!("{error:#}")),
            routing: None,
        },
    }
}

fn secrets_equal(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    difference == 0
}

pub fn set_preferred(
    state_dir: &Path,
    secret: &str,
    pool: &str,
    account: Option<&str>,
) -> Result<RoutingSnapshot> {
    send(
        state_dir,
        &Request::SetPreferred {
            secret: secret.to_owned(),
            pool: pool.to_owned(),
            account: account.map(str::to_owned),
        },
    )
}

pub fn routing_status(state_dir: &Path, secret: &str) -> Result<RoutingSnapshot> {
    send(
        state_dir,
        &Request::RoutingStatus {
            secret: secret.to_owned(),
        },
    )
}

fn send(state_dir: &Path, request: &Request) -> Result<RoutingSnapshot> {
    let path = socket_path(state_dir);
    let mut stream = StdUnixStream::connect(&path)
        .with_context(|| format!("connect to running daemon at {}", path.display()))?;
    stream.set_read_timeout(Some(CLIENT_TIMEOUT))?;
    stream.set_write_timeout(Some(CLIENT_TIMEOUT))?;
    serde_json::to_writer(&mut stream, request)?;
    stream.write_all(b"\n")?;
    let mut line = String::new();
    BufReader::new(stream)
        .take(MAX_MESSAGE_BYTES as u64)
        .read_line(&mut line)?;
    let response: Response = serde_json::from_str(&line).context("decode control response")?;
    if !response.ok {
        bail!(
            response
                .error
                .unwrap_or_else(|| "control request failed".into())
        )
    }
    response
        .routing
        .context("control response omitted routing status")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::AffinityStore;

    #[test]
    fn constant_time_comparison_handles_different_lengths() {
        assert!(secrets_equal("secret", "secret"));
        assert!(!secrets_equal("secret", "secrex"));
        assert!(!secrets_equal("secret", "secret-longer"));
    }

    #[tokio::test]
    async fn live_switch_is_authenticated_persisted_and_visible() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("comradex.toml");
        fs::write(
            &config_path,
            r#"[proxy]
installation_secret = "0123456789abcdef"
affinity_key = "0123456789abcdef0123456789abcdef"
state_dir = "state"

[listeners.default]
address = "127.0.0.1:10100"
pool = "default"

[pools.default]
members = ["a", "b"]

[accounts.a]
kind = "inbound"

[accounts.b]
kind = "inbound"
"#,
        )
        .unwrap();
        let config = Arc::new(Config::load(&config_path).unwrap());
        let state_dir = config.proxy.state_dir.clone().unwrap();
        fs::create_dir_all(&state_dir).unwrap();
        let affinity = Arc::new(
            AffinityStore::load(
                state_dir.join("affinity.json"),
                &config.proxy.affinity_key,
                100,
                100_000,
                Duration::from_secs(60),
            )
            .unwrap(),
        );
        let router = Arc::new(Router::new(&config, affinity));
        let server = ControlServer::bind(
            &state_dir,
            config_path.clone(),
            config.clone(),
            router.clone(),
        )
        .unwrap();
        assert_eq!(
            fs::metadata(socket_path(&state_dir))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let task = tokio::spawn(server.run());

        let state = state_dir.clone();
        let unauthorized = tokio::task::spawn_blocking(move || {
            set_preferred(&state, "wrong-secret", "default", Some("b"))
        })
        .await
        .unwrap();
        assert!(
            unauthorized
                .unwrap_err()
                .to_string()
                .contains("unauthorized")
        );

        let state = state_dir.clone();
        let routing = tokio::task::spawn_blocking(move || {
            set_preferred(&state, "0123456789abcdef", "default", Some("b"))
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(routing.preferred_accounts["default"], "b");
        assert!(!routing.active_accounts.contains_key("default"));
        assert!(
            fs::read_to_string(&config_path)
                .unwrap()
                .contains("preferred = \"b\"")
        );
        assert_eq!(
            router.routing_snapshot().await.preferred_accounts["default"],
            "b"
        );

        let selected = router
            .select(
                "default",
                &config.pools["default"],
                Some(router.affinity.key("fresh")),
                None,
            )
            .await
            .unwrap();
        assert_eq!(selected.account_id, "b");
        let state = state_dir.clone();
        let routing =
            tokio::task::spawn_blocking(move || routing_status(&state, "0123456789abcdef"))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(routing.active_accounts["default"], "b");

        task.abort();
        let _ = task.await;
        assert!(!socket_path(&state_dir).exists());
    }
}
