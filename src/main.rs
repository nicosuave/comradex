use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::{Parser, Subcommand};
use comradex::{
    codex_process::{self, ProcessControl, SystemProcesses},
    config::Config,
    install,
    proxy::App,
    routing::{AffinityStore, Router},
    service,
    state::Stats,
};
use rand::RngCore;
use tokio::signal;
use tracing::{info, warn};

#[derive(Parser)]
#[command(name = "comradex", version, about)]
struct Cli {
    /// Comradex configuration file [default: ~/.config/comradex/comradex.toml]
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: CommandName,
}

#[derive(Subcommand)]
enum CommandName {
    Init,
    Check,
    Serve,
    Install {
        /// Codex config.toml to point at Comradex
        /// [default: $CODEX_HOME/config.toml, or ~/.codex/config.toml]
        #[arg(long)]
        codex_config: Option<PathBuf>,
        #[arg(long, default_value = "default")]
        listener: String,
        /// SIGTERM running Codex app-server processes so they pick up the new
        /// openai_base_url (active turns may be interrupted)
        #[arg(long)]
        restart_codex: bool,
    },
    Uninstall {
        /// SIGTERM running Codex app-server processes so they pick up the
        /// restored openai_base_url (active turns may be interrupted)
        #[arg(long)]
        restart_codex: bool,
    },
    /// SIGTERM running Codex app-server processes (the desktop app respawns
    /// its app-server); needed after openai_base_url changes on disk
    RestartCodex,
    Account {
        #[command(subcommand)]
        command: AccountCommand,
    },
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    Login {
        account: String,
    },
    Stats,
}

#[derive(Subcommand)]
enum AccountCommand {
    /// Add a managed account: create its isolated codex_home, add it to a
    /// pool, log it in, and restart the daemon
    Add {
        name: String,
        #[arg(long, default_value = "default")]
        pool: String,
        /// Skip the interactive device login (run `comradex login <name>` later)
        #[arg(long)]
        no_login: bool,
    },
    /// List configured accounts and their login state
    List,
    /// Remove an account from the configuration and all pools
    Remove {
        name: String,
        /// Also delete the account's codex_home directory (credentials)
        #[arg(long)]
        purge: bool,
    },
}

#[derive(Subcommand)]
enum ServiceCommand {
    Install,
    Uninstall,
    Status,
    /// Restart the daemon so it reloads comradex.toml (needed after config
    /// edits such as adding an account)
    Restart,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("comradex=info".parse()?),
        )
        .init();
    let cli = Cli::parse();
    let config_path = match cli.config {
        Some(path) => path,
        None => default_config_path(std::env::var_os("HOME"))?,
    };
    match cli.command {
        CommandName::Init => init(&config_path),
        CommandName::Check => {
            let c = load_config(&config_path)?;
            println!(
                "valid: {} listeners, {} pools, {} accounts",
                c.listeners.len(),
                c.pools.len(),
                c.accounts.len()
            );
            Ok(())
        }
        CommandName::Serve => serve(&config_path).await,
        CommandName::Install {
            codex_config,
            listener,
            restart_codex,
        } => {
            let codex_config = match codex_config {
                Some(path) => path,
                None => default_codex_config_path(
                    std::env::var_os("CODEX_HOME"),
                    std::env::var_os("HOME"),
                )?,
            };
            install_config(&config_path, &codex_config, &listener)?;
            handle_running_codex(restart_codex)
        }
        CommandName::Uninstall { restart_codex } => {
            let config = load_config(&config_path)?;
            install::uninstall(&state_dir(&config).join("install.json"))?;
            handle_running_codex(restart_codex)
        }
        CommandName::RestartCodex => handle_running_codex(true),
        CommandName::Account { command } => account_command(&config_path, command),
        CommandName::Service { command } => service_command(&config_path, command),
        CommandName::Login { account } => login(&config_path, &account),
        CommandName::Stats => {
            let config = load_config(&config_path)?;
            println!(
                "{}",
                fs::read_to_string(state_dir(&config).join("stats.json"))
                    .context("daemon has not written stats yet")?
            );
            Ok(())
        }
    }
}

fn default_config_path(home: Option<std::ffi::OsString>) -> Result<PathBuf> {
    let home = home
        .filter(|value| !value.is_empty())
        .context("HOME is not set; pass --config")?;
    Ok(PathBuf::from(home).join(".config/comradex/comradex.toml"))
}

fn default_codex_config_path(
    codex_home: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
) -> Result<PathBuf> {
    if let Some(codex_home) = codex_home.filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(codex_home).join("config.toml"));
    }
    let home = home
        .filter(|value| !value.is_empty())
        .context("neither CODEX_HOME nor HOME is set; pass --codex-config")?;
    Ok(PathBuf::from(home).join(".codex/config.toml"))
}

fn load_config(path: &Path) -> Result<Config> {
    if !path.exists() {
        bail!(
            "no configuration at {} (run `comradex init` to create it, or pass --config)",
            path.display()
        )
    }
    Config::load(path)
}

fn init(path: &Path) -> Result<()> {
    if path.exists() {
        bail!("{} already exists", path.display())
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create config directory {}", parent.display()))?;
    }
    let mut secret = [0u8; 16];
    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut secret);
    rand::rng().fill_bytes(&mut key);
    let text = format!(
        r#"[proxy]
upstream = "https://chatgpt.com/backend-api/codex"
switch_at = 80
max_inflight = 64
max_upgrades = 32
responses_websocket_mode = "raw"
installation_secret = "{}"
affinity_key = "{}"

[listeners.default]
address = "127.0.0.1:10100"
pool = "default"

[pools.default]
members = ["caller"]

[accounts.caller]
kind = "inbound"
"#,
        URL_SAFE_NO_PAD.encode(secret),
        URL_SAFE_NO_PAD.encode(key)
    );
    fs::write(path, text)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    println!("created {}", path.display());
    Ok(())
}

async fn serve(path: &Path) -> Result<()> {
    let config = Arc::new(load_config(path)?);
    let state = state_dir(&config);
    fs::create_dir_all(&state)?;
    let affinity = Arc::new(AffinityStore::load(
        state.join("affinity.json"),
        &config.proxy.affinity_key,
        config.proxy.max_affinity_entries,
        config.proxy.max_affinity_bytes,
        Duration::from_secs(config.proxy.affinity_idle_days * 86_400),
    )?);
    let router = Arc::new(Router::new(&config, affinity));
    let stats = Arc::new(Stats::default());
    let app = App::new(config.clone(), router.clone(), stats.clone())?;
    let mut tasks = tokio::task::JoinSet::new();
    for (name, listener) in config.listeners.clone() {
        tasks.spawn(app.clone().run_listener(name, listener));
    }
    let background_config = config.clone();
    let background_router = router.clone();
    let background_stats = stats.clone();
    let background_app = app.clone();
    let background = tokio::spawn(async move {
        let mut interval = tokio::time::interval(background_config.snapshot_interval());
        loop {
            interval.tick().await;
            if let Err(e) = background_router.affinity.flush().await {
                warn!(error = %e, "affinity snapshot failed");
            }
            if let Err(e) = background_app.flush_file_owners().await {
                warn!(error = %e, "file-owner snapshot failed");
            }
            if let Err(e) = background_stats
                .write(
                    state_dir(&background_config).join("stats.json"),
                    &background_router,
                )
                .await
            {
                warn!(error = %e, "stats snapshot failed");
            }
        }
    });
    let listener_error = tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            None
        },
        listener = tasks.join_next() => {
            Some(match listener {
                Some(Ok(Ok(()))) => anyhow::anyhow!("listener exited unexpectedly"),
                Some(Ok(Err(error))) => error.context("listener failed"),
                Some(Err(error)) => error.into(),
                None => anyhow::anyhow!("all listeners exited unexpectedly"),
            })
        }
    };
    info!("shutting down");
    background.abort();
    let _ = background.await;
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    app.shutdown_connections().await;
    router.clear_inflight().await;
    router.affinity.flush().await?;
    app.flush_file_owners().await?;
    stats.write(state.join("stats.json"), &router).await?;
    if let Some(error) = listener_error {
        return Err(error);
    }
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        signal::ctrl_c().await?;
        Ok(())
    }
}

fn service_command(config_path: &Path, command: ServiceCommand) -> Result<()> {
    match command {
        ServiceCommand::Install => {
            let config = load_config(config_path)?;
            let plist = service::install(config_path, &state_dir(&config))?;
            println!("installed and started {}", plist.display());
        }
        ServiceCommand::Uninstall => match service::uninstall()? {
            Some(path) => println!("stopped service and removed {}", path.display()),
            None => println!("service is not installed"),
        },
        ServiceCommand::Status => {
            if service::status()? {
                println!("service is running");
            } else {
                println!("service is not running");
            }
        }
        ServiceCommand::Restart => {
            service::restart()?;
            println!("service restarted");
        }
    }
    Ok(())
}

fn install_config(config_path: &Path, codex_config: &Path, listener_name: &str) -> Result<()> {
    let config = load_config(config_path)?;
    let listener = config
        .listeners
        .get(listener_name)
        .with_context(|| format!("unknown listener {listener_name}"))?;
    let url = format!(
        "http://{}/{}/v1",
        listener.address, config.proxy.installation_secret
    );
    install::install(codex_config, &state_dir(&config).join("install.json"), &url)?;
    println!("installed openai_base_url = {url}");
    Ok(())
}

fn account_command(config_path: &Path, command: AccountCommand) -> Result<()> {
    match command {
        AccountCommand::Add {
            name,
            pool,
            no_login,
        } => {
            load_config(config_path)?;
            let text = fs::read_to_string(config_path)
                .with_context(|| format!("read {}", config_path.display()))?;
            let updated = comradex::accounts::add_account(&text, &name, &pool)?;
            write_config_validated(config_path, &updated)?;
            println!("added account {name} to pool {pool}");
            reload_daemon()?;
            if no_login {
                println!("run `comradex login {name}` to log the account in");
            } else {
                login(config_path, &name)?;
            }
            Ok(())
        }
        AccountCommand::List => {
            let config = load_config(config_path)?;
            for (name, account) in &config.accounts {
                let pools: Vec<&str> = config
                    .pools
                    .iter()
                    .filter(|(_, pool)| pool.members.iter().any(|member| member == name))
                    .map(|(pool_name, _)| pool_name.as_str())
                    .collect();
                let detail = match account {
                    comradex::config::AccountConfig::Inbound => "inbound (caller-owned)".into(),
                    comradex::config::AccountConfig::CodexHome { path } => {
                        let state = if path.join("auth.json").exists() {
                            "logged in"
                        } else {
                            "not logged in"
                        };
                        format!("codex_home {} ({state})", path.display())
                    }
                };
                println!("{name}: {detail}, pools: [{}]", pools.join(", "));
            }
            Ok(())
        }
        AccountCommand::Remove { name, purge } => {
            let config = load_config(config_path)?;
            let text = fs::read_to_string(config_path)
                .with_context(|| format!("read {}", config_path.display()))?;
            let (updated, _) = comradex::accounts::remove_account(&text, &name)?;
            write_config_validated(config_path, &updated)?;
            println!("removed account {name}");
            reload_daemon()?;
            // Resolve the home from the already-loaded config so relative
            // paths are anchored to the config directory, not the CWD.
            if let Some(comradex::config::AccountConfig::CodexHome { path }) =
                config.accounts.get(&name)
            {
                if purge {
                    match fs::remove_dir_all(path) {
                        Ok(()) => println!("deleted {}", path.display()),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => {
                            return Err(error)
                                .with_context(|| format!("delete {}", path.display()));
                        }
                    }
                } else if path.exists() {
                    println!(
                        "credentials kept at {} (pass --purge to delete them)",
                        path.display()
                    );
                }
            }
            Ok(())
        }
    }
}

/// Persist an edited configuration only after the full loader accepts it: the
/// candidate is written next to the config so relative paths resolve
/// identically, validated with Config::load, then swapped into place.
fn write_config_validated(config_path: &Path, text: &str) -> Result<()> {
    let parent = config_path.parent().unwrap_or(Path::new("."));
    let mut temp = tempfile::Builder::new()
        .suffix(".toml")
        .tempfile_in(parent)?;
    std::io::Write::write_all(&mut temp, text.as_bytes())?;
    temp.as_file().sync_all()?;
    Config::load(temp.path()).context("validate updated configuration")?;
    if let Ok(metadata) = fs::metadata(config_path) {
        temp.as_file().set_permissions(metadata.permissions())?;
    }
    temp.persist(config_path).map_err(|error| error.error)?;
    Ok(())
}

/// Bounce the daemon after a config change when it is installed as a service;
/// otherwise leave a reminder. Login is not needed before the restart because
/// the daemon re-reads each account's auth.json per request.
fn reload_daemon() -> Result<()> {
    if service::installed().unwrap_or(false) {
        service::restart()?;
        println!("service restarted");
    } else {
        println!("restart the comradex daemon to pick up the configuration change");
    }
    Ok(())
}

/// Rewriting openai_base_url on disk is not enough while long-lived Codex
/// app-server processes keep the old value in memory: warn by default, SIGTERM
/// them when requested.
fn handle_running_codex(restart: bool) -> Result<()> {
    let control = SystemProcesses;
    let processes = control.list()?;
    if processes.is_empty() {
        if restart {
            println!("no Codex app-server processes are running");
        }
        return Ok(());
    }
    let pids = processes
        .iter()
        .map(|process| process.pid.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    if !restart {
        eprintln!(
            "warning: {} Codex app-server process(es) still running (PID {pids}); \
             they keep using the previous openai_base_url until restarted. \
             Run `comradex restart-codex` (active turns may be interrupted).",
            processes.len()
        );
        return Ok(());
    }
    println!("stopping Codex app-server process(es) {pids} (active turns may be interrupted)");
    let outcome = codex_process::restart(&processes, &control);
    if !outcome.stopped.is_empty() {
        let stopped = outcome
            .stopped
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        println!("stopped PID {stopped}; the Codex App respawns its app-server automatically");
    }
    for (pid, error) in &outcome.failed {
        eprintln!("failed to stop PID {pid}: {error}");
    }
    if !outcome.surviving.is_empty() {
        let surviving = outcome
            .surviving
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        eprintln!(
            "PID {surviving} still running after SIGTERM; stop them manually if Codex keeps using the old URL"
        );
    }
    Ok(())
}

fn login(config_path: &Path, account_name: &str) -> Result<()> {
    let config = load_config(config_path)?;
    let account = config
        .accounts
        .get(account_name)
        .with_context(|| format!("unknown account {account_name}"))?;
    let comradex::config::AccountConfig::CodexHome { path } = account else {
        bail!("inbound caller account is owned by Codex App and cannot be logged in here")
    };
    fs::create_dir_all(path)?;
    let status = Command::new("codex")
        .arg("login")
        .arg("--device-auth")
        .env("CODEX_HOME", path)
        .status()
        .context("launch codex device login")?;
    if !status.success() {
        bail!("codex login exited with {status}")
    }
    Ok(())
}

fn state_dir(config: &Config) -> PathBuf {
    config.proxy.state_dir.clone().expect("filled by load")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn config_path_defaults_to_user_config_directory() {
        let path = default_config_path(Some(OsString::from("/Users/example"))).unwrap();
        assert_eq!(
            path,
            PathBuf::from("/Users/example/.config/comradex/comradex.toml")
        );
    }

    #[test]
    fn config_path_requires_home_when_flag_is_absent() {
        assert!(default_config_path(None).is_err());
        assert!(default_config_path(Some(OsString::new())).is_err());
    }

    #[test]
    fn codex_config_prefers_codex_home() {
        let path = default_codex_config_path(
            Some(OsString::from("/custom/codex")),
            Some(OsString::from("/Users/example")),
        )
        .unwrap();
        assert_eq!(path, PathBuf::from("/custom/codex/config.toml"));
    }

    #[test]
    fn codex_config_falls_back_to_home_and_rejects_empty_codex_home() {
        let path = default_codex_config_path(
            Some(OsString::new()),
            Some(OsString::from("/Users/example")),
        )
        .unwrap();
        assert_eq!(path, PathBuf::from("/Users/example/.codex/config.toml"));
        assert!(default_codex_config_path(None, None).is_err());
    }
}
