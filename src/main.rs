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
    #[arg(long, default_value = "comradex.toml", global = true)]
    config: PathBuf,
    #[command(subcommand)]
    command: CommandName,
}

#[derive(Subcommand)]
enum CommandName {
    Init,
    Check,
    Serve,
    Install {
        #[arg(long)]
        codex_config: PathBuf,
        #[arg(long, default_value = "default")]
        listener: String,
    },
    Uninstall,
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
enum ServiceCommand {
    Install,
    Uninstall,
    Status,
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
    match cli.command {
        CommandName::Init => init(&cli.config),
        CommandName::Check => {
            let c = Config::load(&cli.config)?;
            println!(
                "valid: {} listeners, {} pools, {} accounts",
                c.listeners.len(),
                c.pools.len(),
                c.accounts.len()
            );
            Ok(())
        }
        CommandName::Serve => serve(&cli.config).await,
        CommandName::Install {
            codex_config,
            listener,
        } => install_config(&cli.config, &codex_config, &listener),
        CommandName::Uninstall => {
            let config = Config::load(&cli.config)?;
            install::uninstall(&state_dir(&config).join("install.json"))
        }
        CommandName::Service { command } => service_command(&cli.config, command),
        CommandName::Login { account } => login(&cli.config, &account),
        CommandName::Stats => {
            let config = Config::load(&cli.config)?;
            println!(
                "{}",
                fs::read_to_string(state_dir(&config).join("stats.json"))
                    .context("daemon has not written stats yet")?
            );
            Ok(())
        }
    }
}

fn init(path: &Path) -> Result<()> {
    if path.exists() {
        bail!("{} already exists", path.display())
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
    let config = Arc::new(Config::load(path)?);
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
            let config = Config::load(config_path)?;
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
    }
    Ok(())
}

fn install_config(config_path: &Path, codex_config: &Path, listener_name: &str) -> Result<()> {
    let config = Config::load(config_path)?;
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

fn login(config_path: &Path, account_name: &str) -> Result<()> {
    let config = Config::load(config_path)?;
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
