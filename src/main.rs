use std::{
    fs,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::{Parser, Subcommand};
use comradex::{
    auth_lock::HomeAuthLock,
    codex_process::{self, ProcessControl, SystemProcesses},
    config::Config,
    control, install,
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
        /// Claude settings.json to update (only for a Claude listener)
        #[arg(long, conflicts_with_all = ["codex_config", "desktop", "restart_codex"])]
        claude_settings: Option<PathBuf>,
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
        /// Also wire native Desktop backend calls through configured proxy.desktop (macOS).
        /// Fully quit and reopen Desktop afterwards to pick up its launch environment.
        #[arg(long)]
        desktop: bool,
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
    /// Internal browser launcher used by the official provider CLI.
    #[command(hide = true)]
    BrowserOpen {
        #[arg(long)]
        executable: PathBuf,
        #[arg(long)]
        profile: PathBuf,
        url: String,
    },
    Service {
        #[command(subcommand)]
        command: ServiceCommand,
    },
    /// Show configuration, service, Codex wiring, accounts, and live traffic
    Status {
        /// Print the raw stats snapshot as JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum AccountCommand {
    /// Add a managed account: create its isolated codex_home, add it to a
    /// pool, log it in, and restart the daemon
    Add {
        name: String,
        /// Add a Claude subscription account
        #[arg(long)]
        claude: bool,
        /// Pool to join [default: "claude" with --claude, otherwise "default"]
        #[arg(long)]
        pool: Option<String>,
        /// Skip the interactive sign-in (run `comradex account login <name>` later)
        #[arg(long)]
        no_login: bool,
        /// Sign in using this account's persistent browser profile
        #[arg(long, conflicts_with_all = ["no_login", "no_browser"])]
        browser: bool,
        /// Use the ordinary login flow even if this name has a retained browser profile
        #[arg(long)]
        no_browser: bool,
    },
    /// Create a provider account in its own browser, then connect it to Comradex
    New {
        /// Name for the account (prompted when omitted)
        name: Option<String>,
        /// Create a Claude account instead of a ChatGPT/Codex account
        #[arg(long, conflicts_with = "codex")]
        claude: bool,
        /// Create a ChatGPT/Codex account without prompting for the provider
        #[arg(long)]
        codex: bool,
        /// Pool to join [default: "claude" or "default", according to provider]
        #[arg(long)]
        pool: Option<String>,
    },
    /// Open this account's browser profile on the provider's website
    Browser { name: String },
    /// Read current reset credits and their exact expiration timestamps
    ResetCredits {
        name: String,
        #[arg(long)]
        json: bool,
    },
    /// Consume one specific reset credit (requires --confirm)
    UseReset {
        name: String,
        #[arg(long)]
        credit_id: String,
        #[arg(long, required = true)]
        confirm: bool,
        /// Reuse this ID when retrying an ambiguous result
        #[arg(long)]
        request_id: Option<String>,
    },
    /// List configured accounts and their sign-in state
    List,
    /// Prefer an account for new work without interrupting active turns
    #[command(alias = "switch")]
    Prefer {
        /// Account to prefer; omit when using --clear
        name: Option<String>,
        #[arg(long, default_value = "default")]
        pool: String,
        /// Return the pool to automatic account selection
        #[arg(long, conflicts_with = "name", required_unless_present = "name")]
        clear: bool,
    },
    /// Use an account last for new work without interrupting active turns
    Preserve {
        /// Account to preserve; omit when using --clear
        name: Option<String>,
        #[arg(long, default_value = "default")]
        pool: String,
        /// Clear the preserved account for this pool
        #[arg(long, conflicts_with = "name", required_unless_present = "name")]
        clear: bool,
    },
    /// Sign an account in through its official provider CLI
    Login {
        name: String,
        /// Create/reuse a persistent browser profile for this account
        #[arg(long, conflicts_with = "no_browser")]
        browser: bool,
        /// Use the ordinary login flow even if this account has a browser profile
        #[arg(long)]
        no_browser: bool,
    },
    /// Connect an inbound account to your existing Codex login
    Connect {
        name: String,
        /// Existing Codex home [default: CODEX_HOME or ~/.codex]
        #[arg(long)]
        codex_home: Option<PathBuf>,
    },
    /// Remove an account from the configuration and all pools
    Remove {
        name: String,
        /// Also delete the browser profile and isolated account home (external
        /// logins cannot be purged); also cleans up after an earlier plain remove
        #[arg(long)]
        purge: bool,
    },
}

#[derive(Subcommand)]
enum ServiceCommand {
    Install,
    /// Start an installed service without interrupting it when already running
    Start,
    Uninstall,
    Status,
    /// Show recent service stdout and stderr with log paths and modification times
    Logs,
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
            for (name, listener) in &c.listeners {
                let base = format!(
                    "http://{}/{}",
                    listener.address, c.proxy.installation_secret
                );
                println!(
                    "listener {name} openai_base_url (Codex 0.153+ context): {base}/backend-api/codex"
                );
                println!("listener {name} openai_base_url (older clients):       {base}/v1");
            }
            Ok(())
        }
        CommandName::Serve => serve(&config_path).await,
        CommandName::Install {
            claude_settings,
            codex_config,
            listener,
            restart_codex,
            desktop,
        } => {
            let config = load_config(&config_path)?;
            let listener_config = config
                .listeners
                .get(&listener)
                .context("unknown listener")?;
            if config.is_claude_pool(&listener_config.pool) {
                anyhow::ensure!(
                    !desktop && !restart_codex && codex_config.is_none(),
                    "Codex install options cannot target a Claude pool"
                );
                let settings = claude_settings.unwrap_or_else(|| {
                    let home = std::env::var_os("CLAUDE_CONFIG_DIR")
                        .map(PathBuf::from)
                        .unwrap_or_else(|| {
                            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                                .join(".claude")
                        });
                    home.join("settings.json")
                });
                let url = format!(
                    "http://{}/{}",
                    listener_config.address, config.proxy.installation_secret
                );
                install::install_claude(
                    &settings,
                    &state_dir(&config).join("claude-install.json"),
                    &url,
                )?;
                println!(
                    "installed Claude Code gateway in {}; restart Claude Code to apply it",
                    settings.display()
                );
                return Ok(());
            }
            anyhow::ensure!(
                claude_settings.is_none(),
                "--claude-settings requires a Claude listener"
            );
            let desktop_install = if desktop {
                let config = load_config(&config_path)?;
                let listener = config.proxy.desktop.as_ref().context(
                    "configure [proxy.desktop] address = '127.0.0.1:8000' and pool before install --desktop",
                )?;
                Some((
                    state_dir(&config).join("desktop-install.json"),
                    format!(
                        "http://localhost:{}/{}/backend-api",
                        listener.address.port(),
                        config.proxy.installation_secret
                    ),
                ))
            } else {
                None
            };
            let codex_config = match codex_config {
                Some(path) => path,
                None => default_codex_config_path(
                    std::env::var_os("CODEX_HOME"),
                    std::env::var_os("HOME"),
                )?,
            };
            if let Some((record, url)) = &desktop_install {
                install::check_desktop_install(record, url)?;
            }
            install_config(&config_path, &codex_config, &listener)?;
            if let Some((record, url)) = desktop_install {
                install::install_desktop(&record, &url)?;
                println!(
                    "installed Desktop backend URL; fully quit and reopen Desktop to apply it"
                );
            }
            handle_running_codex(restart_codex)
        }
        CommandName::Uninstall { restart_codex } => {
            let config = load_config(&config_path)?;
            install::uninstall_claude(&state_dir(&config).join("claude-install.json"))?;
            let desktop_record = state_dir(&config).join("desktop-install.json");
            let desktop_installed = desktop_record.exists();
            install::uninstall_desktop(&desktop_record)?;
            if desktop_installed {
                println!("restored Desktop backend URL; fully quit and reopen Desktop to apply it");
            }
            install::uninstall(&state_dir(&config).join("install.json"))?;
            handle_running_codex(restart_codex)
        }
        CommandName::RestartCodex => handle_running_codex(true),
        CommandName::Account { command } => account_command(&config_path, command),
        CommandName::BrowserOpen {
            executable,
            profile,
            url,
        } => comradex::browser::open(&executable, &profile, &url),
        CommandName::Service { command } => service_command(&config_path, command),
        CommandName::Status { json } => status(&config_path, json),
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
    init_with_codex_home(
        path,
        comradex::accounts::default_codex_home().ok().as_deref(),
    )
}

fn init_with_codex_home(path: &Path, codex_home: Option<&Path>) -> Result<()> {
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
max_bridge_sessions = 256
bridge_idle_seconds = 900
bridge_admission_timeout_millis = 2000
responses_websocket_mode = "http_bridge"
installation_secret = "{}"
affinity_key = "{}"

[listeners.default]
address = "127.0.0.1:10100"
pool = "default"

[pools.default]
members = ["app"]

[accounts.app]
kind = "inbound"
"#,
        URL_SAFE_NO_PAD.encode(secret),
        URL_SAFE_NO_PAD.encode(key)
    );
    let connected = codex_home
        .and_then(|home| comradex::accounts::connect_existing_account(&text, "app", home).ok());
    let uses_existing_login = connected.is_some();
    let text = connected.unwrap_or(text);
    write_config_validated(path, &text)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    println!("created {}", path.display());
    if uses_existing_login {
        println!("connected app to your existing Codex login");
    } else {
        println!(
            "no supported login found in the local Codex auth.json; app uses the requesting client's login"
        );
        println!(
            "after signing in with Codex, run `comradex account connect app` to enable usage tracking"
        );
    }
    Ok(())
}

async fn serve(path: &Path) -> Result<()> {
    while serve_once(path).await? {}
    Ok(())
}

async fn serve_once(path: &Path) -> Result<bool> {
    let startup = std::time::Instant::now();
    info!(
        pid = std::process::id(),
        version = env!("CARGO_PKG_VERSION"),
        "daemon startup: loading configuration and routing state"
    );
    #[cfg(unix)]
    if let Some((soft, hard)) = open_file_limits() {
        info!(soft, hard, "open file limits");
    }
    let config_path =
        fs::canonicalize(path).with_context(|| format!("resolve config {}", path.display()))?;
    let config = Arc::new(load_config(path)?);
    let state = state_dir(&config);
    fs::create_dir_all(&state)?;
    let affinity = Arc::new(AffinityStore::load(
        state.join("affinity.json"),
        &config.proxy.affinity_key,
        Duration::from_secs(config.proxy.affinity_idle_days * 86_400),
    )?);
    let router = Arc::new(Router::new(&config, affinity));
    info!(
        elapsed_ms = startup.elapsed().as_millis(),
        "daemon startup: routing state loaded; opening control socket"
    );
    let stats = Arc::new(Stats::default());
    let control_server = control::ControlServer::bind(
        &state,
        config_path,
        config.clone(),
        router.clone(),
        stats.clone(),
    )?;
    let reload = control_server.reload_requested();
    let usage_refresh_requested = control_server.usage_refresh_requested();
    info!(
        elapsed_ms = startup.elapsed().as_millis(),
        "daemon startup: control socket bound; initializing transports and stores"
    );
    let app = App::new(config.clone(), router.clone(), stats.clone())?;
    let mut control_task = tokio::spawn(control_server.with_app(app.clone()).run());
    info!(
        elapsed_ms = startup.elapsed().as_millis(),
        "daemon startup: initialization complete; starting listeners"
    );
    let mut tasks = tokio::task::JoinSet::new();
    for (name, listener) in config.listeners.clone() {
        tasks.spawn(app.clone().run_listener(name, listener));
    }
    if let Some(listener) = config.proxy.desktop.clone() {
        tasks.spawn(app.clone().run_desktop_listener(listener));
    }
    let background_config = config.clone();
    let background_router = router.clone();
    let background_stats = stats.clone();
    let background = tokio::spawn(async move {
        let mut interval = tokio::time::interval(background_config.snapshot_interval());
        loop {
            interval.tick().await;
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
    let refresh_app = app.clone();
    let refresh_background = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(
            comradex::auth::PROACTIVE_REFRESH_INTERVAL_SECONDS,
        ));
        loop {
            interval.tick().await;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            refresh_app.refresh_managed_accounts_at(now).await;
        }
    });
    let usage_refresh_app = app.clone();
    let usage_refresh_background = tokio::spawn(async move {
        usage_refresh_app
            .run_usage_refresh(usage_refresh_requested)
            .await;
    });
    let mut reload_requested = false;
    let listener_error = tokio::select! {
        _ = reload.notified() => {
            reload_requested = true;
            None
        },
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
        },
        control = &mut control_task => {
            Some(match control {
                Ok(Ok(())) => anyhow::anyhow!("control server exited unexpectedly"),
                Ok(Err(error)) => error.context("control server failed"),
                Err(error) => error.into(),
            })
        }
    };
    info!("shutting down");
    background.abort();
    let _ = background.await;
    if !control_task.is_finished() {
        control_task.abort();
        let _ = control_task.await;
    }
    refresh_background.abort();
    let _ = refresh_background.await;
    usage_refresh_background.abort();
    let _ = usage_refresh_background.await;
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    app.shutdown_connections().await;
    router.clear_inflight().await;
    stats.write(state.join("stats.json"), &router).await?;
    if let Some(error) = listener_error {
        return Err(error);
    }
    Ok(reload_requested)
}

#[cfg(unix)]
fn open_file_limits() -> Option<(u64, u64)> {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limits` points to initialized writable storage for getrlimit.
    let result = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) };
    (result == 0).then_some((limits.rlim_cur, limits.rlim_max))
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
        ServiceCommand::Start => {
            service::start()?;
            println!("service is running");
        }
        ServiceCommand::Uninstall => match service::uninstall()? {
            Some(path) => println!("stopped service and removed {}", path.display()),
            None => println!("service is not installed"),
        },
        ServiceCommand::Status => {
            println!("{}", service::status_description()?);
        }
        ServiceCommand::Logs => {
            println!("{}", service::logs()?);
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
    anyhow::ensure!(
        !config.is_claude_pool(&listener.pool),
        "Codex installation requires a Codex listener"
    );
    let url = format!(
        "http://{}/{}/v1",
        listener.address, config.proxy.installation_secret
    );
    let installed_url =
        install::install(codex_config, &state_dir(&config).join("install.json"), &url)?;
    println!("installed openai_base_url = {installed_url}");
    if let Some(alternate) = install::alternate_url(&installed_url) {
        println!("alternate openai_base_url = {alternate}");
        println!(
            "use the .../backend-api/codex URL for Codex 0.153+ with \
             [features.context_management] experimental_mode = true and the .../v1 URL \
             for older clients; `comradex install` rewrites this value in place from \
             that flag, or edit openai_base_url by hand to switch shapes"
        );
    }
    Ok(())
}

fn status(config_path: &Path, json: bool) -> Result<()> {
    let config = load_config(config_path)?;
    let state = state_dir(&config);
    let mut snapshot = fs::read(state.join("stats.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<comradex::state::StatsSnapshot>(&bytes).ok());
    let live_routing = control::routing_status(&state, &config.proxy.installation_secret).ok();
    if let (Some(snapshot), Some(routing)) = (&mut snapshot, &live_routing) {
        snapshot.routing = routing.clone();
    }
    if json {
        let snapshot = snapshot.context("daemon has not written stats yet")?;
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        return Ok(());
    }

    println!("config   {}", config_path.display());
    println!(
        "         {} listener(s), {} pool(s), {} account(s)",
        config.listeners.len(),
        config.pools.len(),
        config.accounts.len()
    );

    let service = match (service::installed(), service::status()) {
        (Ok(true), Ok(true)) => "running".to_owned(),
        (Ok(true), Ok(false)) => {
            let suffix = service::last_stderr_line()
                .ok()
                .flatten()
                .map(|stderr| {
                    format!(
                        "; last stderr line{}: {}",
                        stderr_timestamp_suffix(stderr.log_modified_at_unix),
                        stderr.line
                    )
                })
                .unwrap_or_default();
            format!("installed but not running{suffix} (run `comradex service start`)")
        }
        (Ok(false), _) => "not installed (run `comradex service install`)".to_owned(),
        (Err(error), _) | (_, Err(error)) => format!("unknown ({error})"),
    };
    println!("service  {service}");

    match install::installed_record(&state.join("install.json")) {
        Some(record) => {
            println!(
                "codex    routed through Comradex via {}",
                record.codex_config.display()
            );
            println!("         openai_base_url = {}", record.installed_url);
            if let Some(alternate) = install::alternate_url(&record.installed_url) {
                println!(
                    "         alternate   URL = {alternate} \
                     (backend shape for Codex 0.153+ context management, /v1 for older clients)"
                );
            }
        }
        None => println!("codex    not routed through Comradex (run `comradex install`)"),
    }

    let routing = live_routing
        .as_ref()
        .or_else(|| snapshot.as_ref().map(|snapshot| &snapshot.routing));
    println!("\naccounts");
    let width = config.accounts.keys().map(String::len).max().unwrap_or(0);
    for (name, account) in &config.accounts {
        let pools: Vec<&str> = config
            .pools
            .iter()
            .filter(|(_, pool)| pool.members.iter().any(|member| member == name))
            .map(|(pool_name, _)| pool_name.as_str())
            .collect();
        let pools = if pools.is_empty() {
            "unused".to_owned()
        } else {
            format!("pool {}", pools.join(", "))
        };
        let routing_status = routing.and_then(|routing| routing.account_states.get(name));
        let state = account_status_state(account, routing_status);
        println!("  {name:width$}  {:36}  {pools}", state);
        if let Some(credits) = routing_status.and_then(|state| state.reset_credits.as_ref()) {
            print_reset_credits(credits);
        }
    }

    println!("\npools");
    let width = config.pools.keys().map(String::len).max().unwrap_or(0);
    for (name, pool) in &config.pools {
        let preferred = routing
            .and_then(|routing| routing.preferred_accounts.get(name))
            .or(pool.preferred.as_ref())
            .map_or("automatic", String::as_str);
        let preserved = match routing {
            Some(routing) => routing.preserved_accounts.get(name),
            None => pool.preserved.as_ref(),
        }
        .map_or("none", String::as_str);
        let active = routing
            .and_then(|routing| routing.active_accounts.get(name))
            .map_or("no fresh work yet", String::as_str);
        // Display honesty (fix1): `active` is the last fresh pick only and never reflects
        // bound/select_exact traffic; `wired` is the last account actually sent upstream.
        let wired = routing
            .and_then(|routing| routing.wired_accounts.get(name))
            .map_or("nothing wired yet", String::as_str);
        println!(
            "  {name:width$}  preferred {preferred}, preserved {preserved}, active {active}, wired {wired}"
        );
    }

    println!("\ntraffic");
    match snapshot {
        Some(stats) => {
            if let Some(memory) = &stats.memory {
                println!(
                    "  Rust allocations: {} live, {} peak; bridge history: {} live, {} peak; turn copies: {} live, {} peak",
                    human_bytes(memory.rust_live_bytes),
                    human_bytes(memory.rust_peak_bytes),
                    human_bytes(memory.bridge_continuation_bytes),
                    human_bytes(memory.bridge_continuation_peak_bytes),
                    human_bytes(memory.bridge_turn_copy_bytes),
                    human_bytes(memory.bridge_turn_copy_peak_bytes),
                );
            }
            println!(
                "  {} HTTP request(s) and {} bridge turn(s) in flight, {} open connection(s)",
                stats.inflight_http, stats.inflight_bridge_turns, stats.open_upgrades
            );
            println!(
                "  {} sticky conversation(s) remembered ({})",
                stats.affinity_entries,
                human_bytes(stats.affinity_bytes)
            );
            if stats.active_spool_bytes > 0 {
                println!(
                    "  {} buffered on disk",
                    human_bytes(stats.active_spool_bytes)
                );
            }
            println!(
                "  refresh scheduler: {} sweep(s), {} account check(s), {} refreshed, {} failure(s) ({} need login)",
                stats.refresh_scheduler_ticks,
                stats.refresh_accounts_checked,
                stats.refresh_successes,
                stats.refresh_failures,
                stats.refresh_reauth_required,
            );
            println!(
                "  usage fetcher: {} account check(s), {} succeeded, {} failure(s)",
                stats.usage_fetch_accounts_checked,
                stats.usage_fetch_successes,
                stats.usage_fetch_failures,
            );
            if stats.refresh_last_sweep_unix > 0 {
                println!(
                    "  last refresh sweep unix {}, last successful refresh unix {}",
                    stats.refresh_last_sweep_unix, stats.refresh_last_success_unix
                );
            }
            if stats.usage_fetch_last_success_unix > 0 {
                println!(
                    "  last successful usage fetch unix {}",
                    stats.usage_fetch_last_success_unix
                );
            }
        }
        None => println!("  no snapshot yet (the daemon writes one every few seconds)"),
    }
    Ok(())
}

fn stderr_timestamp_suffix(timestamp: Option<u64>) -> String {
    timestamp
        .map(|timestamp| format!(" (log modified unix {timestamp})"))
        .unwrap_or_default()
}

fn account_availability(status: &comradex::routing::AccountRoutingStatus) -> String {
    if status.available {
        return if status.reauth_required {
            "; sign-in needed for renewal (current access still usable)".to_owned()
        } else {
            String::new()
        };
    }
    let reason = match status
        .unavailable_reason
        .as_deref()
        .unwrap_or("unavailable")
    {
        "quota" => "rate limited",
        "temporary_failure" => "temporarily unavailable",
        "login_in_progress" => "login in progress",
        "needs_login" => "sign-in required",
        "access_token_rejected" => "access token rejected",
        reason => reason,
    };
    let retry = status.retry_at_unix.and_then(|deadline| {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let deadline = u64::try_from(deadline).ok()?;
        Some(human_duration(deadline.saturating_sub(now)))
    });
    match retry {
        Some(retry) => format!("; {reason}, retry in {retry}"),
        None => format!("; {reason}"),
    }
}

fn account_status_state(
    account: &comradex::config::AccountConfig,
    status: Option<&comradex::routing::AccountRoutingStatus>,
) -> String {
    if matches!(
        account,
        comradex::config::AccountConfig::Inbound | comradex::config::AccountConfig::ClaudeInbound
    ) {
        return "Requesting client's login".to_owned();
    }
    let Some(status) = status else {
        return match account {
            comradex::config::AccountConfig::ClaudeHome { path }
                if comradex::claude::auth::read(path).is_ok() =>
            {
                "usage unavailable".into()
            }
            comradex::config::AccountConfig::CodexHome { path }
                if path.join("auth.json").exists() =>
            {
                "usage unavailable".to_owned()
            }
            _ => "sign-in required".to_owned(),
        };
    };
    let availability = account_availability(status);
    if !availability.is_empty() {
        return availability
            .strip_prefix("; ")
            .unwrap_or(&availability)
            .to_owned();
    }
    usage_remaining_summary(status).unwrap_or_else(|| "usage pending".to_owned())
}

fn usage_remaining_summary(status: &comradex::routing::AccountRoutingStatus) -> Option<String> {
    let mut windows = status
        .usage_windows
        .iter()
        .filter_map(|(name, window)| window.used_percent.map(|used| (name, window, used)))
        .collect::<Vec<_>>();
    windows.sort_by_key(|(name, window, _)| {
        (
            window.limit_window_seconds.unwrap_or(u64::MAX),
            name.as_str(),
        )
    });
    if windows.is_empty() {
        return status
            .usage_percent
            .map(|used| format!("{}% left", 100_u8.saturating_sub(used)));
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Some(
        windows
            .into_iter()
            .map(|(name, window, used)| {
                let label = usage_window_label(name, window.limit_window_seconds);
                let remaining = 100_u8.saturating_sub(used);
                match window
                    .reset_at_unix
                    .and_then(|reset| u64::try_from(reset).ok())
                {
                    Some(reset) if reset > now => format!(
                        "{label} {remaining}% left (resets in {})",
                        compact_duration(reset - now)
                    ),
                    _ => format!("{label} {remaining}% left"),
                }
            })
            .collect::<Vec<_>>()
            .join("; "),
    )
}

fn usage_window_label(name: &str, seconds: Option<u64>) -> String {
    match seconds {
        Some(seconds) if seconds % 86_400 == 0 => format!("{}d", seconds / 86_400),
        Some(seconds) if seconds % 3_600 == 0 => format!("{}h", seconds / 3_600),
        _ => name.to_owned(),
    }
}

fn compact_duration(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..86_400 => format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60),
        _ => format!("{}d {}h", seconds / 86_400, (seconds % 86_400) / 3600),
    }
}

fn human_duration(seconds: u64) -> String {
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m {}s", seconds / 60, seconds % 60),
        _ => format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60),
    }
}

/// Short, plain-language state for one account.
fn account_state(account: &comradex::config::AccountConfig) -> String {
    match account {
        comradex::config::AccountConfig::ClaudeInbound => "Claude Code client's login".to_owned(),
        comradex::config::AccountConfig::ClaudeHome { path } => {
            if comradex::claude::auth::read(path).is_ok() {
                "Claude signed in".into()
            } else {
                "Claude sign-in required".into()
            }
        }
        comradex::config::AccountConfig::Inbound => "Requesting client's login".to_owned(),
        comradex::config::AccountConfig::CodexHome { path } => {
            if path.join("auth.json").exists() {
                "signed in".to_owned()
            } else {
                "not signed in".to_owned()
            }
        }
    }
}

fn human_bytes(bytes: usize) -> String {
    match bytes {
        0..1024 => format!("{bytes} B"),
        1024..1_048_576 => format!("{:.1} KB", bytes as f64 / 1024.0),
        _ => format!("{:.1} MB", bytes as f64 / 1_048_576.0),
    }
}

fn print_reset_credits(snapshot: &comradex::reset_credits::ResetCreditsSnapshot) {
    println!(
        "    {} reset credit(s) available (checked {})",
        snapshot.available_count_at(chrono::Utc::now()),
        snapshot.observed_at_unix
    );
    if let Some(credits) = &snapshot.credits {
        for credit in credits {
            println!(
                "      {} · {} · {} · expires {}",
                credit.id,
                credit.title.as_deref().unwrap_or(&credit.reset_type),
                credit.status,
                credit.expires_at.as_deref().unwrap_or("never")
            );
        }
    }
    if let Some(error) = &snapshot.error {
        println!("      Credit details unavailable: {error}");
    }
}

fn account_command(config_path: &Path, command: AccountCommand) -> Result<()> {
    match command {
        AccountCommand::Add {
            name,
            pool,
            no_login,
            claude,
            browser,
            no_browser,
        } => {
            load_config(config_path)?;
            let pool = pool.unwrap_or_else(|| if claude { "claude" } else { "default" }.into());
            let text = fs::read_to_string(config_path)
                .with_context(|| format!("read {}", config_path.display()))?;
            let updated = if claude {
                comradex::accounts::add_claude_account(&text, &name, &pool)?
            } else {
                comradex::accounts::add_account(&text, &name, &pool)?
            };
            let browser = if !no_login
                && !no_browser
                && (browser || comradex::browser::has_profile(config_path, &name)?)
            {
                Some(comradex::browser::AccountBrowser::prepare(
                    config_path,
                    &name,
                )?)
            } else {
                None
            };
            write_config_validated(config_path, &updated)?;
            println!("added account {name} to pool {pool}");
            reload_daemon()?;
            if no_login {
                println!("run `comradex account login {name}` to sign the account in");
            } else {
                login(config_path, &name, browser.as_ref())?;
            }
            Ok(())
        }
        AccountCommand::New {
            name,
            claude,
            codex,
            pool,
        } => new_account(config_path, name, claude, codex, pool),
        AccountCommand::Browser { name } => {
            let config = load_config(config_path)?;
            let account = config
                .accounts
                .get(&name)
                .with_context(|| format!("unknown account {name}"))?;
            let claude = managed_provider(account)?;
            comradex::browser::AccountBrowser::prepare(config_path, &name)?
                .open(comradex::browser::provider_url(claude))
        }
        AccountCommand::ResetCredits { name, json } => {
            let config = load_config(config_path)?;
            let credits = control::read_reset_credits(&state_dir(&config), &name)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&credits)?);
            } else {
                println!("{name}");
                print_reset_credits(&credits);
            }
            Ok(())
        }
        AccountCommand::UseReset {
            name,
            credit_id,
            confirm,
            request_id,
        } => {
            if !confirm {
                bail!("--confirm is required to consume a reset credit");
            }
            let config = load_config(config_path)?;
            let request_id = request_id.unwrap_or_else(|| {
                let mut bytes = [0u8; 16];
                rand::rng().fill_bytes(&mut bytes);
                URL_SAFE_NO_PAD.encode(bytes)
            });
            println!("Reset request ID: {request_id} (reuse this ID if the outcome is unknown)");
            let result =
                control::use_reset_credit(&state_dir(&config), &name, &credit_id, &request_id)?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        AccountCommand::Login {
            name,
            browser,
            no_browser,
        } => {
            let config = load_config(config_path)?;
            managed_provider(
                config
                    .accounts
                    .get(&name)
                    .with_context(|| format!("unknown account {name}"))?,
            )?;
            let browser = if !no_browser
                && (browser || comradex::browser::has_profile(config_path, &name)?)
            {
                Some(comradex::browser::AccountBrowser::prepare(
                    config_path,
                    &name,
                )?)
            } else {
                None
            };
            login(config_path, &name, browser.as_ref())
        }
        AccountCommand::Connect { name, codex_home } => {
            load_config(config_path)?;
            let home = match codex_home {
                Some(home) => home,
                None => comradex::accounts::existing_codex_home()?,
            };
            let text = fs::read_to_string(config_path)
                .with_context(|| format!("read {}", config_path.display()))?;
            let updated = comradex::accounts::connect_existing_account(&text, &name, &home)?;
            service::while_daemon_stopped(|| write_config_validated(config_path, &updated))?;
            println!("connected account {name} to the existing Codex login");
            println!(
                "if the daemon is running outside the macOS service, restart it to apply the change"
            );
            Ok(())
        }
        AccountCommand::Prefer { name, pool, clear } => {
            debug_assert!(name.is_some() || clear);
            let config = load_config(config_path)?;
            let pool_config = config
                .pools
                .get(&pool)
                .with_context(|| format!("unknown pool {pool}"))?;
            if let Some(name) = &name
                && !pool_config.members.contains(name)
            {
                bail!("account {name} is not a member of pool {pool}")
            }
            match control::set_preferred(
                &state_dir(&config),
                &config.proxy.installation_secret,
                &pool,
                name.as_deref(),
            ) {
                Ok(routing) => {
                    match name {
                        Some(name) => println!(
                            "pool {pool} now prefers {name} for new work; active turns were not interrupted"
                        ),
                        None => println!(
                            "pool {pool} now selects accounts automatically; active turns were not interrupted"
                        ),
                    }
                    if let Some(active) = routing.active_accounts.get(&pool) {
                        println!("active account for fresh work: {active}");
                    }
                    Ok(())
                }
                Err(_control_error) if !control::socket_path(&state_dir(&config)).exists() => {
                    let text = fs::read_to_string(config_path)
                        .with_context(|| format!("read {}", config_path.display()))?;
                    let updated =
                        comradex::accounts::set_preferred_account(&text, &pool, name.as_deref())?;
                    write_config_validated(config_path, &updated)?;
                    match name {
                        Some(name) => println!(
                            "saved {name} as the preferred account for pool {pool}; it will apply when the daemon starts"
                        ),
                        None => println!(
                            "saved automatic selection for pool {pool}; it will apply when the daemon starts"
                        ),
                    }
                    Ok(())
                }
                Err(control_error) => Err(control_error).context(
                    "live routing request did not complete; check `comradex status` before retrying",
                ),
            }
        }
        AccountCommand::Preserve { name, pool, clear } => {
            debug_assert!(name.is_some() || clear);
            let config = load_config(config_path)?;
            let pool_config = config
                .pools
                .get(&pool)
                .with_context(|| format!("unknown pool {pool}"))?;
            if let Some(name) = &name
                && !pool_config.members.contains(name)
            {
                bail!("account {name} is not a member of pool {pool}")
            }
            match control::set_preserved(
                &state_dir(&config),
                &config.proxy.installation_secret,
                &pool,
                name.as_deref(),
            ) {
                Ok(routing) => {
                    match name {
                        Some(name) => println!(
                            "pool {pool} now uses {name} last for new work; active turns were not interrupted"
                        ),
                        None => println!(
                            "pool {pool} no longer preserves an account; active turns were not interrupted"
                        ),
                    }
                    if let Some(active) = routing.active_accounts.get(&pool) {
                        println!("active account for fresh work: {active}");
                    }
                    Ok(())
                }
                Err(_control_error) if !control::socket_path(&state_dir(&config)).exists() => {
                    let text = fs::read_to_string(config_path)
                        .with_context(|| format!("read {}", config_path.display()))?;
                    let updated =
                        comradex::accounts::set_preserved_account(&text, &pool, name.as_deref())?;
                    write_config_validated(config_path, &updated)?;
                    match name {
                        Some(name) => println!(
                            "saved {name} as the preserved account for pool {pool}; it will apply when the daemon starts"
                        ),
                        None => println!(
                            "cleared the preserved account for pool {pool}; it will apply when the daemon starts"
                        ),
                    }
                    Ok(())
                }
                Err(control_error) => Err(control_error).context(
                    "live routing request did not complete; check `comradex status` before retrying",
                ),
            }
        }
        AccountCommand::List => {
            let config = load_config(config_path)?;
            let width = config.accounts.keys().map(String::len).max().unwrap_or(0);
            for (name, account) in &config.accounts {
                let pools: Vec<&str> = config
                    .pools
                    .iter()
                    .filter(|(_, pool)| pool.members.iter().any(|member| member == name))
                    .map(|(pool_name, _)| pool_name.as_str())
                    .collect();
                let pools = if pools.is_empty() {
                    "none (unused)".to_owned()
                } else {
                    pools.join(", ")
                };
                println!("{name:width$}  {:16}  pool {pools}", account_state(account));
            }
            Ok(())
        }
        AccountCommand::Remove { name, purge } => {
            let config = load_config(config_path)?;
            if purge && !config.accounts.contains_key(&name) {
                return purge_removed_account(config_path, &config, &name);
            }
            let browser_profile = if purge {
                comradex::browser::purgeable_profile(config_path, &name)?
            } else {
                None
            };
            if purge
                && let Some(path) = config
                    .accounts
                    .get(&name)
                    .and_then(|account| account.home())
            {
                comradex::accounts::validate_purge_home(config_path, &name, path)?;
            }
            let text = fs::read_to_string(config_path)
                .with_context(|| format!("read {}", config_path.display()))?;
            let (updated, _) = comradex::accounts::remove_account(&text, &name)?;
            write_config_validated(config_path, &updated)?;
            println!("removed account {name}");
            reload_daemon()?;
            // Resolve the home from the already-loaded config so relative
            // paths are anchored to the config directory, not the CWD.
            let home = config
                .accounts
                .get(&name)
                .and_then(|account| account.home());
            if purge {
                return purge_account_storage(&name, browser_profile.as_deref(), home);
            }
            let retry = format!("run `comradex account remove {name} --purge` to delete them");
            if comradex::browser::has_profile(config_path, &name)? {
                println!(
                    "browser cookies kept at {} ({retry})",
                    comradex::browser::profile_path(config_path, &name)?.display()
                );
            }
            if let Some(path) = home.filter(|path| path.exists()) {
                if comradex::accounts::validate_purge_home(config_path, &name, path).is_ok() {
                    println!("credentials kept at {} ({retry})", path.display());
                } else {
                    println!("existing Codex login kept at {}", path.display());
                }
            }
            Ok(())
        }
    }
}

/// Attempt every deletion even if one fails, so a stuck profile cannot strand
/// credentials (or vice versa); whatever remains can be retried by name.
fn purge_account_storage(name: &str, profile: Option<&Path>, home: Option<&Path>) -> Result<()> {
    let mut failures = Vec::new();
    for (label, path) in [("browser profile ", profile), ("", home)] {
        let Some(path) = path else { continue };
        match fs::remove_dir_all(path) {
            Ok(()) => println!("deleted {label}{}", path.display()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => failures.push(format!("delete {}: {error}", path.display())),
        }
    }
    if !failures.is_empty() {
        bail!(
            "{}; retry with `comradex account remove {name} --purge`",
            failures.join("; ")
        )
    }
    Ok(())
}

/// Cookies, and an isolated home at its default location, can outlive the
/// config entry after a plain remove or an interrupted purge.
fn purge_removed_account(config_path: &Path, config: &Config, name: &str) -> Result<()> {
    let profile = comradex::browser::purgeable_profile(config_path, name)?;
    let mut home = None;
    if comradex::accounts::validate_name(name).is_ok() {
        let path = std::path::absolute(config_path)?
            .parent()
            .context("configuration has no parent directory")?
            .join("accounts")
            .join(name);
        let normalized = comradex::config::normalize_codex_home(&path)?;
        // Never delete a home that another configured account still uses.
        let in_use = config.accounts.values().any(|account| {
            account.home().is_some_and(|home| {
                comradex::config::normalize_codex_home(home).is_ok_and(|home| home == normalized)
            })
        });
        if path.exists()
            && !in_use
            && comradex::accounts::validate_purge_home(config_path, name, &path).is_ok()
        {
            home = Some(path);
        }
    }
    if profile.is_none() && home.is_none() {
        bail!("unknown account {name}")
    }
    purge_account_storage(name, profile.as_deref(), home.as_deref())
}

/// Persist an edited configuration only after the full loader accepts it: the
/// candidate is written next to the config so relative paths resolve
/// identically, validated with Config::load, then swapped into place.
fn write_config_validated(config_path: &Path, text: &str) -> Result<()> {
    comradex::config::write_validated(config_path, text)
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

fn prompt_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    io::stdout().flush()?;
    let mut line = String::new();
    if io::stdin().read_line(&mut line)? == 0 {
        bail!("setup cancelled (end of input)")
    }
    Ok(line.trim().to_owned())
}

fn managed_provider(account: &comradex::config::AccountConfig) -> Result<bool> {
    match account {
        comradex::config::AccountConfig::ClaudeHome { .. } => Ok(true),
        comradex::config::AccountConfig::CodexHome { .. } => Ok(false),
        _ => bail!("this account uses the requesting client's login; add a managed account first"),
    }
}

fn new_account(
    config_path: &Path,
    name: Option<String>,
    claude: bool,
    codex: bool,
    pool: Option<String>,
) -> Result<()> {
    if !io::stdin().is_terminal() {
        bail!(
            "account new is interactive; run it in a terminal, or use account add <name> --browser"
        )
    }
    load_config(config_path)?;
    let name = match name {
        Some(name) => name,
        None => prompt_line("Account name: ")?,
    };
    comradex::accounts::validate_name(&name)?;
    let claude = if claude || codex {
        claude
    } else {
        loop {
            match prompt_line("Provider [codex/claude]: ")?
                .to_ascii_lowercase()
                .as_str()
            {
                "codex" | "chatgpt" => break false,
                "claude" => break true,
                _ => println!("Enter codex or claude."),
            }
        }
    };
    let pool = pool.unwrap_or_else(|| if claude { "claude" } else { "default" }.into());
    // Validate before asking the user to create a provider account. Re-read after
    // their browser session so unrelated config edits during setup survive.
    let candidate = account_add_text(config_path, &name, &pool, claude)?;
    let mut validation = tempfile::NamedTempFile::new_in(
        fs::canonicalize(config_path)?
            .parent()
            .context("config parent")?,
    )?;
    validation.write_all(candidate.as_bytes())?;
    Config::load(validation.path())?;
    let browser = comradex::browser::AccountBrowser::prepare(config_path, &name)?;
    browser.open(comradex::browser::provider_url(claude))?;
    println!("Create the provider account in this window and choose any subscription you need.");
    println!("Keep this window signed in. Its cookies will be reused for {name}.");
    prompt_line(
        "When the account is ready, press Enter to connect it to Comradex (Ctrl-C to stop): ",
    )?;
    let updated = account_add_text(config_path, &name, &pool, claude)?;
    write_config_validated(config_path, &updated)?;
    println!(
        "added account {name} to pool {pool}; resume with `comradex account login {name}` if login is interrupted"
    );
    reload_daemon()?;
    login(config_path, &name, Some(&browser))?;
    println!("account {name} is ready in pool {pool}");
    Ok(())
}

fn account_add_text(config_path: &Path, name: &str, pool: &str, claude: bool) -> Result<String> {
    let text = fs::read_to_string(config_path)?;
    if claude {
        comradex::accounts::add_claude_account(&text, name, pool)
    } else {
        comradex::accounts::add_account(&text, name, pool)
    }
}

fn login(
    config_path: &Path,
    account_name: &str,
    browser: Option<&comradex::browser::AccountBrowser>,
) -> Result<()> {
    let config = load_config(config_path)?;
    let account = config
        .accounts
        .get(account_name)
        .with_context(|| format!("unknown account {account_name}"))?;
    if let comradex::config::AccountConfig::ClaudeHome { path } = account {
        let launcher = browser.map(|browser| browser.launcher()).transpose()?;
        let launcher_path = launcher.as_ref().map(|dir| dir.path().join("browser"));
        return service::while_daemon_stopped(|| {
            comradex::claude::auth::login_with_browser(path, launcher_path.as_deref())
        });
    }
    let comradex::config::AccountConfig::CodexHome { path } = account else {
        bail!(
            "this uses the requesting client's login; use account connect to link an existing login"
        )
    };
    if let Some(browser) = browser {
        browser.open(comradex::browser::CODEX_DEVICE_URL)?;
        println!("Enter the device code below in this account's browser window.");
    }
    service::while_daemon_stopped(|| {
        login_managed_home_with(path, |path| {
            let status = Command::new("codex")
                .args(comradex::accounts::CODEX_DEVICE_LOGIN_ARGS)
                .env("CODEX_HOME", path)
                .status()
                .context("launch codex device login")?;
            if !status.success() {
                bail!("codex login exited with {status}")
            }
            Ok(())
        })
    })
}

fn login_managed_home_with(path: &Path, run_login: impl FnOnce(&Path) -> Result<()>) -> Result<()> {
    fs::create_dir_all(path)?;
    let _guard = HomeAuthLock::acquire(path)?;
    run_login(path)?;
    comradex::auth::validate_existing_login(path)
        .context("Codex login finished without a readable file-based ChatGPT login")
}

fn state_dir(config: &Config) -> PathBuf {
    config.proxy.state_dir.clone().expect("filled by load")
}

#[cfg(test)]
mod tests {
    #[test]
    fn reset_cli_requires_target_credit_and_explicit_confirmation() {
        assert!(
            Cli::try_parse_from([
                "comradex",
                "account",
                "use-reset",
                "work",
                "--credit-id",
                "one"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from(["comradex", "account", "use-reset", "work", "--confirm"]).is_err()
        );
        assert!(
            Cli::try_parse_from([
                "comradex",
                "account",
                "use-reset",
                "work",
                "--credit-id",
                "one",
                "--confirm",
                "--request-id",
                "stable"
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from(["comradex", "account", "reset-credits", "work", "--json"]).is_ok()
        );
    }

    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn init_links_existing_login_and_falls_back_when_unavailable() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("custom-home");
        fs::create_dir_all(&home).unwrap();
        fs::write(
            home.join("auth.json"),
            r#"{"tokens":{"access_token":"test-access-token"}}"#,
        )
        .unwrap();
        let connected = directory.path().join("connected.toml");
        init_with_codex_home(&connected, Some(&home)).unwrap();
        let config = load_config(&connected).unwrap();
        assert!(
            matches!(&config.accounts["app"], comradex::config::AccountConfig::CodexHome { path } if path == &fs::canonicalize(&home).unwrap())
        );
        assert_eq!(config.pools["default"].members, vec!["app"]);
        assert_eq!(config.pools["default"].preferred, None);
        for (index, home) in [
            None,
            Some(directory.path()),
            Some(directory.path().join("missing").as_path()),
        ]
        .into_iter()
        .enumerate()
        {
            let fallback = directory.path().join(format!("fallback-{index}.toml"));
            init_with_codex_home(&fallback, home).unwrap();
            assert!(matches!(
                load_config(&fallback).unwrap().accounts["app"],
                comradex::config::AccountConfig::Inbound
            ));
        }
    }

    #[test]
    fn account_connect_accepts_custom_codex_home() {
        let cli = Cli::try_parse_from([
            "comradex",
            "account",
            "connect",
            "app",
            "--codex-home",
            "/tmp/custom-codex",
        ])
        .unwrap();
        assert!(
            matches!(cli.command, CommandName::Account { command: AccountCommand::Connect { name, codex_home: Some(path) } } if name == "app" && path == Path::new("/tmp/custom-codex"))
        );
    }

    #[test]
    fn remove_purge_rejects_external_login_before_changing_configuration() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("existing-codex");
        fs::create_dir_all(&home).unwrap();
        let auth = r#"{"tokens":{"access_token":"test-access-token"}}"#;
        fs::write(home.join("auth.json"), auth).unwrap();
        let config_path = directory.path().join("comradex.toml");
        init_with_codex_home(&config_path, Some(&home)).unwrap();
        let original = fs::read_to_string(&config_path).unwrap();
        let updated = comradex::accounts::add_account(&original, "other", "default").unwrap();
        write_config_validated(&config_path, &updated).unwrap();
        let error = account_command(
            &config_path,
            AccountCommand::Remove {
                name: "app".to_owned(),
                purge: true,
            },
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("cannot purge an external Codex login")
        );
        assert_eq!(fs::read_to_string(&config_path).unwrap(), updated);
        assert_eq!(fs::read_to_string(home.join("auth.json")).unwrap(), auth);
    }
    use std::ffi::OsString;

    #[test]
    fn available_bearer_reports_renewal_login_separately() {
        let status = comradex::routing::AccountRoutingStatus {
            available: true,
            reauth_required: true,
            ..Default::default()
        };
        assert_eq!(
            account_availability(&status),
            "; sign-in needed for renewal (current access still usable)"
        );
    }

    #[test]
    fn account_status_shows_remaining_quota_instead_of_signed_in() {
        let account = comradex::config::AccountConfig::CodexHome {
            path: PathBuf::from("managed"),
        };
        let status = comradex::routing::AccountRoutingStatus {
            available: true,
            usage_windows: BTreeMap::from([
                (
                    "primary".to_owned(),
                    comradex::routing::QuotaWindowStatus {
                        used_percent: Some(19),
                        limit_window_seconds: Some(18_000),
                        ..Default::default()
                    },
                ),
                (
                    "secondary".to_owned(),
                    comradex::routing::QuotaWindowStatus {
                        used_percent: Some(81),
                        limit_window_seconds: Some(604_800),
                        ..Default::default()
                    },
                ),
            ]),
            ..Default::default()
        };

        assert_eq!(
            account_status_state(&account, Some(&status)),
            "5h 81% left; 7d 19% left"
        );
    }

    #[test]
    fn account_status_authentication_error_hides_stale_usage() {
        let account = comradex::config::AccountConfig::CodexHome {
            path: PathBuf::from("managed"),
        };
        let status = comradex::routing::AccountRoutingStatus {
            available: false,
            unavailable_reason: Some("needs_login".to_owned()),
            usage_percent: Some(12),
            ..Default::default()
        };

        assert_eq!(
            account_status_state(&account, Some(&status)),
            "sign-in required"
        );
    }

    #[test]
    fn managed_login_holds_auth_lock_for_entire_child_action() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");

        login_managed_home_with(&home, |child_home| {
            assert!(HomeAuthLock::try_acquire(child_home)?.is_none());
            fs::write(
                child_home.join("auth.json"),
                r#"{"tokens":{"access_token":"test-access-token"}}"#,
            )?;
            Ok(())
        })
        .unwrap();

        assert!(HomeAuthLock::try_acquire(&home).unwrap().is_some());
    }

    #[test]
    fn failed_managed_login_releases_auth_lock() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");

        let error = login_managed_home_with(&home, |_| bail!("simulated child failure"))
            .expect_err("child failure should propagate");

        assert!(error.to_string().contains("simulated child failure"));
        assert!(HomeAuthLock::try_acquire(&home).unwrap().is_some());
    }

    #[test]
    fn managed_login_rejects_success_without_readable_chatgpt_credentials() {
        for contents in [
            None,
            Some("{}"),
            Some("not-json"),
            Some(r#"{"OPENAI_API_KEY":"private-test-key"}"#),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let home = directory.path().join("managed");
            let error = login_managed_home_with(&home, |child_home| {
                if let Some(contents) = contents {
                    fs::write(child_home.join("auth.json"), contents)?;
                }
                Ok(())
            })
            .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("without a readable file-based ChatGPT login")
            );
            assert!(!format!("{error:#}").contains("private-test-key"));
            assert!(HomeAuthLock::try_acquire(&home).unwrap().is_some());
        }
    }

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

    #[test]
    fn account_prefer_cli_accepts_an_account_or_clear_but_not_both() {
        let cli =
            Cli::try_parse_from(["comradex", "account", "prefer", "work", "--pool", "p"]).unwrap();
        assert!(matches!(
            cli.command,
            CommandName::Account {
                command: AccountCommand::Prefer {
                    name: Some(name),
                    pool,
                    clear: false,
                },
            } if name == "work" && pool == "p"
        ));

        let cli = Cli::try_parse_from(["comradex", "account", "prefer", "--clear"]).unwrap();
        assert!(matches!(
            cli.command,
            CommandName::Account {
                command: AccountCommand::Prefer {
                    name: None,
                    pool,
                    clear: true,
                },
            } if pool == "default"
        ));

        assert!(Cli::try_parse_from(["comradex", "account", "prefer", "work", "--clear"]).is_err());
        assert!(Cli::try_parse_from(["comradex", "account", "prefer"]).is_err());
    }

    #[test]
    fn account_preserve_cli_accepts_an_account_or_clear_but_not_both() {
        let cli = Cli::try_parse_from(["comradex", "account", "preserve", "work", "--pool", "p"])
            .unwrap();
        assert!(matches!(
            cli.command,
            CommandName::Account {
                command: AccountCommand::Preserve {
                    name: Some(name),
                    pool,
                    clear: false,
                },
            } if name == "work" && pool == "p"
        ));

        let cli = Cli::try_parse_from(["comradex", "account", "preserve", "--clear"]).unwrap();
        assert!(matches!(
            cli.command,
            CommandName::Account {
                command: AccountCommand::Preserve {
                    name: None,
                    pool,
                    clear: true,
                },
            } if pool == "default"
        ));

        assert!(
            Cli::try_parse_from(["comradex", "account", "preserve", "work", "--clear"]).is_err()
        );
        assert!(Cli::try_parse_from(["comradex", "account", "preserve"]).is_err());
    }

    #[test]
    fn service_start_is_a_first_class_command() {
        let cli = Cli::try_parse_from(["comradex", "service", "start"]).unwrap();
        assert!(matches!(
            cli.command,
            CommandName::Service {
                command: ServiceCommand::Start
            }
        ));
    }

    #[test]
    fn service_logs_is_a_first_class_command() {
        let cli = Cli::try_parse_from(["comradex", "service", "logs"]).unwrap();
        assert!(matches!(
            cli.command,
            CommandName::Service {
                command: ServiceCommand::Logs
            }
        ));
    }

    #[test]
    fn durations_are_concise_for_status_output() {
        assert_eq!(human_duration(12), "12s");
        assert_eq!(human_duration(125), "2m 5s");
        assert_eq!(human_duration(7_500), "2h 5m");
    }
}
