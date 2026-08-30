use std::{
    fs,
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

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

/// One process-shutdown budget shared by async cleanup and Tokio runtime
/// destruction. Calling `begin` more than once never extends the deadline.
struct ShutdownBudget {
    timeout: Duration,
    deadline: Option<tokio::time::Instant>,
}

impl ShutdownBudget {
    fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            deadline: None,
        }
    }

    fn begin(&mut self) -> tokio::time::Instant {
        self.begin_at(tokio::time::Instant::now())
    }

    fn begin_at(&mut self, now: tokio::time::Instant) -> tokio::time::Instant {
        *self.deadline.get_or_insert(now + self.timeout)
    }

    fn remaining(&self) -> Duration {
        self.remaining_at(tokio::time::Instant::now())
    }

    fn remaining_at(&self, now: tokio::time::Instant) -> Duration {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or(self.timeout)
    }
}

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
        #[arg(long, default_value = "default")]
        pool: String,
        /// Skip the interactive sign-in (run `comradex account login <name>` later)
        #[arg(long)]
        no_login: bool,
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
    /// Sign an account in through the official Codex device flow
    Login { name: String },
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
    /// Start an installed service without interrupting it when already running
    Start,
    Uninstall,
    Status,
    /// Restart the daemon so it reloads comradex.toml (needed after config
    /// edits such as adding an account)
    Restart,
}

fn main() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("build Tokio runtime")?;
    let mut shutdown_budget = ShutdownBudget::new(SHUTDOWN_TIMEOUT);
    let result = runtime.block_on(run(&mut shutdown_budget));

    // Dropping a Tokio runtime can otherwise wait indefinitely for
    // `spawn_blocking` work. Use whatever remains of the same absolute
    // shutdown budget used by `serve`'s async cleanup.
    runtime.shutdown_timeout(shutdown_budget.remaining());
    result
}

async fn run(shutdown_budget: &mut ShutdownBudget) -> Result<()> {
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
        CommandName::Serve => serve(&config_path, shutdown_budget).await,
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
    fs::write(path, text)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    println!("created {}", path.display());
    Ok(())
}

async fn serve(path: &Path, shutdown_budget: &mut ShutdownBudget) -> Result<()> {
    let config_path =
        fs::canonicalize(path).with_context(|| format!("resolve config {}", path.display()))?;
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
    let control_server = control::ControlServer::bind(
        &state,
        config_path,
        config.clone(),
        router.clone(),
        stats.clone(),
    )?;
    let mut control_task = tokio::spawn(control_server.run());
    let app = App::new(config.clone(), router.clone(), stats.clone()).await?;
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
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
    let refresh_app = app.clone();
    let refresh_background = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(
            comradex::auth::PROACTIVE_REFRESH_INTERVAL_SECONDS,
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            refresh_app.refresh_managed_accounts_at(now).await;
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
    let shutdown_deadline = shutdown_budget.begin();
    background.abort();
    if !control_task.is_finished() {
        control_task.abort();
    }
    refresh_background.abort();
    tasks.abort_all();
    let shutdown_result = tokio::time::timeout_at(shutdown_deadline, async {
        let _ = background.await;
        let _ = control_task.await;
        let _ = refresh_background.await;
        while tasks.join_next().await.is_some() {}

        app.shutdown_connections().await;
        router.clear_inflight().await;

        // Attempt every final snapshot even if an earlier one fails so the
        // remaining shutdown diagnostics are still as fresh as possible.
        let mut first_error = None;
        if let Err(error) = router.affinity.flush().await {
            warn!(error = %error, "final affinity snapshot failed");
            first_error = Some(error.context("final affinity snapshot failed"));
        }
        if let Err(error) = app.flush_file_owners().await {
            warn!(error = %error, "final file-owner snapshot failed");
            if first_error.is_none() {
                first_error = Some(error.context("final file-owner snapshot failed"));
            }
        }
        if let Err(error) = stats.write(state.join("stats.json"), &router).await {
            warn!(error = %error, "final stats snapshot failed");
            if first_error.is_none() {
                first_error = Some(error.context("final stats snapshot failed"));
            }
        }
        first_error
    })
    .await;
    let shutdown_error = match shutdown_result {
        Ok(error) => error,
        Err(_) => {
            warn!(
                timeout_seconds = SHUTDOWN_TIMEOUT.as_secs(),
                "shutdown deadline exceeded; exiting without waiting for remaining cleanup"
            );
            Some(anyhow::anyhow!(
                "shutdown did not complete within {} seconds",
                SHUTDOWN_TIMEOUT.as_secs()
            ))
        }
    };
    if let Some(error) = listener_error {
        return Err(error);
    }
    if let Some(error) = shutdown_error {
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
        ServiceCommand::Start => {
            service::start()?;
            println!("service is running");
        }
        ServiceCommand::Uninstall => match service::uninstall()? {
            Some(path) => println!("stopped service and removed {}", path.display()),
            None => println!("service is not installed"),
        },
        ServiceCommand::Status => {
            if service::status()? {
                println!("service is running");
            } else if service::installed()? {
                println!("service is installed but not running");
                if let Some(stderr) = service::last_stderr_line()? {
                    println!(
                        "last stderr line{}: {}",
                        stderr_timestamp_suffix(stderr.log_modified_at_unix),
                        stderr.line
                    );
                }
                println!("run `comradex service start`");
            } else {
                println!("service is not installed (run `comradex service install`)");
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
        Some(record) => println!(
            "codex    routed through Comradex via {}",
            record.codex_config.display()
        ),
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
        let availability = routing
            .and_then(|routing| routing.account_states.get(name))
            .map(account_availability)
            .unwrap_or_default();
        println!(
            "  {name:width$}  {:24}  {pools}{availability}",
            account_state(account)
        );
    }

    println!("\npools");
    let width = config.pools.keys().map(String::len).max().unwrap_or(0);
    for (name, pool) in &config.pools {
        let preferred = routing
            .and_then(|routing| routing.preferred_accounts.get(name))
            .or(pool.preferred.as_ref())
            .map_or("automatic", String::as_str);
        let active = routing
            .and_then(|routing| routing.active_accounts.get(name))
            .map_or("no fresh work yet", String::as_str);
        println!("  {name:width$}  preferred {preferred}, active {active}");
    }

    println!("\ntraffic");
    match snapshot {
        Some(stats) => {
            println!(
                "  {} request(s) in flight, {} open connection(s)",
                stats.inflight_http, stats.open_upgrades
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
            if stats.refresh_last_sweep_unix > 0 {
                println!(
                    "  last refresh sweep unix {}, last successful refresh unix {}",
                    stats.refresh_last_sweep_unix, stats.refresh_last_success_unix
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
        return String::new();
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
        comradex::config::AccountConfig::Inbound => "Codex App login".to_owned(),
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
                println!("run `comradex account login {name}` to sign the account in");
            } else {
                login(config_path, &name)?;
            }
            Ok(())
        }
        AccountCommand::Login { name } => login(config_path, &name),
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

fn login(config_path: &Path, account_name: &str) -> Result<()> {
    let config = load_config(config_path)?;
    let account = config
        .accounts
        .get(account_name)
        .with_context(|| format!("unknown account {account_name}"))?;
    let comradex::config::AccountConfig::CodexHome { path } = account else {
        bail!("this is the Codex App's own login and cannot be logged in here")
    };
    service::while_daemon_stopped(|| {
        login_managed_home_with(path, |path| {
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
        })
    })
}

fn login_managed_home_with(path: &Path, run_login: impl FnOnce(&Path) -> Result<()>) -> Result<()> {
    fs::create_dir_all(path)?;
    let _guard = HomeAuthLock::acquire(path)?;
    run_login(path)
}

fn state_dir(config: &Config) -> PathBuf {
    config.proxy.state_dir.clone().expect("filled by load")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn managed_login_holds_auth_lock_for_entire_child_action() {
        let directory = tempfile::tempdir().unwrap();
        let home = directory.path().join("managed");

        login_managed_home_with(&home, |child_home| {
            assert!(HomeAuthLock::try_acquire(child_home)?.is_none());
            Ok(())
        })
        .unwrap();

        assert!(HomeAuthLock::try_acquire(&home).unwrap().is_some());
    }

    #[test]
    fn shutdown_budget_uses_one_absolute_deadline() {
        let start = tokio::time::Instant::now();
        let mut budget = ShutdownBudget::new(Duration::from_secs(15));

        assert_eq!(budget.remaining_at(start), Duration::from_secs(15));
        let deadline = budget.begin_at(start);
        assert_eq!(deadline, start + Duration::from_secs(15));
        assert_eq!(
            budget.begin_at(start + Duration::from_secs(10)),
            deadline,
            "starting another shutdown phase must not extend the deadline"
        );
        assert_eq!(
            budget.remaining_at(start + Duration::from_secs(4)),
            Duration::from_secs(11)
        );
        assert_eq!(
            budget.remaining_at(start + Duration::from_secs(20)),
            Duration::ZERO
        );
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
    fn durations_are_concise_for_status_output() {
        assert_eq!(human_duration(12), "12s");
        assert_eq!(human_duration(125), "2m 5s");
        assert_eq!(human_duration(7_500), "2h 5m");
    }
}
