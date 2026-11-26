use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use claude_codex_server::{
    backend::{BackendConfig, BackendProtocol},
    server::{Event, Server, ServerConfig},
    store::Store,
    transport,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    version = concat!("0.170.0 (claude-codex-server ", env!("CARGO_PKG_VERSION"), "; core protocol subset)"),
    name = "claude-codex-server",
    about = "Codex app-server protocol over Claude Code's streaming subprocess"
)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
    #[arg(long, global = true)]
    sock: Option<PathBuf>,
    /// Desktop bootstrap currently passes features.code_mode_host=true.
    #[arg(short = 'c', long = "config", global = true)]
    config: Vec<String>,
    #[arg(long, default_value = "stdio://", global = true)]
    listen: String,
    #[arg(long, global = true)]
    stdio: bool,
    #[arg(
        long,
        env = "CLAUDE_CODE_EXECUTABLE",
        default_value = "claude",
        global = true
    )]
    claude: PathBuf,
    /// Passed directly to Claude; repeat for each argument. Intended for deployment configuration.
    #[arg(long, allow_hyphen_values = true, global = true)]
    claude_arg: Vec<String>,
    /// Separate state directory; never points at or modifies CODEX_HOME.
    #[arg(long, env = "CLAUDE_CODEX_HOME", global = true)]
    state_dir: Option<PathBuf>,
    /// Default model: sonnet for Claude; the agent's own default for ACP agents.
    #[arg(long, global = true)]
    model: Option<String>,
    /// Serve an Agent Client Protocol (v1) agent instead of Claude, e.g. `opencode`.
    #[arg(long, global = true)]
    acp_agent: Option<PathBuf>,
    /// Passed to the ACP agent; repeat for each argument, e.g. `--acp-arg=acp`.
    #[arg(
        long,
        allow_hyphen_values = true,
        global = true,
        requires = "acp_agent"
    )]
    acp_arg: Vec<String>,
    /// Model provider id reported to Codex (default: the agent executable name).
    #[arg(long, global = true, requires = "acp_agent")]
    provider_id: Option<String>,
    /// Model provider display name (default: the provider id).
    #[arg(long, global = true, requires = "acp_agent")]
    provider_name: Option<String>,
    #[arg(long, default_value_t = 60, global = true)]
    initialize_timeout_seconds: u64,
    #[arg(long, default_value_t = 300, global = true)]
    approval_timeout_seconds: u64,
    /// Keep a thread's Claude process this long after a turn so the next turn
    /// reuses it and its prompt cache; 0 starts a new process for every turn.
    #[arg(long, default_value_t = 3600, global = true)]
    idle_session_timeout_seconds: u64,
    /// Idle Claude processes kept across all threads; the oldest closes first.
    #[arg(long, default_value_t = 8, global = true)]
    max_idle_sessions: usize,
    /// Claude model that automatically reviews ACP agent permission requests.
    #[arg(long, default_value = "sonnet", global = true)]
    auto_review_model: String,
    /// Deadline for one automatic review; it then fails closed.
    #[arg(long, default_value_t = 90, global = true)]
    auto_review_timeout_seconds: u64,
    /// Replace PATH with the one the user's interactive login shell configures,
    /// so a LaunchAgent-started server matches the user's terminal.
    #[arg(long, global = true)]
    login_shell_path: bool,
}

#[derive(Subcommand)]
enum Command {
    #[command(hide = true)]
    SandboxProxyConnect { host: String, port: u16 },
    /// Relay an ACP agent's stdio MCP connection to the desktop tool bridge.
    #[cfg(unix)]
    #[command(hide = true)]
    McpBridge {
        #[arg(long)]
        socket: PathBuf,
    },
    /// Run the Codex-compatible app-server (also the default without a command).
    AppServer {
        #[command(subcommand)]
        action: Option<AppServerAction>,
    },
    /// Install this binary and its isolated macOS desktop connection.
    #[cfg(unix)]
    Install {
        /// Skip the dedicated localhost SSH connection; the local desktop
        /// router then reaches Claude without Remote Login.
        #[arg(long)]
        no_ssh: bool,
        /// Native authentication context; upgrades preserve the saved choice.
        #[arg(long, value_enum)]
        auth_session: Option<claude_codex_server::install::AuthSession>,
    },
    /// Add selected ACP models to the existing local desktop router.
    #[cfg(unix)]
    LocalAcp {
        /// Expose another model from the same agent. Repeat to include more.
        #[arg(long = "include-model", value_name = "MODEL")]
        include_models: Vec<String>,
    },
    /// Add MLX-Serve models to the existing picker using native Codex tools.
    #[cfg(unix)]
    Mlx {
        /// MLX-Serve's OpenAI-compatible API base URL.
        #[arg(long, default_value = claude_codex_server::local_provider::MLX_URL)]
        url: String,
    },
    /// Add a self-hosted Responses-compatible endpoint to the local picker.
    #[cfg(unix)]
    LocalProvider {
        /// OpenAI-compatible base URL; the server must implement /responses.
        #[arg(long)]
        url: String,
        #[arg(long, default_value = "Self-hosted")]
        name: String,
        /// Working context window for models whose server does not advertise one.
        #[arg(long)]
        context_window: Option<u64>,
        /// Environment variable containing the server's API key (never the key itself).
        #[arg(long)]
        env_key: Option<String>,
        /// Server implements Codex custom tools and client-side tool search.
        #[arg(long)]
        codex_tools: bool,
        /// Server supports the Responses reasoning-effort parameter.
        #[arg(long)]
        reasoning: bool,
    },
    /// Check executables, service, local SSH, and native Claude authentication.
    #[cfg(unix)]
    Doctor,
    /// Manage the per-user macOS LaunchAgent.
    #[cfg(unix)]
    Service {
        #[arg(value_enum)]
        action: claude_codex_server::install::ServiceAction,
        /// Explicitly allow stopping active conversations.
        #[arg(long)]
        force: bool,
    },
    /// Stand in for genuine Codex as the desktop's local CLI. The desktop's
    /// app-server launch is routed between Codex and the Claude facade
    /// (reached through --sock); every other invocation runs Codex unchanged.
    #[cfg(unix)]
    Route {
        /// The genuine Codex executable the desktop would otherwise run.
        #[arg(long, env = "CLAUDE_CODEX_ROUTER_CODEX")]
        codex: PathBuf,
        /// Also append router diagnostics to this file.
        #[arg(long)]
        log: Option<PathBuf>,
        /// Codex command-line arguments exactly as the desktop passed them.
        #[arg(last = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Remove the service and managed SSH entries, keeping conversations and keys.
    #[cfg(unix)]
    Uninstall {
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum AppServerAction {
    /// Proxy bytes to the private desktop socket; does not open the state store.
    Proxy,
}

fn main() -> Result<()> {
    let args = Args::parse();
    #[cfg(unix)]
    if let Some(Command::Route {
        codex,
        args: codex_args,
        ..
    }) = &args.command
        && !claude_codex_server::router::is_app_server_launch(codex_args)
    {
        use std::os::unix::process::CommandExt;
        // Replace this process so exit status, signals, and stdio are Codex's own.
        let error = std::process::Command::new(codex).args(codex_args).exec();
        return Err(error).with_context(|| format!("Cannot run {}", codex.display()));
    }
    // Children inherit PATH, so set it before the runtime starts other threads.
    if args.login_shell_path {
        match claude_codex_server::shell_env::login_shell()
            .context("Cannot determine the login shell")
            .and_then(|shell| claude_codex_server::shell_env::login_path(&shell))
        {
            // SAFETY: single-threaded; no runtime or other thread exists yet.
            Ok(path) => unsafe { std::env::set_var("PATH", path) },
            Err(error) => eprintln!("Keeping the inherited PATH: {error:#}"),
        }
    }
    #[cfg(target_os = "macos")]
    let codex_connection = if let Some(Command::Route {
        codex,
        args: codex_args,
        ..
    }) = &args.command
    {
        // SAFETY: this is before the runtime or any other threads are started.
        unsafe { claude_codex_server::router::prepare_local_launch(codex, codex_args)? }
    } else {
        None
    };
    #[cfg(all(unix, not(target_os = "macos")))]
    let codex_connection = None;
    #[cfg(unix)]
    let router_child = codex_connection.is_some();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(
        args,
        #[cfg(unix)]
        codex_connection,
    ));
    // A router child must exit when Codex exits even if the desktop still has
    // stdin open. Tokio's blocking stdin reader otherwise prevents shutdown.
    #[cfg(unix)]
    if router_child {
        runtime.shutdown_timeout(Duration::from_secs(1));
    }
    result
}

async fn run(
    args: Args,
    #[cfg(unix)] codex_connection: Option<std::os::unix::net::UnixStream>,
) -> Result<()> {
    let acp = args
        .acp_agent
        .clone()
        .map(|executable| {
            claude_codex_server::acp::AcpAgent::new(
                executable,
                args.acp_arg.clone(),
                args.provider_id.clone(),
                args.provider_name.clone(),
                args.model.clone(),
                args.initialize_timeout_seconds,
            )
        })
        .transpose()?;
    if let Some(Command::SandboxProxyConnect { host, port }) = &args.command {
        return claude_codex_server::ssh_proxy::connect(host, *port);
    }
    #[cfg(unix)]
    if let Some(Command::McpBridge { socket }) = &args.command {
        return claude_codex_server::acp::run_bridge(socket).await;
    }
    #[cfg(unix)]
    match &args.command {
        Some(Command::Mlx { url }) => {
            let mut config = claude_codex_server::local_provider::Config::new(url, "MLX-Serve");
            config.reasoning = true;
            return claude_codex_server::install::Installation::current(None)?
                .configure_local_provider(config)
                .await;
        }
        Some(Command::LocalProvider {
            url,
            name,
            context_window,
            env_key,
            codex_tools,
            reasoning,
        }) => {
            let mut config = claude_codex_server::local_provider::Config::new(url, name);
            config.context_window = *context_window;
            config.env_key = env_key.clone();
            config.codex_tools = *codex_tools;
            config.reasoning = *reasoning;
            config.selected_models = args.model.clone().into_iter().collect();
            return claude_codex_server::install::Installation::current(None)?
                .configure_local_provider(config)
                .await;
        }
        Some(Command::LocalAcp { include_models }) => {
            let agent = acp.context("Pass --acp-agent and --model for the desktop connection")?;
            return claude_codex_server::install::Installation::current(None)?
                .configure_local_acp(agent, include_models.clone())
                .await;
        }
        Some(Command::Install {
            no_ssh,
            auth_session,
        }) => {
            return claude_codex_server::install::Installation::current(acp.clone())?
                .install(&args.claude, !no_ssh, *auth_session)
                .await;
        }
        Some(Command::Doctor) => {
            return claude_codex_server::install::Installation::current(acp.clone())?
                .doctor(&args.claude)
                .await;
        }
        Some(Command::Service { action, force }) => {
            return claude_codex_server::install::Installation::current(acp.clone())?
                .service(*action, *force)
                .await;
        }
        Some(Command::Route {
            codex,
            log,
            args: codex_args,
        }) => {
            let socket = match &args.sock {
                Some(path) => Some(path.clone()),
                None => default_socket().ok(),
            };
            return claude_codex_server::router::run(
                codex,
                codex_args,
                socket,
                log.clone(),
                codex_connection,
            )
            .await;
        }
        Some(Command::Uninstall { force }) => {
            return claude_codex_server::install::Installation::current(acp.clone())?
                .uninstall(*force)
                .await;
        }
        _ => {}
    }
    // This flag enables a Codex-specific host service; it does not change Claude
    // execution. Reject other overrides instead of silently discarding settings.
    for config in &args.config {
        if config != "features.code_mode_host=true" {
            bail!("Unsupported Codex config override: {config}");
        }
    }
    if matches!(
        args.command,
        Some(Command::AppServer {
            action: Some(AppServerAction::Proxy)
        })
    ) {
        let socket = match args.sock {
            Some(path) => path,
            None => default_socket()?,
        };
        return proxy(socket).await;
    }
    if args.sock.is_some() {
        bail!("--sock requires app-server proxy");
    }
    #[cfg(unix)]
    let local_acp = if !args.stdio && args.listen == "unix://" && acp.is_none() {
        let socket = default_socket()?;
        claude_codex_server::local_acp::LocalAcp::root_for_socket(&socket).and_then(|root| {
            match claude_codex_server::local_acp::LocalAcp::read(root) {
                Ok(config) => config.map(|config| (root.to_owned(), config)),
                Err(error) => {
                    eprintln!("Local ACP configuration is unavailable: {error:#}");
                    None
                }
            }
        })
    } else {
        None
    };
    let endpoint = if args.stdio {
        "stdio://".to_owned()
    } else if args.listen == "unix://" {
        let path = default_socket()?;
        std::fs::create_dir_all(path.parent().unwrap())?;
        format!("unix://{}", path.display())
    } else {
        args.listen
    };
    // Each agent keeps its own threads; one process owns a state directory.
    let state_name = match &acp {
        Some(agent) => format!("claude-codex-server-{}", agent.provider_id),
        None => "claude-codex-server".to_owned(),
    };
    let state = args.state_dir.unwrap_or_else(|| {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".local/state")
            .join(&state_name)
    });
    // Claude threads use Claude's native auto mode; ACP agents are reviewed by Claude.
    let reviewer = acp
        .is_some()
        .then(|| claude_codex_server::auto_review::ReviewerConfig {
            executable: args.claude.clone(),
            model: args.auto_review_model.clone(),
            timeout: Duration::from_secs(args.auto_review_timeout_seconds),
        });
    let (backend, default_model) = match acp {
        Some(agent) => {
            if !args.claude_arg.is_empty() {
                bail!("--claude-arg does not apply to an ACP agent; use --acp-arg");
            }
            claude_codex_server::protocol::set_provider(claude_codex_server::protocol::Provider {
                id: agent.provider_id,
                name: agent.provider_name,
            });
            (
                BackendConfig {
                    executable: agent.executable,
                    extra_args: agent.args,
                    initialize_timeout: Duration::from_secs(args.initialize_timeout_seconds),
                    protocol: BackendProtocol::Acp,
                },
                agent
                    .model
                    .unwrap_or_else(|| claude_codex_server::acp::DEFAULT_MODEL.into()),
            )
        }
        None => (
            BackendConfig {
                executable: args.claude,
                extra_args: args.claude_arg,
                initialize_timeout: Duration::from_secs(args.initialize_timeout_seconds),
                protocol: BackendProtocol::Claude,
            },
            args.model.unwrap_or_else(|| "sonnet".into()),
        ),
    };
    let config = ServerConfig {
        backend,
        default_cwd: std::env::current_dir()?,
        default_model,
        approval_timeout: Duration::from_secs(args.approval_timeout_seconds),
        idle_session_timeout: Duration::from_secs(args.idle_session_timeout_seconds),
        max_idle_sessions: args.max_idle_sessions,
        reviewer,
    };
    let store = Store::open(state)?;
    let (events, receiver) = mpsc::channel(512);
    let server = Server::new(config, store, events.clone())?;
    // Register handlers before a listener can advertise readiness.
    let termination = termination()?;
    let engine = tokio::spawn(server.run(receiver));
    let shutdown = CancellationToken::new();
    #[cfg(unix)]
    let local_acp =
        local_acp.map(|(root, config)| tokio::spawn(config.supervise(root, shutdown.clone())));
    let result = tokio::select! {
        result = transport::serve(&endpoint, events.clone(), shutdown.clone()) => result,
        _ = termination => Ok(()),
    };
    shutdown.cancel();
    let _ = events.send(Event::Shutdown).await;
    let _ = engine.await;
    #[cfg(unix)]
    if let Some(worker) = local_acp {
        let _ = worker.await;
    }
    result
}

fn default_socket() -> Result<PathBuf> {
    // Deliberately do not default to CODEX_HOME: the facade must never bind the
    // real Codex control socket, even when launched without deployment settings.
    let home = std::env::var_os("CLAUDE_CODEX_SOCKET_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Set CLAUDE_CODEX_SOCKET_DIR to a dedicated absolute directory for unix:// or proxy"
            )
        })?;
    if !home.is_absolute() {
        bail!("CLAUDE_CODEX_SOCKET_DIR must be absolute");
    }
    Ok(home.join("app-server-control.sock"))
}

#[cfg(unix)]
async fn proxy(path: PathBuf) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let stream = tokio::net::UnixStream::connect(path).await?;
    let (mut read, mut write) = stream.into_split();
    let input = async {
        tokio::io::copy(&mut tokio::io::stdin(), &mut write).await?;
        match write.shutdown().await {
            // The WebSocket peer may have completed its close handshake before
            // stdin reaches EOF. That is a normal proxy shutdown on macOS.
            Err(error) if error.kind() == std::io::ErrorKind::NotConnected => Ok(()),
            result => result,
        }
    };
    let output = async {
        let mut stdout = tokio::io::stdout();
        tokio::io::copy(&mut read, &mut stdout).await?;
        stdout.flush().await
    };
    tokio::try_join!(input, output)?;
    Ok(())
}

#[cfg(not(unix))]
async fn proxy(_path: PathBuf) -> Result<()> {
    bail!("Desktop SSH proxy requires Unix sockets")
}

fn termination() -> Result<impl std::future::Future<Output = ()>> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("Register SIGTERM handler")?;
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("Register SIGINT handler")?;
    Ok(async move {
        #[cfg(unix)]
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    })
}
