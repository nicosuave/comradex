# Codex desktop through a dedicated localhost SSH connection

The macOS installer copies the facade to a stable per-user location and creates a
login-session LaunchAgent, a dedicated localhost SSH identity, and the
`claude-codex-local` SSH alias. Claude runs in the GUI login session, where its
normal authentication is available. SSH transports the desktop connection.

## Install

You need macOS, Codex desktop, and an authenticated
Claude Code CLI supporting `--system-prompt-snapshot` (tested with 2.1.294).
Run `claude auth login` in a local terminal if necessary. Enable **Remote Login**
for your user in **System Settings > General > Sharing**. The installer does not
enable Remote Login or change system settings. Run these commands as your normal
login user, without `sudo`:

```sh
brew install nicosuave/tap/claudex
claudex install
```

Homebrew installs the signed, notarized release binary; Rust is not required.
`claudex install` sets up the desktop service and connection and checks readiness.
For optional diagnostics afterward, run `claudex doctor`; it does not install or
upgrade anything.
You can also unpack the matching archive from
[GitHub Releases](https://github.com/nicosuave/claudex/releases), verify its
`.sha256` checksum, and run `./claudex install`.

To build from source instead, install Rust with edition 2024 support and run:

```sh
cargo build --locked --release --bin claude-codex-server
./target/release/claude-codex-server install
```

Use `mbx build` in place of `cargo build` when Boxington is installed.
After installation, the checkout can move: the installed binary lives at
`~/Library/Application Support/claude-codex/bin/claude-codex-server`.

The installer resolves `claude` and genuine `codex` from PATH, with a fallback to
`/Applications/Codex.app/Contents/Resources/codex` for Codex. Use `--claude
/absolute/path/to/claude` when needed. `CLAUDE_CODEX_PLUGIN_EXECUTABLE` selects a
particular genuine Codex binary; `CLAUDE_CODEX_PLUGIN_HOME` selects its existing
home, normally `~/.codex`. These paths are recorded in the dedicated launchers.
The facade does not copy credentials or replace either CLI.

In **Codex Settings > Connections**, add or select **claude-codex-local**. Refresh
the picker if it was already open. Choose identity-file authentication and select
`~/Library/Application Support/claude-codex/id_ed25519` (the installer prints the
expanded path). Port is **22**; the display name can be **Claude**. Select this
connection for your project/chat and choose a Claude model, such as `opus`.
Do not substitute plain `localhost`, which can select your ordinary SSH identity.

### One local window without SSH

The desktop can instead run Claude and Codex side by side on its local
connection: Claude models appear in the same model picker and Claude chats in
the same sidebar. The desktop app is not modified. It honors `CODEX_CLI_PATH`
for its local Codex CLI, and the installer writes a stand-in at
`~/Library/Application Support/claude-codex/router/codex`. Use
`claudex install --no-ssh` if you don't want the SSH connection; Remote Login is
then not required.

Install sets `CODEX_CLI_PATH` to the stand-in for your login session with
`launchctl setenv`, and adds LaunchAgent `com.claude-codex.desktop-env` to set it
again at each login. The desktop then uses the stand-in however it is opened:
Dock, Spotlight, login item, or its own relaunch after an update. A desktop that
was already running keeps its old environment, so quit and reopen it once after
the first install. The Codex CLI doesn't read the variable, so terminals that
inherit it are unaffected. `claudex uninstall` removes the LaunchAgent and the
variable. ACP instances don't set it; launch the desktop with their stand-in
explicitly:

```sh
open -a ChatGPT --env CODEX_CLI_PATH="$HOME/Library/Application Support/claude-codex-<provider-id>/router/codex"
```

The stand-in runs the Codex CLI bundled with the desktop app,
unchanged, for every command except the desktop's own app-server launch. That
launch gets a router (`claudex route`) which:

- sends everything to genuine Codex, with your normal `~/.codex` home and login,
  except Claude models and Claude chats, which go to the Claude service;
- appends Claude models to Codex's model list (Codex keeps the default);
- merges chat lists from both, ordered by the requested sort key;
- moves a new, still-empty chat to Claude when its first message uses a Claude
  model (the desktop starts chats before you pick a model).

On macOS, the launcher replaces itself with genuine Codex and runs the router
alongside it over a private socket. Codex remains the desktop's direct child,
so native browser and app-tool services can validate its signed process ancestry.
The desktop, Codex executable, and browser authorization checks remain unchanged.

A chat stays on the backend it started with. Choosing a GPT model in a Claude
chat, or a Claude model in a Codex chat that already has messages, is refused
with a message asking you to start a new chat. If the Claude service restarts,
the router reconnects while keeping Codex running. Claude requests receive a
temporary reconnect error until it is ready; they are never sent to Codex or
automatically replayed. Existing threads with unknown ownership also wait for
the service so the router can identify their backend. New and already-known
Codex chats continue to work. Set `CLAUDE_CODEX_ROUTER_CODEX` to use a different
genuine Codex executable. See [the desktop router contract](docs/router-desktop-flow.md)
for creation, metadata, model-selection and reconnection behavior.

Prompt-bar predictions run through the local router and native Claude prediction
forks. See [prompt predictions](docs/prompt-predictions.md) for the compatibility
contract and verification. No desktop app patch is needed.

### ACP models in the local picker

With the local Claudex router already installed, sign in to the agent in its
own CLI and run:

```sh
claudex local-acp --acp-agent agy --acp-arg=acp \
  --provider-name Antigravity --model gemini-4-argon \
  --include-model gemini-4-neon --include-model gemini-4-hydrogen \
  --include-model gemini-pro-orchestrator-neon \
  --include-model gemini-pro-orchestrator-g1-pro
```

This updates the installed Claudex binary and restarts the existing login
service. That service supervises the ACP facade on a private local Unix socket.
The command uses the existing desktop launcher and needs neither SSH nor another
LaunchAgent. The ACP provider has its own saved threads.

Reopen Codex Desktop, start a new local chat, and select **Gemini 4 Argon**.
Choose **Full access** for the ACP chat. ACP agents execute their own tools and
Claudex cannot apply Codex's workspace sandbox to them. Codex stays the default in
the combined picker, and existing chats keep their backend. The picker exposes
the models selected through `--model` and repeated `--include-model` arguments
when the agent advertises them in its ACP catalog. Use ACP model identifiers.

To change the selection, rerun `claudex local-acp` with the desired models. To
disable local ACP, remove the installer's `local-acp.json` from
`~/Library/Application Support/claude-codex`, restart the idle service, and reopen
Codex Desktop. The ACP chat store remains under `state/local-acp`.

### Self-hosted models in the local picker

With the local Claudex router already installed and MLX-Serve running:

```sh
claudex mlx
# A different MLX-Serve endpoint:
claudex mlx --url http://127.0.0.1:11234/v1
# A generic Responses-compatible server (context is a fallback when not advertised):
claudex local-provider --url http://server:8080/v1 --name vLLM \
  --model local/qwen --context-window 32768
```

Reopen the desktop app, start a new local chat, and select a model marked
**(MLX-Serve)** or your provider's name. GPT, Claude and configured ACP models
remain in the same picker. Self-hosted inference uses genuine Codex's tools,
permissions and conversation storage; Codex connects directly to the server's
Responses API. These commands configure one self-hosted endpoint and do not
change Codex's global provider or `openai_base_url`, replace the ACP provider,
or restart the Claude service.

By default self-hosted chats expose ordinary Codex function tools and configured
MCP servers. Cloud app connectors and hosted web search are disabled for those
chats: ordinary Responses servers, including current MLX-Serve, need not
implement Codex's specialized deferred-tool/code-mode wire types. Eagerly
sending the full connector catalog makes local prefill impractical. Other models
retain their connector configuration. For a server that implements those custom
tools and client-side tool search, `--codex-tools` opts into native code mode and
retains the user's app settings. `--reasoning` enables reasoning-effort controls
when the server supports that parameter (the MLX-Serve preset enables it).

Discovery uses the standard `/models` list, advertised capabilities when
available, each model's working context window, and text/image modalities.
Servers without context metadata require `--context-window`; missing modality
metadata defaults to text. `--model` restricts discovery to that model, and
`--env-key VARIABLE_NAME` uses an explicitly configured API-key environment
variable without storing its value. The router refreshes metadata when listing
models. If the server is unavailable, the saved model list remains available and
the other providers continue working; local requests never fall back to GPT.

The configuration and generated catalog live in `local-provider.json` and
`local-provider-models.json` under `~/Library/Application Support/claude-codex`.
Routing identities for empty desktop chats moved to a self-hosted model live in
`local-provider-threads.json`;
retain this file to reopen those chats. Native Codex owns their actual history.
An empty chat can change providers before its first message. Once it has
messages, start a new chat to switch between self-hosted and another provider.

### Connect from another computer

On the remote Mac, run `claudex install`, then reconnect that Mac's
existing SSH connection in the client desktop. This enables the combined
Codex/Claude model picker over ordinary desktop SSH, using your existing SSH
authentication and normal Codex home. Local desktop routing alone does not
configure incoming SSH connections.

The installer adds a managed block to the remote user's zsh environment or bash
login profile. It selects the router only when the desktop launches its SSH
login-shell payload, preserves an existing `CODEX_INSTALL_DIR` override, and
skips the ordinary Codex daemon bootstrap. `codex app-server proxy` then opens
a stdio router connection to genuine Codex and the running Claude service.
Ordinary SSH shells and CLI commands keep their existing behavior. Remote routing
is included by default; `claudex uninstall` removes the managed shell block.
Remote installation currently supports zsh and bash login shells.

Use **Workspace write** and **Approve for me** for native Claude auto review.
Bash uses Claude's OS sandbox; native file tools use scoped permissions. Explicit
sandbox escapes use the selected automatic or manual reviewer. MCP integrations
and hooks remain trusted services outside that sandbox. Full access and manual approval modes are also
available. General read-only agent profiles are unsupported; desktop background
helpers can use single-turn ephemeral read-only sessions with all execution
tools and customizations disabled. See [compatibility](COMPATIBILITY.md).

### Development sandbox settings

To allow routine network access and writes to build caches, create
`~/Library/Application Support/claude-codex/state/workspace-defaults.json`:

```json
{
  "network_access": true,
  "writable_roots": ["/absolute/path/to/cache"]
}
```

For a custom `--state-dir`, place the file there instead. Roots must be existing
absolute directories; `~` is not expanded. The service reads this file at startup
and reports its grants through `config/read`. Restart the idle service and refresh
the desktop connection after editing it. New chats and explicit selections of the
Workspace preset use these defaults. Existing chats retain their saved policy
until you reselect the preset; explicit `sandboxPolicy` requests always win,
including `networkAccess: false`. Without this file, the restrictive defaults stay
unchanged. Do not put this host configuration in a repository's Claude settings.

In your user Claude settings (`~/.claude/settings.json`), these native options can
remove common development friction:

```json
{
  "sandbox": {
    "allowUnsandboxedCommands": true,
    "enableWeakerNetworkIsolation": true,
    "network": { "allowLocalBinding": true }
  }
}
```

Unsandboxed retries use native automatic review under **Approve for me** and
desktop approval under **Ask for approval**; an existing `false` prohibits them.
Explicit native ask/deny rules remain authoritative. On macOS,
`enableWeakerNetworkIsolation` permits access to the system TLS
trust service for Go tools such as `gh`; it does not disable certificate checking.
This opens access to that service beyond the network proxy boundary. Local binding
is honored only when the selected workspace policy grants network access. Unix
sockets and command exclusions remain restricted. Existing native deny rules are
preserved. Each new turn reloads native settings; changing them does not require
a service restart.

Native skills and nested instructions load normally in workspace sessions. Direct
file tools can edit authorized workspace files automatically, but outside and
protected destinations are blocked. For those changes, Claude must request an
explicit Bash escape, which goes to the selected reviewer, or is denied outright
in **Never ask** mode. Claude's auto-memory for the current project stays
writable and is reviewed the same way. Callback handling failures stop the native
process instead of allowing the write. Workspace sessions require Claude Code
2.1.294 or newer. See [native loading and permissions](docs/native-skill-settings-boundary.md).

On macOS, the facade repairs Claude's Git-over-SSH proxy authentication through a
native session environment hook. Git SSH traffic stays inside the sandbox and
uses its authenticated proxy; network-disabled policies still block it. SSH keys
and host-key checks remain native. Plain `ssh` can still need a reviewed
unsandboxed retry. Codex can permit native TCP directly when
network access is enabled without a managed proxy. The two runtimes do not have
identical network enforcement or classifier decisions.

If native auto review denies an action, explicit subsequent authorization in chat
(such as "I allow that" in response to the denial) tells Claude to retry that same
action once through native review. It does not automatically allow execution or
change reviewer mode. If the reviewer still denies it, **Ask for approval** in
that chat provides the native manual review path. A terminal classifier denial
does not itself create an approval callback. Do not add a global allow rule to
recover one action; explicit deny rules and managed restrictions still apply.

### Other agents

An [ACP](https://agentclientprotocol.com) agent installs as a separate connection, next to the Claude one. Sign in with the agent's own CLI first:

```sh
claudex install --acp-agent opencode --acp-arg=acp --provider-name OpenCode
```

This creates `~/Library/Application Support/claude-codex-<provider-id>`, LaunchAgent `com.claude-codex.desktop.<provider-id>` and SSH host `claude-codex-<provider-id>` (here `claude-codex-opencode`), each independent of the Claude installation. Pass the same `--acp-agent` (or `--provider-id`) to `doctor`, `service` and `uninstall` to select that instance. `--provider-id` defaults to the executable name. Use `--initialize-timeout-seconds` for agents that start slowly. Automatic review for the agent uses the installed, signed-in Claude Code CLI as its reviewer; without it, every automatically reviewed request is denied (fail closed) and can be approved from the desktop.

## Manage the service

Use the installed executable after moving or removing the build checkout:

```sh
facade="$HOME/Library/Application Support/claude-codex/bin/claude-codex-server"
"$facade" doctor
"$facade" service status
"$facade" service stop
"$facade" service start
"$facade" service restart
```

`doctor` checks native executable compatibility, the private WebSocket handshake,
the dedicated SSH route, and Claude's authentication status. It does not make a
model request. `service status` prints installed, loaded, and ready separately.
The service label is `com.claude-codex.desktop`; logs are in
`~/Library/Application Support/claude-codex/logs/server.log`.

Native Claude authentication can differ between the macOS login security session
and a separate service security session. New installations use the login session;
upgrades preserve the existing service's choice. To change it, run
`claudex install --auth-session login` or `claudex install --auth-session isolated`.
The choice is saved for later reinstalls and shown by `service status`. This does
not copy credentials or change Claude's login. If a native request works in your
terminal but the service reports expired OAuth, try the other session and retry
the chat. `doctor` checks native auth status in its caller's context; it does not
prove that a service model request can authenticate.

To upgrade, run `brew upgrade nicosuave/tap/claudex`, then `claudex install`.
Homebrew updates the command-line tool; the second command refreshes the separate
stable binary used by the desktop service. Both steps are currently required.
For a source installation, build the new source and run its `install` command again. The
installer copies the binary and refreshes the service, preserving conversation
state, the dedicated identity, and unrelated SSH entries. Wait for active chats
to finish first. Stop, restart, and uninstall refuse active saved turns unless
you explicitly pass `--force`; forcing interrupts those turns. SSH transport
reconnection preserves live turns, but restarting the facade process does not.

```sh
"$facade" service uninstall  # Remove only the service LaunchAgent; keep the SSH connection and desktop routing.
"$facade" uninstall          # Also remove managed SSH entries and desktop launch routing.
```

Both retain conversations, binaries, logs, and identity files. Full uninstall
removes only the exact dedicated key authorization, not other authorized keys.
Existing SSH config symlinks and file permissions are preserved. Legacy shell
entry points delegate to the native installer/service manager; preparation-only
mode is no longer available.

## Installation boundary

All instance files are under `~/Library/Application Support/claude-codex`, except
its LaunchAgent in `~/Library/LaunchAgents` and the managed SSH entries in
`~/.ssh/config` and `~/.ssh/authorized_keys`. The alias is prepended as a literal
Host block so desktop discovery can find it. It restricts identity selection to
the dedicated key and pins the localhost SSH host key. The authorized key accepts
only loopback connections, disables forwarding and PTYs, and uses a forced
launcher. It can still execute commands as your user; it is not a separate OS
security principal.

| Environment variable | Dedicated value |
| --- | --- |
| `CODEX_INSTALL_DIR` | Instance `bin` directory |
| `CODEX_HOME` | Instance `codex-home` directory |
| `CLAUDE_CODEX_HOME` | Instance `state` directory |
| `CLAUDE_CODEX_SOCKET_DIR` | Instance `sockets` directory |
| `CLAUDE_CODE_EXECUTABLE` | Resolved Claude CLI path |
| `CLAUDE_CODEX_PLUGIN_EXECUTABLE` | Resolved genuine Codex CLI path |
| `CLAUDE_CODEX_PLUGIN_HOME` | Existing Codex home |

Genuine Codex supplies plugin services from its existing registry and auth;
facade conversations use separate storage. The ordinary Codex connection and
shell profiles remain unchanged. `CODEX_SSH_SKIP_APP_SERVER_BOOT=true` makes SSH
use the already-running login-session service via `app-server proxy`. These
variables configure the service only; Claude sessions do not inherit them.

The launcher passes `--login-shell-path`: at startup the service replaces the
LaunchAgent's minimal PATH with the one your interactive login shell configures
(`$SHELL -l -i`), so Claude sessions, desktop commands and plugin servers find the
same tools as your terminal. Only PATH is imported. Restart the service after
changing PATH in your shell profile. If the shell does not report PATH within 10
seconds, the service logs a warning and keeps the minimal PATH.

## Verification and limitations

The protocol is pinned to Codex CLI 0.170.0. It is a supported subset, not complete
feature parity. See [COMPATIBILITY.md](COMPATIBILITY.md) and the
[desktop integration audit](docs/desktop-integration-audit.md). Native desktop
pixels/menu interaction have not been automatically verified. Transport/RPC
checks and opt-in live model probes cover separate parts of the integration.

These optional probes make real Claude model requests and archive their test chats:

```sh
mbx build --locked --example desktop_smoke --example reconnect_smoke
./target/debug/examples/desktop_smoke claude-codex-local opus
./target/debug/examples/reconnect_smoke claude-codex-local opus
```

The first checks model/folder discovery, desktop-tool exchange, saved-path resume,
timeline, host commands and queued native recall. The second reconnects during a
pending desktop tool, checks stable request identity and returns a fresh token.
For startup problems, inspect `doctor` and the instance log. Refresh the desktop
connection after an upgrade to clear cached host capability information.
