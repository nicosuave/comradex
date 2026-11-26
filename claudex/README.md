# Claudex

Use Claude Code alongside Codex in the same desktop window. Claudex adds Claude
models to the model picker, combines both backends' chats in the sidebar, and
routes each conversation to its native runtime. GPT chats run through genuine
Codex; Claude chats run through your installed, authenticated Claude Code.

The router also lets Codex and Claude delegate work to each other through shared
agent tools. You can connect from another computer over SSH, use other agents
through ACP, or run Claudex as a standalone Codex-compatible app-server.

Claudex is written in Rust. It does not modify the desktop app bundle or replace
either agent's authentication.

## Install

```sh
brew install nicosuave/tap/claudex
```

You need macOS, the Codex desktop app, and Claude Code. Sign in with
`claude auth login` before setting up the desktop connection. Workspace-write
sessions require Claude Code 2.1.294 or newer.

Prebuilt macOS and Linux binaries for ARM64 and x86-64, with SHA-256 checksums,
are on [GitHub Releases](https://github.com/nicosuave/claudex/releases).
macOS binaries are Developer ID signed and notarized. Desktop installation is
macOS-only; Linux can run the standalone app-server.

## Quickstart

Set up the Claude service and local desktop router as your login user, without
`sudo`:

```sh
claudex install --no-ssh
```

Quit and reopen the desktop app once so it picks up the router. Stay on the
ordinary local connection, start a new chat, and choose a Claude model such as
`opus` or `sonnet`. GPT models and existing Codex chats remain available in the
same window. This setup does not require Remote Login.

To also create a separate Claude-only connection, enable **System Settings >
General > Sharing > Remote Login** for your user and run `claudex install`
without `--no-ssh`. Select `claude-codex-local` in **Settings > Connections**.
See [desktop setup](DESKTOP.md#install) for identity-file selection and other
connection settings.

## Ways to use Claudex

| Mode | Use it for |
| --- | --- |
| [Desktop router](#desktop-router) | GPT and Claude models, chats, and cross-harness agents in one window |
| [Remote desktop over SSH](#connect-from-another-computer) | The combined model picker on another Mac |
| [Dedicated Claude connection](DESKTOP.md#install) | A separate Claude-only desktop connection |
| [Other agents over ACP](#other-agents-over-acp) | OpenCode, Gemini CLI, and other ACP v1 agents |
| [Standalone app-server](#standalone-app-server) | Claude behind the Codex thread/turn protocol for your own client |

## Desktop router

The router sits between the desktop and two backends: genuine Codex and the
Claude service. It combines their model catalogs and chat lists. Codex owns
configuration and project administration; each backend keeps its own
conversations and executes its own tools.

- **Choose a model when starting a chat.** GPT models go to Codex; Claude models
  go to Claude Code. The router honors saved model defaults and moves an empty,
  prewarmed Codex chat to Claude if you choose a Claude model before its first
  message. Project and Git metadata are preserved.
- **Continue on the same backend.** Once a chat has messages, it stays with its
  backend. To move from GPT to Claude or vice versa, start a new chat. The router
  rejects a cross-backend model change in an existing conversation.
- **Browse both histories together.** Model discovery, sorted and paginated chat
  lists, and sidebar sections span both backends. Within a shared section,
  Claude chats follow Codex chats; ordering cannot interleave the two backends.
- **Recover after a service restart.** The router reconnects to Claude while
  keeping Codex running. Claude requests receive a temporary reconnect error;
  submitted messages are never automatically replayed or sent to Codex.
  Restarting the Claude service interrupts its active turns. Requests for
  threads whose backend is not yet known wait for ownership to be resolved.

The installer configures `CODEX_CLI_PATH` for the desktop login session and
restores it at each login. The generated launcher at
`~/Library/Application Support/claude-codex/router/codex` runs `claudex route`
for desktop app-server launches and genuine Codex for other commands. The
default is the Codex binary bundled with the desktop;
`CLAUDE_CODEX_ROUTER_CODEX` can select another executable.

See [local desktop routing](DESKTOP.md#one-local-window-without-ssh) for launch
details and [the router contract](docs/router-desktop-flow.md) for thread
creation, model selection, metadata, and reconnection behavior. Claude
[prompt-bar predictions](docs/prompt-predictions.md) also use the router.

To add self-hosted Responses-compatible models to the same picker, use
`claudex local-provider`; `claudex mlx` is the MLX-Serve preset. Reopen the
desktop app to load the models. These chats use genuine Codex's tools and
permissions with direct inference; GPT, Claude and ACP routing stay available.
See [self-hosted setup](DESKTOP.md#self-hosted-models-in-the-local-picker).

## Cross-harness agents

In a routed chat, Codex can delegate a task to Claude, and Claude can delegate to
Codex. Both receive the `claudex_agents` tools: `spawn_agent`, `list_agents`,
`wait_agent`, `send_message`, `followup_task`, and `interrupt_agent`.

For example, ask a Codex chat:

> Use claudex_agents to spawn a Claude agent with model opus to review the parser
> changes without editing files. Wait for its report, then address the findings.

Children are native sessions linked to the parent's task tree. They receive a
fresh task briefing and inherit the parent's working directory and permissions;
delegation fails if the target cannot reproduce those permissions. You can open
the child chats and continue them directly. Native same-harness delegation is
also available.

Shared delegation requires the router; a direct Claude-only facade connection
does not provide it. See [shared delegation](docs/shared-delegation.md) for tool
arguments, permission boundaries, and lifecycle behavior.

## Connect from another computer

On the remote Mac, install Claudex and run `claudex install` as the user you
connect to. Enable Remote Login there, then reconnect that Mac's ordinary SSH
connection in the client desktop. GPT and Claude models appear together using
your existing SSH authentication and Codex home.

The installer adds a managed shell block that selects the router for desktop SSH
launches. Ordinary SSH shells and CLI commands keep their existing behavior;
remote routing currently supports zsh and bash login shells. See
[remote desktop setup](DESKTOP.md#connect-from-another-computer) for details.

## Other agents over ACP

Claudex can expose [Agent Client Protocol](https://agentclientprotocol.com) v1
agents through the same Codex-compatible interface. Sign in with the agent's own
CLI first. To put selected ACP models alongside Claude and Codex in the same
local desktop picker, use [local ACP setup](DESKTOP.md#acp-models-in-the-local-picker).
For a separate desktop connection, use:

```sh
claudex install --acp-agent opencode --acp-arg=acp --provider-name OpenCode
```

Select `claude-codex-opencode` in **Settings > Connections**. Each agent gets its
own service, state, SSH identity, and connection. Pass the same `--acp-agent` or
`--provider-id` flags to `doctor`, `service`, and `uninstall` to select it.

ACP agents execute their own tools and require full access. Their permission
requests become desktop approvals; optional automatic review uses Claude.
Models, effort, and fork support depend on the agent. Workspace sandboxing,
rollback, and structured output are unsupported. See
[agent setup](DESKTOP.md#other-agents) and the
[ACP compatibility mapping](COMPATIBILITY.md#acp-agents).

## Feature support matrix

Claude support covers a subset of the Codex protocol. Genuine Codex chats retain
their native behavior; the router does not give Claude every Codex capability.

| Area | Claude support |
| --- | --- |
| Conversations | Start, resume, rename, archive, delete, search, fork, revert, and rollback; recovery does not undo files or external effects |
| Turns | Streaming, steering, queues, interruption, context usage, and native compaction |
| Models | Installed Claude catalog, per-model effort, and Fast mode where advertised by Claude |
| Tools and input | Native Claude tools, text and images, desktop dynamic tools, and structured output; shell output arrives with native tool results |
| Plugins and MCP | Claude-native integrations plus Codex plugin services through genuine Codex; no Codex hook execution around Claude turns or interactive MCP elicitation |
| Subagents | Claude `Agent` activity appears as linked, read-only child threads; router-delegated native sessions accept direct input |
| Permissions | Workspace-write, native automatic or manual review, and full access; read-only and external sandbox profiles are unsupported |
| Desktop | Combined models and history, project placement, sections, SSH reconnect, and best-effort prompt predictions |
| Unsupported | Plan/review modes, realtime voice, audio/video input, transcript import, and Claude account/quota management through Codex |

**Workspace write** uses Claude's OS-enforced Bash sandbox and scoped native file
permissions. **Approve for me** uses Claude's native automatic reviewer. Full
access with `never` uses `bypassPermissions`; `never` within the workspace sandbox
uses `dontAsk`. MCP integrations and hooks remain trusted services outside the
Bash sandbox. See [sandbox settings](DESKTOP.md#development-sandbox-settings)
and [COMPATIBILITY.md](COMPATIBILITY.md) for exact behavior and limitations.

## Updates and troubleshooting

Wait for active chats to finish, then update both the Homebrew command and the
separate binary used by the service:

```sh
brew upgrade nicosuave/tap/claudex
claudex install --no-ssh
```

If you use the dedicated SSH connection, run `claudex install` without
`--no-ssh` instead. Reconnect remote desktop connections after upgrading.
Reinstallation preserves conversations and the dedicated SSH identity.

```sh
claudex doctor             # Check the installed setup; makes no model request
claudex service status     # Show installed, loaded, and ready state
claudex service restart    # Restart an idle service
```

If Claude models are missing after the first install, quit and reopen the
desktop app. Logs live under
`~/Library/Application Support/claude-codex/logs/`: `router.log` for routing and
`server.log` for the Claude service. `claudex uninstall` removes the managed
service and routing setup while retaining conversations, binaries, logs, and
identity files. See [service management](DESKTOP.md#manage-the-service).

## Standalone app-server

For a client that accepts an app-server command:

```sh
claudex app-server --stdio
```

This serves Claude directly, without the combined Codex router. Other agents can
use the same interface:

```sh
claudex app-server --stdio --acp-agent opencode --acp-arg=acp --provider-name OpenCode
claudex app-server --stdio --acp-agent gemini --acp-arg=--experimental-acp --provider-name Gemini
```

The facade targets the vendored Codex CLI **0.170.0** schema. Unsupported methods
return `-32601`; unsupported non-null options return `-32602`. See the
[app-server guide](docs/app-server.md) for source builds, transports, options,
state storage, a JSON-RPC client exchange, and verification commands.

## Reference

| Guide | Contents |
| --- | --- |
| [Desktop setup](DESKTOP.md) | Local and SSH installation, launch configuration, sandbox settings, authentication, service management, and uninstall |
| [Compatibility](COMPATIBILITY.md) | Supported methods, tool mappings, permissions, lifecycle, and ACP limitations |
| [Router contract](docs/router-desktop-flow.md) | Task creation, saved defaults, metadata, backend ownership, and reconnect behavior |
| [Shared delegation](docs/shared-delegation.md) | Cross-harness agent tools, inherited permissions, task trees, and recovery |
| [Prompt predictions](docs/prompt-predictions.md) | Native Claude prediction forks, cache behavior, and desktop compatibility |
| [Plugins](docs/plugins-design.md) | Codex plugin services, skills, authentication, and tool execution |
| [Native loading and permissions](docs/native-skill-settings-boundary.md) | Claude skills, settings, file guards, and sandbox boundaries |
| [App-server development](docs/app-server.md) | Source builds, protocol examples, transports, tests, and implementation references |
| [Desktop integration audit](docs/desktop-integration-audit.md) | Recorded transport and live-model evidence, with validation limits |
| [Releasing](docs/releasing.md) | CI, release artifacts, macOS signing, and Homebrew delivery |
