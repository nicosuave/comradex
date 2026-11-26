# Codex plugins through Claude

The dedicated launcher sets `CLAUDE_CODEX_PLUGIN_EXECUTABLE` to the genuine Codex
binary and `CLAUDE_CODEX_PLUGIN_HOME` to the existing Codex home. The facade keeps
its own conversation state. Codex owns plugin installation, configuration,
authentication, skills discovery, and MCP/app execution; Claude owns model turns.
No credentials are copied and no Codex model turns are submitted.

`codex_plugins.rs` supplies a concurrent JSON-RPC subprocess transport with bounded
startup, error propagation, cancellation and process-group cleanup.
`plugin_runtime.rs::Manager` forwards plugin, skill, hook and MCP management RPCs
and plugin-only config writes to a persistent Codex process. Mixed plugin/facade
config transactions fail. OAuth completion notifications reach the desktop.

Each Claude session discovers enabled skills and native tool descriptors in a
separate Codex runtime. Skills are listed with their real SKILL.md paths; selected
skills load their contents. The existing Claude SDK MCP bridge exposes tools with
validated arguments and routes calls to `mcpServer/tool/call`. Original tool names,
UI resource descriptors, connector context, structured results and `_meta` survive
in the desktop timeline. Thread-disabled plugins remove their owned skills, MCP
servers and connector tools without changing persistent user configuration.

Desktop-owned tools use `item/tool/call` back to the connected desktop, preserving
the facade thread, turn and call identity. They must not execute through the
private Codex thread, which would have the wrong desktop identity. SSH desktop
source supplies these dynamic schemas even though local desktop-MCP injection is
not available over SSH. Sites uses the genuine authenticated connector transport;
Visualize uses its installed skill, response reference and remote file APIs.
A complete standalone `visualize{...}` reference is normalized to the desktop
private-use delimiters when Claude omits those glyphs; quoted code is unchanged.

Codex hooks can be managed but are not executed around Claude model turns.
Interactive MCP elicitation fails explicitly; complete interactive authentication
in Codex settings. Plugin MCP services and native Claude hooks remain trusted
integrations outside the native Bash sandbox. Native Claude plugins are still
loaded by Claude itself; legacy native inventory is the unconfigured fallback.

Tests cover transport lifecycle, original RPC errors, native metadata retention,
thread exclusions, local plugin installation/skill discovery/MCP random nonce,
actual Claude execution of that nonce, actual read-only Sites listing, and
Visualize directive/file hydration. Desktop pixel rendering remains unverified.

Visualization output receives an explicit cwd-contained per-chat directory. The desktop cannot render arbitrary paths under the genuine Codex plugin home. Remote hydration uses the newer process/spawn host protocol for realpath and file reads; that protocol is implemented alongside command/exec.
