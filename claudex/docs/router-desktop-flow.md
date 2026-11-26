# Local desktop router contract

For cross-harness child agents, see [shared delegation](shared-delegation.md).

The local router must implement the desktop's task-creation transaction, not only
`thread/start` followed immediately by `turn/start`. This reference was checked
against the installed desktop build 20156, bundled Codex CLI
`0.162.0-alpha.17.2`, and Claudex's vendored `0.160.0` schema. The metadata and
settings contracts below already exist in that vendored schema.

## Creation and submission

1. The desktop reads config and lists models. Genuine Codex owns config, project
   administration and its own chats; the router adds the Claude model catalog.
2. The desktop starts or reuses an empty prewarmed thread. When no model is sent,
   the router reads genuine Codex's effective config for that cwd. A saved Claude
   default must select Claude, rather than creating a Codex chat with that model.
3. Before submitting the first message, the desktop persists project placement
   through `thread/metadata/update`. Projectless tasks still call this method,
   with `projectId: ""`. The facade saves the assignment and returns the thread;
   this must not be acknowledged as a no-op. The desktop/genuine Codex project
   registry owns the project identities.
4. Model and permission selections can call `thread/settings/update` before the
   turn. Claude accepts idle-thread updates, saves the resulting settings and
   emits `thread/settings/updated`. A still-empty Codex thread can migrate to
   Claude at this point or when its first Claude turn arrives. Project and Git
   metadata must survive that migration.
5. The desktop's final `turn/start` request contains both execution settings and
   client context. `collaborationMode.settings.model` takes precedence over the
   top-level model, matching both backends. Routing and normalization must agree
   about this selection. Completed chats remain on their original backend.
6. Resume restores the facade subscription before a follow-up turn. Reads,
   metadata patches, names and archive operations continue to address the same
   thread identity and persisted record.

Current source evidence: `.vite/build/main-CqEoQDOU.js`, project-assignment
`write` near character offset 2599745; `.vite/build/src-BJv5Mk7n.js`, thread-settings update
near 874722, thread-start request preparation near 891254, prewarm preparation
near 895892, and the final turn expression near 828990. Extracted app code stays
outside the repository. `scripts/extract-desktop-turn-fixture.mjs` evaluates the
final expression with controlled inputs; the checked-in fixture records the
bundle and expression SHA-256 hashes so it can be refreshed deliberately.

Codex's injected `mcp_servers.node_repl`, `mcp_servers.artifact_session`,
`mcp_servers.cua_repl`, `memories.version`, and `memories.dual_write` configuration
describes its own runtime. Claude filters these known overrides, warns, and uses
native memory/MCP configuration. The new MCP entries include enabled launch
definitions and disabled placeholders. Filtering them restores chat creation;
it does not connect Claude to native CUA or viewer-bound artifact sessions.
Genuine Codex requests retain their runtime configuration. Approval and sandbox
settings retain their native semantics; workspace tests use the desktop's actual
workspace-write and automatic-review payload, rather than substituting full access.

## Native self-hosted providers

The opt-in `claudex local-provider` configuration adds a Responses-compatible
endpoint to the merged picker; `claudex mlx` supplies the MLX-Serve preset.
These models stay on genuine Codex. Only their start/resume/fork
requests receive the local provider and generated model catalog; GPT's global
provider, catalog and inference URL are retained. The catalog uses the server's
working context window and text/image modalities, and disables hosted search
and remote compaction for the self-hosted provider.

Ordinary self-hosted endpoints use direct function tools with cloud apps disabled
on their per-thread configuration. Current MLX-Serve supports function/namespace
tools but not specialized tool-search or custom code-mode calls; deferral therefore
cannot be advertised, and an eager cloud connector catalog would otherwise
dominate local prefill. Compatible endpoints can opt in with `--codex-tools`.
Native tools, configured MCP servers, AGENTS instructions and existing providers
retain their usual behavior.

Codex fixes each chat's provider at creation. If the desktop chooses MLX for a
verified still-empty prewarm, the router creates a replacement native chat,
restores applicable settings and project metadata, and durably aliases its id
before releasing notifications or the first turn. Only typed thread identities
are translated; user content, tool arguments, turn/item ids and rollout paths
retain their bytes. Aliases are shared across router processes. The native MCP
caller is authenticated against the actual native id while delegation retains
the public id.

After a chat has messages, cross-provider changes require a new chat. Normal
facade adoption continues to handle empty chats selected for Claude or ACP.
An unavailable MLX server retains cached picker metadata and does not change
the native GPT/Claude/ACP routes or silently replay a request on another model.

## Service lifecycle

A Claude service restart must not turn a Claude model into a Codex model. The
router retains learned model/thread ownership, reports a temporary reconnect
error for Claude requests, and reconnects the private socket while keeping the
Codex child alive. It restores the handshake/catalog and resumes a thread's
subscription before its next turn. It never automatically replays a submitted
message. Codex requests continue to work during the outage. The desktop's own
connection stays up, so when the facade goes away the router ends the Claude
turns the desktop saw start (`turn/completed` as interrupted, then an idle
status) and resolves their outstanding server requests; the restarted facade
records the same turns as interrupted.

Routing facts are scoped. While the router waits on the facade for one thread
(its ownership probe, subscription before a turn, or adoption), only that
thread's later requests wait, in order; other chats and genuine Codex requests
proceed. Only the Claude catalog and the saved default model, which decide
where a new thread goes, hold every request.

## Validation and limits

`tests/router.rs` covers projectless creation with the extracted turn payload,
metadata and settings before first submission, metadata-preserving migration,
saved defaults, first/follow-up turns, resume, and service restart. `tests/wire.rs`
checks metadata persistence and upstream response/notification schemas. The
native integration probe repeats creation, two real workspace/auto-review Opus
turns, restart, another turn on the same router, metadata/rename/archive, and a native Bash printf with captured command output.
The test-only fake Claude reports a simulated managed-policy control state; it
is not evidence that an OS sandbox is enforced.

This remains a supported protocol subset. Plan mode, read-only profiles and
mutations of settings for an already-running native turn are not implemented.
Idle settings updates must not claim to change a running turn's reviewer/model.
These limits are distinct from the basic desktop create-task/submit transaction.
