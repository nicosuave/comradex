# Desktop integration boundaries

The facade implements a subset of the Codex app-server contract pinned to CLI
0.166.0. Schema validation establishes message shape; deterministic tests establish
facade behavior; live Claude and SSH probes establish their runtime paths. None
of these alone proves every native desktop interaction or rendered result.

The desktop uses protocol version gates for workflows including queue, timeline
and attachments. The facade must implement the advertised supported contracts
and reject unsupported options explicitly. Downgrading the advertised version
is not an alternative: it prevents the desktop from connecting.

## Supported workflow map

| Workflow | Implementation and evidence |
| --- | --- |
| SSH connection | Private WebSocket proxy, initialization and native desktop-tool exchange; deterministic transport tests and live SSH probe |
| Folder selection | Metadata, directory listing and directory creation; wire tests and live probe |
| Model/effort selection | Native Claude catalog and supported effort levels; wire tests and live discovery |
| Start and resume | Desktop-shaped requests, saved path, initial history pages and snapshot cursors; wire tests and native recall |
| Steer and queue | Ownership, ordering, cancellation and persistence; deterministic blocked-input cases and native steering probe |
| Stop | Interrupt drains/reaps backend and closes pending items; deterministic tests |
| Permissions | Native auto review, manual review, exact-input/cwd session grants, native file prompts and disclosed sandbox escape requests; deterministic and opt-in native tests |
| Questions | Single/multiple-choice pickers, freeform answers, fresh IDs, stale-response fencing and dismissal; deterministic tests |
| Desktop tools | Reverse RPC through turn owner, stable pending IDs, text/error results and plugin metadata preservation |
| SSH reconnect | Active native turn survives detachment; pending requests replay and connected-time deadlines pause; installed reconnect probe |
| History recovery | Native transcript-prefix fork, revert and rollback; missing UUID fails closed; random-code exclusion probe |
| Attachments | Durable generic metadata, pagination and descendant filtering; deterministic tests |
| Worktrees | Desktop Git worker owns creation/archive/restore; facade supplies attachment metadata and additive environment overlays; UI orchestration unverified |
| Plugins | Genuine Codex discovery/administration and MCP execution, desktop reverse RPC, plugin skills; isolated plugin nonce and read-only Sites/Visualize probes |
| Host process/file APIs | Filesystem hydration, commands and PTYs; deterministic process tests and installed file-hydration probe |
| Context usage | Latest top-level Claude request including cache tokens, separate aggregate billing, persisted replay and legacy unknown migration |
| Idle instruction refresh | Same native history with refreshed base/developer instructions and prompt snapshots disabled; native random-code recall probe |

Choose a project on the Claude host when selecting its models. Some projectless
composer flows route model lookup through the current project/host instead of
the execution-host selector; the facade cannot answer requests sent to a different
connection.

## Trust and unsupported surfaces

Host command/file RPCs are privileged local client services, distinct from Claude
model tools. Connect only trusted local clients. The transport has no independent
network authentication layer; browser Origin headers are rejected. Workspace
restrictions apply to native Claude Bash/file tools. MCP integrations and hooks
remain trusted outside the Bash sandbox.

Read-only/external sandbox profiles, restrictive shell-environment filtering,
arbitrary config writes, interactive MCP elicitation, voice and remote control
are unsupported. Codex plugin hooks can be managed but do not run around Claude
turns; native Claude hooks retain their behavior. Standalone skills remain
Claude-owned. Generic native tools without an equivalent desktop approval
contract use an explicit question fallback.

SSH reconnect preserves a live backend; a facade process restart interrupts it.
History recovery changes conversation context, not filesystem/external effects
or Claude persistent memory. Stable request identity is not an exactly-once
external-effects guarantee across a complete desktop process crash.

## Verification evidence

Development acceptance on October 8, 2026 included native model discovery,
desktop request options, random-token desktop-tool exchange, saved-path resume,
timeline, host command, queued native recall and probe archival over real SSH.
Separate native probes checked removed-history exclusion, sandbox boundaries,
ordinary edits with zero manual prompts, one explicit sandbox escape approval,
and genuine Codex plugin execution. Sites was invoked read-only. File metadata
and content hydration of a generated Visualize reference were verified.

The installed desktop's path checks require generated visualization files inside
the chat's permitted roots; the facade supplies a per-chat output directory under
cwd. Actual rendered pixels and menu clicks remain unverified because native UI
automation was unavailable. Reconnect after upgrades to refresh cached host
permission/capability metadata. These historical runtime probes do not substitute
for verification of a new release build.

Run the current deterministic suite as documented in the
[app-server guide](app-server.md#verification).
Native opt-in tests and the examples in [DESKTOP.md](../DESKTOP.md) make real model
requests. Private local inspection artifacts and transcripts are not distributed.
The vendored schema and upstream licenses are included; owning implementation
modules are `src/server.rs`, `src/backend.rs`, `src/store.rs`, `src/desktop.rs`,
`src/translate.rs`, `src/dynamic_tools.rs`, and `src/plugin_runtime.rs`.
