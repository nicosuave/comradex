# Shared delegation

The local desktop router exposes one `claudex_agents` catalog to both harnesses.
Codex uses a thread-scoped stdio MCP server through the existing `mcp-bridge`
helper. The router also supplies model-facing dynamic declarations for desktop
clients. Claude receives the catalog through its native SDK MCP server as
`claudex_agents__<tool>` tools. Both transports call the same router dispatcher.
Native same-harness delegation remains available.

## Tools

| Tool | Arguments | Behavior |
| --- | --- | --- |
| `spawn_agent` | `harness`, `model`, `message`; optional `name`, `effort` | Starts a native child session and returns its `agent_id`. |
| `list_agents` | None | Returns this task tree's delegated agents, statuses and reports. |
| `wait_agent` | `agent_id`; optional `timeout_ms` | Waits for completion, interruption or error; a timeout returns current state without stopping the child. Maximum 60 seconds. |
| `send_message` | `agent_id`, `message` | Steers an active turn; queues input for an idle child's next follow-up. |
| `followup_task` | `agent_id`, `message` | Continues the same native child session; steers it if already active. |
| `interrupt_agent` | `agent_id` | Interrupts the child and its running descendants through native turn controls. |

For example, a Codex parent can call:

```json
{
  "harness": "claude",
  "model": "opus",
  "name": "Review parser behavior",
  "message": "Review the parser changes without editing files. Return actionable findings with source references."
}
```

A Claude parent uses `harness: "codex"` and a model from Codex's catalog. Cross-
harness children receive a fresh task briefing; transcripts are not forked.
The selected harness's model catalog is checked before creating the child.

## Permissions and scope

The caller's thread is bound by its transport. Tool arguments cannot select a
parent thread, cwd, sandbox, approval policy or reviewer. Spawn uses the effective
settings reported by the native server and checks the child's effective settings
before submitting its task. Working directory, writable roots, network policy,
temporary-directory restrictions, approval policy and reviewer must agree.
Disabled plugin selections and project placement are inherited too.

Codex MCP calls must carry the native `_meta.threadId` matching their bound
endpoint. Native same-harness children that inherit a parent's endpoint are
rejected on that endpoint; they cannot delegate with the parent's permissions.
Explicit shared-tool children receive their own scoped endpoint. Codex clients
that omit this native metadata cannot use the stdio delegation tools.

Native MCP approval prompts retain their existing behavior. The router forwards
them to the desktop without approving them or changing reviewer mode. Claude
children retain the facade's native permission modes, OS sandbox, scoped file
hooks and fail-closed callbacks. Codex children retain genuine Codex enforcement.

Only local execution environments are supported. Custom permission profiles,
external sandboxes and permission settings that the target cannot reproduce are
rejected. In particular, Claude currently cannot reproduce read-only profiles,
granular approval policies or temporary-directory exclusions. Unknown effective
permissions cause an error rather than a fallback to broader access.

The router retains authoritative start/resume/settings snapshots. It avoids
rejoining an active Codex parent: the installed app-server can invalidate pending
tool callbacks on that path. If permission settings change without a fresh
native settings report, Codex delegation fails until its settings are refreshed
between turns. Requests alone are not treated as effective settings.

Codex reports settings only when they change, while the desktop restates every
turn's settings. Requests are therefore compared with the snapshot after the
same normalization as the child check: canonical roots, cwd as an implicit
writable root, and `guardian_subagent` as `auto_review`. A restatement keeps the
snapshot confirmed; any other difference, or a different permissions profile
id, waits for the native report.

Child turns are started on behalf of the calling turn, so they carry its id as
`parentTurnId` and its causal root as `rootTurnId`. Claude turns record that
root, and are otherwise their own root, as in Codex.

Agent control is scoped to the caller's task tree. Unrelated chats cannot message,
wait for or interrupt each other's agents through these tools. Maximum depth is
8; at most 10 delegated agents may be starting or running in one tree.

## Desktop and lifecycle

Children use real native thread identities. The router adds parent/source/name
metadata, emits clickable `spawnAgent` collaboration activity, and preserves the
link when history is read or resumed. Parent and ancestor queries span both
backends; combined child lists have their own cursors. Ordinary top-level lists
hide delegated children, matching native subagent presentation. The native
sessions can accept direct input; embedded Claude `Agent` transcripts retain
their existing read-only behavior.

Opening a delegated Claude chat hydrates and subscribes to its native thread,
even while its turn is running. Desktop resume defaults do not replace the
child's inherited permissions, instructions, model or tools. Explicit
`thread/settings/update` and turn requests remain available for changing settings
and sending direct input.

Persistent child metadata and final reports are atomically written in a private
directory beside the facade socket (`<socket stem>.agents`). Native backends own
the transcripts. Ephemeral child metadata is kept in memory only. The router
does not automatically retry a submitted task after a timeout or disconnect.

Parent interruption stops its running descendants. Router shutdown interrupts
its owned children and waits for their native completion. After a crash, a new
router reconciles unfinished children whose previous owner process is gone,
preserves completed reports and interrupts still-active orphaned turns.
Another live router's children are not stopped during recovery.

These tools are provided by the local router, not by a standalone remote facade
connection. New Codex chats receive the model-facing declarations at creation;
previously enabled chats retain their saved declarations. Older Claude chats
receive the merged catalog on idle resume without losing their existing tools.

## Verification

`tests/router.rs` exercises both transports and directions, native approval
round trips, tree isolation, nested delegation, steering, queued input,
follow-up, interruption, pagination, reload and disconnect/crash recovery.
Deterministic native peers validate routing and settings; they do not enforce
an OS sandbox.

The opt-in `native_shared_delegation` test uses installed genuine Codex and Claude
executables with their normal authentication. It makes model calls in an isolated
test service, approves only the requested delegation MCP calls through the native
approval protocol without persisting approvals, verifies both child results and
parent links, then archives its generated test threads. It does not install or
restart the user's service.

Set `CLAUDEX_NATIVE_FACADE_SOCKET` to an installed service's control socket to
exercise that service instead. Only generated verification chats are archived.
The `native_side_chat_preserves_parent_snapshot_and_followup_recall` test uses
the same native executable/socket settings to verify temporary side-chat forks,
instruction overrides, boundary injection and follow-up recall.

```sh
CLAUDEX_NATIVE_CODEX=/absolute/path/to/codex \
CLAUDEX_NATIVE_CLAUDE=/absolute/path/to/claude \
mbx test --locked --features test-backend --test router native_shared_delegation -- --ignored --nocapture
```
