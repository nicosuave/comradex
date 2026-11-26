# Native skills and workspace permissions

Workspace sessions retain Claude's native `user,project,local` discovery and the
existing reviewer modes: `acceptEdits` for user review, `auto` for **Approve for
me**, and `dontAsk` for **never ask**. An explicitly supplied setting-source list
still limits discovery. Full-access sessions retain their ordinary native launch;
with **never ask** they run in `bypassPermissions`.

## Native loading

Each workspace session has a stable isolated user profile under the facade's
state directory. It contains sanitized user settings and links to the existing
native skills, commands, agents, rules and instructions. Claude discovers and
loads these features itself, including nested project skills and local
instructions; the facade does not inject skill bodies or implement another skill
loader. Project paths and hook working directories remain real paths.

The profile preserves the original secure-storage and plugin namespaces, selected
MCP definitions and project trust state. It never invents project trust or copies
OAuth tokens/account state. Profile JSON is owner-only. Native transcripts remain
in the original projects directory, so existing session resumes and forks keep
their native identity. Per-session profiles avoid sharing mutable settings across
concurrent chats, and refresh before each process launch after the previous
backend has stopped. Explicit per-turn environment and plugin overrides remain
native settings overlays.

## Permission contract

Managed-only permission rules and the admin-required command sandbox keep
repository grants from widening the host's shell policy. Host-scoped Edit rules
preserve automatic work inside the authorized roots, including in `dontAsk` mode.
Native policy is verified before user input: active permission rules, OS write
roots, protected paths and network restrictions must match the host boundary.
Genuine enterprise policy remains native and can restrict the session further.

Native `auto` simulates `acceptEdits` before classification. Project
`additionalDirectories` can therefore allow outside file writes even while
managed-only rules are active. Every workspace launch registers a host-owned
SDK `PreToolUse` callback for `Write`, `Edit` and `NotebookEdit` to close that gap.

- Normal file edits inside authorized roots return a neutral hook result, keeping
  native deny/ask checks intact.
- Outside destinations, symlink escapes, invalid inputs and protected paths are
  explicitly denied. Existing ancestors are resolved for newly created paths.
- Direct writes to `.git`, `.codex`, `.agents` and native configuration are denied.
  Native configuration is protected entry by entry around its `projects`
  transcript namespace, because a wholesale deny rule would override native's
  auto-memory exception.
- The session's auto-memory directory is the one exception outside the roots.
  The guard learns it from native's `system/init` message and accepts it only in
  the default `projects/<project>/memory` shape, so a repository-configured
  `autoMemoryDirectory` cannot open another path. Memory writes then follow the
  selected native reviewer, like other outside edits.
- Outside or protected changes must use an explicit Bash escape. Those retain the
  selected native reviewer: manual approval, native auto classification, or
  denial without prompting in `dontAsk` mode. A configured prohibition on escapes
  stays authoritative.

This intentionally changes direct outside file-tool approval into a denial with
a reviewed Bash route. It does not force auto review into manual review or force
approval for ordinary workspace work. Trusted hooks and MCP services remain
executable integrations outside the command sandbox. The path check is not a
race-free OS fence against hostile concurrent filesystem mutation.

## Fail-closed callback handling

Claude treats hook protocol errors and hook timeouts as permission to continue.
The backend therefore handles callback requests directly in its stdout reader,
never through the UI approval queue. Invalid tool inputs and path-resolution
errors produce a successful protocol reply containing an explicit denial.
Malformed/unknown callback envelopes, resolver task failures, blocked replies,
transport errors and event-consumer overload terminate the native process group.
Callback detection uses envelope fields only, never tool inputs. The stdout reader
never waits on the event consumer: events behind a slow consumer are buffered
(up to 128 MiB) so a pending callback is always read and answered, and only a
backlog beyond that limit is treated as overload.
The callback deadline is five seconds, below the native hook's sixty seconds.
Supervisor cancellation, drop and panic also terminate the backend group.

Registration and policy verification repeat on every native process start,
including resumed turns and reviewer changes. They depend on `--managed-settings`
and `list_permission_rules`, so workspace sessions require Claude Code 2.1.294 or
newer; startup errors say so. Profiles idle for 30 days are removed on the next
launch and rebuilt on demand.

## Validation

The Rust suite exercises path boundaries, missing parents, dangling/cyclic
symlinks, protected case aliases, malformed callbacks, pre-initialization callback
handling, reviewer/resume registration, broken pipes, blocked delivery and event
backpressure. Profile tests cover refresh, session separation, native feature and
transcript links, owner-only permissions, and active-rule verification.

The opt-in native fixture uses a local canned Anthropic endpoint and disposable
configuration, with no paid calls:

```sh
mbx build --locked --bin claude-codex-server
bun tests/native_hooks.cjs target/debug/claude-codex-server /path/to/claude
```

It exercises the actual facade and native CLI across one persisted session:
manual review, auto allow, auto deny, and never ask. It checks native skill/local
instruction discovery, trusted hooks and real paths, two writable roots, direct
and symlink outside denials, OS shell denials and reviewed escapes. Canned
classifier responses verify routing and execution, not live classifier judgment.

Set `NATIVE_SSH_REPOSITORY` to an SSH repository URL to additionally verify real
Git SSH reads in each reviewer mode and rejection with network access disabled.
On macOS a native SessionStart environment hook replaces the known broken native
Git SSH tunnel with the facade's authenticated HTTP CONNECT helper. It uses the
same native proxy and leaves command review, SSH authentication, host-key checks,
and filesystem restrictions intact. Custom SSH commands and unsandboxed commands
are unchanged. The workaround requires native SessionStart hooks to be enabled.

## Rejected alternatives

Disabling settings sources loses native skills. `projectConfigRoot` also suppresses
nested discovery and redirects project paths. Asking for every edit changes
ordinary workspace behavior. Removing native directory grants once does not
survive settings refresh. A custom skill adapter would duplicate native loading.
The SDK guard keeps those responsibilities in Claude while bounding file writes.
