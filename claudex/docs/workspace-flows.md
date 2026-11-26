# Desktop workspace and attachment boundary

Evidence is from development-time inspection of the installed desktop, the
then-pinned Codex 0.160.0 schema, and upstream Codex source. Bundle locations below
are byte offsets in the inspected version. Extracted desktop code is not shipped.

## Owning path

`main-B6ZOwXa3.js` `createManagedWorktree` (2,037,009) calls the desktop's
`requestGitWorker({method:"create-worktree", ...})`, supplying source cwd,
starting state, name, environment config path, worktrees root and stream ID.
The desktop worker owns Git operations, setup scripts, progress, owner metadata,
and cleanup. The facade must not create another checkout or implement an
invented `worktree/create` RPC.

The managed attachment coordinator (`#p`, 2,377,200) reads the calling thread,
checks the `threadAttachments` version gate, invokes that service, then calls
`thread/attachment/add` with:

```json
{"threadId":"...","attachmentType":"worktree","identityKey":"<git-root>","payload":{"root":"<git-root>","workspaceRoot":"<workspace-root>","sourceCwd":"<source-cwd>"}}
```

It updates Git worker owner metadata separately. Attachment registration failure
is reported as `registrationError` while preserving the created checkout. The
existing-checkout path checks ownership and archived metadata before registration.
It enumerates descendant threads using `thread/list` with `ancestorThreadId`,
`sourceKinds:["subAgentThreadSpawn"]`, `modelProviders:[]`, `archived` false and
true, `useStateDbOnly:true`, `limit:100`, and cursor. These filters must not turn
into an unfiltered list of unrelated threads.

`app-shared-6c00c2afcf84.js` (1,856,795) defines the attachment minimum version as
`0.155.0-alpha.2`. The installed desktop's advertised-version feature gate is a
separate concern from the existence of a server handler.

## Attachment protocol

The pinned schema at lines 24113–24304 defines arbitrary string attachment types
and JSON payloads, stable thread-local type/identity keys, add outcomes
`created|existing`, paginated list, remove, and `thread/attachment/updated`.
The facade stores these values generically, including file metadata and future
attachment types. Listing is ordered by creation time and stable ID; cursors
bind to a thread and remain usable when the prior page's last item is removed.
An existing identity retains its original payload. Removal only changes metadata.

Desktop pull-request writes use `pull_request` with `{url,root,headBranch}`
(`app-shared`, 5,454,028). Its worktree/PR query pages through attachment lists
(`app-shared`, 5,203,721), and invalidates on attachment updated notifications
(5,205,728). The facade emits notifications only after durable record saves.

Archive recovery is also desktop-owned: `main` `pZ`/`mZ` near 2,361,999 stores
`archived_worktree` with `{worktree: <recovery metadata>, pullRequests:[...]}`;
restoration adds back the worktree and PR associations and removes the archived
association. Keeping arbitrary payloads intact is necessary for recovery.

## Environment overlay

`app-shared` `Zrn` (2,874,519) requests the desktop bridge's
`worktree-shell-environment-config`, reads config, and merges the worktree
`{set,exclude}` environment. `Hrn` removes excluded names from `set`, applies
worktree additions, and removes newly set names from exclusions. `Urn` emits
flattened `shell_environment_policy.*` keys. Its fallback policy inherits all.
`main` `readWorktreeShellEnvironment` (1,414,600) routes to the Git worker;
the local terminal manager also requests that worker environment (3,270,113).

`workspaces::shell_environment_overrides` accepts the additive subset:
`inherit=all`, string `set`, empty `exclude/include_only/filters`,
`ignore_default_excludes=true`, and `experimental_use_profile=false`. It supports
nested and flattened config and distinguishes absent policy from explicit empty
overlay. Integrate the result into native Claude environment settings and persist
it with thread settings. Do not silently drop these fields during normalization.

Nonempty exclusions/inclusions, restricted inheritance, default secret filtering,
and profile evaluation are explicitly rejected. Codex's owning implementation
is `codex-rs/protocol/src/shell_environment.rs`: it builds *tool subprocess*
environments. Filtering the entire Claude process would also filter its own
authentication and service configuration and is not an equivalent implementation.

The inspected desktop's local Git and terminal services do not establish that
local worktree creation depends on facade `command/exec`. The visible cloud Git
adapter (`main`, 2,096,756) uses `command/exec` with nullable env overrides and
streaming, but cloud environment support is outside this local flow. Do not claim
hidden desktop client services are provided merely because host exec/fs exist.

## Validation boundary

Attachment tests verify persistence roundtrip, distinct type identities,
idempotency, metadata-only removal, pagination across deletion, and pinned
response/notification schema shapes. Workspace tests create an isolated temporary
Git repository and detached worktree, run a child in that cwd with the translated
overlay, and check explicit rejection of incompatible policies. They do not
exercise desktop UI orchestration. A separate live Claude Bash probe on October 8,
2026 confirmed a random environment token reached the native tool while the
existing Bash ask permission remained intact.
