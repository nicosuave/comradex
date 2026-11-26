# App-server development

Claudex exposes the Codex app-server thread/turn protocol over Claude Code's
bidirectional `claude -p` transport, or an ACP v1 agent. Clients send Codex JSON-RPC
messages; the selected agent performs inference and tool execution. No Node or
Python bridge is required for the Claude transport.

For desktop installation and the combined GPT/Claude router, start with the
[README](../README.md). This guide covers running the facade directly.

## Build and run

Requires Rust with edition 2024 support and an installed, authenticated `claude`
executable. Claude uses its normal configured authentication. The facade neither
reads credentials nor changes login settings.

```sh
mbx build --locked --bin claude-codex-server
./target/debug/claude-codex-server app-server --stdio
```

Use `cargo build` if Boxington (`mbx`) is unavailable. The Cargo target is named
`claude-codex-server`; release archives and Homebrew expose it as `claudex`.
The default model is `sonnet`; override it with `--model opus` or a Claude model
identifier.

For a client that lets you configure its app-server command, set the executable
to the built binary and arguments to `app-server --stdio`. For desktop
connections, see [desktop setup](../DESKTOP.md).

Other transports:

```sh
./target/debug/claude-codex-server --listen ws://127.0.0.1:4510
./target/debug/claude-codex-server --listen unix:///tmp/claude-codex.sock
```

WebSockets accept loopback IP bindings only. All browser Origin headers are
rejected. `unix://` carries WebSockets as the desktop expects; newline JSON
framing is available as `unix-lines:///absolute/path`. Unix sockets use mode
`0600`; only stale sockets owned by the current user may be removed. Live sockets,
files, and symlinks are retained. All transports are intended for trusted local
clients, with no network authentication layer. Logs go to stderr; stdout carries
only protocol frames in stdio mode.

## Options and state

| Option | Purpose |
| --- | --- |
| `--claude /absolute/path/to/claude` | Backend executable; also `CLAUDE_CODE_EXECUTABLE` |
| `--state-dir /absolute/path` | Facade persistence; also `CLAUDE_CODEX_HOME` |
| `--model sonnet` | Default model alias or identifier |
| `--claude-arg=--safe-mode` | Forward an administrator-selected argument; repeat as needed |
| `--initialize-timeout-seconds 60` | Backend control handshake deadline |
| `--approval-timeout-seconds 300` | Deny unanswered permission requests after this much connected time |
| `--acp-agent opencode` | Serve an [ACP v1 agent](../COMPATIBILITY.md#acp-agents) instead of Claude |
| `--acp-arg=acp` | Forward an argument to the ACP agent; repeat as needed |
| `--provider-id`, `--provider-name` | Model provider reported to Codex for an ACP agent (default: the executable name) |
| `--auto-review-model sonnet` | Claude model that reviews an ACP agent's permission requests when a thread selects automatic review |
| `--auto-review-timeout-seconds 90` | Automatic ACP review deadline; failure or timeout denies the request |
| `--idle-session-timeout-seconds 3600` | Keep a thread's agent process between turns; `0` starts a process per turn |
| `--max-idle-sessions 8` | Idle agent processes kept across threads; the oldest closes first |

State defaults to `~/.local/state/claude-codex-server`
(`claude-codex-server-<provider-id>` for an ACP agent), independently of
`CODEX_HOME`. Only one facade process may use a state directory. The facade
stores its own thread/item history there, while Claude retains its native session
transcripts. Both are needed for durable conversation recall. Deleting facade
metadata does not delete Claude's transcripts.

The macOS installer uses its instance's `state` directory instead; see
[the installation boundary](../DESKTOP.md#installation-boundary).

## Minimal client exchange

Stdio and `unix-lines://` use one JSON object per line. TCP and Unix WebSockets
use one JSON object per text frame. As with Codex, outgoing messages omit
`jsonrpc`. String and integer request IDs are preserved.

Send `initialize`, await its response, then send `initialized`:

```json
{"id":1,"method":"initialize","params":{"clientInfo":{"name":"my-client","version":"1"},"capabilities":{"experimentalApi":true}}}
{"method":"initialized"}
```

Start a thread, read `result.thread.id`, then substitute that ID into the turn
request:

```json
{"id":2,"method":"thread/start","params":{"model":"sonnet","sandbox":"danger-full-access","approvalPolicy":"on-request"}}
{"id":3,"method":"turn/start","params":{"threadId":"<thread-id>","input":[{"type":"text","text":"Hello","text_elements":[]}]}}
```

The client receives `turn/started`, item start/delta/completion events, and exactly
one `turn/completed` for an accepted turn. Responses and notifications can
interleave. Wait for `turn/completed` before starting another turn on that thread;
separate threads may run concurrently.

A permission request is a **server-initiated request** with its own ID. For
commands and file changes, respond using the exact ID:

```json
{"id":"<approval-request-id>","result":{"decision":"accept"}}
```

The supported decisions are `accept`, `acceptForSession`, `decline`, and `cancel`.
Session grants apply only to identical tool input and cwd within the same thread;
broader permission amendments are rejected. Multi-select native questions use the
installed desktop option-picker extension. See
[COMPATIBILITY.md](../COMPATIBILITY.md) for request and recovery semantics.

Workspace-write uses Claude's OS-enforced Bash sandbox and scoped file
permissions; startup verifies the resolved native restrictions before sending
user input. Desktop **Approve for me** selects native Claude auto review. Manual
review remains supported. Full access (`danger-full-access` with `never`) selects
Claude's `bypassPermissions`; `never` inside the workspace sandbox selects
`dontAsk`. Eligible unsandboxed retries use the selected reviewer: native
automatic review under **Approve for me**, or desktop approval under manual
review. Explicit native ask/deny rules remain authoritative. MCP integrations and
hooks remain trusted services outside the Bash sandbox. Read-only/external
profiles are rejected. The generated desktop launcher also enables genuine Codex
plugin services using the existing plugin registry and authentication; see
[plugin integration](plugins-design.md).

## Verification

```sh
mbx test --locked --all-targets --features test-backend
mbx clippy --locked --all-targets --features test-backend -- -D warnings
cargo fmt --all -- --check
```

The deterministic suite drives the real facade executable with fake native
backends. It checks responses, notifications, and approvals against the vendored
schema, plus stream reconciliation, interruption, background continuations,
restart, fork metadata, history pagination, socket transports, routing, and shared
delegation. Fake backends are available only with the `test-backend` feature.
They do not prove that native OS sandboxing is enforced or that the desktop
renders every interaction correctly.

Recorded live transport evidence is in the
[desktop integration audit](desktop-integration-audit.md); router and delegation
coverage is described in their [contract](router-desktop-flow.md#validation-and-limits)
and [guide](shared-delegation.md#verification).

On a heavily loaded host, use `RUST_TEST_THREADS=1 TOKIO_WORKER_THREADS=2`.
If the local `mbx` test launcher stalls, compile with
`mbx test --no-run --all-targets --features test-backend`, then run the reported
test executables directly; this preserves compiler caching.

The live smoke example exercises inference, recall after restarting the facade,
and a fork that remains anchored while the source conversation advances:

```sh
mbx build --locked --bin claude-codex-server --example smoke
./target/debug/examples/smoke ./target/debug/claude-codex-server
```

To also verify a real command approval, add `--exercise-approval`. The example
approves only the exact command `printf FACADE_TOOL_OK`, denies other tool
requests, and uses a temporary workspace with Claude safe mode. It makes real
model calls and leaves native Claude session records in Claude's normal storage.

The smoke example adds an explicit Bash ask rule so its `printf` command requires
a callback. See [desktop verification](../DESKTOP.md#verification-and-limitations)
for installed-service and SSH reconnect probes.

## Design and references

`router.rs` owns combined-backend routing; `router_delegation.rs` owns shared
agent dispatch. `transport.rs` owns framing and connections; `server.rs` owns
RPC ordering, subscriptions, turns, and approvals; `backend.rs` owns Claude
subprocess/control I/O; `translate.rs` owns item lifecycles; `store.rs` owns
atomic facade persistence. `subagents.rs` maps native Claude subagents to Codex
child threads.

A completed turn is published only after backend cleanup. A Claude
background-agent continuation stays within its initiating Codex turn until
Claude reports idle. Eligible idle native sessions are retained between turns;
see the process-lifetime options above.

Primary references:

- [Codex app-server protocol](https://developers.openai.com/codex/app-server)
- [Codex source](https://github.com/openai/codex/tree/main/codex-rs/app-server-protocol)
- [Claude Agent SDK control implementation](https://github.com/anthropics/claude-agent-sdk-python/blob/main/src/claude_agent_sdk/_internal/query.py)
- [Claude Agent SDK subprocess implementation](https://github.com/anthropics/claude-agent-sdk-python/blob/main/src/claude_agent_sdk/_internal/transport/subprocess_cli.py)

The checked-in schema was generated with
`codex app-server generate-json-schema --experimental` from Codex CLI 0.170.0,
rather than fetched from a moving branch. See [NOTICE](../NOTICE) for attribution.
See the [0.170 conformance notes](codex-0.170-conformance.md) for changes since
0.166.0.

CI runs formatting, Clippy, and deterministic tests on Linux and macOS. See
[releasing](releasing.md) for the tagged Linux workflow, local macOS signing,
and Homebrew update process.
