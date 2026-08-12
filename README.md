# Comradex

> Workers of all accounts, unite.

Comradex is a small Rust relay that gives the native Codex App/CLI a sticky, quota-aware collective of ChatGPT accounts without rewriting Codex sessions or rollouts. Raw mode preserves Responses WebSocket bytes; the two frame-aware modes intentionally inspect and, where required for safe recovery, reframe Responses traffic.

The current implementation intentionally supports only native Codex traffic. It has no provider translation, request history, GUI, telemetry, or conversation cache.

## Quick start

Install the latest release with Homebrew:

```sh
brew install nicosuave/tap/comradex
```

Create the default configuration, verify it, and start the relay:

```sh
comradex init
comradex check
comradex serve
```

In another terminal, point Codex at the running relay:

```sh
comradex install
```

Restart the Codex app after installation, or let Comradex restart its background processes for you:

```sh
comradex restart-codex
```

That's enough to route the Codex App's existing account through Comradex. To add another account:

```sh
comradex account add personal_2
```

The command updates the configuration, restarts the daemon if it is installed as a service, and walks through device login. See [Accounts](#accounts) for the manual setup and credential details.

## Installation

### Prebuilt binaries

Homebrew is the shortest path:

```sh
brew install nicosuave/tap/comradex
```

Or install a prebuilt release from GitHub with `cargo-binstall`:

```sh
cargo binstall --git https://github.com/nicosuave/comradex comradex
```

### From source

```sh
git clone git@github.com:nicosuave/comradex.git
cd comradex
cargo run -- init
cargo run -- check
cargo run -- serve
```

## Configuration

Every command reads `~/.config/comradex/comradex.toml` by default; `init` creates it there. Pass `--config <path>` to use a different location.

The daemon reads the configuration once at startup. Restart it after adding an account or changing pools or listeners. With a manually started daemon, stop and rerun `comradex serve`. With the macOS service, run `comradex service restart`.

### Accounts

The generated configuration contains the special `app` account. It forwards the Codex App's inbound `Authorization` and `ChatGPT-Account-Id` headers and never stores or refreshes them.

Add a managed account in one step:

```sh
comradex account add personal_2
```

This appends a `codex_home` account (home directory `accounts/personal_2` next to the config), adds it to the pool (`--pool` chooses another), validates the edited configuration through the full loader before persisting it, restarts the daemon if the service is installed, and runs interactive device login. Pass `--no-login` to defer authentication.

Use these commands to inspect or remove accounts:

```sh
comradex account list
comradex account login personal_2
comradex account remove personal_2
```

`account list` shows each account's login state. `account remove` removes it from the configuration and every pool but keeps its credential directory unless `--purge` is passed.

The equivalent manual configuration is:

```toml
[accounts.personal_2]
kind = "codex_home"
path = "/absolute/path/to/comradex/accounts/personal-2"

[pools.default]
members = ["app", "personal_2"]
```

Then authenticate through the official client:

```sh
comradex account login personal_2
```

This executes `codex login --device-auth` with `CODEX_HOME` set to the isolated directory. Absolute account paths are used unchanged; relative paths are resolved against the canonical directory containing `comradex.toml`, just like a relative `proxy.state_dir`.

For each request, the daemon reads the account's `auth.json`, derives a missing account ID from the ID-token claims, and uses Codex's current OAuth refresh contract when the access token is near expiry or receives a 401. A bounded background sweep checks managed accounts once per minute and refreshes only tokens within five minutes of expiry, so rarely selected accounts do not depend on request-time refresh. Refreshes are single-flight per normalized, non-overlapping account home and atomically rotate `auth.json`; permanent refresh rejection marks only that account as requiring device login. The Codex App's own credentials remain unmanaged.

On macOS, `comradex account login` temporarily unloads an installed, running Comradex LaunchAgent while the official Codex client writes the selected account home, then restores the service and waits for its listeners. This prevents login and daemon refresh from racing over rotating credentials.

## Connecting Codex

```sh
comradex install
```

This edits `$CODEX_HOME/config.toml` (falling back to `~/.codex/config.toml`; override with `--codex-config`) and changes only the root `openai_base_url`. It preserves a symlinked `config.toml`, including dotfiles-managed configurations, and never sets `model_provider` or reads or writes `sessions/` or `rollouts/`.

The line it writes looks like this:

```toml
openai_base_url = "http://127.0.0.1:10100/<installation_secret>/v1"
```

The address belongs to the selected listener; `--listener` chooses the listener and therefore the account pool that serves the traffic. `<installation_secret>` is the random per-installation token written to `comradex.toml` by `init`.

Codex treats the value as an ordinary base URL, so every request arrives with the secret as a path prefix. Comradex rejects requests without `/<installation_secret>/v1`, strips the prefix from accepted requests, selects an account from the listener's pool, substitutes that account's credentials, and forwards the path, query, and body unchanged to `proxy.upstream`. To keep bearer tokens and ChatGPT account metadata pinned to their intended destination, loaded configurations require this upstream to be exactly `https://chatgpt.com/backend-api/codex`; custom hosts, cleartext HTTP, and URL variants are rejected. The listener binds to loopback, but the secret also prevents other local software from discovering an open relay to your accounts.

Reinstalling updates the Comradex URL without losing the original pre-Comradex value. That value is kept in `state/install.json`; `comradex uninstall` restores it, but refuses if somebody changed the Codex configuration after installation.

### Restarting Codex

Changing the file on disk does not update long-lived `codex app-server` processes that already loaded the old `openai_base_url`. `install` and `uninstall` warn when such processes are running. Pass `--restart-codex` to either command, or restart them separately:

```sh
comradex restart-codex
```

This sends SIGTERM only to matching processes owned by the current user: a Codex binary running the `app-server` subcommand or a `codex-code-mode-host` entrypoint. It never uses a broad `*codex*` match or SIGKILL. Active turns may be interrupted; the desktop app respawns its app-server automatically.

## Running as a macOS service

An installed Comradex binary can manage its own LaunchAgent:

```sh
comradex service install
comradex service status
comradex service restart
comradex service uninstall
```

`service restart` validates the edited configuration before stopping anything, then restarts the LaunchAgent and waits for every listener to answer a health probe. A broken edit fails validation and leaves the running daemon untouched. The configuration path comes from the installed plist, so the service always restarts against the configuration it actually uses regardless of `--config`.

The installer records the exact executable and configuration paths, validates the generated plist, starts the daemon at login, and writes logs beneath Comradex's state directory. Installation waits for launchd to report a running PID and verifies every configured listener with a Comradex-specific HTTP probe. A failed replacement restores the previous Comradex plist and loaded job when possible.

The service manages only `com.nicosuave.comradex`; it does not inspect, stop, or remove OpenCodex or any other relay. Stop OpenCodex first if it owns the same listener port. Codex configuration installation and service installation are separate reversible operations.

`service status` reports whether launchd currently has a running Comradex process. On SIGTERM, Comradex stops the listeners, aborts and joins tracked HTTP/WebSocket connection tasks, clears in-flight counters, and only then writes final affinity, file-owner, and statistics snapshots. Active requests are terminated rather than gracefully completed during shutdown.

## How routing works

Each listener maps to an account pool. Comradex uses Codex's continuity signals to keep related work on the same healthy account, while quota thresholds affect only the admission of new threads.

### Affinity and ownership

Routing recognizes client turn state, accepted Codex session/conversation headers, parent-thread and turn-metadata IDs, request `client_metadata.thread_id`, `previous_response_id`, and `prompt_cache_key`. Values are persisted only as keyed BLAKE3 hashes. Conflicting hard owners and unknown previous-response or turn-state anchors fail closed instead of crossing accounts.

Uploaded Codex files are also account-owned. Comradex records the creating account from successful `/files` responses, pins `/files/{file_id}/uploaded` finalization and Responses requests containing `file_id` to that account, and fails closed for conflicting or partially known multi-file ownership. Raw file IDs are hashed in a separate bounded `file-owners.json` snapshot and are never persisted directly; account authentication failures do not erase ownership.

HTTP requests and the frame-aware WebSocket modes enforce file ownership found in request bodies. Raw WebSocket mode intentionally cannot inspect frame-local file IDs.

### Quotas, retries, and streaming

Existing healthy bindings stay put even after usage crosses `switch_at`; the threshold controls only admission of new threads.

A pre-output quota response, account-scoped connection-establishment failure, or selected gateway failure may use one alternate only for native Responses or idempotent methods, never for hard account-owned continuity. Shared DNS and network-reachability failures remain account-neutral and do not rotate credentials. Successful or ambiguous Live Voice creation is never replayed. On HTTP, a managed-account 401 gets one same-account refresh retry, and a 401/403 never crosses accounts. No response is retried after visible output. When no alternate is eligible, the original upstream rejection and its `Retry-After` or reset headers are preserved; primary, secondary, and tertiary reset windows bound quota cooldowns.

Requests up to 256 KiB are replayed from memory by default; larger requests use a temporary file, and all bodies have a hard cap. Responses and upgraded streams are forwarded with backpressure and are never retained.

### Responses WebSocket modes

Select behavior with `proxy.responses_websocket_mode`:

- `http_bridge` is the default. It accepts downstream WebSocket frames and sends each `response.create` through the account-aware HTTP/SSE pipeline. Frame-level conversation metadata takes precedence over connection-level session and cache hints, so a new conversation on an existing downstream socket receives fresh quota-aware routing. Turn state, previous responses, and owned files remain pinned to their account. The bridge supersedes an older in-flight turn when a new create arrives and converts bounded, validated SSE or JSON lifecycle events back into WebSocket text frames.
- `raw` preserves the original handshake-pinned byte relay. One account owns the socket, and Comradex does not inspect its frames, so this mode cannot rotate accounts between conversations on a reused socket.
- `direct` keeps upstream WebSocket transport while routing and tracking each `response.create` frame independently. It supports multiplexed, out-of-order turns, reconnects only before visible output, refreshes an expired credential on the same account before considering one alternate, and can remove a stale `previous_response_id` only when the request contains a verified self-contained full resend. If safe internal replay is unavailable, it preserves Codex's canonical `previous_response_not_found` retry classifier while removing account-scoped details.

Direct mode uses the explicit refresh-then-alternate sequence above. Live Voice upgrades are separate from these modes and remain raw, call-bound relays.

HTTP bridge sessions have their own `proxy.max_bridge_sessions` limit (256 by default), separate from `proxy.max_upgrades`, which continues to bound raw/direct upstream upgrades and Live Voice. At bridge capacity, Comradex closes the least-recently-used idle session before admitting a replacement. Sessions with active turns are never evicted. Idle bridge sessions close after `proxy.bridge_idle_seconds` (900 by default), and admission waits up to `proxy.bridge_admission_timeout_millis` (2000 by default) for a closing session before returning a retryable `503 at_capacity` response with `Retry-After: 1`.

### Live Voice

Private Codex Live Voice call creation is account-bound. The daemon accepts only the exact successful `/v1/realtime/calls/{id}` `Location` form, stores only an immutable keyed digest in a bounded two-hour atomic snapshot, rejects ambiguous query forms, and pins every supported sideband WebSocket form to that exact healthy account across daemon restarts.

Missing, malformed, stale, conflicting, or unavailable bindings fail closed. Sideband frames and call SDP are never logged or retained.

## Docker verification

```sh
docker build -t comradex .
docker run --rm comradex --help
```

The model-driven review uses a separate container so its device credentials never touch the host:

```sh
docker build -f Dockerfile.review -t comradex-review .
docker volume create comradex-review-auth
docker run --rm -it -v comradex-review-auth:/root/.codex comradex-review login --device-auth
```

Running against the real Codex backend requires account authentication and should mount the configuration, state, and account directories explicitly. Isolated account homes must be writable if the daemon is expected to rotate OAuth credentials; run the container with the host account directories' numeric UID/GID rather than weakening their `0600` permissions.

## Status and statistics

```sh
comradex status
comradex status --json
```

`status` summarizes the configuration, LaunchAgent, Codex wiring, accounts, and live traffic. `--json` prints the bounded local `stats.json` snapshot written by the daemon. When running from source, use `cargo run -- status`. There is deliberately no credentialed admin HTTP endpoint.
