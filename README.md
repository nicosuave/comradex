# Comradex

> Workers of all accounts, unite.

Comradex is a small Rust relay that gives the native Codex App/CLI a sticky, quota-aware collective of ChatGPT accounts without rewriting Codex sessions or rollouts. Raw mode preserves Responses WebSocket bytes; the two frame-aware modes intentionally inspect and, where required for safe recovery, reframe Responses traffic.

The current implementation intentionally supports only native Codex traffic. It has no provider translation, request history, GUI, telemetry, or conversation cache.

## Quick start

Install the latest release with Homebrew:

```sh
brew install nicosuave/tap/comradex
```

Or install a prebuilt release with cargo-binstall directly from GitHub:

```sh
cargo binstall --git https://github.com/nicosuave/comradex comradex
```

Then create and validate a configuration:

```sh
comradex init
comradex check
comradex serve
```

Every command reads `~/.config/comradex/comradex.toml` by default; `init` creates it there. Pass `--config <path>` to use a different location.

To build from source instead:

```sh
git clone git@github.com:nicosuave/comradex.git
cd comradex
cargo run -- init
cargo run -- check
cargo run -- serve
```

In another terminal, install the listener into Codex's existing config:

```sh
comradex install
```

This edits `$CODEX_HOME/config.toml` (falling back to `~/.codex/config.toml`; override with `--codex-config`) and changes only the root `openai_base_url`. It preserves a symlinked `config.toml` (including dotfiles-managed configurations), never sets `model_provider`, and never reads or writes `sessions/` or `rollouts/`. Reinstalling updates the Comradex URL without losing the original pre-Comradex value. `uninstall` restores that original value, but refuses if somebody changed it after installation.

The single line it writes looks like:

```toml
openai_base_url = "http://127.0.0.1:10100/<installation_secret>/v1"
```

The address is the chosen listener's (`--listener` selects which listener — and therefore which pool — serves the traffic), and `<installation_secret>` is the random per-installation token `init` generated into `comradex.toml`. Codex treats the whole thing as an ordinary base URL, so every request it makes arrives with the secret as a path prefix. The daemon rejects any request whose path does not start with `/<installation_secret>/v1`: the listener binds to loopback, but any local process can open a loopback port, and the secret prefix is what stops other software from discovering an open relay to your accounts. Comradex strips the prefix, applies pool routing to pick an account, swaps in that account's credentials, and forwards the request (path, query, and body intact) to `proxy.upstream` — `https://chatgpt.com/backend-api/codex` by default. The original `openai_base_url` is kept in `state/install.json` as the recovery record `uninstall` restores from.

Rewriting the config on disk is not enough while long-lived `codex app-server` processes (the Codex desktop app's background host, or CLI hosts) keep the previous `openai_base_url` in memory. `install` and `uninstall` warn when such processes are running; pass `--restart-codex`, or run `comradex restart-codex`, to send SIGTERM to exactly those processes (active turns may be interrupted). Matching is narrow — a Codex binary running the `app-server` subcommand, or a `codex-code-mode-host` entrypoint, owned by the current user — never a broad `*codex*` pattern, and never SIGKILL. The desktop app respawns its app-server automatically.

On macOS, an installed Comradex binary can manage its own LaunchAgent:

```sh
comradex service install
comradex service status
comradex service restart
comradex service uninstall
```

The daemon reads `comradex.toml` once at startup, so after editing it (adding an account, changing pools or listeners) run `comradex service restart`. It validates the edited configuration first — a broken edit fails the restart and leaves the running daemon untouched — then bounces the LaunchAgent and waits for every listener to answer a health probe. The config path comes from the installed plist, so it restarts against the configuration the service actually uses regardless of `--config`.

The service installer records the exact executable and configuration paths, validates the generated plist, starts the daemon at login, and writes logs beneath Comradex's state directory. Installation waits for launchd to report a running PID and verifies every configured listener with a Comradex-specific HTTP probe; a failed replacement restores the previous Comradex plist and loaded job when possible. It manages only `com.nicosuave.comradex`: it does not inspect, stop, or remove OpenCodex or any other relay. Stop OpenCodex first if it owns the same listener port. Codex configuration installation and service installation remain separate reversible operations.

`service status` reports whether launchd currently has a running Comradex process. `SIGTERM` stops the listeners, aborts and joins tracked HTTP/WebSocket connection tasks, clears in-flight counters, and only then writes final affinity, file-owner, and statistics snapshots. Active requests are terminated rather than gracefully completed during shutdown.

The generated configuration contains the special `caller` account. It forwards the Codex App's inbound `Authorization` and `ChatGPT-Account-Id` headers and never stores or refreshes them.

Add a managed account in one step:

```sh
comradex account add personal_2
```

That appends a `codex_home` account (home directory `accounts/personal_2` next to the config), adds it to the pool (`--pool` to choose another), validates the edited configuration through the full loader before persisting it, restarts the daemon if the service is installed, and runs the interactive device login (`--no-login` to defer it). `comradex account list` shows each account's login state; `comradex account remove <name>` takes it out of the configuration and every pool, keeping the credentials directory unless `--purge` is passed.

The equivalent manual configuration:

```toml
[accounts.personal_2]
kind = "codex_home"
path = "/absolute/path/to/comradex/accounts/personal-2"

[pools.default]
members = ["caller", "personal_2"]
```

Then authenticate through the official client:

```sh
comradex login personal_2
```

That executes `codex login --device-auth` with `CODEX_HOME` set to the isolated directory. Absolute account paths are used unchanged; relative account paths are resolved against the canonical directory containing `comradex.toml`, just like a relative `proxy.state_dir`. The daemon reads its `auth.json` for each request, derives a missing account ID from the ID-token claims, and uses Codex's current OAuth refresh contract when the access token is near expiry or receives a 401. Refreshes are single-flight per isolated account and atomically rotate `auth.json`; inbound caller credentials remain unmanaged.

## Routing contract

Routing recognizes client turn state, accepted Codex session/conversation headers, parent-thread and turn-metadata IDs, request `client_metadata.thread_id`, `previous_response_id`, and `prompt_cache_key`. Values are persisted only as keyed BLAKE3 hashes. Conflicting hard owners and unknown previous-response/turn-state anchors fail closed instead of crossing accounts.

Uploaded Codex files are account-owned. Comradex records the creating account from successful `/files` responses, pins `/files/{file_id}/uploaded` finalization and Responses requests containing `file_id` to that account, and fails closed for conflicting or partially-known multi-file ownership. Raw file IDs are hashed in a separate bounded `file-owners.json` snapshot and are never persisted directly; account authentication failures do not erase ownership. HTTP requests and the frame-aware WebSocket modes enforce file ownership found in request bodies; raw WebSocket mode intentionally cannot inspect frame-local file IDs.

Responses WebSocket behavior is selected with `proxy.responses_websocket_mode`:

- `raw` is the default and preserves the original handshake-pinned byte relay. One account owns the socket, and Comradex does not inspect its frames.
- `http_bridge` accepts downstream WebSocket frames and sends each `response.create` through the account-aware HTTP/SSE pipeline. It supersedes an older in-flight turn when a new create arrives and converts bounded, validated SSE or JSON lifecycle events back into WebSocket text frames.
- `direct` keeps upstream WebSocket transport while routing and tracking each `response.create` frame independently. It supports multiplexed, out-of-order turns, reconnects only before visible output, refreshes an expired credential on the same account before considering one alternate, and can remove a stale `previous_response_id` only when the request contains a verified self-contained full resend.

Live Voice upgrades are separate from these modes and remain raw, call-bound relays.

Existing healthy bindings stay put even after usage crosses `switch_at`. The threshold controls only admission of new threads. A pre-output quota response, genuine connection-establishment failure, or selected gateway failure may use one alternate only for native Responses or idempotent methods, never for hard account-owned continuity. Successful or ambiguous Live Voice creation is never replayed. On HTTP, a managed-account 401 gets one same-account refresh retry and a 401/403 never crosses accounts. Direct WebSocket mode uses the explicit refresh-then-alternate sequence described above. No response is retried after visible output. `Retry-After` and primary/secondary/tertiary reset windows bound quota cooldowns.

Requests up to 256 KiB are replayed from memory by default; larger requests use a temporary file and all bodies have a hard cap. Responses and upgraded streams are forwarded with backpressure and are never retained.

Private Codex Live Voice call creation is also account-bound. The daemon accepts only the exact successful `/v1/realtime/calls/{id}` `Location` form, stores only an immutable keyed digest in a bounded two-hour atomic snapshot, rejects ambiguous query forms, and pins every supported sideband WebSocket form to that exact healthy account across daemon restarts. Missing, malformed, stale, conflicting, or unavailable bindings fail closed; sideband frames and call SDP are never logged or retained.

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

Running against the real Codex backend requires account authentication and should mount the configuration/state/account directories explicitly. Isolated account homes must be writable if the daemon is expected to rotate OAuth credentials; run the container with the host account directories' numeric UID/GID rather than weakening their `0600` permissions.

## Status

`cargo run -- stats` prints the bounded local `stats.json` written by the daemon. There is deliberately no credentialed admin HTTP endpoint.
