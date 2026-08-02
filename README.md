# Comradex

> Workers of all accounts, unite.

Comradex is a small Rust relay that gives the native Codex App/CLI a sticky, quota-aware collective of ChatGPT accounts without rewriting Codex sessions, rollouts, Responses payloads, SSE events, or WebSocket frames.

The current implementation intentionally supports only native Codex traffic. It has no provider translation, request history, GUI, telemetry, or conversation cache.

## Quick start

```sh
git clone git@github.com:nicosuave/comradex.git
cd comradex
cargo run -- init
cargo run -- check
cargo run -- serve
```

In another terminal, install the listener into Codex's existing config:

```sh
cargo run -- install --codex-config "$CODEX_HOME/config.toml"
```

This changes only the root `openai_base_url`. It never sets `model_provider` and never reads or writes `sessions/` or `rollouts/`. `uninstall` restores the previous value, but refuses if somebody changed it after installation.

The generated configuration contains the special `caller` account. It forwards the Codex App's inbound `Authorization` and `ChatGPT-Account-Id` headers and never stores or refreshes them.

Add isolated accounts and pools manually:

```toml
[accounts.personal_2]
kind = "codex_home"
path = "/absolute/path/to/comradex/accounts/personal-2"

[pools.default]
members = ["caller", "personal_2"]
```

Then authenticate through the official client:

```sh
cargo run -- login personal_2
```

That executes `codex login --device-auth` with `CODEX_HOME` set to the isolated directory. The daemon reads its `auth.json` for each request, derives a missing account ID from the ID-token claims, and uses Codex's current OAuth refresh contract when the access token is near expiry or receives a 401. Refreshes are single-flight per isolated account and atomically rotate `auth.json`; inbound caller credentials remain unmanaged.

## Routing contract

Routing recognizes client turn state, accepted Codex session/conversation headers, parent-thread and turn-metadata IDs, request `client_metadata.thread_id`, `previous_response_id`, and `prompt_cache_key`. Values are persisted only as keyed BLAKE3 hashes. Conflicting hard owners and unknown previous-response/turn-state anchors fail closed instead of crossing accounts.

Existing healthy bindings stay put even after usage crosses `switch_at`. The threshold controls only admission of new threads. A pre-output quota response, genuine connection-establishment failure, or selected gateway failure may use one alternate only for native Responses or idempotent methods, never for hard account-owned continuity. Successful or ambiguous Live Voice creation is never replayed. A managed-account 401 gets one same-account refresh retry; a 401/403 never crosses accounts, and no response is retried after headers or body bytes have reached the client. `Retry-After` and primary/secondary reset windows bound quota cooldowns.

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
