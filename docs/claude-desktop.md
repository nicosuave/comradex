# Claude Desktop gateway

Comradex can serve Claude Desktop on 3P through a loopback Anthropic Messages
gateway. The picker can offer local MLX-Serve models, GPT models from the
existing ChatGPT subscription pool, and configured local ACP agents. Claude
Desktop owns the chat history. Claude's tool loop runs on the MLX and GPT
routes. A selected ACP agent runs its own turn and streams its reply to
Claude Desktop.

## Configure Comradex

Add these sections to the existing Comradex configuration. Generate a dedicated
gateway key with `openssl rand -hex 32` and substitute it for the placeholder.
Use the port of your running MLX-Serve instance.

```toml
[proxy.claude_desktop]
address = "127.0.0.1:4598"
api_key = "replace-me"
mlx_serve = "127.0.0.1:11234"

[proxy.claude_desktop.codex]
pool = "codex-subscriptions"

[proxy.claude_desktop.codex.models]
claude-sonnet-gpt-astra = "gpt-6-astra"
```

The `codex` sections are optional. Their pool must contain only existing
managed `codex_home` accounts. Set each alias to a model available to that
pool. Alias names start with `claude-sonnet-gpt-`. Remove `mlx_serve` for a
GPT-only gateway, or omit the Codex sections for a local-only gateway.
A request goes to the backend of its selected model. GPT requests use the
pool's regular account selection, session affinity, and quota handling.

### Add an ACP agent

Add a picker entry to the same profile. Sign in through the agent's own CLI
before using it here. Use absolute paths for the executable and working
directory. Add an optional `model` field to select a model that the agent
advertises through ACP.

```toml
[proxy.claude_desktop.acp.claude-sonnet-acp-antigravity]
display_name = "Antigravity (ACP)"
executable = "/opt/homebrew/bin/agy"
args = ["acp"]
cwd = "/path/to/workspace"
path = "/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin"
```

Add more agents with distinct `claude-sonnet-acp-` aliases. For an ACP-only
gateway, omit both `mlx_serve` and the Codex sections. The gateway runs the
configured program as the service user in the specified working directory.
Set `path` to the agent's executable search path if it uses tools or
interpreters outside the service's PATH. Use absolute directories separated
by colons. The setting applies only to the selected agent and its children.
Use an agent and workspace whose native tool policy you trust.

Run `comradex check` against the updated configuration. On macOS with the
managed service, apply the change with `comradex service restart`.
The gateway accepts its key as a Bearer token or in `x-api-key`. Its
`GET /v1/models` response lists configured GPT and ACP aliases and local
MLX-Serve chat models. The gateway checks the local catalog before dispatch
and again before releasing a successful response. MLX entries advertise 1M context
when upstream metadata reports support. The gateway waits up to 300 seconds
for MLX-Serve to send response headers. Set `mlx_response_timeout_seconds`
in `[proxy.claude_desktop]` to adjust that timeout.

## Configure Claude Desktop

Open **Developer → Configure Third-Party Inference…** and select **Gateway**
with **Static API key**. Enter the gateway origin, such as
`http://127.0.0.1:4598`, and provide the dedicated key. Use discovery or enter
the exact alias IDs from the model catalog in the model list. Leave the
regular claude.ai sign-in available through the deployment mode chooser.

Set `inferenceGatewayAutoModeServerChecks` to `false` in the third-party
configuration to use Claude's built-in Auto mode review. The gateway returns an
error for provider-side review requests. Use standard client-executed tools
with inline schemas on the GPT route.

The GPT route supports streaming and non-streaming Messages, text and image
inputs, client tool calls and results, and token usage. GPT output limits are
set by the Codex backend. The route skips Claude-specific thinking blocks on
replay. The MLX route forwards Messages to MLX-Serve's native API. Both routes
return usage on Messages responses.

For ACP, select the agent by its display name. Each Messages request starts
a fresh ACP session and supplies the Claude chat transcript as context. ACP
agent replies stream as text. Agent tools follow the agent's native policy.
The gateway declines new ACP permission requests because Claude Desktop's
Messages interface has no ACP approval channel. This route has no shared
native session IDs with [ACP chats in Codex Desktop](../claudex/DESKTOP.md#acp-models-in-the-local-picker).

For context windows below Claude Desktop's assumed size, set
`CLAUDE_CODE_MAX_CONTEXT_TOKENS` for that deployment. Verify Code and Cowork
with the chosen models before switching a regular workflow to this profile.

[Claude Desktop gateway setup](https://claude.com/docs/third-party/claude-desktop/gateway) ·
[Claude Desktop deployment modes](https://support.claude.com/en/articles/18466091-plan-identity-and-sign-in) ·
[MLX-Serve Messages API](https://github.com/ddalcu/mlx-serve/blob/main/docs/api.md#post-v1messages-anthropic) ·
[Codex Responses request](https://github.com/openai/codex/blob/main/codex-rs/codex-api/src/common.rs)
