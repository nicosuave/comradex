# Approval translation

Session grants are owned by one facade thread and match the complete tool input,
tool name, and working directory. They do not modify native permission settings,
apply Claude permission suggestions, or grant access to a command prefix,
directory, or tool generally. The `never` approval policy overrides cached grants.
Forks must not inherit grants. Grant persistence and stale-response fencing are
the server's responsibility, not the pure helpers in `src/approvals.rs`.

Claude's `permission_suggestions` may contain `setMode: acceptEdits` or directory
grants; automatically applying these would exceed an exact-call approval. Native
`updatedPermissions` supports session destinations, but those belong to a native
process and do not by themselves preserve authorization across the facade's
per-turn processes. See `cli/structuredIO.ts` and
`utils/permissions/PermissionUpdateSchema.ts` in the inspected Claude source.

## Native question picker contract

The installed desktop bundle supports `item/tool/requestOptionPicker` despite its
absence from the vendored Codex 0.166.0 schema. This is a desktop extension, not a
claim about all app-server clients. The inspected bundle was
`/Applications/ChatGPT.app/Contents/Resources/app.asar`:

- `webview/assets/app-shared-6c00c2afcf84.js` registers and routes the request,
  with `threadId`, `turnId`, `question`, `options`, `allowMultiple`, `submitLabel`,
  and `skipLabel`.
- `webview/assets/pending-request-item-panel-66c58f1a7e3a.js` renders options as
  checkbox buttons when `allowMultiple` is true, otherwise radio buttons.
  Option selections are label strings. It emits
  `{action: "submit" | "skip" | "dismiss", selectedOptions: string[],
  freeformAnswer: string | null}`. Freeform text can accompany a selected option.
- Options use `{label: string, description?: string | null}`.

Single-select-only native calls continue using the pinned
`item/tool/requestUserInput` protocol. Calls containing a multi-select question
use one picker per question, sequentially. Each picker requires its own RPC ID,
but the server retains the original native request ID until all answers arrive.
Skip/dismiss denies the native request; no partial answers are submitted.

`tools/AskUserQuestionTool/AskUserQuestionTool.ts` in the inspected Claude source
defines native answers as a map of question text to string, with multi-select
answers comma-separated. The helper preserves selected labels, their response
order, and freeform text in that representation. Duplicate question text is
rejected because that native map could not represent both answers without loss.
