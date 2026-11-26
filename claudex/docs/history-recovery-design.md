# Native history recovery

`history_recovery::plan_prefix` resolves a requested facade prefix to a native
Claude session ID and inclusive message UUID. The facade must never implement
recovery by changing only its displayed turns.

## Integration contract

- Persist `#[serde(default)] turn_anchors: BTreeMap<String, NativeAnchor>` in each
  record. Capture the final observed native user/assistant UUID and its native
  session ID for each terminal turn. Do not manufacture an endpoint from the
  preceding turn if the current turn's native persistence is unknown.
- Resolve `Full`, `BeforeTurn`, `ThroughTurn`, or `Rollback` while the source is
  idle. Reject ephemeral native history. `for_fork` parses the mutually exclusive
  `beforeTurnId` and `lastTurnId` options.
- Before mutating durable state, obtain the plan. Truncate facade turns to
  `retained_len`, prune discarded turn anchors and item timing entries, and
  refresh derived preview/token state. Preserve the retained anchors' original
  session IDs, including when they predate a previous recovery branch.
- Allocate a fresh native `session_id`, set `has_session = false`, and set
  `fork_from` and `backend_message_id` from `plan.anchor`. For `anchor = None`,
  clear both: the next prompt starts a fresh native session.
- Save all the above as one durable record update before returning success.
  The next turn uses the existing backend's `--resume`, `--fork-session`, and
  `--resume-session-at` path. Never use ordinary latest-session resume after
  applying a prefix.
- A fork copies only retained history/anchors into its new facade record.
  `thread/revert` keeps the facade thread ID and returns metadata with empty
  `turns`, pagination cursors, and `thread/reverted`. Legacy `thread/rollback`
  returns `{thread}` with populated retained turns. Register its explicit schema
  because the pinned schema export omits that legacy method.
- Root-level integration must validate idle/loaded/history-mode requirements and
  verify the resumed model cannot recall removed content. The module tests cover
  boundary selection, fail-closed legacy handling, and native-session selection;
  they do not claim live Claude validation.

## Evidence

- `protocol/codex-0.170.0.json`, `ThreadForkParams`: `beforeTurnId` excludes its
  turn; `lastTurnId` includes its turn, cannot be combined with `beforeTurnId`,
  and cannot target an in-progress turn.
- Same schema, `ThreadRevertParams` and `ThreadRevertResponse`: revert replaces
  paginated durable history with the prefix before the turn; it does not undo
  filesystem changes; response turns are empty and use pagination cursors.
- Upstream Codex paths `codex-rs/app-server-protocol/src/protocol/v2/thread.rs`
  and `codex-rs/core/src/context_manager/history.rs` provide legacy rollback
  context: a positive turn count removes that suffix, including all turns when
  the count exceeds the available history.
- Claude's `--resume`, `--fork-session`, and `--resume-session-at` behavior is
  checked by the native prefix-recovery probes described below. The backend
  supplies the stored native session/message identifiers rather than synthesizing
  a transcript. These flags are version-sensitive; the supported runtime is
  documented in the README.

The persisted `tracks_turn_anchors` marker prevents a previous turn's UUID from substituting for a newer turn that failed before native startup. Old facade records lack per-turn UUIDs. A latest UUID can recover their full
snapshot, but cannot identify an earlier prefix. Such requests fail explicitly.
If native Claude can no longer find a recorded UUID (for example after native
history compaction), native startup must fail rather than substitute a different
history. Recovery changes conversation history only; it does not rewind files,
external actions, or Claude's independent persistent memory.

Live acceptance on October 8, 2026: both exclusive earlier fork and paginated revert retained the first random code and excluded the second code from native Claude recall. `tests/history_wire.rs` also validates recovery across facade restart and rejects recovery retaining a native startup-failed endpoint.
