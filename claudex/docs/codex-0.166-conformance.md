# Codex app-server 0.160.0 → 0.166.0 conformance

This checklist covers every difference between the vendored 0.160.0 and
0.166.0 schemas (`codex app-server generate-json-schema --experimental`),
plus the installed desktop's version gates in that range, with the facade's
disposition for each. The schema list was generated mechanically, so nothing
is omitted. The facade now advertises 0.166.0.

Dispositions:

- **Done**: implemented and covered by tests.
- **Unchanged**: the facade's existing output already conforms, usually
  because a new field is optional and the facade does not produce it. The
  reason is given.
- **Codex**: the method is forwarded to the configured genuine Codex
  executable, so its output is Codex's own.
- **N/A**: the facade does not serve the containing method. The reason is given.

## Client requests (12 new)

| Method | Disposition |
| --- | --- |
| `thread/sample` | Done: a read-only side question on the parent's warm process, checked against `expectedTurnId` |
| `thread/sample/cancel` | Done: cancels the side question and fails the sample |
| `thread/readState/update` | Done: durable read state with revision conflicts, as upstream |
| `thread/items/read` | Done: stored order, unknown ids omitted, 1 to 100 ids |
| `thread/attachmentOwner/list` | Done: persisted owners; the router merges Claude owners with Codex's |
| `thread/memory/read` | Done: `summary: null`; Codex memories are not part of a Claude thread's context |
| `thread/prediction/request` | Done: `-32601 not implemented yet`, the same answer genuine Codex 0.166 gives (probed) |
| `feedback/save` | Done: genuine Codex writes the archive, and a Claude thread adds its saved record |
| `account/bedrock/checkGovCloudRequirements` | N/A: Codex account management; the router sends it to Codex |
| `account/gcp/setup` | N/A: as above |
| `accountSession/list` | N/A: as above |
| `accountSession/switch` | N/A: as above |

## Server notifications (2 new)

| Notification | Disposition |
| --- | --- |
| `thread/readState/changed` | Done: after a mark, and when a terminal result needs attention |
| `thread/prediction/updated` | N/A: answers `thread/prediction/request`, which Codex does not implement |

## Changed types the facade accepts or emits

| Type | Change | Disposition |
| --- | --- | --- |
| `TurnStartParams` | `parentTurnId`, `rootTurnId` | Done; delegated children send them |
| `TurnStartParams`, `TurnSteerParams` | `responsesapiClientMetadataPatch` | Done: accepted, ignored like `responsesapiClientMetadata` |
| `Turn` | `rootTurnId` | Done: the turn's own id unless supplied; native subagent turns inherit it |
| `ThreadListParams` | `excludedThreadIds`, `attachedToThreadId` | Done, in the facade and in router merges |
| `ThreadListResponse` | `readStates` | Done, merged by the router per backend |
| `ThreadReadResponse` | `readState` | Done |
| `ThreadForkParams` | `attachToSource` | Done: persistent side chats; deleting the source requires deleting them first |
| `ThreadForkParams` | `experimentalPredictionMode`, `experimentalDisablePredictionMcp` | Done |
| `ThreadItem` | item-level `startedAtMs`, `completedAtMs` | Done on every returned history item and completed turn |
| `ThreadItem` (`subAgentActivity`) | `model`, `reasoningEffort` | Unchanged: Claude subagents are `collabAgentToolCall` items, which already carry both |
| `ThreadItem` (`imageGeneration`) | `generationId`, `inputImages`, file-id variant | Unchanged: Claude produces no image generation items |
| `CodexErrorInfo` | `modelNotFound`, `policyCapacityUnavailable` | Unchanged: Claude failures report `codexErrorInfo: null` |
| `MessagePhase` | `partial_answer` added | Unchanged: the facade emits `final_answer` or null, both still valid |
| `AdditionalContextEntry` | `name` | Unchanged: the whole object is passed to Claude as reference text |
| `HooksListParams` | `threadId` | Done: the thread's cwd selects its hooks |
| `ListMcpServerStatusParams` | `threadId` (already in 0.160, previously rejected) | Done: accepted |
| `HookEventName`, `HookMetadata`, `HookRunSummary` | new events, `environmentId`, `sourcePath` type | Codex |
| `SkillMetadata`, `SkillSummary` | `path` type | Codex |
| `McpServerStatus` | `toolListMeta` | Codex |
| `McpServerOauthLoginResponse`, `McpServerOauthLoginCompletedNotification` | `loginId` | Codex |
| `ConfigRequirementsReadResponse`, `ConfigRequirements`, `ApplicationRequirements`, `BrowserUseRequirements`, `NetworkRequirements` | `supportsIndependentSpeedModes`, `otel`, `storage`, `extension`, `reviewDomains` | Unchanged: optional; the facade reports only its own sandbox and approval requirements |
| `ProcessExitedNotification` | `outputDrainTimedOut` | Unchanged: optional |
| `Account` | `chatgpt.workspaceName`, `gcp` variant | Unchanged: the facade reports `account: null` |
| `ThreadGoalSetParams`, `ThreadGoalClearParams` | `origin` | N/A: goals are a Codex runtime feature; `thread/goal/get` returns null and set/clear stay unsupported |
| `TurnToolOutput` | `retain` | N/A: `turn/start.toolOutput` is not supported for Claude threads |

## Changed types in methods the facade does not serve

| Type | Change | Reason |
| --- | --- | --- |
| `EnvironmentAddParams` | `skills`, `websocketRequestId` | `environment/add` |
| `ModelProviderCapabilitiesReadResponse` | `namespaceTools` removed | `modelProvider/capabilities/read` |
| `MisalignmentErrorDetails` | `reviewTarget` | OpenAI misalignment review |
| `CyberAccessProgram`, `ReviewStartParams`, `ReviewStartResponse`, `ThreadSearchOccurrencesParams` | descriptions only | no wire change |
| `PlanType` | `sci`, `sci_plus` | ChatGPT plans |
| `RealtimeVoice`, `RealtimeVoicesList`, `ThreadRealtimeStartParams` | voices, `v3`, `voiceSessionId` | realtime voice |
| `ReviewDelivery` | `detached` removed | `review/start` |
| `ResponseItem`, `ContentItem` | `encrypted_content`, `status`, `additional_tools`, `annotations`, `logprobs` | Responses API items; the facade emits thread items |
| `ThreadUsage` | `nativeUsageUsdMicros` | `account/tokenUsage` |

## Desktop gates between 0.160.0 and 0.166.0

The installed desktop's gates turn on at these versions. Usage was traced in
the desktop bundle.

| Gate | Version | What the desktop does | Disposition |
| --- | --- | --- | --- |
| `threadGoalProvenance` | 0.161.0-alpha.5 | `origin` on `thread/goal/set`/`clear` | N/A: unsupported for Claude threads before and after |
| `threadAttachmentOwners` | 0.162.0-alpha.1 | `thread/attachmentOwner/list` for worktree cleanup and settings | Done |
| `incrementalTools` | 0.162.0-alpha.17 | `features.incremental_tools` config | Done: `features.*` config is accepted and ignored |
| `misalignmentReviewTarget` | 0.162.0-alpha.17 | review target after a misalignment failure | N/A: never reported for Claude |
| `turnLineage` | 0.162.0-alpha.17 | keeps `parentTurnId`/`rootTurnId` on `turn/start` | Done |
| `capacityErrorRetries` | 0.162.0-alpha.18 | no client auto-retry for `serverOverloaded` | Unchanged: never reported; Claude Code retries overloads itself |
| `experimentalPredictionForkMode` | 0.162.0-alpha.20 | prediction forks and title refresh | Done, including title refresh's `outputSchema` |
| `threadReadState` | 0.163.0-alpha.4 | read receipts | Done |
| `preserveForkPrefix` | 0.163.0-alpha.4 | `features.*` config | Done: ignored |
| `outputTokenReplay` | 0.163.0-alpha.6 | `features.*` config | Done: ignored |
| `asyncCompaction` | 0.163.0-alpha.11 | `features.*` config | Done: ignored |
| `realtimeVoiceSessionId` | 0.163.0-alpha.11 | realtime voice | N/A |
| `predictionDisableMcp` | 0.164.0-alpha.7 | prediction fork flag | Done |
| `threadSample` | 0.164.0-alpha.11 | `thread/sample` predictions | Done |
| `feedbackSave` | 0.164.0-alpha.11 | `feedback/save` on local stdio connections | Done |
| `threadCallableApps` | 0.164.0-alpha.11 | not checked anywhere in the desktop | N/A |
| `threadMemorySummary` | 0.165.0-alpha.1 | `thread/memory/read` for realtime voice | Done |
| `attachedThreads` | 0.165.0-alpha.1 | side chat listing and child-first deletion | Done |
| `persistentSideChats` | 0.166.0-alpha.1 | `thread/fork` with `attachToSource` | Done |
| `skillCatalogDeltas` | 0.166.0-alpha.1 | `features.*` config | Done: ignored |
| `guardianNativeMultiAgentHistory` | 0.166.0-alpha.3 | `features.*` config | Done: ignored |
