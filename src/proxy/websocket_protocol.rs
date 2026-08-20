//! Pure protocol state for account-aware Responses WebSocket relays.
//!
//! This module deliberately contains no sockets, tasks, authentication, or
//! account selection.  A relay can use it to associate multiplexed upstream
//! events with requests and to decide whether replay or close recovery is
//! protocol-safe before performing any side effects.

use std::collections::{HashMap, HashSet, VecDeque};

use serde_json::Value;

pub const DEFAULT_MAX_PENDING_TURNS: usize = 64;
pub const HARD_MAX_PENDING_TURNS: usize = 1_024;
pub const DEFAULT_MAX_IDENTIFIER_BYTES: usize = 512;
pub const HARD_MAX_IDENTIFIER_BYTES: usize = 4_096;
pub const DEFAULT_MAX_TRACKED_TOOL_CALLS: usize = 1_024;
pub const HARD_MAX_TRACKED_TOOL_CALLS: usize = 16_384;
pub const HARD_MAX_REPLAYS_PER_TURN: u8 = 1;
pub const HARD_MAX_AUTH_REFRESH_REPLAYS_PER_TURN: u8 = 1;
pub const HARD_MAX_AUTH_FAILOVER_REPLAYS_PER_TURN: u8 = 1;
pub const MAX_CLASSIFIED_CODE_BYTES: usize = 128;
pub const MAX_CLASSIFIED_MESSAGE_BYTES: usize = 2_048;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TurnId(u64);

impl TurnId {
    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolLimits {
    pub max_pending_turns: usize,
    pub max_identifier_bytes: usize,
    pub max_tracked_tool_calls: usize,
    /// Non-auth replay budget. Authentication has two independent stages.
    pub max_replays_per_turn: u8,
    pub max_auth_refresh_replays_per_turn: u8,
    pub max_auth_failover_replays_per_turn: u8,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_pending_turns: DEFAULT_MAX_PENDING_TURNS,
            max_identifier_bytes: DEFAULT_MAX_IDENTIFIER_BYTES,
            max_tracked_tool_calls: DEFAULT_MAX_TRACKED_TOOL_CALLS,
            max_replays_per_turn: 1,
            max_auth_refresh_replays_per_turn: 1,
            max_auth_failover_replays_per_turn: 1,
        }
    }
}

impl ProtocolLimits {
    pub fn validate(self) -> Result<Self, ProtocolError> {
        if self.max_pending_turns == 0 || self.max_pending_turns > HARD_MAX_PENDING_TURNS {
            return Err(ProtocolError::InvalidLimits("max_pending_turns"));
        }
        if self.max_identifier_bytes == 0 || self.max_identifier_bytes > HARD_MAX_IDENTIFIER_BYTES {
            return Err(ProtocolError::InvalidLimits("max_identifier_bytes"));
        }
        if self.max_tracked_tool_calls == 0
            || self.max_tracked_tool_calls > HARD_MAX_TRACKED_TOOL_CALLS
        {
            return Err(ProtocolError::InvalidLimits("max_tracked_tool_calls"));
        }
        if self.max_replays_per_turn > HARD_MAX_REPLAYS_PER_TURN {
            return Err(ProtocolError::InvalidLimits("max_replays_per_turn"));
        }
        if self.max_auth_refresh_replays_per_turn > HARD_MAX_AUTH_REFRESH_REPLAYS_PER_TURN {
            return Err(ProtocolError::InvalidLimits(
                "max_auth_refresh_replays_per_turn",
            ));
        }
        if self.max_auth_failover_replays_per_turn > HARD_MAX_AUTH_FAILOVER_REPLAYS_PER_TURN {
            return Err(ProtocolError::InvalidLimits(
                "max_auth_failover_replays_per_turn",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    InvalidLimits(&'static str),
    NotResponseCreate,
    PendingLimitReached,
    IdentifierTooLong(&'static str),
    TurnIdExhausted,
    UnknownTurn(TurnId),
    DuplicateResponseId,
    ResponseCreatedHasNoPendingTurn,
    EventAssociationAmbiguous,
    ReplayNotEligible(ReplayRefusal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullResendRefusal {
    NoPreviousResponse,
    InputIsNotAnArray,
    InsufficientHistory,
    UnmatchedToolOutput,
    TooManyToolCalls,
    FileBacked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullResendSafety {
    Eligible,
    Refused(FullResendRefusal),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateAnalysis {
    pub previous_response_id: Option<String>,
    pub input_item_count: usize,
    pub has_file_references: bool,
    pub full_resend: FullResendSafety,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceNumber {
    Signed(i64),
    Unsigned(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalKind {
    Completed,
    Failed,
    Cancelled,
    Incomplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    Completed,
    Failed,
    Cancelled,
    Incomplete,
    RejectedInput,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettledTurn {
    pub id: TurnId,
    pub response_id: Option<String>,
    pub settlement: Settlement,
    pub finite_sequence_visible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTurn {
    id: TurnId,
    original_previous_response_id: Option<String>,
    previous_response_id: Option<String>,
    response_id: Option<String>,
    response_created: bool,
    response_event_count: u32,
    downstream_visible: bool,
    last_visible_sequence: Option<SequenceNumber>,
    replay_count: u8,
    auth_refresh_replay_count: u8,
    auth_failover_replay_count: u8,
    analysis: CreateAnalysis,
}

impl PendingTurn {
    pub fn id(&self) -> TurnId {
        self.id
    }

    pub fn previous_response_id(&self) -> Option<&str> {
        self.previous_response_id.as_deref()
    }

    pub fn original_previous_response_id(&self) -> Option<&str> {
        self.original_previous_response_id.as_deref()
    }

    pub fn response_id(&self) -> Option<&str> {
        self.response_id.as_deref()
    }

    pub fn response_created(&self) -> bool {
        self.response_created
    }

    pub fn response_event_count(&self) -> u32 {
        self.response_event_count
    }

    pub fn downstream_visible(&self) -> bool {
        self.downstream_visible
    }

    pub fn last_visible_sequence(&self) -> Option<SequenceNumber> {
        self.last_visible_sequence
    }

    pub fn replay_count(&self) -> u8 {
        self.replay_count
    }

    pub fn auth_refresh_replay_count(&self) -> u8 {
        self.auth_refresh_replay_count
    }

    pub fn auth_failover_replay_count(&self) -> u8 {
        self.auth_failover_replay_count
    }

    pub fn analysis(&self) -> &CreateAnalysis {
        &self.analysis
    }

    fn is_precreated(&self) -> bool {
        !self.response_created && self.response_event_count == 0 && self.response_id.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventAssociation {
    pub turn_ids: Vec<TurnId>,
    pub event_type: Option<String>,
    pub response_id: Option<String>,
    pub terminal: Option<TerminalKind>,
    pub failure: FailureClassification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    None,
    Quota,
    Authentication { requires_reauthentication: bool },
    PreviousResponseNotFound,
    Transient,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureClassification {
    pub kind: FailureKind,
    pub code: Option<String>,
    pub message: Option<String>,
    pub response_id: Option<String>,
}

impl FailureClassification {
    fn none() -> Self {
        Self {
            kind: FailureKind::None,
            code: None,
            message: None,
            response_id: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayMode {
    OriginalRequest,
    FreshRequestWithoutPreviousResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayTarget {
    /// Routing is failure-specific and remains the integration's decision.
    Unspecified,
    /// Refresh credentials and reconnect to the account that just failed.
    SameAccountAfterRefresh,
    /// Exclude the account that just failed and select another account.
    AlternateAccount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayPlan {
    pub mode: ReplayMode,
    pub target: ReplayTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayRefusal {
    UnknownTurn,
    MultiplePendingTurns,
    AlreadyReplayed,
    AuthReplaySequenceExhausted,
    ResponseCreated,
    PriorResponseEvent,
    DownstreamVisible,
    FiniteSequenceVisible,
    FileBacked,
    ResponseIdAssigned,
    HardContinuity,
    UnsafeFullResend(FullResendRefusal),
    UnsupportedFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayDecision {
    Eligible(ReplayMode),
    Refused(ReplayRefusal),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayContext {
    /// True when the failure event being classified carries a response ID.
    /// Quota failures may still be pre-created capacity rejection envelopes;
    /// other failures with an ID are treated as accepted work.
    pub current_event_has_response_id: bool,
}

impl ReplayContext {
    pub fn from_failure(failure: &FailureClassification) -> Self {
        Self {
            current_event_has_response_id: failure.response_id.is_some(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamEnd {
    Close { code: u16 },
    Eof,
    TransportError { process_wide: bool },
    MissingResponseCreatedTimeout,
    UpstreamIdleTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnEndDisposition {
    /// A clean 1000 close before any response event is deterministic input
    /// rejection and must not be transparently replayed.
    RejectedInput,
    /// Emit a synthetic stream_incomplete terminal event for this turn.
    StreamIncomplete,
    /// Settle as stream_incomplete, but emit no synthetic frame because a
    /// numeric sequence from the old generation is already downstream-visible.
    StreamIncompleteNoSynthetic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnEndAction {
    pub turn_id: TurnId,
    pub disposition: TurnEndDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownstreamEndAction {
    KeepOpen,
    Close1011,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamEndPlan {
    pub turns: Vec<TurnEndAction>,
    pub downstream: DownstreamEndAction,
    pub penalize_account: bool,
    pub process_wide: bool,
}

pub struct ProtocolState {
    limits: ProtocolLimits,
    next_turn_id: u64,
    pending: VecDeque<PendingTurn>,
    response_index: HashMap<String, TurnId>,
}

impl ProtocolState {
    pub fn new(limits: ProtocolLimits) -> Result<Self, ProtocolError> {
        Ok(Self {
            limits: limits.validate()?,
            next_turn_id: 1,
            pending: VecDeque::new(),
            response_index: HashMap::new(),
        })
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn pending(&self) -> impl Iterator<Item = &PendingTurn> {
        self.pending.iter()
    }

    pub fn turn(&self, id: TurnId) -> Option<&PendingTurn> {
        self.pending.iter().find(|turn| turn.id == id)
    }

    pub fn admit_response_create(&mut self, frame: &Value) -> Result<TurnId, ProtocolError> {
        if self.pending.len() >= self.limits.max_pending_turns {
            return Err(ProtocolError::PendingLimitReached);
        }
        let analysis = analyze_response_create(frame, self.limits)?;
        let id = TurnId(self.next_turn_id);
        self.next_turn_id = self
            .next_turn_id
            .checked_add(1)
            .ok_or(ProtocolError::TurnIdExhausted)?;
        let previous_response_id = analysis.previous_response_id.clone();
        self.pending.push_back(PendingTurn {
            id,
            original_previous_response_id: previous_response_id.clone(),
            previous_response_id,
            response_id: None,
            response_created: false,
            response_event_count: 0,
            downstream_visible: false,
            last_visible_sequence: None,
            replay_count: 0,
            auth_refresh_replay_count: 0,
            auth_failover_replay_count: 0,
            analysis,
        });
        Ok(id)
    }

    /// Associates and records an upstream event without assuming it was sent
    /// downstream. Pass an anchor hint only when it was extracted by a trusted
    /// parser; multiple pending turns sharing that anchor are returned together.
    pub fn observe_upstream_event(
        &mut self,
        event: &Value,
        previous_response_anchor_hint: Option<&str>,
    ) -> Result<EventAssociation, ProtocolError> {
        if let Some(anchor) = previous_response_anchor_hint {
            validate_identifier(
                anchor,
                self.limits.max_identifier_bytes,
                "previous_response_anchor_hint",
            )?;
        }
        let event_type =
            event_type(event).map(|value| bounded_owned(value, MAX_CLASSIFIED_CODE_BYTES));
        let response_id = response_id(event).map(str::to_owned);
        if let Some(id) = response_id.as_deref() {
            validate_identifier(id, self.limits.max_identifier_bytes, "response_id")?;
        }
        let failure = classify_failure(event);
        let terminal = terminal_kind(event_type.as_deref());

        let mut turn_ids = if event_type.as_deref() == Some("response.created") {
            vec![self.associate_response_created(response_id.as_deref())?]
        } else if let Some(id) = response_id.as_deref() {
            if let Some(turn_id) = self.response_index.get(id).copied() {
                vec![turn_id]
            } else if terminal.is_some() {
                self.associate_precreated_terminal_response_id(id)?
                    .into_iter()
                    .collect()
            } else {
                Vec::new()
            }
        } else if failure.kind == FailureKind::PreviousResponseNotFound {
            self.match_previous_response_miss(previous_response_anchor_hint)
        } else if self.pending.len() == 1 {
            self.pending
                .front()
                .map(|turn| turn.id)
                .into_iter()
                .collect()
        } else {
            Vec::new()
        };
        turn_ids.sort_by_key(|id| id.0);
        turn_ids.dedup();

        if event_type
            .as_deref()
            .is_some_and(|kind| kind.starts_with("response."))
        {
            for id in &turn_ids {
                if let Some(turn) = self.pending.iter_mut().find(|turn| turn.id == *id) {
                    turn.response_event_count = turn.response_event_count.saturating_add(1);
                }
            }
        }

        Ok(EventAssociation {
            turn_ids,
            event_type,
            response_id,
            terminal,
            failure,
        })
    }

    /// Records visibility only after the downstream send succeeds.
    pub fn mark_downstream_delivered(
        &mut self,
        id: TurnId,
        event: &Value,
    ) -> Result<(), ProtocolError> {
        let turn = self
            .pending
            .iter_mut()
            .find(|turn| turn.id == id)
            .ok_or(ProtocolError::UnknownTurn(id))?;
        turn.downstream_visible = true;
        if let Some(sequence) = finite_sequence_number(event) {
            turn.last_visible_sequence = Some(sequence);
        }
        Ok(())
    }

    /// Evaluates replay against state preceding the current failure event.
    /// Callers should classify and decide before recording that terminal event.
    pub fn replay_decision(
        &self,
        id: TurnId,
        failure: FailureKind,
        context: ReplayContext,
    ) -> ReplayDecision {
        match self.replay_plan(id, failure, context) {
            Ok(plan) => ReplayDecision::Eligible(plan.mode),
            Err(reason) => ReplayDecision::Refused(reason),
        }
    }

    /// Returns both the replay body mode and the required routing action.
    /// Authentication deliberately has a distinct two-stage budget:
    /// same-account forced refresh, then alternate-account failover.
    pub fn replay_plan(
        &self,
        id: TurnId,
        failure: FailureKind,
        context: ReplayContext,
    ) -> Result<ReplayPlan, ReplayRefusal> {
        let Some(turn) = self.turn(id) else {
            return Err(ReplayRefusal::UnknownTurn);
        };
        if self.pending.len() != 1 {
            return Err(ReplayRefusal::MultiplePendingTurns);
        }
        if turn.last_visible_sequence.is_some() {
            return Err(ReplayRefusal::FiniteSequenceVisible);
        }
        if turn.downstream_visible {
            return Err(ReplayRefusal::DownstreamVisible);
        }
        if turn.response_created {
            return Err(ReplayRefusal::ResponseCreated);
        }
        if turn.response_event_count > 0 {
            return Err(ReplayRefusal::PriorResponseEvent);
        }
        if turn.analysis.has_file_references {
            return Err(ReplayRefusal::FileBacked);
        }
        if context.current_event_has_response_id {
            return Err(ReplayRefusal::ResponseIdAssigned);
        }

        let supported = matches!(
            failure,
            FailureKind::Quota
                | FailureKind::Authentication { .. }
                | FailureKind::PreviousResponseNotFound
                | FailureKind::Transient
        );
        if !supported {
            return Err(ReplayRefusal::UnsupportedFailure);
        }

        let target = if let FailureKind::Authentication {
            requires_reauthentication,
        } = failure
        {
            if turn.auth_failover_replay_count > 0 {
                return Err(ReplayRefusal::AuthReplaySequenceExhausted);
            }
            if !requires_reauthentication
                && turn.auth_refresh_replay_count < self.limits.max_auth_refresh_replays_per_turn
            {
                ReplayTarget::SameAccountAfterRefresh
            } else if turn.auth_failover_replay_count
                < self.limits.max_auth_failover_replays_per_turn
            {
                ReplayTarget::AlternateAccount
            } else {
                return Err(ReplayRefusal::AuthReplaySequenceExhausted);
            }
        } else {
            if turn.replay_count >= self.limits.max_replays_per_turn {
                return Err(ReplayRefusal::AlreadyReplayed);
            }
            ReplayTarget::Unspecified
        };

        let mode = if turn.previous_response_id.is_none() {
            if failure == FailureKind::PreviousResponseNotFound {
                return Err(ReplayRefusal::HardContinuity);
            }
            ReplayMode::OriginalRequest
        } else {
            match turn.analysis.full_resend {
                FullResendSafety::Eligible => ReplayMode::FreshRequestWithoutPreviousResponse,
                FullResendSafety::Refused(reason) => {
                    return Err(ReplayRefusal::UnsafeFullResend(reason));
                }
            }
        };

        Ok(ReplayPlan { mode, target })
    }

    pub fn prepare_replay_plan(
        &mut self,
        id: TurnId,
        failure: FailureKind,
        context: ReplayContext,
    ) -> Result<ReplayPlan, ProtocolError> {
        let plan = self
            .replay_plan(id, failure, context)
            .map_err(ProtocolError::ReplayNotEligible)?;
        let response_id = self.turn(id).and_then(|turn| turn.response_id.clone());
        if let Some(response_id) = response_id.as_deref() {
            self.response_index.remove(response_id);
        }
        let turn = self
            .pending
            .iter_mut()
            .find(|turn| turn.id == id)
            .ok_or(ProtocolError::UnknownTurn(id))?;
        match plan.target {
            ReplayTarget::SameAccountAfterRefresh => {
                turn.auth_refresh_replay_count = turn.auth_refresh_replay_count.saturating_add(1);
            }
            ReplayTarget::AlternateAccount => {
                turn.auth_failover_replay_count = turn.auth_failover_replay_count.saturating_add(1);
            }
            ReplayTarget::Unspecified => {}
        }
        turn.replay_count = turn.replay_count.saturating_add(1);
        turn.response_id = None;
        turn.response_created = false;
        turn.response_event_count = 0;
        if plan.mode == ReplayMode::FreshRequestWithoutPreviousResponse {
            turn.previous_response_id = None;
        }
        Ok(plan)
    }

    pub fn prepare_replay(
        &mut self,
        id: TurnId,
        failure: FailureKind,
        context: ReplayContext,
    ) -> Result<ReplayMode, ProtocolError> {
        self.prepare_replay_plan(id, failure, context)
            .map(|plan| plan.mode)
    }

    pub fn skip_auth_refresh_stage(&mut self, id: TurnId) -> Result<(), ProtocolError> {
        let turn = self
            .pending
            .iter_mut()
            .find(|turn| turn.id == id)
            .ok_or(ProtocolError::UnknownTurn(id))?;
        turn.auth_refresh_replay_count = turn.auth_refresh_replay_count.saturating_add(1);
        Ok(())
    }

    pub fn reset_auth_sequence_after_account_switch(
        &mut self,
        id: TurnId,
    ) -> Result<(), ProtocolError> {
        let turn = self
            .pending
            .iter_mut()
            .find(|turn| turn.id == id)
            .ok_or(ProtocolError::UnknownTurn(id))?;
        turn.auth_refresh_replay_count = 0;
        turn.auth_failover_replay_count = 0;
        Ok(())
    }

    pub fn settle(
        &mut self,
        id: TurnId,
        settlement: Settlement,
    ) -> Result<SettledTurn, ProtocolError> {
        let index = self
            .pending
            .iter()
            .position(|turn| turn.id == id)
            .ok_or(ProtocolError::UnknownTurn(id))?;
        let turn = self
            .pending
            .remove(index)
            .expect("position came from deque");
        if let Some(response_id) = turn.response_id.as_deref() {
            self.response_index.remove(response_id);
        }
        Ok(SettledTurn {
            id,
            response_id: turn.response_id,
            settlement,
            finite_sequence_visible: turn.last_visible_sequence.is_some(),
        })
    }

    pub fn classify_upstream_end(&self, end: UpstreamEnd) -> UpstreamEndPlan {
        let process_wide = matches!(end, UpstreamEnd::TransportError { process_wide: true });
        let watchdog = matches!(
            end,
            UpstreamEnd::MissingResponseCreatedTimeout | UpstreamEnd::UpstreamIdleTimeout
        );
        let clean = matches!(end, UpstreamEnd::Close { code: 1000 });
        let turns: Vec<_> = self
            .pending
            .iter()
            .map(|turn| {
                let disposition = if clean && turn.is_precreated() && !turn.downstream_visible {
                    TurnEndDisposition::RejectedInput
                } else if turn.last_visible_sequence.is_some() {
                    TurnEndDisposition::StreamIncompleteNoSynthetic
                } else {
                    TurnEndDisposition::StreamIncomplete
                };
                TurnEndAction {
                    turn_id: turn.id,
                    disposition,
                }
            })
            .collect();
        let downstream = if turns
            .iter()
            .any(|turn| turn.disposition == TurnEndDisposition::StreamIncompleteNoSynthetic)
        {
            DownstreamEndAction::Close1011
        } else {
            DownstreamEndAction::KeepOpen
        };
        let has_incomplete = turns.iter().any(|turn| {
            matches!(
                turn.disposition,
                TurnEndDisposition::StreamIncomplete
                    | TurnEndDisposition::StreamIncompleteNoSynthetic
            )
        });
        UpstreamEndPlan {
            turns,
            downstream,
            penalize_account: has_incomplete && !process_wide && !watchdog,
            process_wide,
        }
    }

    fn associate_response_created(
        &mut self,
        response_id: Option<&str>,
    ) -> Result<TurnId, ProtocolError> {
        if let Some(response_id) = response_id
            && let Some(id) = self.response_index.get(response_id)
        {
            return Ok(*id);
        }
        let turn = self
            .pending
            .iter_mut()
            .find(|turn| turn.response_id.is_none() && !turn.response_created)
            .ok_or(ProtocolError::ResponseCreatedHasNoPendingTurn)?;
        turn.response_created = true;
        if let Some(response_id) = response_id {
            if self.response_index.contains_key(response_id) {
                return Err(ProtocolError::DuplicateResponseId);
            }
            turn.response_id = Some(response_id.to_owned());
            self.response_index.insert(response_id.to_owned(), turn.id);
        }
        Ok(turn.id)
    }

    /// Associates a terminal that assigned an ID without first emitting
    /// `response.created`. Guessing is safe only when exactly one pending turn
    /// is still wholly pre-created; otherwise the event remains unassociated.
    ///
    /// A relay that may transparently replay the terminal should call this
    /// before `replay_plan`, then call `observe_upstream_event` only if it will
    /// deliver or settle the event. This preserves pre-event replay semantics.
    pub fn associate_precreated_terminal_response_id(
        &mut self,
        response_id: &str,
    ) -> Result<Option<TurnId>, ProtocolError> {
        validate_identifier(response_id, self.limits.max_identifier_bytes, "response_id")?;
        if let Some(turn_id) = self.response_index.get(response_id).copied() {
            return Ok(Some(turn_id));
        }
        let mut candidates = self
            .pending
            .iter()
            .filter(|turn| turn.is_precreated())
            .map(|turn| turn.id);
        let Some(candidate) = candidates.next() else {
            return Ok(None);
        };
        if candidates.next().is_some() {
            return Ok(None);
        }

        let turn = self
            .pending
            .iter_mut()
            .find(|turn| turn.id == candidate)
            .expect("candidate came from pending turns");
        turn.response_id = Some(response_id.to_owned());
        self.response_index
            .insert(response_id.to_owned(), candidate);
        Ok(Some(candidate))
    }

    fn match_previous_response_miss(&self, anchor_hint: Option<&str>) -> Vec<TurnId> {
        if let Some(anchor) = anchor_hint {
            return self
                .pending
                .iter()
                .filter(|turn| turn.previous_response_id.as_deref() == Some(anchor))
                .map(|turn| turn.id)
                .collect();
        }
        if self.pending.len() == 1 {
            return self
                .pending
                .front()
                .map(|turn| turn.id)
                .into_iter()
                .collect();
        }
        Vec::new()
    }
}

pub fn analyze_response_create(
    frame: &Value,
    limits: ProtocolLimits,
) -> Result<CreateAnalysis, ProtocolError> {
    limits.validate()?;
    let object = frame.as_object().ok_or(ProtocolError::NotResponseCreate)?;
    if object.get("type").and_then(Value::as_str) != Some("response.create") {
        return Err(ProtocolError::NotResponseCreate);
    }
    let previous_response_id = object
        .get("previous_response_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    if let Some(previous_response_id) = previous_response_id.as_deref() {
        validate_identifier(
            previous_response_id,
            limits.max_identifier_bytes,
            "previous_response_id",
        )?;
    }
    let input = object.get("input");
    let input_item_count = input.and_then(Value::as_array).map_or(0, Vec::len);
    let has_file_references = input.is_some_and(has_file_reference);
    let full_resend = analyze_full_resend(
        previous_response_id.as_deref(),
        input,
        has_file_references,
        limits,
    );
    Ok(CreateAnalysis {
        previous_response_id,
        input_item_count,
        has_file_references,
        full_resend,
    })
}

/// Returns a verified fresh replay payload with the stale anchor removed.
pub fn fresh_replay_without_previous_response(
    frame: &Value,
    limits: ProtocolLimits,
) -> Result<Value, ProtocolError> {
    let analysis = analyze_response_create(frame, limits)?;
    match analysis.full_resend {
        FullResendSafety::Eligible => {}
        FullResendSafety::Refused(reason) => {
            return Err(ProtocolError::ReplayNotEligible(
                ReplayRefusal::UnsafeFullResend(reason),
            ));
        }
    }
    let mut fresh = frame.clone();
    fresh
        .as_object_mut()
        .expect("analysis accepted an object")
        .remove("previous_response_id");
    Ok(fresh)
}

pub fn classify_failure(event: &Value) -> FailureClassification {
    let response_id =
        response_id(event).map(|value| bounded_owned(value, DEFAULT_MAX_IDENTIFIER_BYTES));
    let Some(error) = event
        .get("error")
        .or_else(|| {
            event
                .get("response")
                .and_then(|response| response.get("error"))
        })
        .and_then(Value::as_object)
        .or_else(|| {
            (event.get("type").and_then(Value::as_str) == Some("error"))
                .then(|| event.as_object())
                .flatten()
        })
    else {
        return FailureClassification::none();
    };
    let nonempty_string = |key| {
        error
            .get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
    };
    let raw_code = nonempty_string("code").or_else(|| nonempty_string("type"));
    let code =
        raw_code.map(|value| bounded_owned(value, MAX_CLASSIFIED_CODE_BYTES).to_ascii_lowercase());
    let error_type = nonempty_string("error_type")
        .or_else(|| nonempty_string("type"))
        .map(|value| bounded_owned(value, MAX_CLASSIFIED_CODE_BYTES).to_ascii_lowercase())
        .unwrap_or_default();
    let param = error
        .get("param")
        .and_then(Value::as_str)
        .map(|value| bounded_owned(value, MAX_CLASSIFIED_CODE_BYTES).to_ascii_lowercase())
        .unwrap_or_default();
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(|value| bounded_owned(value, MAX_CLASSIFIED_MESSAGE_BYTES));
    let normalized_message = message.as_deref().unwrap_or_default().to_ascii_lowercase();
    let normalized_code = code.as_deref().unwrap_or_default();
    let stale_message = normalized_message
        .strip_suffix('.')
        .unwrap_or(&normalized_message);

    let previous_response_not_found = normalized_code == "previous_response_not_found"
        || (param == "previous_response_id"
            && normalized_message.contains("previous response")
            && normalized_message.contains("not found"))
        || (normalized_code == "invalid_request_error"
            && param.is_empty()
            && matches!(
                stale_message,
                "invalid `previous_response_id`" | "invalid previous_response_id"
            ));
    let quota = [
        "rate_limit_exceeded",
        "usage_limit_reached",
        "insufficient_quota",
        "usage_not_included",
        "quota_exceeded",
        "overloaded_error",
        "server_is_overloaded",
    ]
    .contains(&normalized_code)
        || normalized_message.contains("usage limit")
        || normalized_message.contains("insufficient quota")
        || normalized_message.contains("server is overloaded");
    let authentication = [
        "invalid_api_key",
        "authentication_error",
        "account_auth_invalidated",
        "session_expired",
        "token_expired",
    ]
    .contains(&normalized_code)
        || error_type == "authentication_error";
    let requires_reauthentication = [
        "reauthentication required",
        "re-authentication required",
        "sign in again",
        "log in again",
        "login again",
    ]
    .iter()
    .any(|marker| normalized_message.contains(marker));
    let transient = [
        "server_error",
        "bad_gateway",
        "service_unavailable",
        "gateway_timeout",
        "upstream_unavailable",
        "stream_incomplete",
    ]
    .contains(&normalized_code);

    let kind = if previous_response_not_found {
        FailureKind::PreviousResponseNotFound
    } else if authentication {
        FailureKind::Authentication {
            requires_reauthentication,
        }
    } else if quota {
        FailureKind::Quota
    } else if transient {
        FailureKind::Transient
    } else {
        FailureKind::Other
    };
    FailureClassification {
        kind,
        code,
        message,
        response_id,
    }
}

pub fn response_id(event: &Value) -> Option<&str> {
    event
        .get("response")
        .and_then(|response| response.get("id"))
        .or_else(|| event.get("response_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
}

pub fn finite_sequence_number(event: &Value) -> Option<SequenceNumber> {
    let value = event.get("sequence_number")?;
    if let Some(value) = value.as_i64() {
        return Some(SequenceNumber::Signed(value));
    }
    value.as_u64().map(SequenceNumber::Unsigned)
}

pub fn terminal_kind(event_type: Option<&str>) -> Option<TerminalKind> {
    match event_type {
        Some("response.completed") => Some(TerminalKind::Completed),
        Some("response.failed") => Some(TerminalKind::Failed),
        Some("response.cancelled") => Some(TerminalKind::Cancelled),
        Some("response.incomplete") => Some(TerminalKind::Incomplete),
        Some("error") => Some(TerminalKind::Failed),
        _ => None,
    }
}

fn event_type(event: &Value) -> Option<&str> {
    event.get("type").and_then(Value::as_str)
}

fn analyze_full_resend(
    previous_response_id: Option<&str>,
    input: Option<&Value>,
    has_file_references: bool,
    limits: ProtocolLimits,
) -> FullResendSafety {
    if previous_response_id.is_none() {
        return FullResendSafety::Refused(FullResendRefusal::NoPreviousResponse);
    }
    let Some(items) = input.and_then(Value::as_array) else {
        return FullResendSafety::Refused(FullResendRefusal::InputIsNotAnArray);
    };
    if items.len() <= 1 {
        return FullResendSafety::Refused(FullResendRefusal::InsufficientHistory);
    }
    if has_file_references {
        return FullResendSafety::Refused(FullResendRefusal::FileBacked);
    }
    match tool_outputs_are_self_contained(items, limits.max_tracked_tool_calls) {
        Ok(true) => FullResendSafety::Eligible,
        Ok(false) => FullResendSafety::Refused(FullResendRefusal::UnmatchedToolOutput),
        Err(()) => FullResendSafety::Refused(FullResendRefusal::TooManyToolCalls),
    }
}

fn tool_outputs_are_self_contained(items: &[Value], max_calls: usize) -> Result<bool, ()> {
    let mut calls: HashMap<&str, HashSet<&str>> = HashMap::from([
        ("function_call", HashSet::new()),
        ("custom_tool_call", HashSet::new()),
        ("apply_patch_call", HashSet::new()),
        ("tool_search_call", HashSet::new()),
    ]);
    let mut consumed: HashSet<(&str, &str)> = HashSet::new();
    let mut tracked = 0usize;
    for item in items {
        let Some(object) = item.as_object() else {
            continue;
        };
        let item_type = object
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let call_id = object
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        if let Some(seen) = calls.get_mut(item_type) {
            if let Some(call_id) = call_id {
                tracked = tracked.saturating_add(1);
                if tracked > max_calls {
                    return Err(());
                }
                seen.insert(call_id);
            }
            continue;
        }
        let call_type = match item_type {
            "function_call_output" => Some("function_call"),
            "custom_tool_call_output" => Some("custom_tool_call"),
            "apply_patch_call_output" => Some("apply_patch_call"),
            "tool_search_output" => Some("tool_search_call"),
            _ => None,
        };
        let Some(call_type) = call_type else {
            // New tool call/output variants must be reviewed explicitly before a
            // request containing them can be replayed without its response anchor.
            // Silently ignoring an unfamiliar pair could move account-owned
            // history to a different account without all of its dependencies.
            if item_type.ends_with("_call") || item_type.ends_with("_output") {
                return Ok(false);
            }
            continue;
        };
        let Some(call_id) = call_id else {
            return Ok(false);
        };
        if !calls[call_type].contains(call_id) || !consumed.insert((call_type, call_id)) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn has_file_reference(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(has_file_reference),
        Value::Object(object) => {
            let item_type = object.get("type").and_then(Value::as_str);
            let direct_file = matches!(item_type, Some("input_file") | Some("input_image"))
                && object
                    .get("file_id")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty());
            let sediment = object
                .get("image_url")
                .and_then(Value::as_str)
                .is_some_and(|value| value.starts_with("sediment://"));
            direct_file || sediment || object.values().any(has_file_reference)
        }
        _ => false,
    }
}

fn validate_identifier(
    value: &str,
    max_bytes: usize,
    field: &'static str,
) -> Result<(), ProtocolError> {
    if value.len() > max_bytes {
        return Err(ProtocolError::IdentifierTooLong(field));
    }
    Ok(())
}

fn bounded_owned(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn state() -> ProtocolState {
        ProtocolState::new(ProtocolLimits::default()).unwrap()
    }

    fn create(input: Value) -> Value {
        json!({"type":"response.create","model":"gpt-test","input":input})
    }

    #[test]
    fn pending_turns_are_bounded_and_response_created_associates_fifo() {
        let mut state = ProtocolState::new(ProtocolLimits {
            max_pending_turns: 2,
            ..ProtocolLimits::default()
        })
        .unwrap();
        let first = state.admit_response_create(&create(json!("one"))).unwrap();
        let second = state.admit_response_create(&create(json!("two"))).unwrap();
        assert_eq!(
            state.admit_response_create(&create(json!("three"))),
            Err(ProtocolError::PendingLimitReached)
        );

        let association = state
            .observe_upstream_event(
                &json!({"type":"response.created","response":{"id":"resp_a"}}),
                None,
            )
            .unwrap();
        assert_eq!(association.turn_ids, [first]);
        let association = state
            .observe_upstream_event(
                &json!({"type":"response.created","response":{"id":"resp_b"}}),
                None,
            )
            .unwrap();
        assert_eq!(association.turn_ids, [second]);
    }

    #[test]
    fn terminals_match_response_ids_out_of_order_and_settle_independently() {
        let mut state = state();
        let first = state.admit_response_create(&create(json!("one"))).unwrap();
        let second = state.admit_response_create(&create(json!("two"))).unwrap();
        state
            .observe_upstream_event(
                &json!({"type":"response.created","response":{"id":"resp_a"}}),
                None,
            )
            .unwrap();
        state
            .observe_upstream_event(
                &json!({"type":"response.created","response":{"id":"resp_b"}}),
                None,
            )
            .unwrap();
        let completed_b = json!({"type":"response.completed","response":{"id":"resp_b"}});
        let association = state.observe_upstream_event(&completed_b, None).unwrap();
        assert_eq!(association.turn_ids, [second]);
        assert_eq!(association.terminal, Some(TerminalKind::Completed));
        state
            .mark_downstream_delivered(second, &completed_b)
            .unwrap();
        state.settle(second, Settlement::Completed).unwrap();
        assert!(state.turn(first).is_some());
        assert!(state.turn(second).is_none());
    }

    #[test]
    fn id_bearing_terminal_before_created_associates_the_sole_precreated_turn() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        let failed = json!({
            "type":"response.failed",
            "response":{"id":"resp_failed","error":{"code":"server_error"}}
        });

        let association = state.observe_upstream_event(&failed, None).unwrap();

        assert_eq!(association.turn_ids, [turn]);
        assert_eq!(association.response_id.as_deref(), Some("resp_failed"));
        assert_eq!(association.terminal, Some(TerminalKind::Failed));
        let pending = state.turn(turn).unwrap();
        assert_eq!(pending.response_id(), Some("resp_failed"));
        assert!(!pending.response_created());
        assert_eq!(pending.response_event_count(), 1);

        let settled = state.settle(turn, Settlement::Failed).unwrap();
        assert_eq!(settled.response_id.as_deref(), Some("resp_failed"));
        assert_eq!(state.pending_len(), 0);
    }

    #[test]
    fn id_bearing_terminal_claims_the_only_remaining_precreated_turn() {
        let mut state = state();
        let first = state.admit_response_create(&create(json!("one"))).unwrap();
        let second = state.admit_response_create(&create(json!("two"))).unwrap();
        state
            .observe_upstream_event(
                &json!({"type":"response.created","response":{"id":"resp_a"}}),
                None,
            )
            .unwrap();

        let association = state
            .observe_upstream_event(
                &json!({"type":"response.incomplete","response":{"id":"resp_b"}}),
                None,
            )
            .unwrap();

        assert_eq!(association.turn_ids, [second]);
        assert_eq!(state.turn(first).unwrap().response_id(), Some("resp_a"));
        assert_eq!(state.turn(second).unwrap().response_id(), Some("resp_b"));
        assert!(!state.turn(second).unwrap().response_created());
    }

    #[test]
    fn id_bearing_terminal_does_not_guess_between_precreated_turns() {
        let mut state = state();
        let first = state.admit_response_create(&create(json!("one"))).unwrap();
        let second = state.admit_response_create(&create(json!("two"))).unwrap();

        let association = state
            .observe_upstream_event(
                &json!({"type":"response.failed","response":{"id":"resp_unknown"}}),
                None,
            )
            .unwrap();

        assert!(association.turn_ids.is_empty());
        assert_eq!(state.turn(first).unwrap().response_id(), None);
        assert_eq!(state.turn(second).unwrap().response_id(), None);
        assert_eq!(state.turn(first).unwrap().response_event_count(), 0);
        assert_eq!(state.turn(second).unwrap().response_event_count(), 0);
    }

    #[test]
    fn finite_sequence_visibility_is_committed_only_after_delivery() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        let event = json!({"type":"codex.keepalive","sequence_number":0});
        assert_eq!(state.turn(turn).unwrap().last_visible_sequence(), None);
        state.mark_downstream_delivered(turn, &event).unwrap();
        assert_eq!(
            state.turn(turn).unwrap().last_visible_sequence(),
            Some(SequenceNumber::Signed(0))
        );

        let mut non_integer_state = ProtocolState::new(ProtocolLimits::default()).unwrap();
        let turn = non_integer_state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        non_integer_state
            .mark_downstream_delivered(turn, &json!({"sequence_number":1.5}))
            .unwrap();
        assert_eq!(
            non_integer_state
                .turn(turn)
                .unwrap()
                .last_visible_sequence(),
            None
        );
    }

    #[test]
    fn response_metadata_is_visible_and_sequenced_without_marking_created() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        let metadata = json!({
            "type":"response.metadata",
            "sequence_number":7,
            "response":{"metadata":{"trace":"opaque"}}
        });

        let association = state.observe_upstream_event(&metadata, None).unwrap();
        assert_eq!(association.turn_ids, [turn]);
        assert_eq!(association.event_type.as_deref(), Some("response.metadata"));
        let pending = state.turn(turn).unwrap();
        assert!(!pending.response_created());
        assert_eq!(pending.response_event_count(), 1);
        assert!(!pending.downstream_visible());

        state.mark_downstream_delivered(turn, &metadata).unwrap();
        let pending = state.turn(turn).unwrap();
        assert!(pending.downstream_visible());
        assert_eq!(
            pending.last_visible_sequence(),
            Some(SequenceNumber::Signed(7))
        );
        assert!(matches!(
            state.replay_decision(turn, FailureKind::Transient, ReplayContext::default()),
            ReplayDecision::Refused(ReplayRefusal::FiniteSequenceVisible)
        ));
    }

    #[test]
    fn replay_requires_a_single_previsible_unsettled_turn() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        assert_eq!(
            state.replay_decision(turn, FailureKind::Quota, ReplayContext::default()),
            ReplayDecision::Eligible(ReplayMode::OriginalRequest)
        );
        let other = state
            .admit_response_create(&create(json!("other")))
            .unwrap();
        assert_eq!(
            state.replay_decision(turn, FailureKind::Quota, ReplayContext::default()),
            ReplayDecision::Refused(ReplayRefusal::MultiplePendingTurns)
        );
        state.settle(other, Settlement::Cancelled).unwrap();
        state
            .mark_downstream_delivered(turn, &json!({"type":"codex.rate_limits"}))
            .unwrap();
        assert_eq!(
            state.replay_decision(turn, FailureKind::Quota, ReplayContext::default()),
            ReplayDecision::Refused(ReplayRefusal::DownstreamVisible)
        );
    }

    #[test]
    fn replay_after_finite_sequence_is_never_allowed() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        state
            .mark_downstream_delivered(turn, &json!({"sequence_number":7}))
            .unwrap();
        assert_eq!(
            state.replay_decision(turn, FailureKind::Transient, ReplayContext::default()),
            ReplayDecision::Refused(ReplayRefusal::FiniteSequenceVisible)
        );
    }

    #[test]
    fn replay_budget_is_consumed_explicitly() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        assert_eq!(
            state
                .prepare_replay(turn, FailureKind::Quota, ReplayContext::default())
                .unwrap(),
            ReplayMode::OriginalRequest
        );
        assert_eq!(
            state.replay_decision(turn, FailureKind::Quota, ReplayContext::default()),
            ReplayDecision::Refused(ReplayRefusal::AlreadyReplayed)
        );
    }

    #[test]
    fn generic_auth_replay_sequence_is_refresh_then_failover_then_stop() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        let auth = FailureKind::Authentication {
            requires_reauthentication: false,
        };

        let refresh = state
            .prepare_replay_plan(turn, auth, ReplayContext::default())
            .unwrap();
        assert_eq!(
            refresh,
            ReplayPlan {
                mode: ReplayMode::OriginalRequest,
                target: ReplayTarget::SameAccountAfterRefresh,
            }
        );
        assert_eq!(state.turn(turn).unwrap().auth_refresh_replay_count(), 1);
        assert_eq!(state.turn(turn).unwrap().auth_failover_replay_count(), 0);
        assert_eq!(state.turn(turn).unwrap().replay_count(), 1);

        let failover = state
            .prepare_replay_plan(turn, auth, ReplayContext::default())
            .unwrap();
        assert_eq!(
            failover,
            ReplayPlan {
                mode: ReplayMode::OriginalRequest,
                target: ReplayTarget::AlternateAccount,
            }
        );
        assert_eq!(state.turn(turn).unwrap().auth_refresh_replay_count(), 1);
        assert_eq!(state.turn(turn).unwrap().auth_failover_replay_count(), 1);
        assert_eq!(state.turn(turn).unwrap().replay_count(), 2);
        assert_eq!(
            state.replay_plan(turn, auth, ReplayContext::default()),
            Err(ReplayRefusal::AuthReplaySequenceExhausted)
        );
    }

    #[test]
    fn reauthentication_required_skips_refresh_and_permanently_consumes_auth_sequence() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        let session_ended = FailureKind::Authentication {
            requires_reauthentication: true,
        };

        let failover = state
            .prepare_replay_plan(turn, session_ended, ReplayContext::default())
            .unwrap();
        assert_eq!(failover.target, ReplayTarget::AlternateAccount);
        assert_eq!(state.turn(turn).unwrap().auth_refresh_replay_count(), 0);
        assert_eq!(state.turn(turn).unwrap().auth_failover_replay_count(), 1);

        let generic_auth = FailureKind::Authentication {
            requires_reauthentication: false,
        };
        assert_eq!(
            state.replay_plan(turn, generic_auth, ReplayContext::default()),
            Err(ReplayRefusal::AuthReplaySequenceExhausted)
        );
    }

    #[test]
    fn auth_replay_consumes_the_non_auth_replay_budget() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        let auth = FailureKind::Authentication {
            requires_reauthentication: false,
        };
        state
            .prepare_replay_plan(turn, auth, ReplayContext::default())
            .unwrap();

        assert_eq!(
            state.replay_decision(turn, FailureKind::Quota, ReplayContext::default()),
            ReplayDecision::Refused(ReplayRefusal::AlreadyReplayed)
        );
        assert_eq!(
            state
                .replay_plan(turn, auth, ReplayContext::default())
                .unwrap()
                .target,
            ReplayTarget::AlternateAccount
        );
    }

    #[test]
    fn id_bearing_precreated_auth_terminal_is_associated_but_not_replayed() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        assert_eq!(
            state
                .associate_precreated_terminal_response_id("resp_auth")
                .unwrap(),
            Some(turn)
        );
        let auth = FailureKind::Authentication {
            requires_reauthentication: false,
        };

        assert_eq!(
            state.replay_plan(
                turn,
                auth,
                ReplayContext {
                    current_event_has_response_id: true,
                },
            ),
            Err(ReplayRefusal::ResponseIdAssigned)
        );
        assert_eq!(state.turn(turn).unwrap().response_id(), Some("resp_auth"));
    }

    #[test]
    fn replay_limits_cannot_expand_codex_lb_budgets() {
        assert_eq!(
            ProtocolState::new(ProtocolLimits {
                max_replays_per_turn: 2,
                ..ProtocolLimits::default()
            })
            .err(),
            Some(ProtocolError::InvalidLimits("max_replays_per_turn"))
        );
        assert_eq!(
            ProtocolState::new(ProtocolLimits {
                max_auth_refresh_replays_per_turn: 2,
                ..ProtocolLimits::default()
            })
            .err(),
            Some(ProtocolError::InvalidLimits(
                "max_auth_refresh_replays_per_turn"
            ))
        );
        assert_eq!(
            ProtocolState::new(ProtocolLimits {
                max_auth_failover_replays_per_turn: 2,
                ..ProtocolLimits::default()
            })
            .err(),
            Some(ProtocolError::InvalidLimits(
                "max_auth_failover_replays_per_turn"
            ))
        );
    }

    #[test]
    fn clean_precreated_close_is_rejected_input_without_replay_or_penalty() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        let plan = state.classify_upstream_end(UpstreamEnd::Close { code: 1000 });
        assert_eq!(
            plan.turns,
            [TurnEndAction {
                turn_id: turn,
                disposition: TurnEndDisposition::RejectedInput,
            }]
        );
        assert_eq!(plan.downstream, DownstreamEndAction::KeepOpen);
        assert!(!plan.penalize_account);
    }

    #[test]
    fn interrupted_unsequenced_turn_gets_synthetic_stream_incomplete() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        state
            .observe_upstream_event(
                &json!({"type":"response.created","response":{"id":"resp_a"}}),
                None,
            )
            .unwrap();
        let plan = state.classify_upstream_end(UpstreamEnd::Eof);
        assert_eq!(
            plan.turns[0],
            TurnEndAction {
                turn_id: turn,
                disposition: TurnEndDisposition::StreamIncomplete,
            }
        );
        assert_eq!(plan.downstream, DownstreamEndAction::KeepOpen);
        assert!(plan.penalize_account);
    }

    #[test]
    fn interrupted_sequenced_turn_requires_1011_without_synthetic_frame() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        state
            .mark_downstream_delivered(
                turn,
                &json!({"type":"response.created","sequence_number":0}),
            )
            .unwrap();
        let plan = state.classify_upstream_end(UpstreamEnd::Eof);
        assert_eq!(
            plan.turns[0].disposition,
            TurnEndDisposition::StreamIncompleteNoSynthetic
        );
        assert_eq!(plan.downstream, DownstreamEndAction::Close1011);
        assert!(plan.penalize_account);
    }

    #[test]
    fn process_wide_transport_failure_is_account_neutral() {
        let mut state = state();
        state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        let plan = state.classify_upstream_end(UpstreamEnd::TransportError { process_wide: true });
        assert!(plan.process_wide);
        assert!(!plan.penalize_account);
    }

    #[test]
    fn watchdog_timeouts_are_account_neutral_and_never_imply_clean_rejection() {
        for end in [
            UpstreamEnd::MissingResponseCreatedTimeout,
            UpstreamEnd::UpstreamIdleTimeout,
        ] {
            let mut state = state();
            let turn = state
                .admit_response_create(&create(json!("hello")))
                .unwrap();
            let plan = state.classify_upstream_end(end);
            assert_eq!(
                plan.turns,
                [TurnEndAction {
                    turn_id: turn,
                    disposition: TurnEndDisposition::StreamIncomplete,
                }]
            );
            assert!(!plan.penalize_account);
            assert_eq!(plan.downstream, DownstreamEndAction::KeepOpen);
        }
    }

    #[test]
    fn failure_classifier_distinguishes_quota_auth_and_previous_response_loss() {
        assert_eq!(
            classify_failure(&json!({"type":"error","error":{"code":"usage_limit_reached"}})).kind,
            FailureKind::Quota
        );
        assert_eq!(
            classify_failure(&json!({"type":"error","error":{"type":"authentication_error","message":"Reauthentication required"}})).kind,
            FailureKind::Authentication {
                requires_reauthentication: true,
            }
        );
        assert_eq!(
            classify_failure(&json!({"type":"error","error":{"code":"invalid_request_error","param":"previous_response_id","message":"Previous response was not found"}})).kind,
            FailureKind::PreviousResponseNotFound
        );
        assert_eq!(
            classify_failure(&json!({"type":"error","code":"usage_limit_reached"})).kind,
            FailureKind::Quota
        );
        assert_eq!(
            classify_failure(&json!({
                "type":"error",
                "status":401,
                "error_type":"authentication_error",
                "message":"token invalidated"
            }))
            .kind,
            FailureKind::Authentication {
                requires_reauthentication: false,
            }
        );

        for message in [
            "Invalid `previous_response_id`.",
            "Invalid `previous_response_id`",
            "Invalid previous_response_id.",
        ] {
            assert_eq!(
                classify_failure(&json!({
                    "type":"error",
                    "status":400,
                    "error":{"type":"invalid_request_error","message":message}
                }))
                .kind,
                FailureKind::PreviousResponseNotFound
            );
        }

        assert_eq!(
            classify_failure(&json!({
                "type":"error",
                "status":400,
                "error":{
                    "type":"invalid_request_error",
                    "code":null,
                    "param":null,
                    "message":"Invalid `previous_response_id`."
                }
            }))
            .kind,
            FailureKind::PreviousResponseNotFound
        );

        for message in [
            "Invalid `other_parameter`.",
            "Invalid `previous_response_id` because the request is malformed.",
        ] {
            assert_eq!(
                classify_failure(&json!({
                    "type":"error",
                    "status":400,
                    "error":{"type":"invalid_request_error","message":message}
                }))
                .kind,
                FailureKind::Other
            );
        }
    }

    #[test]
    fn top_level_error_is_terminal_and_associates_the_pending_turn() {
        let mut state = state();
        let turn = state
            .admit_response_create(&create(json!("hello")))
            .unwrap();
        let association = state
            .observe_upstream_event(
                &json!({"type":"error","code":"server_error","message":"failed"}),
                None,
            )
            .unwrap();
        assert_eq!(association.turn_ids, [turn]);
        assert_eq!(association.terminal, Some(TerminalKind::Failed));
    }

    #[test]
    fn anonymous_previous_response_miss_can_target_all_turns_sharing_an_anchor() {
        let mut state = state();
        let frame = json!({
            "type":"response.create",
            "previous_response_id":"resp_anchor",
            "input":[{"role":"user","content":"old"},{"role":"user","content":"new"}]
        });
        let first = state.admit_response_create(&frame).unwrap();
        let second = state.admit_response_create(&frame).unwrap();
        let event = json!({"type":"error","error":{"code":"previous_response_not_found"}});
        let association = state
            .observe_upstream_event(&event, Some("resp_anchor"))
            .unwrap();
        assert_eq!(association.turn_ids, [first, second]);
    }

    #[test]
    fn matched_tool_output_full_resend_is_eligible_and_anchor_can_be_removed() {
        let frame = json!({
            "type":"response.create",
            "previous_response_id":"resp_old",
            "input":[
                {"type":"function_call","call_id":"call_1","name":"f","arguments":"{}"},
                {"type":"function_call_output","call_id":"call_1","output":"ok"},
                {"role":"user","content":[{"type":"input_text","text":"continue"}]}
            ]
        });
        let analysis = analyze_response_create(&frame, ProtocolLimits::default()).unwrap();
        assert_eq!(analysis.full_resend, FullResendSafety::Eligible);
        let fresh =
            fresh_replay_without_previous_response(&frame, ProtocolLimits::default()).unwrap();
        assert!(fresh.get("previous_response_id").is_none());
        assert_eq!(fresh["input"], frame["input"]);
    }

    #[test]
    fn unmatched_tool_output_is_not_a_self_contained_full_resend() {
        let frame = json!({
            "type":"response.create",
            "previous_response_id":"resp_old",
            "input":[
                {"role":"user","content":"old"},
                {"type":"function_call_output","call_id":"missing","output":"unsafe"}
            ]
        });
        let analysis = analyze_response_create(&frame, ProtocolLimits::default()).unwrap();
        assert_eq!(
            analysis.full_resend,
            FullResendSafety::Refused(FullResendRefusal::UnmatchedToolOutput)
        );
    }

    #[test]
    fn matched_tool_search_output_is_a_self_contained_full_resend() {
        let frame = json!({
            "type":"response.create",
            "previous_response_id":"resp_old",
            "input":[
                {"type":"tool_search_call","call_id":"search_1","arguments":{"query":"spawn agent"}},
                {"type":"tool_search_output","call_id":"search_1","tools":[]},
                {"role":"user","content":"continue"}
            ]
        });
        let analysis = analyze_response_create(&frame, ProtocolLimits::default()).unwrap();
        assert_eq!(analysis.full_resend, FullResendSafety::Eligible);
    }

    #[test]
    fn tool_search_output_without_its_call_is_not_a_self_contained_full_resend() {
        let frame = json!({
            "type":"response.create",
            "previous_response_id":"resp_old",
            "input":[
                {"role":"user","content":"old"},
                {"type":"tool_search_output","call_id":"missing","tools":[]}
            ]
        });
        let analysis = analyze_response_create(&frame, ProtocolLimits::default()).unwrap();
        assert_eq!(
            analysis.full_resend,
            FullResendSafety::Refused(FullResendRefusal::UnmatchedToolOutput)
        );
    }

    #[test]
    fn duplicate_tool_search_output_is_not_a_self_contained_full_resend() {
        let frame = json!({
            "type":"response.create",
            "previous_response_id":"resp_old",
            "input":[
                {"type":"tool_search_call","call_id":"search_1","arguments":{"query":"spawn agent"}},
                {"type":"tool_search_output","call_id":"search_1","tools":[]},
                {"type":"tool_search_output","call_id":"search_1","tools":[]}
            ]
        });
        let analysis = analyze_response_create(&frame, ProtocolLimits::default()).unwrap();
        assert_eq!(
            analysis.full_resend,
            FullResendSafety::Refused(FullResendRefusal::UnmatchedToolOutput)
        );
    }

    #[test]
    fn unknown_call_or_output_like_items_fail_closed_for_full_resend() {
        for item in [
            json!({"type":"future_tool_call","call_id":"future_1"}),
            json!({"type":"future_tool_call_output","call_id":"future_1","output":"unsafe"}),
            json!({"type":"future_tool_output","call_id":"future_1","output":"unsafe"}),
        ] {
            let frame = json!({
                "type":"response.create",
                "previous_response_id":"resp_old",
                "input":[
                    {"role":"user","content":"old"},
                    item
                ]
            });
            let analysis = analyze_response_create(&frame, ProtocolLimits::default()).unwrap();
            assert_eq!(
                analysis.full_resend,
                FullResendSafety::Refused(FullResendRefusal::UnmatchedToolOutput)
            );
        }
    }

    #[test]
    fn file_backed_full_resend_is_never_replayable_on_a_fresh_account() {
        let frame = json!({
            "type":"response.create",
            "previous_response_id":"resp_old",
            "input":[
                {"role":"user","content":"old"},
                {"role":"user","content":[{"type":"input_file","file_id":"file_owned"}]}
            ]
        });
        let analysis = analyze_response_create(&frame, ProtocolLimits::default()).unwrap();
        assert!(analysis.has_file_references);
        assert_eq!(
            analysis.full_resend,
            FullResendSafety::Refused(FullResendRefusal::FileBacked)
        );
    }

    #[test]
    fn safe_full_resend_allows_previous_response_miss_recovery() {
        let frame = json!({
            "type":"response.create",
            "previous_response_id":"resp_old",
            "input":[
                {"role":"user","content":"old"},
                {"role":"assistant","content":"answer"},
                {"role":"user","content":"continue"}
            ]
        });
        let mut state = state();
        let turn = state.admit_response_create(&frame).unwrap();
        let decision = state.replay_decision(
            turn,
            FailureKind::PreviousResponseNotFound,
            ReplayContext::default(),
        );
        assert_eq!(
            decision,
            ReplayDecision::Eligible(ReplayMode::FreshRequestWithoutPreviousResponse)
        );
        state
            .prepare_replay(
                turn,
                FailureKind::PreviousResponseNotFound,
                ReplayContext::default(),
            )
            .unwrap();
        assert_eq!(state.turn(turn).unwrap().previous_response_id(), None);
        assert_eq!(
            state.turn(turn).unwrap().original_previous_response_id(),
            Some("resp_old")
        );
    }
}
