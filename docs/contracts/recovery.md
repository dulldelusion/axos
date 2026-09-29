# Recovery Contract

## Purpose

The recovery contract answers three questions and nothing else:

1. A failure was detected — what is the *smallest authorized* recovery action?
2. Did the action *actually work*?
3. If not, when do we *escalate* instead of trying again?

Recovery in AXOS is a layered stack. The contract draws a hard line
between **execution/verification** (R8) and **policy** (R9):

- **R8** executes exactly one attempt per decision and verifies it against
  authoritative durable evidence. It performs no budget arithmetic and
  chooses no rungs beyond its action-to-rung mapping.
- **R9** chooses the recovery rung, bounds recovery with durable budgets,
  and decides terminal escalation. It never executes or verifies a
  recovery action.

This split exists so that a crash or bug in one layer provably cannot
corrupt the other: R8 cannot spend budget it cannot see; R9 cannot take
any physical action at all.

## Authoritative state

Three tables, all in the AXOS store, all mutated only through the
`TransitionGate`:

- **`incidents`** — one row per failure incident. The incident identity is
  `(job_id, failure_class)`: stable across controller crashes, watchdog
  reevaluations, attempt retries, and process replacement; never derived
  from controller memory. Failure classes: `STALLED`, `DEAD`,
  `LEASE_EXPIRED`, `STALE_AUTHORITY`, `UNCERTAIN`.
- **`recovery_attempts`** — one row per recovery attempt. Every attempt
  binds `(job_id, fencing_token)` and records the full contract fields:
  incident identity, failure classification, recovery rung (+ budget
  context), attempt number, success criterion, failure criterion, observed
  progress delta, resulting state, escalation target.
- **`recovery_policy`** — one row per incident owned by R9: rung position,
  budgets, watermarks, zero-progress count, terminal state.

## Allowed transitions

**R8 attempt states** (`TransitionGate._ATTEMPT_STATES`):
`CREATED`, `RUNNING`, `VERIFYING`, `SUCCEEDED`, `FAILED`, `BLOCKED`,
`UNCERTAIN`. Terminal completion is a single compare-and-swap:
`(RUNNING | VERIFYING | UNCERTAIN) -> SUCCEEDED | FAILED | BLOCKED`.
Decisions are `success | retry | escalate | quarantine | stop`. There is
no single centralized attempt edge table; each driver call supplies the
allowed `from_states`.

**R9 canonical ladder** (`CANONICAL_LADDER` in `exec/policy.py`):

1. "Retry with backoff" — R8 reclaim.
2. "Restart worker" — R8 restart via `dispatch_restart`.
3. "Replace / reassign" — R8 fence and/or restart via existing paths.
4. "Widen scope" — no R9 executor; the rung is traversed durably and
   monotonically before terminal escalation.
5. "Replan / pause" — terminal: persist escalation, enter the human-gated
   `PAUSED_FOR_HUMAN` task state through the gate (sticky under I-17,
   never auto-cleared).

**R9 policy terminals** (sticky once set; no further rung selection,
budget consumption, or execution): `RECOVERY_COMPLETE`,
`BUDGET_EXHAUSTED`, `R5_TERMINAL`, `BLOCKED_CONTRADICTORY`,
`BLOCKED_STATE_UNAVAILABLE`, `SUPERSEDED`.
Rung selection never rewinds: the returned rung is always >= the current
rung, and rung 5 is reached only for terminal escalation.

## Safety invariant

**I-18 / the zero-progress rule:** recovery success is determined by
*verified progress*, not by merely performing a recovery action. A
recovery attempt recorded as `success` with zero progress delta is
rejected by the gate (`complete_recovery_attempt` /
`record_recovery_attempt` raise `TransitionRejected` when
`decision == "success" and progress_delta <= 0`) **and** by a database
`CHECK` constraint
(`CHECK (NOT (decision = 'success' AND progress_delta <= 0))` in
`store/migrations.py`), so even a direct SQL write outside the gate
cannot record a progress-free success.

Per-rung canonical success criteria (`RecoveryController._progress_delta`):

- **reclaim** (rung 2): the lease was actually revoked in durable state —
  fencing token increased, owner cleared, job transitioned to
  `PENDING`/`UNCERTAIN`, ledger evidence present.
- **fence** (rung 3): the stale process that was live before the sweep is
  dead/reaped after it.
- **restart** (rung 3): a replacement worker claimed the job under the
  current token **and** authoritative progress evidence advanced within
  the observation window (`progress_updated_at`, new verified artifacts,
  or a new latest-known-good pointer).

`progress_delta` is `1.0` when the rung's criterion holds on the
before/after evidence diff, otherwise `0.0`. Heartbeats never count as
progress.

**Contract-normative rule:** two consecutive attempts with zero
authoritative progress is failed recovery — a durable escalation signal
to `"r9-policy"` is emitted, and R8 stops creating attempts for the
escalated incident. This is R8's fixed rule, not R9's: escalation *to*
R9 does not depend on R9 being alive.

**Attempt binding:** every attempt binds `(job_id, fencing_token)`. If
the durable token changed between attempt creation and action dispatch,
the attempt aborts to BLOCKED — a stale controller never acts against
new ownership. Exactly one OPEN attempt per incident
(`UNIQUE(incident_id, attempt_number)` plus an in-transaction open-attempt
check; concurrent creators collapse to one authoritative attempt, and
claiming `CREATED -> RUNNING` is a compare-and-swap).

## Failure behavior

The controller is fail-closed throughout:

- Unreadable clock or authoritative state -> `RecoveryError`; nothing is written.
- Contradictory state, changed fencing token, ambiguous action effect, or
  missing authority -> `BLOCKED` / `UNCERTAIN` / `ESCALATE`. The
  controller never guesses.
- The controller never auto-sequences reclaim -> fence -> restart as a
  ladder. Each attempt's action is selected from *current* durable
  evidence (the smallest applicable authorized action); multi-attempt
  sequences emerge from evidence, not from policy.
- A crash between attempt creation and verification is healed by
  read-only reconciliation: a controller never re-dispatches another
  controller's external action — it determines the outcome from durable
  evidence, and any retry is an explicit new attempt bounded by the
  two-attempt rule.
- Unknown or ambiguous durable policy state fails closed
  (`PolicyCorrupt` -> terminal block / manual escalation); R9 never
  guess-retries. Budgets are consumed exactly-once via a durable
  watermark (`consumed_attempt_number`) in a single CAS UPDATE guarded
  by `remaining_budget` bounds.

R9's `_maybe_execute` runs a rung 2/3 restart only when directly
executable (job ownerless, no open attempts, no lingering process);
otherwise the decision defers to the R8 intake path.

## Verification mechanism

Every attempt is verified from authoritative durable evidence **before**
and **after** the action — never from action return codes, heartbeats,
or controller memory:

- `_snapshot()` builds the progress-evidence bundle from the gate's
  read-only methods: job row (status, owner, token, lease bounds),
  `progress_updated_at`, verified artifact count, latest-known-good
  pointer, and the owner process identity from R6 spawn evidence.
- `_verify()` computes the per-rung progress delta and completes the
  attempt as `success` (delta > 0) or `retry`/`escalate` (delta = 0,
  with the two-attempt rule deciding which).
- R9 consumes terminal R8 attempt results through a second watermark
  (`result_consumed_attempt_number`), folding them into
  `zero_progress_count`: two consecutive zero-progress attempts escalate
  one rung; genuine progress resets the count (the ladder never rewinds).
- Ledger evidence: every attempt records `recovery.attempt` and
  `recovery.attempt_state` events; escalation records
  `recovery.policy_terminal` / `recovery.policy_rung_advanced` events.

## Relevant implementation

- `src/axos/exec/recovery.py` — `RecoveryController` (evaluate/drive/
  dispatch/verify/reconcile/intake), `RecoveryConfig` (explicit,
  validated timings), `CanonicalRungProvider`, `RungProvider`,
  `RungContext`, `RecoveryError`, `RecoveryNotReady`.
- `src/axos/exec/policy.py` — `PolicyController` (evaluate, rung/budget/
  escalation policy), `PolicyConfig`, `select_rung` (pure deterministic
  rung selection), `CANONICAL_LADDER`, `PolicyRungProvider`, per-rung
  budgets (1<=2, 2<=2, 3<=3, 4=0, 5=0) and per-incident cap 7.
- `src/axos/store/gate.py` — `create_recovery_attempt`,
  `transition_recovery_attempt`, `complete_recovery_attempt` (I-18
  enforcement), `record_recovery_attempt` (I-18 enforcement),
  `claim_recovery_attempt`, `note_attempt_dispatch`,
  `mark_recovery_attempt_uncertain`, `set_incident_escalated`,
  `find_or_create_recovery_incident`, `open_recovery_incidents`,
  `open_recovery_attempts`, `get_recovery_policy`,
  `ensure_recovery_policy`, `cas_update_recovery_policy`,
  `consume_policy_attempts`.
- `src/axos/store/transitions.py` — `RECLAIM_ACTORS`
  (`recovery-controller` is a reclaim authority; the supervisor is not).
- `src/axos/store/migrations.py` — the `recovery_attempts` table with
  the zero-progress `CHECK` constraint.

## Relevant tests

- `src/axos/tests/test_recovery_r8.py` — R8 controller and recovery
  contract gate: refuses before READY; HEALTHY verdicts produce no
  incident; STALLED/DEAD produce incidents and attempts; reclaim
  verification against durable evidence; zero-progress retry and the
  two-attempt escalation rule.
- `src/axos/tests/test_recovery_r9.py` — R9 ladder, budgets, and
  escalation: rung 1 on a new incident; deterministic `select_rung`
  behavior; budget consumption and watermark accounting; terminal
  escalation including rung 5 human pause.
