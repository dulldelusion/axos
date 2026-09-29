# Reconciliation Contract

## Purpose

Reconciliation converges actual state toward declared desired state —
by observing, diffing, and taking only the narrowest authorized action.
It covers three mechanisms that share one discipline: **observe and
converge, never assume.**

1. **R11 desired-state reconciliation**: converge actual jobs toward a
   declared desired set by materializing missing jobs — and nothing else.
2. **R10 orphan redispatch**: heal scheduler-side dispatch state after a
   crash, without duplicating execution.
3. **R8 crash reconciliation**: determine a recovery action's outcome from
   durable evidence after a crash, without re-dispatching blindly.

## Authoritative state

- **`desired_state_head`** — the current desired set's identity: `version`
  and `snapshot_hash` (sha256 over the canonical JSON of *all* items,
  retired included).
- **`desired_work_items`** — the declared items: `desired_work_id`,
  `spec` (task_id, stage_id, max_attempts, policy — unknown keys are
  rejected), `retired`, `version`.
- **`desired_job_map`** — binds each desired item to the job the gate
  materialized for it. The job identity is deterministic:
  `canonical_job_id(desired_work_id) = "dj-" + sha256("axos-desired-job:v1:"
  + desired_work_id)[:32]` — the gate and the reconciler share the one
  construction, so they can never disagree on it.
- **`reconciliation_runs`** — durable bookkeeping for each converge pass
  (run record, per-batch cursor, result).

## Allowed transitions

The reconciler **creates jobs and does nothing else**. The only
job-creation path is the gate's single idempotent op
`ensure_job_for_desired_state` (actor literally `"reconciler"`; the
work-creator check requires exact membership). It never deletes
anything, never moves a job between states, never completes, fails,
blocks, or quarantines anything, never touches desired state rows
beyond reading them, and owns no execution, lease, artifact, checkpoint,
or recovery authority. It never opens a store transaction itself.

A converge pass pins the desired-state version at the start, verifies
the head's snapshot hash, then walks items in sorted `desired_work_id`
order in fixed-size batches, re-pinning the version before every batch,
with a durable cursor checkpointed after each batch.

**Diff verdicts** (`diff_item`, pure and deterministic):

- desired retired -> `OBSOLETE`
- task row missing/unreadable -> `CONTRADICTORY`
- map row present but job row missing, or job present but map row
  missing, or map's `job_id` != job's `job_id`, or desired spec hash !=
  map row's `spec_hash` -> `CONTRADICTORY`
- no job row -> `DESIRED_MISSING`
- job `PENDING` -> `DESIRED_ALREADY_PRESENT`
- job `CLAIMED/RUNNING/COMMITTING/VERIFYING` -> `DESIRED_ACTIVE`
- job `COMPLETE` -> `DESIRED_COMPLETE`
- job `FAILED` -> `DESIRED_FAILED`
- job `BLOCKED` -> `DESIRED_BLOCKED`
- job `UNCERTAIN` -> `DESIRED_UNCERTAIN`
- anything else (e.g. `QUARANTINED`) -> `CONTRADICTORY`

Only `DESIRED_MISSING` causes a gate call — and only when the task
exists and is not `PAUSED_FOR_HUMAN` (I-17: a human paused that scope,
and the reconciler never creates work under it). Every other verdict is
recorded as a discrepancy and nothing else happens.

**Result precedence** for a pass: `FAILED` > `CONFLICT` > `BLOCKED` >
`CHANGED` > `CONVERGED`.

**R10 orphan redispatch** (scheduler): a `CLAIMED` job owned by a dead
`sched-*` worker is re-dispatched **under the same claim identity**
(`expect_token` carries the durable token) — never re-claimed.
Expired-lease orphans are skipped (R4 observes, R1 recycles); orphans
whose owner still beats are skipped; PENDING candidates under an open
recovery incident are skipped (R8 owns those).

**R8 crash reconciliation** (`_reconcile`): an attempt in `UNCERTAIN` (or
a stale-claimed `RUNNING` with no dispatch record) is resolved by
reading durable action effects (`_action_effect_present`). It never
re-dispatches another controller's external action; any retry is an
explicit new attempt bounded by the two-attempt rule.

## Safety invariant

- A retired desired item is a **tombstone**, recorded as `OBSOLETE` and
  left strictly alone: retirement is not a cancellation (`PENDING` has no
  legal cancel edge), and the reconciler owns no transition authority at
  all.
- Contradictory desired state refuses with **zero mutation** — not even a
  run record. Stale desired-state pins (head version moved) refuse
  before any creation; mid-pass version moves stop creation and close
  the run as `CONFLICT`.
- Exactly one job per desired item even under concurrency: N threads x N
  reconcilers on one missing item produces exactly one job (the atomic
  idempotent gate op).
- The reconciler never adopts a foreign job: a job under the canonical
  identity with no map row is `CONTRADICTORY`, never adopted.

## Failure behavior

- Unreadable desired state (missing tables, snapshot mismatch,
  unparseable row) -> `FAILED` with zero mutation.
- Unreadable actual state mid-pass -> stop creating, close the run as
  `FAILED`. Nothing created so far is rolled back; creation is
  idempotent, so a later pass resumes cleanly.
- R8 crash reconciliation with indeterminate action effect (evidence
  unreadable) -> safe stop with escalation, never guessing.
- A crash between R8 attempt creation and R9 policy accounting heals on
  the next pass via the durable watermark (`consumed_attempt_number`) —
  each attempt is accounted exactly once.

## Verification mechanism

- The head's `snapshot_hash` is recomputed from all items (retired
  included) at pin time and re-verified against the stored hash —
  exactly the function the gate uses (`_desired_snapshot_hash`), so the
  reconciler and the gate cannot diverge on what the desired set is.
- The deterministic `canonical_job_id` construction is shared by
  delegation: one definition, used by both the gate op and the
  reconciler.
- Every pass returns a deterministic report (`run`, `result`,
  `items_examined`, `items_created`, `discrepancies`, `summary`), and the
  run record is finished durably (`finish_reconciliation_run`) with the
  same fields.
- R9's budget watermark and R8's idempotency-keyed restart dispatch
  (`dispatch_restart` checks the durable dispatch evidence before
  spawning) prevent double-execution across reconciler/controller
  crashes.

## Relevant implementation

- `src/axos/exec/reconciler.py` — `Reconciler` (`reconcile()`,
  `evaluate_once()`, background loop), `ReconcilerConfig`,
  `canonical_job_id`, `diff_item`.
- `src/axos/store/gate.py` — `ensure_job_for_desired_state` (the single
  idempotent creation op), `_canonical_desired_job_id`,
  `_desired_snapshot_hash`, `_desired_spec_hash`,
  `_normalize_desired_spec`, `begin_reconciliation_run`,
  `checkpoint_reconciliation_run`, `finish_reconciliation_run`,
  `get_desired_head`, `get_desired_job_map`, `list_desired_items`,
  `DesiredStateConflict`.
- `src/axos/exec/recovery.py` — `_reconcile`,
  `mark_recovery_attempt_uncertain` (gate),
  `_action_effect_present`, `dispatch_restart` idempotency.
- `src/axos/exec/policy.py` — `_consume_created` (exactly-once watermark
  accounting across crashes).

## Relevant tests

- `src/axos/tests/test_reconciliation_r11.py` — R11 desired-state
  reconciliation: durable desired-state snapshot; deterministic snapshot
  identity; canonical job_id stable across runs and reconciler restarts;
  missing item detected (`DESIRED_MISSING`) and created exactly once as
  PENDING; N threads x N reconcilers -> exactly one job; PENDING
  recognized as already-present (no-op); retired items tombstoned;
  contradictory state refuses with zero mutation.
