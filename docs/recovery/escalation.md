# Escalation

Escalation is what happens when a recovery strategy is not producing
verified progress. The rung ladder climbs deliberately, budgets bound the
total cost, and the terminal rung hands control to a human through a
gate that no restart, crash, or automation can clear on its own.

The implementation lives in `src/axos/exec/policy.py` (R9:
`PolicyController`, `select_rung`, `CANONICAL_LADDER`) and the escalation
mechanics in `src/axos/exec/recovery.py` (R8's fixed escalation rule).

## The rung ladder

| Rung | Name | What happens |
|---|---|---|
| 1 | Retry with backoff | R8 reclaims the lease and requeues the job (routine retry). |
| 2 | Restart worker | R8 restarts via `dispatch_restart()` — the full sequence is
  reclaim → terminate → provision fresh → re-claim; the restart action
  is the provision step. |
| 3 | Replace / reassign | R8 fences the stale worker and/or restarts via existing paths. |
| 4 | Widen scope | No R9 executor exists for this rung (stage-widening belongs to
  the R10+ scheduler/reconciler); the rung is still traversed durably
  and monotonically — it is recorded, never skipped, before terminal
  escalation. |
| 5 | Replan / pause | Terminal: the escalation is persisted and the task enters
  `PAUSED_FOR_HUMAN`. See below. |

Rung selection (`select_rung`) is pure, deterministic, and never rewinds:
the returned rung is always ≥ the current rung. It is evaluated in a
fixed order of rules:

1. Authoritative state unavailable → rung 5, `BLOCKED_STATE_UNAVAILABLE`.
2. Contradictory evidence → rung 5, `BLOCKED_CONTRADICTORY`.
3. Incident attempt budget consumed → rung 5, `BUDGET_EXHAUSTED`.
4. Two consecutive zero-progress attempts → escalate one rung (the D6
   rule; see below).
5. Per-rung budget consumed → escalate one rung.
6. Rung 4 (no executor) → traverse to rung 5.
7. R8 intake guard holding with a non-executable rung → skip forward.
8. Otherwise → continue at the current rung.

## When escalation triggers

- **Two consecutive zero-progress attempts.** This is R8's fixed rule:
  after two attempts with `progress_delta == 0`, the incident is
  escalated to `"r9-policy"` (recorded in `escalation_target`). It is
  R8's rule, not R9's — escalation *to* R9 does not depend on R9 being
  alive, so a dead policy controller cannot suppress it
  (`src/axos/exec/recovery.py`;
  `src/axos/tests/test_recovery_r8.py`;
  `src/axos/tests/test_final_hardening_r14.py::test_R14_62_two_zero_progress_attempts_escalate`).
  The incident is durably escalated — never silently retried forever.
- **Per-rung budget consumed** (1≤2, 2≤2, 3≤3, 4=0, 5=0) → escalate one
  rung.
- **Incident budget consumed** (default 7) → `BUDGET_EXHAUSTED`
  (terminal, sticky).
- **Unusable authority or contradictory evidence** → terminal block
  (`BLOCKED_STATE_UNAVAILABLE`, `BLOCKED_CONTRADICTORY`); R9 never
  guess-retries.
- **R9 reconciliation** (`_reconcile_incident`) also consumes *results*:
  open R8 attempts are R8's to drive; only `SUCCEEDED`/`FAILED`/`BLOCKED`
  results are consumed into budgets and rung decisions.

Terminal states are sticky: once `RECOVERY_COMPLETE`,
`BUDGET_EXHAUSTED`, `R5_TERMINAL`, `BLOCKED_CONTRADICTORY`,
`BLOCKED_STATE_UNAVAILABLE`, or `SUPERSEDED` is set on the policy row, no
further rung selection, budget consumption, or execution happens for that
incident (`src/axos/exec/policy.py`).

## PAUSED_FOR_HUMAN semantics

Rung 5 ("Replan / pause") ends automated recovery: the task transitions
to `PAUSED_FOR_HUMAN` through `TransitionGate`, and stays there under
invariant I-17:

- **No exit without a recorded human `APPROVED` approval** — the
  `transition_task()` gate clause requires an `approval_ref` naming an
  `APPROVED` approval for that task (`store/gate.py:413–423`).
- **Sticky across restarts** — a restart, a supervisor loss, lease
  expiry, or repeated recovery attempts never move the task forward;
  `DENIED`, stale, foreign, wrong-task, and duplicate approvals do not
  release the gate (`src/axos/tests/test_store.py::test_14_human_gated_state_sticky_across_restart`).
- **Admission stops** — the scheduler admits nothing under a paused
  task (the real claim path refuses), and the reconciler creates nothing
  under one (`src/axos/tests/test_scheduler_r10.py::test_R10_23_blocked_and_paused_never_scheduled`).
  R9 stands down.
- **In-flight work drains** — the pause does not kill running workers;
  an in-flight job drains to `COMPLETE` on its own, then no new claims
  start (`src/axos/tests/test_final_hardening_r14.py::TestR14BFI12`,
  FI-12 — the pause drill).
- **Never auto-cleared** — no timeout, no heartbeat, no lease event, and
  no recovery attempt exits the state. Only an explicit recorded human
  decision does.

FI-12 is the canonical pause drill: operator pauses mid-flight, in-flight
work drains, no new claims are admitted, the pause survives a supervisor
restart, and an `APPROVED` approval resumes the task so remaining jobs
complete.

## What escalation is not

- Escalation is not a guess at a better action: R9 chooses rungs and
  budgets but never executes or verifies a recovery action; R8 executes
  exactly one attempt per decision and verifies it against evidence.
- Escalation is not silent: every escalation is a durable row
  (`escalation_target`, policy terminal state, ledger events) — an
  incident is never escalated by disappearing.
- Escalation to rung 5 is not a failure of the system: it is the system
  working as designed — refusing to burn more budget on zero-progress
  strategies and asking for human judgment instead.

## Related

- [Recovery model](recovery-model.md) — attempt lifecycle, budgets,
  success/failure conditions.
- [Failure handling](failure-handling.md) — crash injection,
  uncertain completion.
- [Invariants](../invariants/invariants.md) — I-17 and I-18 registry
  entries.
