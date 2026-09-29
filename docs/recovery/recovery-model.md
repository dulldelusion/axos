# Recovery model

This document describes the recovery incident lifecycle as implemented:
how a failure becomes an incident, how an attempt is driven and judged,
and what each field on a recovery record means. The authoritative
definitions live in `src/axos/store/gate.py` (recovery record APIs),
`src/axos/exec/recovery.py` (R8, the attempt driver), and
`src/axos/exec/policy.py` (R9, the rung/budget/escalation controller).

## The critical rule

**Recovery success = verified progress, not action performed.**

A recovery attempt recorded as `success` must carry a positive
authoritative progress delta (invariant I-18). The gate rejects
`decision="success"` with `progress_delta <= 0`, rejects a missing
`observed_effect`, and a database `CHECK` constraint rejects the same
contradiction even on direct inserts (`store/gate.py:2468–2497`,
`2823–2865`; `src/axos/tests/test_store.py::test_16_recovery_attempt_requires_progress_evidence`,
`src/axos/tests/test_recovery_r8.py::test_R8_08_i18_success_requires_positive_delta`).
"Process restarted successfully" with zero progress cannot be recorded as
success — but honest non-success decisions (`retry`, `escalate`,
`BLOCKED`) with zero progress remain representable, because a failed
attempt is still evidence.

Known boundary (recorded, not solved): the store enforces the *shape* of
evidence — present, positive delta — not its *truth*. Semantic
verification is R8's job: `RecoveryController._progress_delta()` and
`_verify()` (`src/axos/exec/recovery.py`) compare authoritative
snapshots taken before and after the action; heartbeats never count.
See `reports/historical/AUDIT-REPORT.md` §B.2.

## Incident identity

An incident is the durable unit of recovery work. Identity is carried by
the `incidents` row:

| Field | Meaning |
|---|---|
| `incident_id` | durable identifier of the incident |
| `scope` | what the incident covers (`job`, `recovery`, `ops`, …) |
| `failure_class` | failure category (e.g. `worker_crash`, `DEAD`) |
| `signature` | canonical signature of the causal shape — same causal shape
  yields the same signature, so a poisoned batch collapses into one
  incident (`test_15_incident_signature_is_canonical`) |
| `stage_id`, `capability_version`, `input_batch_id`, `error_class` | causal coordinates that feed the signature |
| `outcome` | terminal assessment once known (`recovered`, …) |
| `escalated_to` | durable marker once the incident is escalated |

Recovery incidents are created via `gate.find_or_create_recovery_incident()`
— idempotent, so two controllers racing one incident converge on one row
(`src/axos/tests/test_recovery_r8.py::test_R8_29_two_controllers_one_incident`).

## Failure classification

Classification is R7's job (`src/axos/exec/watchdog.py`), and it is
detect-and-classify **only**: the watchdog never reclaims, fences,
schedules, or completes work. Verdicts:

- `HEALTHY`, `STALLED`, `DEAD`.
- Verdict identity is `(job_id, fencing_token)` — a verdict is bound to
  one lease epoch; `DEAD` is terminal for that execution identity.
- Fresh heartbeats do not mask stale durable progress.
- Lease expiry is classified `STALLED`, never reclaimed by the watchdog.
- `DEAD` requires definitive evidence: a terminal worker row, a
  dead/reused process identity, or durable latest-generation
  `worker.proc_reaped`.
- Same-verdict and post-`DEAD` re-evaluations are zero-mutation no-ops.

The handoff to recovery is **durable rows only** — no callback, queue, or
signal exists in the source. R8 and R12 consume the persisted verdicts.

## Recovery rung

A rung names the escalation level of the current recovery strategy.
The canonical ladder (`src/axos/exec/policy.py`, `CANONICAL_LADDER`):

| Rung | Name | Realized by |
|---|---|---|
| 1 | Retry with backoff | R8 `reclaim` (the routine requeue/retry primitive) |
| 2 | Restart worker | R8 `restart` via `dispatch_restart()` — the full sequence is
  reclaim → terminate → provision fresh → re-claim; R8's restart action
  is the provision step |
| 3 | Replace / reassign | R8 `fence` and/or `restart` via existing paths |
| 4 | Widen scope | **no R9 executor** (stage-widening belongs to the R10+
  scheduler/reconciler); the rung is still traversed durably and
  monotonically before terminal escalation |
| 5 | Replan / pause | terminal: persist escalation, enter human-gated
  `PAUSED_FOR_HUMAN` via `TransitionGate` (sticky under I-17, never
  auto-cleared) |

Rung selection (`select_rung` in `exec/policy.py`) is pure and
deterministic and **never rewinds** (returned rung ≥ current rung). The
R8 action→rung mapping is fixed: `reclaim` → rung 2 `"re-claim/requeue"`;
`fence`/`restart` → rung 3 `"replace/reassign"`. Every attempt row
records `rung` and `rung_name`.

## Attempt number

Each incident carries a monotonic sequence of attempts. Fields on the
`recovery_attempts` row:

- `attempt_id` — durable identifier of the attempt.
- `attempt_number` — the incident-local sequence number
  (`UNIQUE(incident_id, attempt_number)`; backstop against double
  accounting).
- `attempt_state` — `CREATED`, `RUNNING`, `VERIFYING`, `SUCCEEDED`,
  `FAILED`, `BLOCKED`, `UNCERTAIN`. Exactly one OPEN attempt per
  incident.
- `idempotency_key` — makes attempt creation idempotent across
  controller crashes.
- `job_id`, `fencing_token` — the attempt is bound to one lease epoch;
  a changed fencing token aborts stale attempts unless durable dispatch
  evidence proves the attempt caused the change.
- `claimed_at`, `budget_context`, `evidence_before`, `evidence_after`,
  `escalation_target` — the full audit trail of the attempt.

Attempt states are transitioned via `gate.transition_recovery_attempt()`,
which takes caller-supplied allowed `from_states` — there is no single
centralized attempt edge table; the lifecycle is reconstructed from the
state sets the controllers use.

## Budget

Budgets bound recovery so a hopeless incident terminates instead of
retrying forever. Budgets are R9's property — R8 performs no budget
arithmetic and records its budget context as deferred to R9.

| Budget | Cap |
|---|---|
| Per-rung, rung 1 | 2 |
| Per-rung, rung 2 | 2 |
| Per-rung, rung 3 | 3 |
| Per-rung, rung 4 | 0 |
| Per-rung, rung 5 | 0 |
| Per-incident | 7 (default) |

No time budget exists in the contract — none is invented. Consumption is
exactly-once: attempts are counted at reconcile time via a durable
watermark (`consumed_attempt_number`) in a single CAS `UPDATE` guarded by
`remaining_budget` bounds; two concurrent controllers produce exactly one
authoritative consumption and the loser re-reads (`exec/policy.py`,
`_consume_created` / `_consume_results`).

## Success condition

An attempt reaches `SUCCEEDED` only when all of the following hold:

1. The action executed against the bound `(job_id, fencing_token)`
   (or durable evidence shows the attempt caused a legitimate token
   change).
2. `observed_effect` is a non-empty description of what was observed.
3. `progress_delta > 0` — positive authoritative progress, measured by
   R8 from before/after snapshots (`RecoveryController._progress_delta`);
   heartbeats never count.
4. `decision="success"` is recorded via `complete_recovery_attempt()`
   with `evidence_after`.

The R14-B universal recovery contract asserts this for *every* incident
in the database, including boot-created forced reclaims: every attempt
row carries a measured progress delta and an I-18-legal decision
(`src/axos/tests/test_final_hardening_r14.py`, post-FI recovery
contract).

## Failure condition

An attempt is a failure (`FAILED`) when the action executed but no
positive progress resulted, or when verification of the action's effects
fails. Repeated failures are what budgets and escalation exist for:

- **Two consecutive zero-progress attempts → escalate** to
  `"r9-policy"` — this is R8's fixed rule, not R9's; escalation *to* R9
  does not depend on R9 being alive (`src/axos/exec/recovery.py`;
  `src/axos/tests/test_final_hardening_r14.py::test_R14_62_two_zero_progress_attempts_escalate`).
- **Budget consumed → `BUDGET_EXHAUSTED`** (terminal; sticky).
- Unknown or ambiguous durable state fails closed: terminal block
  (`BLOCKED_STATE_UNAVAILABLE`, `BLOCKED_CONTRADICTORY`) or manual
  escalation — R9 never guess-retries.

## Observed progress delta

`progress_delta` is the measured, durable difference the attempt
produced, in authoritative units (e.g. durable `progress_done` movement
on the job row, lease/token state changes such as a verified reclaim).
It is *observed* — computed from store state before and after the action
— never self-reported by the action. I-18 ties `decision="success"` to
`progress_delta > 0`; R9's zero-progress escalation rule counts
consecutive attempts whose observed delta is zero; R12's half-open probe
closes only on strictly positive `progress_done` delta over the probe
baseline.

## Resulting state

`resulting_state` records the state the recovery attempt left the world
in (e.g. `RECOVERED`, `REQUEUED`, escalated markers). R9's terminal
states are sticky: `RECOVERY_COMPLETE`, `BUDGET_EXHAUSTED`,
`R5_TERMINAL`, `BLOCKED_CONTRADICTORY`, `BLOCKED_STATE_UNAVAILABLE`,
`SUPERSEDED` (`src/axos/exec/policy.py`). Once a terminal state is set,
no further rung selection, budget consumption, or execution happens for
that incident.

## Escalation target

`escalation_target` records where an attempt's escalation was directed —
canonically `"r9-policy"` after two consecutive zero-progress attempts.
Rung 5's terminal escalation persists the escalation and moves the task
to `PAUSED_FOR_HUMAN`; see [escalation](escalation.md).

## End-to-end: worker dies mid-execution (SIGKILL)

1. Heartbeats stop; the lease expires (TTL passes; heartbeats do not
   renew).
2. R4: `gate.observe_expired_leases()` lists the job (read-only).
3. The watchdog classifies `STALLED` (lease expiry) or `DEAD` (durable
   `worker.proc_reaped` / dead process identity).
4. R1: `gate.reclaim_lease()` revokes — `RUNNING → UNCERTAIN`, fencing
   token incremented.
5. R8: consumes verdict/expiry evidence; selects `reclaim` (rung 2) or
   `fence` (rung 3) if a stale process lingers; verifies against
   authoritative progress (I-18).
6. R9: accounts the attempt against budgets via the watermark; escalates
   the rung on two consecutive zero-progress attempts or budget
   exhaustion.
7. R12: the `DEAD` verdict / failed attempt becomes a breaker signal on
   `JOB`/`TASK`/`GLOBAL` scopes; repeated failures open the breaker and
   the next `claim_job_resilient()` denies admission into the scope.

## Related

- [Escalation](escalation.md) — the ladder and its triggers.
- [Failure handling](failure-handling.md) — crash injection, backup,
  migration, uncertain completion.
- [Invariants](../invariants/invariants.md) — I-18 registry entry.
- `reports/historical/axos-source-export-20260927/AXOS_RECOVERY_ARCHITECTURE.md`
  §7–8 — the implementation export this model condenses.
