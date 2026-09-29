# Recovery

Recovery is how AXOS turns observed failures into durable, verified
progress without human intervention — until the evidence says human
judgment is required. It is a layered stack where each layer owns a
narrow, documented slice of authority and reaches the layers below only
through `TransitionGate` or a named, typed seam. No layer reaches around
its neighbor.

## The critical rule

**Recovery success is determined by verified progress, not by the mere
performance of a recovery action.** A recovery attempt recorded as
`success` must carry a positive authoritative progress delta (invariant
I-18); "the process restarted" with zero progress is rejected by the
gate and by a database `CHECK` constraint. AI reasoning about an action
is not authoritative state — measured durable evidence is.

## Documents

- [Recovery model](recovery-model.md) — the incident lifecycle:
  identity, failure classification, recovery rung, attempt number,
  budget, success and failure conditions, observed progress delta,
  resulting state, and escalation target.
- [Escalation](escalation.md) — the rung ladder, when escalation
  triggers, and the semantics of `PAUSED_FOR_HUMAN` (exits only via a
  recorded human `APPROVED` approval, across restarts — I-17).
- [Failure handling](failure-handling.md) — crash injection (SIGKILL),
  backup/restore, migration failure atomicity, and uncertain completion
  leading into reconciliation.

## The recovery stack at a glance

| Layer | Module | Owns | Never touches |
|---|---|---|---|
| R4 observation | `gate.observe_expired_leases()` | read-only expiry evidence | any mutation |
| R1 revocation | `gate.reclaim_lease()` | lease revocation, token increment | processes |
| R2 fencing | `supervisor.fence_sweep()` | process-group SIGTERM/SIGKILL + reap | job authority |
| R6 boot | `exec/boot.py` | post-restart classification + recovery pass | signaling processes |
| R7 verdicts | `exec/watchdog.py` | HEALTHY/STALLED/DEAD classification | reclaims, fences, scheduling, completion |
| R8 attempts | `exec/recovery.py` | one attempt per decision; verify vs evidence | budgets, rung choice, completion |
| R9 policy | `exec/policy.py` | rung choice, budgets, terminal escalation | execution, verification, fencing tokens |
| R10 admission | `exec/scheduler.py` | PENDING admission, dispatch, orphan redispatch | recovery-owned jobs |
| R11 desired | `exec/reconciler.py` | converge actual → desired | any transition |
| R12 breakers | `exec/resilience.py` | breaker rows | the claim path itself |
| R13 finalization | `exec/finalizer.py` | finalization runs, release checkpoint | everything operational |

R3 (restart) has no dedicated module: restart is realized as R8's
`restart` action via `dispatch_restart()` plus `supervisor.start_worker()`.

## Related

- [Invariants](../invariants/invariants.md) — I-18 (verified progress),
  I-17 (human-gated stickiness).
- [Contracts](../contracts/README.md) — recovery, reconciliation,
  uncertain-completion, and finalization contracts.
- [Operations](../operations/README.md) — bootstrap, testing,
  verification, troubleshooting.
- `reports/historical/axos-source-export-20260927/AXOS_RECOVERY_ARCHITECTURE.md`
  — the read-only implementation export these documents are condensed
  from (release `551d559c…`, migration v12).
