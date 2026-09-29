# 11 — Recovery Engine

The recovery controller owns DETECT→CLASSIFY→DIAGNOSE→RECOVER→VERIFY→
RESUME/ESCALATE. It is the **only** component that escalates, and it never
classifies from raw signals — it consumes health verdicts (`10`) and
observations (`09`).

## 11.1 Escalation ladder

Each rung: trigger, bound, action, verification, and what happens on failure.
Bounds live in the store (per task/stage policy), not in worker memory.

| Rung | Trigger | Bound | Action | Verify by | On failure → |
|---|---|---|---|---|---|
| 1. Retry operation | Transient error, attempt < max | 3 attempts, exp backoff + jitter | Re-run the operation in place | Operation succeeds | Rung 2 |
| 2. Backoff | Repeated transient failure | Per-policy cooldown; job invisible until `not_before` | Delay + requeue | Job re-attempted cleanly | Rung 3 |
| 3. Restart worker | Worker DEAD/STALLED, lease reclaimable | ≤2 restarts per job | Terminate instance, provision fresh, re-claim | New worker heartbeats + progresses | Rung 4 |
| 4. Replace worker | Restart didn't help, or `fast_but_wrong` | New instance, old one quarantined | Provision with same capabilities, different instance | Replacement healthy for observation window | Rung 5 |
| 5. Reassign job | Job repeatedly fails across workers | `max_attempts` (default 3) | Requeue to a different worker | Job progresses elsewhere | Rung 6 |
| 6. Restart stage | Stage-level incident (correlated failures) | ≤1 per stage per task version | Drain stage, reset non-terminal jobs to PENDING, resume | Stage throughput recovers | Rung 7 |
| 7. Reduce concurrency | Resource pressure or error-rate correlated with parallelism | Down to minimum 1 | Lower stage concurrency limit | Error rate drops | Rung 8 |
| 8. Change strategy | Diagnosis: current approach invalid but objective achievable | Journaled decision required | Adjust parameters/methodology config (new version) | Viability check passes | Rung 9 |
| 9. Replan task | Plan itself invalid (schema change, source death) | ≤2 replans per task | New graph version (`08`) | New plan validates | Rung 10 |
| 10. Pause safely | Anything above exhausted, or unsafe to continue | — | Checkpoint, preserve, diagnose | State preserved + verified | Rung 11 |
| 11. Human | PAUSED_FOR_HUMAN with diagnostic package | — | Wait for explicit human decision | Human acts | Terminal states |

**Rung-skipping is allowed downward only with evidence:** e.g. poison
detection jumps straight to quarantine (rung 5→quarantine) because retrying
poison is pure budget burn. Skipping *upward* (human before trying recovery)
is never automatic — that would abdicate autonomy.

## 11.2 Recovery budgets and loop detection

- Every rung has a budget counter in the store, scoped to (task, stage, job)
  as appropriate. Counters survive restarts — a VM crash does not reset the
  "we've tried this 3 times" memory.
- **Loop detector:** if the same job cycles through CLAIMED→FAILED→PENDING
  more than N times in window W, or the same incident signature recurs more
  than M times, the controller stops retrying that path and escalates two
  rungs. Loops are incidents in their own right (`INCIDENT_CLASS =
  recovery_loop`).
- **Recovery-caused-failure guard:** after any recovery action, the
  controller watches the *blast radius*: if the action correlates with new
  failures elsewhere within the window, it rolls back the action (e.g.
  restore previous concurrency) and escalates. Self-healing that makes things
  worse is a first-class failure mode (`30-architecture-risks.md`).
- **Recovery contract (ADR-016, invariant I-18):** every recovery action
  records `attempt → observed effect → progress delta (defined window) →
  output-health delta → decision`. A recovery action is NOT successful
  because a process restarted — heartbeat returning with zero progress is
  *failed recovery*. **Zero progress delta across two attempts = failed
  recovery, escalate**, regardless of process signals.
- **Canonical incident signatures (ADR-016):**
  `{failure_class, stage_id, capability_version, input_batch_id,
  error_class}` — raw error messages excluded, so variable parse errors
  from one poisoned batch collapse to one incident. Per-incident
  recovery-spend budget is independent of per-rung budgets; exhaustion opens
  the recovery-loop circuit breaker.

## 11.3 Supervisor failure

"The supervisor" in AXOS is the set of control-plane services. There is no
single supervisor process whose death stops the world:

1. **Detection:** L5 peer watchers (each service writes a liveness record;
   a designated peer checks it) + the bootstrap monitor (external process).
2. **During outage:** workers with valid leases keep working; heartbeats
   accumulate in the store; no new scheduling occurs. Nothing is lost
   because no in-flight truth lived in the dead service.
3. **Restart:** the bootstrap monitor restarts the service (backoff,
   alerting). The service boots *stateless*: it reads the store, rebuilds
   its view, and resumes. It does not "recover sessions".
4. **Reconciliation:** on boot, every service runs the reconciler once before
   taking action: stale leases reclaimed, orphaned heartbeats aged out,
   half-done expansions resumed idempotently.
5. **If the recovery controller itself dies:** incidents queue as
   observations; on restart it processes them oldest-first. Its budgets are
   in the store, so it cannot double-spend recovery attempts across the
   outage.

## 11.4 VM restart recovery lifecycle

Treat as normal. On boot, before any work starts:

1. Mount and integrity-check the store volume (checksum; refuse to start on
   corruption — alert instead).
2. Open the store; verify ledger tip hash chain (detect torn writes).
3. Load tasks in non-terminal states; load their graph versions.
4. Mark all workers not RETIRED/DEAD as DEAD with cause `vm_restart`
   (their processes are gone by definition).
5. Expire all leases whose `lease_expires_at` passed; mark jobs with live
   leases but dead owners as reclaimable (bump fencing tokens).
6. Move jobs in CLAIMED/RUNNING/COMMITTING with dead owners to UNCERTAIN
   (output may exist — reconcile, don't assume).
7. Run UNCERTAIN reconciliation (`07`): inspect staging, validate, commit
   or requeue.
8. Load latest verified checkpoint per task; verify still valid.
9. Reconcile artifacts: staged-but-uncommitted → classify; COMPLETE jobs
   with missing artifacts → integrity incident.
10. Reconstruct desired state (task specs, stage desired workers).
11. Compute desired−actual diff; emit repair plan.
12. Provision workers to meet desired capacity.
13. Resume scheduler; jobs become claimable.
14. Resume watchdog layers; re-baseline health (fresh evidence required —
    no inheriting pre-crash "healthy").
15. Append `vm_recovery_complete` to the ledger with a summary; notify per
    task notification policy.

No human says "continue task X". The machine already knows.

**Ordering rule (ADR-018):** artifact reconciliation (steps 7+9) completes
*before* checkpoint adoption drives resume (step 13) — recovery never
resumes from a checkpoint whose artifacts haven't been reconciled.

**Pause stickiness (ADR-015, invariant I-17):** tasks in PAUSED_FOR_HUMAN
or with PENDING approvals stay paused across restart. Boot recovery
reconciles and re-surfaces them (approvals re-listed, diagnostic packages
intact); it never auto-resumes a human-gated state, and auto-recovery is
stood down for the paused scope until explicit human resume. A stop must
hold.

## 11.5 Recovery vs replanning vs human (decision rules)

- **Recover** when: diagnosis confidence high, plan still valid, budget
  remains, verification possible. (Worker died → replace.)
- **Replan** when: diagnosis shows the *plan* is invalid (schema changed,
  source dead, methodology contradicted by evidence) but the objective is
  still achievable another way. Requires journaled decision.
- **Human** when: ambiguous requirements, irreversible action pending,
  irreconcilable state, budget exhausted, security concern, or two failed
  replans. Enters PAUSED_FOR_HUMAN with the diagnostic package — never with
  just an error string.
