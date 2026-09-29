# AXOS Recovery Architecture — As Implemented

**Release:** `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
**Migration version:** 12 · **Policy version:** `r9-policy/v1` · **Source:** `~/workspace/axos/`
**Exported:** 2026-09-27 · **Method:** read-only inspection; no source modified.

> Recovery in AXOS is a layered stack — observe (R4/R7), revoke (R1),
> fence (R2), restart (R3/R10), escalate (R5/R9), contain blast radius
> (R12), finalize (R13) — with each layer owning a narrow, documented slice
> of authority. This document describes the actual layering, the actual
> handoffs, and — corrected from the idealized picture — where a failure in
> one layer provably *cannot* cascade into another.

---

## 1. The recovery stack at a glance

| Layer | Module | Owns | Never touches |
|-------|--------|------|---------------|
| R4 observation | `gate.observe_expired_leases()` | read-only expiry evidence | any mutation |
| R1 revocation | `gate.reclaim_lease()` | lease revocation, token increment | processes |
| R2 fencing | `supervisor.fence_sweep()` | process-group SIGTERM/SIGKILL + reap | job authority |
| R6 boot | `exec/boot.py` | post-restart classification + recovery pass | signaling processes |
| R7 verdicts | `exec/watchdog.py` | HEALTHY/STALLED/DEAD classification | reclaims, fences, scheduling, completion |
| R8 attempts | `exec/recovery.py` | one attempt per decision; verify against evidence | budgets, rung choice, completion |
| R9 policy | `exec/policy.py` | rung choice, budgets, terminal escalation | execution, verification, fencing tokens |
| R10 admission | `exec/scheduler.py` | PENDING admission, dispatch, orphan redispatch | recovery-owned jobs |
| R11 desired | `exec/reconciler.py` | converge actual → desired | any transition |
| R12 breakers | `exec/resilience.py` | breaker rows | the claim path itself |
| R13 finalization | `exec/finalizer.py` | finalization runs, release checkpoint | everything operational |

The first architectural fact is the direction of authority: **every layer
reaches the one below only through `TransitionGate` or through a named,
typed seam** (`supervisor.fence_sweep()`, `supervisor.start_worker()`,
`RecoveryController.evaluate()`/`dispatch_restart()`). There is no layer
that reaches around its neighbor.

---

## 2. R4 — lease expiry observation (read-only by construction)

`TransitionGate.observe_expired_leases()` (`store/gate.py:820`) is a pure
read: it lists jobs whose `lease_expires_at <= now` (inclusive) using the
authoritative store timestamp. It cannot revoke, fence, or reschedule — the
function holds no write capability. Consumers:

- `exec/boot.py` — boot-time expired-lease observation, reclaimed via R1.
- The R8 controller — consumes R4 evidence; does not redefine it.

Lease expiry is classified by the watchdog as `STALLED`, never reclaimed by
it; recycling expired leases is R1's job (`gate.reclaim_lease()`).

---

## 3. R1 — lease reclaim (the revocation primitive)

`TransitionGate.reclaim_lease()` (`store/gate.py:927`) is the single
primitive that revokes a lease. In one transaction it verifies the evidence,
**increments the fencing token** (the old token can never become valid
again), clears owner/lease, changes state, and appends ledger evidence:

- `CLAIMED -> PENDING` (requeue)
- `RUNNING | COMMITTING -> UNCERTAIN` (needs resolution, not blind requeue)

Routine reclaim requires actual expiry. **Forced reclaim** requires a `DEAD`
or `STALLED` watchdog verdict plus `incident_id`. Reclaim actors are
`recovery-controller`, `reconciler`, `system`, `operator`, `test` — the
supervisor is deliberately excluded from reclaim authority. R1 never touches
processes: revocation is a store fact; physical containment is R2's job.

---

## 4. R2 — physical fencing (the containment primitive)

`Supervisor.fence_sweep()` (`exec/supervisor.py:557`) detects durable
owner/token divergence (the store says the lease moved; a process still
runs under the old identity) and contains it: signal the **process group**
with `SIGTERM`, then `SIGKILL` if required, reap, and record exactly one
`worker.fence_enforced` ledger event per enforcement. The sweep is used by
boot (`boot.py`) and by the R8 controller (`recovery.py`) — R8 never
signals processes itself. **On unreadable authority the sweep fails closed
and signals nothing.** Process-group (not process) signaling is what
contains the `wedged_spawn_child` case, where a wedged worker spawns a
child that spawns a grandchild — the whole tree shares the worker's process
group.

---

## 5. R6 — boot recovery (post-restart classification)

`exec/boot.py`: phases `STARTING → RECOVERING → READY | BLOCKED`;
dispositions `ADOPT`, `FENCE`, `ALREADY_DEAD`, `PID_REUSE`, `ORPHAN`,
`UNCERTAIN`. On construction the supervisor adopts durable unreaped spawn
evidence, runs `boot_recover()`, validates SQLite integrity and the ledger
chain, observes expired leases (R4 → reclaim through R1), and force-reclaims
live leases whose owner process is definitively dead or reused — using the
existing `fence_sweep()` for physical fencing (boot never signals processes
itself). `READY` means the recovery pass completed, not that every
contradiction is resolved (`fully_recovered` is reported separately).
**Boot only observes** completion/checkpoint integrity; it does not repair
or advance pointers. No new worker may start until the boot report phase is
`READY` — the scheduler, finalizer, resilience controller, and policy
controller all consult this gate before acting.

---

## 6. R7 — watchdog verdicts (detect and classify only)

`exec/watchdog.py` requires runtime readiness (`READY`) before evaluation
and **never reclaims, fences, schedules, or completes work**. Verdicts
`HEALTHY` / `STALLED` / `DEAD`; verdict identity is
`(job_id, fencing_token)`; `DEAD` is terminal for that execution identity.
Fresh heartbeats do not mask stale durable progress. `DEAD` requires
definitive evidence: a terminal worker row, a dead/reused process identity,
or durable latest-generation `worker.proc_reaped`. Verdict transitions
persist through `TransitionGate.record_watchdog_verdict()`; same-verdict and
post-`DEAD` re-evaluations are zero-mutation no-ops.

The watchdog's output is consumed by R8 (`recovery.py`) and by R12's
evidence scan (`resilience.py`). There is no callback, queue, or signal from
the watchdog to recovery in the source — the handoff is **durable rows
only**.

---

## 7. R8 — recovery attempts (execute exactly one, verify against evidence)

`exec/recovery.py`, the `RecoveryController`. Requires boot `READY`.
Consumes R7 verdicts and R4 expiry evidence; does not redefine either. Uses
R1 `reclaim_lease()` for lease revocation and `supervisor.fence_sweep()`
for physical fencing.

- **Actions:** `reclaim`, `fence`, `restart`.
- **Autonomous selection:** owned failed execution → `reclaim`; ownerless
  job with a lingering stale process in an existing incident → `fence`;
  ownerless with no lingering process → no action. `restart` is exposed via
  `dispatch_restart()` for R9/tests and is **not** autonomously selected by
  the R8 evaluation loop.
- **Canonical rung mapping:** `reclaim` → rung 2 `"re-claim/requeue"`;
  `fence`/`restart` → rung 3 `"replace/reassign"`.
- **Attempt binding:** every attempt binds `(job_id, fencing_token)`; a
  changed fencing token aborts stale attempts unless durable dispatch
  evidence proves the change was produced by that attempt.
- **Attempt states:** `CREATED`, `RUNNING`, `VERIFYING`, `SUCCEEDED`,
  `FAILED`, `BLOCKED`, `UNCERTAIN`. The gate's `transition_recovery_attempt()`
  takes caller-supplied allowed `from_states`; there is no single
  centralized attempt edge table.
- **Success criterion:** recovery is successful only with **positive
  authoritative progress** (I-18); heartbeats never count.
- **Fixed rule:** escalation after two consecutive zero-progress attempts to
  `"r9-policy"`. R8 records budget context as deferred to R9 and performs no
  budget arithmetic.
- **Crash reconciliation:** checks durable action effects; does not blindly
  redispatch another controller's action. Safe ambiguity ends in `BLOCKED`,
  `UNCERTAIN`, or escalation — never guessing.

---

## 8. R9 — the recovery ladder, budgets, and escalation

`exec/policy.py`, the `PolicyController`. R9 **chooses** the rung,
**bounds** recovery, and **decides** terminal escalation; it never executes
or verifies a recovery action. It invokes R8 only through
`RecoveryController.evaluate()` and `RecoveryController.dispatch_restart()`,
and reaches the store only through the gate's policy/recovery APIs. It
composes (never reimplements) R8, and reads R6's spawn classifier
(`boot._classify_spawn`) for lingering-process evidence — never touching
processes itself.

**Canonical ladder** (contract §E.1, `CANONICAL_LADDER`):

1. `"Retry with backoff"` — R8 reclaim (the routine requeue/retry primitive).
2. `"Restart worker"` — R8 restart via `dispatch_restart`. Per D1 the full
   rung-2 sequence is reclaim → terminate → provision fresh → re-claim; R8's
   restart action is the provision step, reached after reclaim.
3. `"Replace / reassign"` — R8 fence and/or restart via existing paths.
4. `"Widen scope"` — **no R9 executor** (stage-widening belongs to the R10+
   scheduler/reconciler). The rung is still traversed durably and
   monotonically before terminal escalation.
5. `"Replan / pause"` — terminal: persist escalation, enter the human-gated
   `PAUSED_FOR_HUMAN` task state through `TransitionGate` (sticky under
   I-17; never auto-cleared).

**Budgets** (contract §E.2): per-rung caps 1≤2, 2≤2, 3≤3, 4=0, 5=0 and one
per-incident cap (default 7). No time budget exists in the contract, so none
is invented. Budget consumption is exactly-once: attempts are counted at
reconcile time via a durable watermark (`consumed_attempt_number`) in a
single CAS UPDATE guarded by `remaining_budget` bounds; two concurrent
controllers produce exactly one authoritative consumption and the loser
re-reads.

**Rung selection** (`select_rung`, pure and deterministic): never rewinds
(returned rung ≥ current rung). Rules in order: authoritative state
unavailable → rung 5 `BLOCKED_STATE_UNAVAILABLE`; contradictory evidence →
rung 5 `BLOCKED_CONTRADICTORY`; incident budget consumed → rung 5
`BUDGET_EXHAUSTED`; two consecutive zero-progress attempts (D6) → escalate
one rung; per-rung budget consumed → escalate one rung; rung 4 (no executor)
→ traverse to 5; R8 intake guard holding with non-executable rung → skip
forward; otherwise continue at the current rung.

**Execution path:** one `evaluate()` pass reconciles policy state from
durable R8 evidence, drives R8 (at most one attempt per decision), then
reconciles the results. Rung 2/3 restart executes only when directly
executable (ownerless, no open attempts, no lingering process); otherwise
the decision defers to the R8 intake. Unknown or ambiguous durable state
fails closed (terminal block / manual escalation); R9 never guess-retries.

---

## 9. R10 — scheduler recovery interactions

`exec/scheduler.py` participates in recovery in exactly three bounded ways:

1. **Orphan redispatch:** a `CLAIMED` job owned by a `sched-*` worker with a
   live lease and no unreaped `worker.proc_spawned` evidence is re-dispatched
   **under the same claim identity** (`expect_token` carries the durable
   token) — never re-claimed. Orphans with expired leases are skipped (R4
   observes, R1 recycles). Orphans whose owner still beats are skipped (a
   live scheduler mid-dispatch is never stolen).
2. **Recovery-incident exclusion:** PENDING candidates carrying an open
   `scope="recovery"` incident are skipped — R8 owns those; the scheduler
   must not race R8's replacement dispatch.
3. **Breaker preflight:** the advisory `_breaker_denies()` check routes
   candidates in OPEN/unreadable scopes to `resilience_denied` without
   attempting a claim; the claim transaction revalidates authoritatively.

Dispatch failures leave the claim standing (lease expiry → R4 → R1 recycles
it). The scheduler never rolls back a claim, never marks a job failed, and
never escalates to recovery.

---

## 10. R11 — reconciler recovery interactions

`exec/reconciler.py` creates PENDING jobs only for `DESIRED_MISSING` items
whose task exists and is not `PAUSED_FOR_HUMAN`. It never reclaims, never
fences, never moves a job between states. Its recovery relevance is
indirect: jobs it creates flow into the scheduler's admission path, and the
scheduler's breaker scopes include the `DESIRED` scope resolved through
R11's `desired_job_map` — so a broken desired item's blast radius is
contained by the same breaker that contains its task and job scopes.

---

## 11. R12 — circuit breakers (blast-radius containment)

`exec/resilience.py`, the `ResilienceController`: the reader/evaluator half
of the breaker. It owns no execution authority; its only writes go through
the gate's breaker API. One `evaluate_once()` pass:

1. Boot gate (no mutation before `READY`).
2. **Evidence scan:** watchdog `DEAD` verdicts, terminal-`FAILED` recovery
   attempts, and escalated recovery incidents become idempotent breaker
   signals (deduplicated by the gate) on `JOB` / `TASK` / `GLOBAL` scopes —
   plus a `GLOBAL` recovery-pressure signal while the recovery subsystem is
   under load (open attempts, recent failures, recent escalations).
3. **Threshold:** a `CLOSED` breaker whose windowed `failure_count` reaches
   its threshold opens (CAS; loser re-reads).
4. **Cooldown:** an `OPEN` breaker past `cooldown_until` moves to
   `HALF_OPEN` (CAS). Probe allocation is the gate's job:
   `claim_half_open_probe()` runs inside the claim transaction with probe id
   `"<worker_id>:<job_id>"` and a recorded progress baseline.
5. **Probe evaluation from durable evidence only:** job `COMPLETE` (R5
   commit) or strictly positive `progress_done` delta over the baseline
   closes the breaker; counted failure after the probe reopens it.
   Heartbeats and process existence are never consulted. `OPEN` never
   transitions directly to `CLOSED` — only `HALF_OPEN` probe success closes
   a breaker. Corrupt probe state fails closed (reopen).

**Where breakers bite:** inside `gate.claim_job_resilient()`, which
evaluates scopes in the fixed order `GLOBAL → TASK → DESIRED → JOB` in the
same transaction as the capacity check and the claim. A stale "allow" from
the scheduler's advisory preflight is corrected fail-closed in the
transaction; a stale "deny" merely defers the candidate. The resilience
controller itself cannot deny a claim — it can only move breaker rows.

---

## 12. R13 — finalization (the recovery stack's read-only consumer)

`exec/finalizer.py`: the reader/evaluator half of finalization. It owns no
execution authority and never mutates jobs, tasks, leases, workers,
processes, artifacts, checkpoints, budgets, rungs, desired state, or
breakers. One `evaluate_once()` pass: boot gate → begin the head
generation's run (idempotent) → evaluate every non-finalized run from
durable evidence (CAS on version; loser re-reads) → publish `READY` runs
(the gate re-validates everything authoritatively inside one `write_txn`
and flips to `FINALIZED` atomically, or refuses). A pass over an
already-`FINALIZED` generation performs zero writes. The finalizer never
clears a human gate, never completes a job, never moves a breaker.

---

## 13. Corrected cascade analysis — where failure provably cannot propagate

This section corrects the idealized architecture's cascade picture. Each
claim is grounded in the observed authority boundary.

### 13.1 A wedged or hostile worker cannot take down the control plane

The worker's only store path is `TransitionGate` methods fenced by
(owner, fencing_token, live lease). A `wedged` behavior performs no further
gate interaction at all; it can only hold its process open. Containment is
the supervisor's process-group fence (`fence_sweep()`), which operates on
durable owner/token divergence — it does not depend on the worker
cooperating, heartbeating, or even existing. **There is no call path from
any worker into the supervisor, scheduler, watchdog, recovery, or policy
code.** The worst a worker can do is burn its own lease epoch, which
expires (R4) and is reclaimed (R1) without any other component's
cooperation.

### 13.2 A dead scheduler cannot strand work

Scheduler claims plant a liveness beat atomically with the claim. Another
scheduler instance (or the same one after restart) re-dispatches orphaned
`CLAIMED` jobs whose owner's beat is stale, under the same claim identity —
no re-claim, no duplicate execution (the atomic `CLAIMED -> RUNNING`
transition serializes the race; the loser gets `TransitionRejected` before
executing). Expired-lease orphans are not the scheduler's problem at all:
R4 observes, R1 recycles. **Documented residual:** two schedulers racing one
orphaned dispatch may briefly spawn two processes; exactly one executes.

### 13.3 Watchdog failure cannot cause spurious recovery

The watchdog's only write is `record_watchdog_verdict()`. It cannot reclaim,
fence, schedule, or complete. If the watchdog crashes, verdicts stop
arriving — R8's autonomous selection and R12's evidence scan simply see no
new verdict evidence. R4 expiry observation (a gate read, not a watchdog
function) continues to drive lease recycling independently. **A failed
watchdog degrades classification, not revocation.**

### 13.4 R8 failure cannot corrupt budgets or rungs

R8 performs no budget arithmetic and chooses no rungs beyond its
action-view mapping (reclaim→2, fence/restart→3). Budgets live in R9's
policy rows, consumed exactly-once via watermarked CAS; rung selection is
R9's pure `select_rung()`. If R8 crashes mid-attempt, the attempt row stays
`CREATED`/`RUNNING`/`VERIFYING`/`UNCERTAIN` (an open state), and R9's next
reconcile pass accounts it via the watermark — a crash between R8 attempt
creation and policy accounting heals on the next pass. **R8 cannot spend
budget it cannot see.**

### 13.5 R9 failure cannot execute anything

R9's only execution seams are `RecoveryController.evaluate()` and
`dispatch_restart()`. If R9 crashes, R8's autonomous loop (reclaim/fence on
its own evidence) and the scheduler's orphan redispatch continue; incidents
stay open and budgeted rather than escalating silently. R9's fixed escalation
rule (two consecutive zero-progress attempts → escalate to `"r9-policy"`)
is R8's rule, not R9's — **escalation *to* R9 does not depend on R9 being
alive.**

### 13.6 Breaker-controller failure cannot block or admit work

The resilience controller moves breaker rows; admission decisions happen
inside `gate.claim_job_resilient()`'s transaction. If the controller
crashes: existing `OPEN` breakers stay open (fail-safe: they continue to
deny until cooldown + probe, both evaluated by the gate's stored
`cooldown_until` and the next controller pass — but note the cooldown
*transition* to `HALF_OPEN` is the controller's job, so a dead controller
leaves breakers open longer, never shorter). **A dead breaker controller
fails closed, never open.**

### 13.7 Scheduler/reconciler/finalizer failures are self-contained

Each runs its own `evaluate_once()`; each fails closed on unreadable state;
each performs zero writes when there is nothing to do. The reconciler's
refusals perform zero mutation (not even a run row). The finalizer's
publish is atomic-or-refuse inside one `write_txn`. None of them holds a
lock, lease, or in-memory authority that another component needs.

### 13.8 The store is the single point of failure — and the design admits it

Every cascade above terminates at the SQLite database. The mitigations are
narrow and explicit: `BEGIN IMMEDIATE` serializes writers; one authoritative
timestamp per transaction keeps time monotonic against
`axos_meta.last_commit_ts`; the ledger is hash-chained (tamper-evident);
migrations run atomically. What the design does **not** provide: replication,
failover, backup/restore automation, or tamper-proofing of the ledger. These
are known boundaries, not oversights discovered here — but they are the
boundaries within which all the non-cascade claims above hold.

---

## 14. End-to-end recovery walkthroughs (as implemented)

### 14.1 Worker dies mid-execution (SIGKILL)

1. Heartbeats stop; lease expires (TTL passes; heartbeats do not renew).
2. R4: `observe_expired_leases()` lists the job (read-only).
3. Watchdog classifies `STALLED` (lease expiry) or `DEAD` (durable
   `worker.proc_reaped` / dead process identity).
4. R1: `reclaim_lease()` revokes — `RUNNING -> UNCERTAIN`, token incremented.
5. R8: consumes verdict/expiry evidence; selects `reclaim` (rung 2) or
   `fence` (rung 3) if a stale process lingers; verifies against
   authoritative progress (I-18).
6. R9: accounts the attempt against budgets via watermark; escalates the
   rung on two consecutive zero-progress attempts or budget exhaustion.
7. R12: the `DEAD` verdict / failed attempt becomes a breaker signal on the
   `JOB`/`TASK`/`GLOBAL` scopes; repeated failures open the breaker and the
   next `claim_job_resilient()` denies admission into the scope.

### 14.2 Crash after staging (bytes durable, no commit)

1. The `crash_after_stage` worker stages and verifies artifact bytes, then
   `os._exit(3)` before `commit_artifact`. Job is `COMMITTING`; bytes are
   `VALIDATED` on disk.
2. Lease expires → R4 → R1: `COMMITTING -> UNCERTAIN`.
3. The UNCERTAIN resolver path (`commit_artifact()` from `UNCERTAIN`,
   reclaim-authority actors only, artifact token lineage predating reclaim):
   adopt when the staged bytes verify, requeue when they do not (the
   `corrupt_staged_bytes` injection forces the requeue path).

### 14.3 Wedged worker ignoring SIGTERM

1. `wedged_ignore_sigterm` performs no further gate interaction; the lease
   expires.
2. R1 reclaims the lease (store fact — needs no worker cooperation); the
   fencing token increments, so the wedged process's token is permanently
   stale.
3. `fence_sweep()` detects the divergence, SIGTERMs the process group (no
   effect — SIGTERM is ignored), then SIGKILLs it, reaps, and records
   `worker.fence_enforced`.
4. Any subsequent gate call by the wedged process fails with `LeaseError`.

### 14.4 Breaker trip and recovery

1. Repeated `DEAD` verdicts / failed recovery attempts / escalations on a
   task's jobs → R12 signals → `CLOSED` breaker reaches threshold → `OPEN`.
2. `claim_job_resilient()` denies new claims into the scope inside the claim
   transaction (fail-closed); the scheduler counts `resilience_denied` and
   moves on.
3. Cooldown elapses → controller moves `OPEN -> HALF_OPEN`; the next claim
   allocates the single probe (`<worker_id>:<job_id>`) with a progress
   baseline, inside the transaction.
4. Probe job `COMPLETE`s or shows strictly positive `progress_done` delta →
   `HALF_OPEN -> CLOSED`. Counted failure after the probe → `OPEN` again.

---

## 15. Evidence gaps

- **Runtime behavior was not executed** in this export; all flows above are
  reconstructed from static source. The walkthroughs describe what the code
  paths do, not observed executions.
- **`exec/recovery.py` internals** (the attempt driver, autonomous action
  selection, `RungContext`/`RungProvider`/`CanonicalRungProvider`, the exact
  escalation mechanics) were read in the prior session; per-function line
  references for that module are not re-verified in this pass.
- **Supervisor internals** (`start_worker`, `fence_sweep`, `boot_recover`,
  `_boot_ready`) were read in the prior session; per-function line
  references are not re-verified in this pass.
- **Watchdog internals** (verdict computation, `_classify` logic) were read
  in the prior session; per-function line references are not re-verified in
  this pass.
- The **recovery-attempt edge table** is caller-supplied per call; the
  lifecycle in `AXOS_STATE_MACHINE.md` §7 is reconstructed from the state
  sets the controllers use, not from a single authoritative table.
- **R3 (restart)** as a numbered rung has no dedicated module: restart is
  realized as R8's `restart` action via `dispatch_restart()` plus
  `supervisor.start_worker()`. The rung-number → module mapping for R3 is a
  naming correspondence, not a code unit.
