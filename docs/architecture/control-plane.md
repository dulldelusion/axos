# Control Plane

The control plane is `src/axos/exec/`: process lifecycle and a set of
deliberately narrow controllers. Its fundamental law, enforced by the
authority audit (`src/axos/audit/09_authority_audit.py`), is:

> `exec/` **never** calls `write_txn()`, **never** opens its own
> `sqlite3` connections, **never** issues SQL write statements.

Every controller reads durable evidence through the store and writes
only through `TransitionGate` methods. Each controller opens its own
`Store` handle (one per thread — a `Store` connection is not shared
across threads) and builds thread-local gates. The import direction is
strictly upward: `exec/` imports from `axos.store`; `store/` imports
nothing from `exec/`.

## The authority boundary (F1)

The boundary has three physical parts in `store/db.py`:

1. **The gate holds the writable capability.** `Store` is the writable
   handle; only `TransitionGate` (plus transient migration/setup code)
   may hold one.
2. **`Store.conn` is read-only.** It is a separate connection opened
   with `PRAGMA query_only=ON`.
3. **`ReadOnlyStore` is the interface for every non-gate component.**
   It exposes no writable connection and no transaction capability and
   cannot be escalated.

The writable capability is `Store.write_txn()`: it opens
`BEGIN IMMEDIATE` (serializing writers), assigns **one authoritative
store timestamp per transaction** — monotonic against the persisted
`axos_meta.last_commit_ts` — and is gate-owned. Wall-clock time never
enters durable state directly; worker clocks are never trusted. This
boundary is application-level: raw `sqlite3.connect()` on the database
file is outside the boundary by definition (equivalent to filesystem
access).

A gate mutation generally combines **validation → mutation → ledger
append → commit** in one `write_txn()` transaction; an invalid
transition raises `TransitionRejected` with state left unchanged.

## Components and roles

### Supervisor (`exec/supervisor.py`)

Owns **worker process lifecycle, not job authority**. It spawns real
OS worker processes as new session/process-group leaders
(`python -m axos.exec.worker`), mints one `proc_id` per spawn
(`new_proc_id()`), records worker identity and durable spawn/reap
evidence through the gate, and drives worker-state transitions while
only *reading* job state.

Its two authority-relevant entry points:

- `start_worker()` — called by the scheduler (dispatch) and by R8's
  restart path.
- `fence_sweep()` — called by boot (R6) and R8: detects durable
  owner/token divergence, signals the process **group** with `SIGTERM`
  then `SIGKILL` if required, reaps, and records the durable
  `worker.fence_enforced` ledger milestone. On unreadable authority
  the sweep **fails closed and signals nothing**.

No new worker may start until boot reports `READY`.
`Supervisor.close()` stops the sweep and closes store handles but
deliberately does **not** terminate worker OS processes.

### Worker (`exec/worker.py`)

The subprocess entrypoint (`python -m axos.exec.worker --db …
--worker-id … --proc-id … --job-id … --ttl-s … --behavior …`).
It never manufactures authority and follows a fixed five-step
protocol:

1. **Claim** via `gate.claim_job()` — or, with `--expect-token` (the
   R10 pre-claimed dispatch path), *verify* the durable
   `(owner_worker_id, fencing_token)` triple and exit
   `EXIT_STALE_TOKEN` (7) with zero side effects on mismatch.
2. Read the lease triple back from the store (received, not invented).
3. `transition_job(job, "RUNNING")` (informational; fencing is enforced
   at commit).
4. Run the executor — heartbeats and progress through the gate —
   while a **daemon renewal thread** on its own store connection
   keeps the lease alive via the sanctioned `gate.renew_lease()` path
   (renewal is not a heartbeat and confers no new authority). On
   `LeaseError`, stop immediately (`FencedError`); on SIGTERM, stop
   promptly and commit **nothing** (`InterruptedExecution`, exit 143).
5. On SUCCESS: the R5 protocol `stage_artifact()` →
   `begin_commit()` → `verify_artifact()` → `commit_artifact()` with
   real deterministic artifact bytes (canonical JSON result record;
   `duration_s` excluded to preserve byte-level determinism). On
   FAILURE: `fail_job_execution()`.

Exit codes (`EXIT_OK=0` committed, `EXIT_COMMITTED_FAILURE=10`,
`EXIT_CLAIM_FAILED=3`, `EXIT_FENCED=4`, `EXIT_COMMIT_REJECTED=5`,
`EXIT_FATAL=6`, `EXIT_STALE_TOKEN=7`, `EXIT_SIGTERM=143`) are process
evidence only — never job success.

### Scheduler (`exec/scheduler.py`, R10)

Admission and atomic claim plus dispatch — deliberately narrow: it
never creates or deletes jobs, never reconciles, never moves
breakers, never reclaims leases, never completes work. One
`evaluate_once()` pass does three things:

1. **Claim-liveness refresh** of its own scheduler beats (a live
   scheduler refreshes every pass, so a racing scheduler never
   mistakes its fresh claims for dead orphans).
2. **Orphan recovery:** `CLAIMED` jobs owned by a `sched-*` worker
   with a live lease and no unreaped `worker.proc_spawned` evidence
   are **re-dispatched under the SAME claim identity**
   (`--expect-token` carries the durable token) — never re-claimed.
   Expired-lease orphans are skipped (R4 observes, R1 recycles; the
   scheduler does not touch them). Orphans whose owner still beats
   are skipped (a live scheduler mid-dispatch is never stolen).
3. **PENDING admission, `ORDER BY created_at, job_id`** (no canonical
   job priority exists). Per candidate: skip jobs under an open
   recovery incident (R8 owns those) or under a `PAUSED_FOR_HUMAN`
   task (I-17); mint a fresh `sched-<id>-<uuid>` worker identity; run
   the advisory breaker preflight; then `gate.claim_job_resilient()`
   — which revalidates the breaker **authoritatively inside the claim
   transaction**, so a stale "allow" is corrected fail-closed and a
   stale "deny" merely defers. `False` means a lost race or full
   capacity → backpressure: PENDING stays PENDING, no failure marking,
   no escalation.

Dispatch failures leave the claim **standing** (lease expiry → R4 →
R1 recycles it); the scheduler never rolls back and never invents
repair. Documented residual: two schedulers racing one orphaned
dispatch may briefly spawn two processes; exactly one wins the
atomic `CLAIMED → RUNNING` transition (serialized by
`BEGIN IMMEDIATE`) and the loser gets `TransitionRejected` before
executing, after which R2's fence sweep contains its process group.

### Watchdog (`exec/watchdog.py`, R7)

Detect and classify only — never reclaims, never fences, never
schedules, never completes work. Requires boot `READY` before
evaluation. Verdicts `HEALTHY` / `STALLED` / `DEAD` with verdict
identity `(job_id, fencing_token)`; `DEAD` is terminal for that
execution identity and requires definitive evidence (a terminal
worker row, a dead/reused process identity, or durable
latest-generation `worker.proc_reaped`). Fresh heartbeats do not mask
stale durable progress; lease expiry is classified `STALLED`, never
reclaimed. Verdicts persist via
`TransitionGate.record_watchdog_verdict()`; same-verdict and
post-`DEAD` re-evaluations are zero-mutation no-ops.

Handoff to recovery is **durable rows only** — the watchdog has no
callback, queue, or signal into recovery.

### Recovery controller (`exec/recovery.py`, R8)

Attempt execution and verification. Requires boot `READY`. Consumes
R7 verdicts and R4 expiry evidence; it does not redefine either.
Actions — `reclaim`, `fence`, `restart` — go only through existing
authorities (R1 `reclaim_lease()` for lease revocation,
`supervisor.fence_sweep()` for physical fencing, scheduler-class path
for restart); it never signals processes itself. Canonical rung
mapping: `reclaim` → rung 2 ("re-claim/requeue"), `fence` /
`restart` → rung 3 ("replace/reassign"). `restart` is exposed via
`dispatch_restart()` for R9/tests and is **not** autonomously selected
by the R8 evaluation loop. Every attempt binds `(job_id,
fencing_token)`; a changed token aborts stale attempts unless
durable dispatch evidence proves the attempt caused the change.
Recovery succeeds only on **positive authoritative progress** (I-18);
heartbeats never count. The fixed rule escalates to `"r9-policy"`
after two consecutive zero-progress attempts; R8 records budget
context as deferred to R9 — R8 performs no budget arithmetic.

### Policy controller (`exec/policy.py`, R9)

The policy layer above R8: **chooses** the rung, **bounds** recovery,
**decides** terminal escalation; it never executes or verifies a
recovery action. It invokes R8 only through
`RecoveryController.evaluate()` and
`RecoveryController.dispatch_restart()`. The canonical 5-rung ladder:
1 "Retry with backoff", 2 "Restart worker", 3 "Replace / reassign",
4 "Widen scope" (no R9 executor — stage-widening belongs to the
R10+ scheduler/reconciler; the rung is still traversed durably and
monotonically), 5 "Replan / pause" (terminal: escalate to human and
enter the human-gated `PAUSED_FOR_HUMAN` task state via
`TransitionGate`, sticky under I-17, never auto-cleared). Budgets:
per-rung caps 1≤2, 2≤2, 3≤3, 4=0, 5=0 and one per-incident cap
(default 7); no time budget exists in the contract. Budget
consumption is exactly-once via a durable watermark
(`consumed_attempt_number`) in a single CAS UPDATE guarded by
`remaining_budget` bounds. `select_rung()` is a pure deterministic
function of durable state and never rewinds. Unknown or ambiguous
durable state fails closed. Policy rows carry `policy_version`
`r9-policy/v1`; rows with any other version are refused as
`PolicyCorrupt`.

### Resilience controller (`exec/resilience.py`, R12)

The reader/evaluator half of the circuit breaker: it owns no
execution authority and writes only through the gate's breaker API.
One `evaluate_once()` pass: (1) boot gate; (2) evidence scan —
watchdog `DEAD` verdicts, terminal-`FAILED` recovery attempts, and
escalated incidents become idempotent breaker signals (deduped by
the gate) on JOB / TASK / GLOBAL scopes, plus a GLOBAL
recovery-pressure signal while recovery is under load; (3) threshold
— a CLOSED breaker whose windowed failure count reaches its
threshold opens (CAS; loser re-reads); (4) cooldown — an OPEN breaker
past `cooldown_until` moves to HALF_OPEN; (5) probe evaluation from
**durable evidence only** — job `COMPLETE` (R5 commit) or strictly
positive `progress_done` delta over the probe baseline closes the
breaker; counted failure after the probe reopens it; heartbeats and
process existence are never consulted. `OPEN` never transitions
directly to `CLOSED`; corrupt probe state fails closed (reopen). The
scheduler's claim path is where breakers bite:
`claim_job_resilient()` denies admission into OPEN scopes **inside
the transaction** — the controller itself cannot deny a claim
directly. Breaker scopes, in fixed evaluation order: GLOBAL, TASK,
DESIRED, JOB.

### Reconciler (`exec/reconciler.py`, R11)

Desired-state convergence; the narrowest authority in the system. It
never creates jobs except through the gate's single idempotent
`ensure_job_for_desired_state()` with the literal actor
`"reconciler"`; it never deletes, never moves a job between states,
never completes/fails/quarantines anything, and never calls into the
scheduler, policy, or recovery layers. One `reconcile()` pass pins
the desired-state head version, verifies the head's snapshot hash
over *all* items (retired included), then walks items in sorted
`desired_work_id` order in fixed batches, re-pinning the version
before every batch (a move mid-pass stops creation and closes the
run `CONFLICT`; an internally contradictory desired state refuses
with zero mutation — not even a run row). Only `DESIRED_MISSING`
with an existing, non-paused task creates a job; every other diff
verdict is recorded as a discrepancy. Retired items are tombstones
(`OBSOLETE`), never cancellations — `PENDING` has no legal cancel
edge in `JOB_TRANSITIONS`.

### Finalizer (`exec/finalizer.py`, R13)

The reader/evaluator half of finalization: it owns no execution
authority and never mutates jobs, tasks, leases, workers, processes,
artifacts, checkpoints, budgets, rungs, desired state, or breakers.
One `evaluate_once()` pass: (1) boot gate (no finalization before
`READY`); (2) begin — the desired-state head's generation gets its
finalization run (idempotent re-begin); (3) evaluate — every
non-finalized run is re-evaluated from durable evidence with CAS on
version (loser re-reads, never overwrites); (4) publish — a `READY`
run is published: the gate re-validates everything authoritatively
inside **one** `write_txn` and flips it to `FINALIZED` atomically, or
refuses. No-op discipline: a pass over an already-`FINALIZED`
generation performs zero writes. The finalizer never clears a human
gate, never completes a job, never moves a breaker.

### Boot recovery (`exec/boot.py`, R6)

Boot phases `STARTING → RECOVERING → READY | BLOCKED`. On
construction the supervisor adopts durable unreaped spawn evidence,
runs `boot_recover()`, validates SQLite integrity and the ledger
chain, observes expired leases (R4 → reclaim via R1), and
force-reclaims live leases whose owner process is definitively
dead/reused — using the existing supervisor `fence_sweep()` for
physical fencing (boot never signals processes itself). Dispositions
per persisted runtime record: `ADOPT` only when process identity
matches AND durable job authority is current; otherwise `FENCE` /
`ALREADY_DEAD` / `PID_REUSE` (never kill) / `ORPHAN` / `UNCERTAIN`.
`READY` means the recovery pass completed, **not** that every
contradiction is resolved (`fully_recovered` is reported separately).
Boot only *observes* completion/checkpoint integrity; it never
repairs or advances pointers.

## Heartbeats

Heartbeats are observation evidence with no authority:

- Sequenced per `(worker_id, proc_id)`; duplicate or out-of-order
  `hb_seq` is rejected without mutation.
- A heartbeat naming a job requires current owner and a matching
  fencing token; stale or fenced heartbeats journal
  `worker.heartbeat_fenced` and raise `LeaseError`.
- **A heartbeat never extends a lease, never confers authority, and
  is never evidence of success.** Renewal (`gate.renew_lease()`) is a
  separate sanctioned path — executed by the worker's daemon renewal
  thread on its own store connection — and requires owner, matching
  token, active status, and an **unexpired** lease. It extends only
  the current triple; it can never resurrect a superseded token or
  steal another lease.
- Heartbeat progress, when present, flows through the same
  `_apply_progress()` path as explicit progress updates; progress is
  accepted only in `CLAIMED` or `RUNNING` with current
  owner/token/live lease (F3).
- Lease expiry is observed read-only by
  `gate.observe_expired_leases()` with inclusive expiry
  (`lease_expires_at <= now`) against the authoritative store clock.

## Who may write what

| Component | May write | May never write |
|-----------|-----------|-----------------|
| `TransitionGate` | everything, transactionally | — (the bottleneck) |
| Supervisor | ledger process events; worker-row transitions | job authority |
| Worker | heartbeats, progress, artifacts, failure — only under its lease triple | anything without owner/token/live-lease |
| Scheduler | claims, scheduler beats | job states, leases, breakers, recovery |
| Reconciler | desired jobs (one op), run bookkeeping | any transition, any execution state |
| Watchdog | verdicts | anything authoritative |
| Recovery (R8) | reclaims, attempts, incidents | budgets, rungs, completion |
| Policy (R9) | policy rows, rung, escalation, terminal task pause | execution, verification, fencing tokens |
| Resilience (R12) | breaker rows | jobs, leases, recovery |
| Finalizer (R13) | finalization runs, R5 release checkpoint | everything operational |

## Corrected interaction map

Controllers coordinate **only through durable rows**; several
arrows that an idealized picture would draw do not exist:

- The scheduler does **not** talk to workers and workers do **not**
  talk to the scheduler. The scheduler's only worker-facing call is
  `supervisor.start_worker()`; the worker's only store-facing path is
  `TransitionGate` methods.
- The worker never calls the supervisor, the watchdog, recovery, or
  policy. Its only recovery input is the gate refusing its next call
  (`LeaseError` → `FencedError` → stop).
- The watchdog has **no arrow into recovery**: it persists verdicts;
  the R8 controller reads them.
- The resilience controller has **no arrow into the claim path**: it
  moves breaker rows; the gate's `claim_job_resilient()` enforces
  them transactionally.
- Policy (R9) has **no arrow into leases, processes, or fencing
  tokens**: it reaches execution only through
  `RecoveryController.evaluate()` and `dispatch_restart()`.
- Boot has **no arrow into processes**: it observes and reclaims
  through the gate and delegates physical fencing to the
  supervisor's existing sweep.
- The reconciler has **no arrow into the scheduler**: it creates
  PENDING jobs; the scheduler independently admits them.

The runtime call flow, end to end:

```
reconciler.reconcile() ──► gate.ensure_job_for_desired_state()   [PENDING]
scheduler.evaluate_once() ──► gate.claim_job_resilient()
        │   (atomic: capacity + breaker + claim + scheduler beat)
        ▼
supervisor.start_worker(...) ──► subprocess: python -m axos.exec.worker
worker: claim_job() [or --expect-token verify] ──► transition_job(RUNNING)
        ──► heartbeat()/progress() ; renewal thread: renew_lease()
        ──► SUCCESS: stage → begin_commit → verify → commit [COMPLETE]
        ──► FAILURE: fail_job_execution() [FAILED]
watchdog.evaluate() ──► gate.record_watchdog_verdict()   [detect only]
recovery.evaluate() ◄── R7 verdicts, R4 expiry evidence
        ├─► gate.reclaim_lease()            [R1]
        ├─► supervisor.fence_sweep()        [physical fencing]
        └─► supervisor.start_worker()      [restart via dispatch_restart]
policy.evaluate() ──► reconcile durable R8 evidence ──► rc.evaluate()/dispatch_restart()
resilience.evaluate_once() ──► gate.record_breaker_signal()/transition_breaker()
finalizer.evaluate_once() ──► gate.begin/evaluate/publish_finalization()
boot_recover() ──► gate.observe_expired_leases() → gate.reclaim_lease() [R4→R1]
               ──► supervisor.fence_sweep()
```

See [ownership-model.md](ownership-model.md) for the lease/claim/
fencing chain, [state-machine.md](state-machine.md) for the
transition graphs each of these controllers moves through, and
[data-flow.md](data-flow.md) for the write path and ledger.
