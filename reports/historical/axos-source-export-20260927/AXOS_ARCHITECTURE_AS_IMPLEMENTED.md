# AXOS Architecture — As Implemented

**Release:** `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
**Migration version:** 12 · **Policy version:** `r9-policy/v1` · **Source:** `~/workspace/axos/`
**Exported:** 2026-09-27 · **Method:** read-only inspection of every file listed below; no source modified.

> This document describes what the code actually does, not what any design
> document says it should do. Every claim names the file and function it was
> observed in. Where behavior could not be verified from source, that is
> stated explicitly.

Files inspected in full: `store/db.py`, `store/gate.py` (5,613 lines),
`store/transitions.py`, `store/migrations.py`, `store/__init__.py`,
`exec/supervisor.py`, `exec/worker.py`, `exec/synthetic.py`,
`exec/identity.py`, `exec/boot.py`, `exec/watchdog.py`, `exec/recovery.py`,
`exec/policy.py`, `exec/scheduler.py`, `exec/reconciler.py`,
`exec/resilience.py`, `exec/finalizer.py`, `exec/__init__.py`, and
`release_manifest.json`. The release identity fields above are taken
verbatim from `release_manifest.json` (`"release_id"`, `"migration_version"`,
`"policy_version"`).

---

## 1. System at a glance

AXOS is a single-process-capable execution OS whose **authoritative state is
one SQLite database in WAL mode**. All authority — who may execute what, in
which lease epoch, with which fencing token — lives in the store layer
(`store/`). Everything else is a controller that *reads* durable evidence and
*requests* state changes through the store's single mutation bottleneck,
`TransitionGate` (`store/gate.py`). No execution component writes SQL
directly; the authority audit (`audit/09_authority_audit.py`) greps for that
(A16).

Five subsystems, in dependency order:

| # | Subsystem | Modules | Role |
|---|-----------|---------|------|
| 1 | Authority core | `store/db.py`, `store/gate.py`, `store/transitions.py`, `store/migrations.py` | SQLite store, lease/fencing authority, transition graphs, tamper-evident ledger |
| 2 | Execution substrate | `exec/identity.py`, `exec/supervisor.py`, `exec/worker.py`, `exec/synthetic.py`, `exec/boot.py` | OS process lifecycle, worker entrypoint, deterministic fault injection, boot recovery |
| 3 | Work intake | `exec/scheduler.py`, `exec/reconciler.py` | PENDING admission + dispatch (R10), desired-state convergence (R11) |
| 4 | Observation & recovery | `exec/watchdog.py`, `exec/recovery.py`, `exec/policy.py`, `exec/resilience.py` | Verdicts (R7), attempt execution (R8), ladder/budgets/escalation (R9), circuit breakers (R12) |
| 5 | Release | `exec/finalizer.py` | Finalization runs + R5 release checkpoint + publish (R13) |

The import direction is strictly **upward toward `store/`**: every
`exec/*.py` controller imports `TransitionGate` (and `open_store`/`migrate`)
from `axos.store`; `store/` imports nothing from `exec/`. The one deliberate
exception to pure upward flow is `exec/policy.py`, which imports
`RecoveryController` from `.recovery` and `boot._classify_spawn` from `.boot`
— it *composes* R8 and reads R6's classifier, never reimplementing either.

---

## 2. Authority core (`store/`)

### 2.1 `store/db.py` — the store itself

- `Store` wraps one SQLite database in **WAL mode**. `Store.conn` is a
  **separate connection opened `PRAGMA query_only=ON`**; `ReadOnlyStore`
  exposes reads only and no transaction capability, so non-gate components
  physically cannot write through it.
- The writable capability is `Store.write_txn()`, which opens
  `BEGIN IMMEDIATE` (serializing writers) and assigns **one authoritative
  store timestamp per transaction**, monotonic against the persisted
  `axos_meta.last_commit_ts`. Wall-clock time never enters durable state
  directly; every timestamped row carries the transaction's authoritative
  time.
- Exceptions: `TransitionRejected` (a legal-but-refused transition),
  `LeaseError` (lost fencing authority), `StoreError` (base),
  `PolicyConflict`/`BreakerConflict`/`FinalizationConflict`
  (lost compare-and-swap races), `DesiredStateConflict`.

### 2.2 `store/gate.py` — the mutation bottleneck

`TransitionGate` owns every normal authoritative mutation. A gate mutation
generally combines **validation → mutation → ledger append → commit** in one
`write_txn()` transaction. The ledger is **SHA-256 hash chained** through
`TransitionGate._append_event()` — tamper-*evident*, not tamper-proof (the
database itself is not signed or encrypted).

Key gate operations and their callers:

- **Jobs:** `claim_job()` / `claim_job_resilient()` (scheduler, worker),
  `transition_job()` (always *rejects* the `COMPLETE` target), `renew_lease()`,
  `observe_expired_leases()` (read-only; expiry `lease_expires_at <= now`),
  `reclaim_lease()` (R1; CLAIMED→PENDING, RUNNING|COMMITTING→UNCERTAIN),
  `ingest_heartbeat()`, `update_job_progress()`, `fail_job_execution()`.
- **R5 completion (the only path to COMPLETE):**
  `stage_artifact()` → `begin_commit()` → `verify_artifact()` →
  `commit_artifact()`. `commit_job_result()` exists only as a rejecting
  compatibility stub — the direct transition path is dead code by design.
- **Artifacts/checkpoints:** `stage_artifact()` computes the content hash
  itself (SHA-256 over the bytes after `fsync`), `verify_checkpoint()`,
  `invalidate_checkpoint()` (authorized VERIFIED→CORRUPT path for
  `system`/`operator`/`test`).
- **Process evidence:** durable spawn/reap/fence facts are ledger milestones
  appended via `gate.append_event("worker.proc_spawned" | "worker.proc_reaped" |
  "worker.fence_enforced", …)` by the supervisor — they are *events*, not
  separate gate methods.
- **Watchdog (R7):** `record_watchdog_verdict()`, `watchdog_verdicts_for()`.
- **Recovery (R8):** `find_or_create_recovery_incident()`,
  `transition_recovery_attempt()` (callers pass allowed `from_states`; there
  is no single centralized attempt edge table), `recovery_attempts_for()`,
  `open_recovery_attempts()`, `set_incident_escalated()`,
  `set_incident_outcome()`, `open_recovery_incidents()`,
  `escalated_recovery_incidents()`, `recovery_incidents_with_open_policy()`.
- **Policy (R9):** `get_recovery_policy()`, `ensure_recovery_policy()`,
  `cas_update_recovery_policy()`, `consume_policy_attempts()`.
- **Scheduler (R10):** `pending_jobs()`, `sched_claimed_orphans()`,
  `refresh_scheduler_claim_beats()`, `withdraw_scheduler_claim_beat()`,
  `clear_scheduler_claim_beats()`, `scheduler_claim_live()`,
  `claim_job_resilient()` — capacity predicate + breaker revalidation +
  claim + scheduler-beat planting in **one** `write_txn()` (see §7).
- **Reconciler (R11):** `get_desired_head()`, `get_desired_job_map()`,
  `get_desired_work_id_for_job()`, `ensure_job_for_desired_state()`,
  `begin/checkpoint/finish_reconciliation_run()`.
- **Breakers (R12):** `record_breaker_signal()`, `get_breaker_state()`,
  `list_breaker_states()`, `transition_breaker()`, `claim_half_open_probe()`,
  `breaker_allows()`.
- **Finalization (R13):** `begin_finalization_run()`,
  `evaluate_finalization()`, `publish_finalization()`,
  `list_finalization_runs()`.

### 2.3 `store/transitions.py` — the legal graphs

Pure transition tables (verified by direct import; full graphs in
`AXOS_STATE_MACHINE.md`): `TASK_TRANSITIONS`, `JOB_TRANSITIONS`,
`WORKER_TRANSITIONS`, `ARTIFACT_TRANSITIONS`, `APPROVAL_TRANSITIONS`,
`CHECKPOINT_TRANSITIONS`.

### 2.4 `store/migrations.py` — schema evolution, v1–v12

Migrations are numbered through **v12**, each run atomically under
`BEGIN IMMEDIATE`. v12 adds `artifact_stagings`, preserving per-job staging
provenance when identical bytes deduplicate to one artifact row.

### 2.5 `store/__init__.py`

Re-exports: `open_store`, `migrate`, `TransitionGate`, the exception
hierarchy, canonical-identity and finalization-manifest helpers.

---

## 3. Execution substrate (`exec/`)

### 3.1 `exec/identity.py` — five identities, never collapsed

`worker_id` (durable logical worker), `proc_id` (one OS process instance;
a restart always mints a fresh one via `new_proc_id()`), `job_id` (store
truth), `lease_id` = `job_id#fencing_token` (one lease epoch), and
`fencing_token` (monotonic authority counter; higher = newer; an old token
can never become valid again). A worker with the same `worker_id` but a new
`proc_id` inherits nothing and must claim a fresh lease.

### 3.2 `exec/supervisor.py` — OS process lifecycle only

The supervisor owns **processes, not job authority**: it spawns workers as
new session/process-group leaders via `subprocess.Popen(
["python", "-m", "axos.exec.worker", …])`, records durable spawn evidence
(`worker.proc_spawned` ledger events), and drives worker-state transitions
while only *reading* job state. Its two authority-relevant entry points are
`start_worker()` (called by the scheduler and R8's restart path) and
`fence_sweep()` (called by boot and R8): the sweep detects durable
owner/token divergence, signals the process **group** with `SIGTERM`, then
`SIGKILL` if required, reaps, and records `worker.fence_enforced`. On
unreadable authority the sweep **fails closed and signals nothing**.
`Supervisor.close()` stops the sweep and closes store handles but
deliberately does **not** terminate worker OS processes. No new worker may
start until boot reports `READY`.

### 3.3 `exec/worker.py` — the subprocess entrypoint

`python -m axos.exec.worker --db … --worker-id … --proc-id … --job-id …
--ttl-s … --behavior …` implements a fixed five-step protocol and never
manufactures authority:

1. **Claim** via `gate.claim_job()` — or, with `--expect-token` (the R10
   pre-claimed dispatch path), *verify* the durable
   `(owner_worker_id, fencing_token)` triple and exit 7 (`EXIT_STALE_TOKEN`)
   with zero side effects on mismatch.
2. Read the lease triple back from the store (received, not invented).
3. `transition_job(job, "RUNNING")` (informational; fencing enforced at commit).
4. Run `SyntheticExecutor` — heartbeats and progress through the gate —
   while a **daemon renewal thread** on its own store connection keeps the
   lease alive via the sanctioned `gate.renew_lease()` path (renewal is not a
   heartbeat and confers no new authority). On `LeaseError`, stop immediately
   (`FencedError`); on SIGTERM, stop promptly and commit **nothing**
   (`InterruptedExecution`, exit 143).
5. On SUCCESS: the R5 protocol `stage_artifact()` → `begin_commit()` →
   `verify_artifact()` → `commit_artifact()` with real deterministic artifact
   bytes (canonical JSON result record; `duration_s` excluded to preserve
   byte-level determinism). On FAILURE: `fail_job_execution()`. Exit codes
   are process evidence only — never job success: `0` committed,
   `10` committed failure, `3` claim lost, `4` fenced, `5` commit rejected,
   `6` fatal store error, `7` stale token, `143` SIGTERM.

### 3.4 `exec/synthetic.py` — deterministic fault injection

`SyntheticExecutor` runs *inside* the worker process and performs no real
work: no LLMs, no network, no randomness. `BehaviorSpec` kinds include
`success_immediate`, `success_delayed`, `controlled_failure`,
`heartbeat_loop` (no commit), `hang`, `wedged` variants (no further gate
interaction — only the supervisor's process-group fence can contain them),
`crash`, `crash_after_stage` (stages + verifies bytes, then `os._exit(3)`
*before* commit — the durable VALIDATED bytes are left for the UNCERTAIN
resolver to adopt or requeue; `corrupt_staged_bytes` overwrites the staged
file post-verification to force the requeue path), `sigkill_self`, and
`expire_then_commit` (requires `--no-renew`: sleeps past the TTL, then the
commit attempt itself is the fencing test).

### 3.5 `exec/boot.py` — boot recovery (R6)

Boot phases `STARTING → RECOVERING → READY | BLOCKED`; dispositions
`ADOPT`, `FENCE`, `ALREADY_DEAD`, `PID_REUSE`, `ORPHAN`, `UNCERTAIN`. On
construction the supervisor adopts durable unreaped spawn evidence, runs
`boot_recover()`, validates SQLite integrity and the ledger chain, observes
expired leases (R4 → reclaim via R1), and force-reclaims live leases whose
owner process is definitively dead/reused — using the existing supervisor
`fence_sweep()` for physical fencing (boot never signals processes itself).
`READY` means the recovery pass completed, **not** that every contradiction
is resolved (`fully_recovered` is reported separately). Boot only *observes*
completion/checkpoint integrity; it never repairs or advances pointers.

---

## 4. Work intake

### 4.1 `exec/scheduler.py` — admission + atomic claim + dispatch (R10)

The scheduler is deliberately narrow: it never creates or deletes jobs,
never reconciles, never moves breakers, never reclaims leases, never
completes work. One `evaluate_once()` pass does three things:

1. **Claim-liveness refresh** of its own beats (a live scheduler refreshes
   every pass, so a racing scheduler never mistakes its fresh claims for
   dead orphans).
2. **Orphan recovery:** CLAIMED jobs owned by a `sched-*` worker with a live
   lease and no unreaped `worker.proc_spawned` evidence are **re-dispatched
   under the SAME claim identity** (`expect_token` carries the durable
   token) — never re-claimed. Expired-lease orphans are skipped (R4
   observes, R1 recycles; the scheduler does not touch them). Orphans whose
   owner still beats are skipped (a live scheduler mid-dispatch is never
   stolen).
3. **PENDING admission, `ORDER BY created_at, job_id`** (the repo has no
   canonical job priority). Per candidate: skip jobs under an open recovery
   incident (R8 owns those) or under a `PAUSED_FOR_HUMAN` task (I-17);
   mint a fresh `sched-<id>-<uuid>` worker identity; run the advisory
   breaker preflight (`_breaker_denies`: any OPEN or unreadable scope in
   GLOBAL → TASK → DESIRED → JOB denies); then
   `gate.claim_job_resilient()` — which revalidates the breaker **authoritatively
   inside the claim transaction**, so a stale "allow" is corrected
   fail-closed and a stale "deny" merely defers. `False` = lost race or
   full capacity → backpressure: PENDING stays PENDING, no failure marking,
   no escalation.

Dispatch failures leave the claim **standing** (lease expiry → R4 → R1
recycles it); the scheduler never rolls back and never invents repair.

### 4.2 `exec/reconciler.py` — desired-state convergence (R11)

The narrowest authority in the system: it never creates jobs except through
the gate's single idempotent `ensure_job_for_desired_state()` with the
literal actor `"reconciler"`; it never deletes, never moves a job between
states, never completes/fails/quarantines anything, and never calls into
the scheduler, policy, or recovery layers. One `reconcile()` pass pins the
desired-state head version, verifies the head's snapshot hash over *all*
items (retired included), then walks items in sorted `desired_work_id`
order in fixed batches, re-pinning the version before every batch (a move
mid-pass stops creation and closes the run `CONFLICT`; an internally
contradictory desired state refuses with zero mutation — not even a run
row). Only `DESIRED_MISSING` with an existing, non-paused task creates a
job; every other diff verdict is recorded as a discrepancy and nothing else
happens. Retired items are tombstones (`OBSOLETE`), never cancellations —
`PENDING` has no legal cancel edge in `JOB_TRANSITIONS`.

---

## 5. Observation and recovery

### 5.1 `exec/watchdog.py` — detect and classify only (R7)

Requires runtime readiness (`READY`) before evaluation. It **never reclaims,
fences, schedules, or completes work**. Verdicts `HEALTHY` / `STALLED` /
`DEAD`; verdict identity is `(job_id, fencing_token)`; `DEAD` is terminal
for that execution identity. Fresh heartbeats do not mask stale durable
progress; lease expiry is classified `STALLED`, never reclaimed. `DEAD`
requires definitive evidence: a terminal worker row, a dead/reused process
identity, or durable latest-generation `worker.proc_reaped`. Verdict
transitions persist via `TransitionGate.record_watchdog_verdict()`; same-
verdict and post-`DEAD` re-evaluations are zero-mutation no-ops.

### 5.2 `exec/recovery.py` — attempt execution and verification (R8)

Requires boot `READY`. Consumes R7 verdicts and R4 expiry evidence; it does
not redefine either. It uses **R1 `reclaim_lease()`** for lease revocation
and **`supervisor.fence_sweep()`** for physical fencing — it never signals
processes itself. Actions: `reclaim`, `fence`, `restart`. Autonomous
selection: owned failed execution → `reclaim`; ownerless job with a
lingering stale process in an existing incident → `fence`; ownerless with no
lingering process → no action. `restart` is exposed via `dispatch_restart()`
for R9/tests and is **not** autonomously selected by the R8 evaluation loop.
Canonical rung mapping: `reclaim` → rung 2 `"re-claim/requeue"`,
`fence`/`restart` → rung 3 `"replace/reassign"`. Every attempt binds
`(job_id, fencing_token)`; a changed token aborts stale attempts unless
durable dispatch evidence proves the attempt caused the change. Recovery
succeeds only on **positive authoritative progress** (I-18); heartbeats
never count. R8's fixed rule escalates after two consecutive zero-progress
attempts to `"r9-policy"`, and records budget context as deferred to R9 —
R8 performs no budget arithmetic.

### 5.3 `exec/policy.py` — ladder, budgets, escalation (R9)

R9 *chooses* the rung, *bounds* recovery, and *decides* terminal escalation;
it never executes or verifies a recovery action. It invokes R8 only through
`RecoveryController.evaluate()` and `RecoveryController.dispatch_restart()`.
The canonical 5-rung ladder (§E.1): 1 "Retry with backoff" (R8 reclaim),
2 "Restart worker" (D1's full sequence is reclaim → terminate → provision
fresh → re-claim; R8's restart is the provision step), 3 "Replace /
reassign" (R8 fence and/or restart), 4 "Widen scope" (**no R9 executor** —
stage-widening belongs to the R10+ scheduler/reconciler; the rung is still
traversed durably and monotonically), 5 "Replan / pause" (terminal:
escalate to human and enter the human-gated `PAUSED_FOR_HUMAN` task state
via `TransitionGate`, sticky under I-17, never auto-cleared). Budgets:
per-rung caps 1≤2, 2≤2, 3≤3, 4=0, 5=0 and one per-incident cap (default 7);
no time budget exists in the contract, so none is invented. Budget
consumption is exactly-once via a durable watermark
(`consumed_attempt_number`) in a single CAS UPDATE guarded by
`remaining_budget` bounds. `select_rung()` is a pure deterministic function
of durable state; it never rewinds (returned rung ≥ current rung). Every
policy transition is one `write_txn` with `version=version+1 WHERE version=?`;
unknown/ambiguous durable state fails closed (terminal block / manual
escalation).

### 5.4 `exec/resilience.py` — circuit breakers (R12)

The controller is the reader/evaluator half of the breaker: it owns no
execution authority and writes only through the gate's breaker API. One
`evaluate_once()` pass: (1) boot gate; (2) evidence scan — watchdog `DEAD`
verdicts, terminal-`FAILED` recovery attempts, and escalated incidents become
idempotent breaker signals (deduped by the gate) on JOB / TASK / GLOBAL
scopes, plus a GLOBAL recovery-pressure signal while the recovery subsystem
is under load; (3) threshold — a CLOSED breaker whose windowed failure count
reaches its threshold opens (CAS; loser re-reads); (4) cooldown — an OPEN
breaker past `cooldown_until` moves to HALF_OPEN (probe allocation is the
gate's job, inside the claim transaction, as probe id
`"<worker_id>:<job_id>"`); (5) probe evaluation from **durable evidence
only** — job `COMPLETE` (R5 commit) or strictly positive `progress_done`
delta over the probe baseline closes the breaker; counted failure after the
probe reopens it; heartbeats and process existence are never consulted.
`OPEN` never transitions directly to `CLOSED` — only HALF_OPEN probe success
closes a breaker; corrupt probe state fails closed (reopen). The
scheduler's claim path is where breakers bite: `claim_job_resilient()`
denies admission into OPEN scopes inside the transaction.

---

## 6. Release finalization (`exec/finalizer.py`, R13)

The finalizer is the reader/evaluator half of finalization: it owns no
execution authority and never mutates jobs, tasks, leases, workers,
processes, artifacts, checkpoints, budgets, rungs, desired state, or
breakers. One `evaluate_once()` pass: (1) boot gate (no finalization before
`READY`); (2) begin — the desired-state head's generation gets its
finalization run (idempotent re-begin); (3) evaluate — every non-finalized
run is re-evaluated from durable evidence with CAS on version (loser
re-reads, never overwrites); (4) publish — a `READY` run is published: the
gate re-validates everything authoritatively inside **one** `write_txn` and
flips it to `FINALIZED` atomically, or refuses. No-op discipline: a pass
over an already-`FINALIZED` generation performs zero writes. The finalizer
never clears a human gate, never completes a job, never moves a breaker.

---

## 7. Actual dependency and call map

### 7.1 Module imports (compile-time)

```
exec/supervisor.py  → axos.store (TransitionGate, open_store, migrate)
exec/worker.py      → axos.store (gate API), exec.identity, exec.synthetic
exec/synthetic.py   → axos.store (TransitionGate, LeaseError), exec.identity
exec/boot.py        → axos.store (TransitionGate)
exec/scheduler.py   → axos.store (TransitionGate, open_store, migrate)
exec/reconciler.py  → axos.store (TransitionGate, open_store, migrate)
exec/watchdog.py    → axos.store (TransitionGate, read-only methods)
exec/recovery.py    → axos.store (TransitionGate); receives a supervisor object
exec/policy.py      → axos.store (…); .recovery (RecoveryController);
                      .boot (_classify_spawn); composes both, reimplements neither
exec/resilience.py  → axos.store (TransitionGate, breaker API)
exec/finalizer.py   → axos.store (TransitionGate, finalization API)
store/*             → imports nothing from exec/
```

### 7.2 Runtime call flow (corrected, not idealized)

```
INTAKE
reconciler.reconcile() ──► gate.ensure_job_for_desired_state()      [PENDING job]
scheduler.evaluate_once() ──► gate.claim_job_resilient()            [PENDING→CLAIMED,
                              │            atomic: capacity + breaker + claim + beat
                              ▼
                    supervisor.start_worker(worker_id, job_id, expect_token)
                              │
                              ▼  subprocess: python -m axos.exec.worker
EXECUTION
worker: claim_job() [or --expect-token verify] ──► transition_job(RUNNING)
        ──► SyntheticExecutor: ingest_heartbeat() / update_job_progress()
        ──► renewal thread: renew_lease()   (own store connection)
        ──► SUCCESS: stage_artifact → begin_commit → verify_artifact → commit_artifact [COMPLETE]
        ──► FAILURE: fail_job_execution()   [FAILED]

OBSERVATION → RECOVERY
watchdog.evaluate() ──► gate.record_watchdog_verdict()   [detect only; no authority]
recovery.evaluate()  ◄── R7 verdicts, R4 expiry evidence
        ├─► gate.reclaim_lease()            [R1 lease revocation]
        ├─► supervisor.fence_sweep()        [physical fencing; never signals itself]
        └─► supervisor.start_worker()       [restart via dispatch_restart, R9/tests]
policy.evaluate()  ──► reconcile (durable R8 evidence) ──► rc.evaluate()
        ──► rc.dispatch_restart()  ──► reconcile     [ladder/budgets/escalation only]
resilience.evaluate_once() ──► gate.record_breaker_signal() / transition_breaker()
        [breakers bite inside gate.claim_job_resilient(), not in the controller]
finalizer.evaluate_once() ──► gate.begin/evaluate/publish_finalization()
boot_recover() ──► gate.observe_expired_leases() → gate.reclaim_lease() [R4→R1]
               ──► supervisor.fence_sweep()        [never signals itself]
```

### 7.3 What the corrected map changes versus the idealized picture

- The scheduler does **not** talk to workers and workers do **not** talk to
  the scheduler. The scheduler's only worker-facing call is
  `supervisor.start_worker()`; the worker's only store-facing path is
  `TransitionGate` methods. The scheduler never sees a worker's heartbeats.
- The worker never calls the supervisor, the watchdog, recovery, or policy.
  Its recovery-related input is purely the gate refusing its next
  call (`LeaseError` → `FencedError` → stop).
- The watchdog has **no arrow into recovery**: it persists verdicts; the R8
  controller reads them. There is no callback, queue, or signal from
  watchdog to recovery in the source.
- The resilience controller has **no arrow into the claim path**: it moves
  breaker rows; the gate's `claim_job_resilient()` enforces them
  transactionally. The controller cannot deny a claim directly.
- Policy (R9) has **no arrow into leases, processes, or fencing tokens**: it
  reaches execution only through `RecoveryController.evaluate()` and
  `dispatch_restart()`, and reaches the store only through the gate's
  policy/recovery APIs.
- Boot has **no arrow into processes**: it observes and reclaims through the
  gate and delegates physical fencing to the supervisor's existing sweep.
- The reconciler has **no arrow into the scheduler**: it creates PENDING
  jobs; the scheduler independently admits them. Coordination is through
  durable rows only.

---

## 8. Authority boundary (F1) — who may write what

| Component | May write | May never write |
|-----------|-----------|-----------------|
| `TransitionGate` | everything, transactionally | — (the bottleneck) |
| Supervisor | ledger process events; worker rows (transitions) | job authority |
| Worker | heartbeats, progress, artifacts, failure — only under its lease triple | anything without owner/token/live-lease |
| Scheduler | claims, scheduler beats | jobs' states, leases, breakers, recovery |
| Reconciler | desired jobs (one op), run bookkeeping | any transition, any execution state |
| Watchdog | verdicts | anything authoritative |
| Recovery (R8) | reclaims, attempts, incidents | budgets, rungs, completion |
| Policy (R9) | policy rows, rung, escalation, terminal task pause | execution, verification, fencing tokens |
| Resilience (R12) | breaker rows | jobs, leases, recovery |
| Finalizer (R13) | finalization runs, R5 release checkpoint | everything in §6 |

---

## 9. Evidence gaps and things deliberately not verified

- **Runtime behavior was not executed.** All claims are static-source claims;
  no component was run, so liveness properties (e.g. "the renewal thread
  keeps a live worker unfenced") are documented from the code's stated
  intent, not observed.
- **`_ATTEMPT_OPEN` gate internals** beyond what `resilience.py` asserts:
  the resilience controller asserts at runtime that
  `gate._ATTEMPT_OPEN == ("CREATED", "RUNNING", "VERIFYING", "UNCERTAIN")`.
  `policy.py` independently defines `_R8_OPEN_STATES =
  {"CREATED","RUNNING","VERIFYING","UNCERTAIN"}` and `_R8_RESULT_STATES =
  {"SUCCEEDED","FAILED","BLOCKED"}`; the summary records gate attempt states
  `CREATED, RUNNING, VERIFYING, SUCCEEDED, FAILED, BLOCKED, UNCERTAIN`.
- **Recovery (`exec/recovery.py`) internals** (attempt driver, autonomous
  action selection, `RungContext`, `RungProvider`, `CanonicalRungProvider`)
  were read in the prior session and are summarized here; the per-function
  line references for that module are not re-verified in this pass.
- **Reconciler `diff_item` lists `"VERIFYING"`** among job statuses mapped to
  `DESIRED_ACTIVE`, but `VERIFYING` is not a legal job status in
  `JOB_TRANSITIONS` (`store/transitions.py`). Observed discrepancy; the
  practical effect is nil (no job row can carry that status), but the table
  is wider than the graph.
- **`audit/` and `tests/`** were not re-read in this pass except through the
  release manifest's file hashes; claims about test coverage come from the
  manifest and prior session.
- The ledger's tamper-evidence rests on a hash chain inside the same
  database it protects; there is no external anchoring. This is a known,
  recorded limitation, not a defect found here.
