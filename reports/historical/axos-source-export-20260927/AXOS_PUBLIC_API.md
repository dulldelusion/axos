# AXOS Public API — workload-facing interfaces (as-is, read-only export)

Source: `~/workspace/axos/`, inspected 2026-09-28. Signatures and docstrings
copied from the actual source via AST; behaviors described are what the code
does. "Workload" here = external code that wants to declare work, execute it,
and consume results through AXOS — the surfaces in this document are the ones
a workload would actually touch.

Conventions used below:
- **Caller responsibilities** = what the caller must get right (the gate
  rejects otherwise, rolling the transaction back with state unchanged).
- **AXOS responsibilities** = what the gate guarantees once the call is
  accepted.

## 0. Wiring a workload: `store/db.py` handles

### `open_store(path, clock=None) -> Store`
- **Location:** `store/db.py`, re-exported by `store/__init__.py`.
- **Purpose:** Open (creating if needed) the authoritative store — the
  WRITABLE capability.
- **Caller responsibilities:** Hand the returned `Store` ONLY to
  `TransitionGate` (or transient migration/setup code). Never hand it to a
  worker, scheduler, or any non-gate component.
- **AXOS responsibilities:** Verified pragmas (WAL enforced, synchronous=
  NORMAL, foreign_keys=ON, busy_timeout=5000); `Store.conn` is read-only
  (`PRAGMA query_only=ON` — writes raise `sqlite3.OperationalError`);
  monotonic store clock persisted in `axos_meta.last_commit_ts`.

### `open_readonly_store(path) -> ReadOnlyStore`
- **Location:** `store/db.py`, re-exported by `store/__init__.py`.
- **Purpose:** The store interface for every non-gate component: `execute(sql,
  params)` for read queries, `path`, `close()`. No writable connection, no
  transaction capability; cannot be escalated.

### `migrate(store)` / `applied_versions(store)`
- **Location:** `store/migrations.py`, re-exported by `store/__init__.py`.
- **Purpose:** Apply pending versioned migrations (1–12) in order; re-running
  is a no-op; failures roll back fully and record nothing.

### Error surface (all in `store/db.py`, re-exported by `store/__init__.py`)
`StoreError` (base) → `TransitionRejected` (refused transition/creation),
`LeaseError` (lease conflict, stale token, not owner), `MigrationError`,
`PolicyConflict` (R9 CAS lost), `DesiredStateConflict` (R11 identity
collision), `BreakerConflict` (R12 CAS lost), `FinalizationConflict` (R13 CAS
lost). Plus `StorageEngineError = sqlite3.Error` (in `store/db.py` only, not
re-exported — for fail-closed classification when the DB itself is
unreadable).

## 1. `TransitionGate` — `store/gate.py`

Constructed as `TransitionGate(store, staging_root=None)`; `staging_root`
defaults to `<db-dir>/axos-staging/` so every gate over the same DB resolves
the identical staging root. One `Store` connection must not be shared across
threads — worker code builds one gate per thread.

### 1.1 Task & job lifecycle

#### `create_task(task_id: str | None, objective: dict, budgets: dict, actor: str) -> dict`
- **Purpose:** Declare a unit of work.
- **State transition:** creates task in `PROPOSED`.
- **Inputs:** `task_id` (None = gate mints one), `objective` (dict),
  `budgets` (dict), `actor` (must be in `WORK_CREATOR_ACTORS`; anything
  starting `worker:` or any other string is rejected — I-16).
- **Outputs:** the created task row (dict).
- **Invariants:** actor check before any mutation.
- **Caller responsibilities:** supply a non-worker actor; `objective`/`budgets`
  as dicts.
- **AXOS responsibilities:** stamps authoritative store time; appends ledger
  event; task starts `PROPOSED`.

#### `transition_task(task_id, to_state, actor, reason=None, approval_ref=None, pause_reason=None, pause_diagnostic=None) -> dict`
- **Purpose:** Move a task along the task graph.
- **State transitions:** exactly `TASK_TRANSITIONS` (PROPOSED→AUTHORIZED/
  REJECTED; AUTHORIZED→PLANNED/PAUSED_FOR_HUMAN; PLANNED→EXECUTING;
  EXECUTING→VERIFYING/PAUSED_FOR_HUMAN/FAILED/CANCELLED;
  VERIFYING→FINALIZED/PAUSED_FOR_HUMAN; BLOCKED→EXECUTING;
  PAUSED_FOR_HUMAN→EXECUTING/CANCELLED; REJECTED/FAILED/CANCELLED/FINALIZED
  terminal).
- **Invariants:** unlisted transitions raise `TransitionRejected` with zero
  mutation. I-17: `PAUSED_FOR_HUMAN` cannot be exited without a recorded human
  `APPROVED` approval (`approval_ref` naming a genuine approval) — sticky
  across restarts.
- **Caller responsibilities:** name a listed transition; to resume from
  `PAUSED_FOR_HUMAN`, present the `APPROVED` approval.
- **AXOS responsibilities:** atomic transition + ledger event, stamped with
  store time.

#### `create_job(job_id: str | None, task_id: str, stage_id: str | None, actor: str, max_attempts: int = 3, policy: dict | None = None) -> dict`
- **Purpose:** Declare a unit of execution under a task.
- **State transition:** creates job in `PENDING`.
- **Inputs:** `job_id` (None = minted), `task_id` (must exist), `stage_id`,
  `actor` (I-16 actor check as for tasks), `max_attempts`, `policy` dict.
- **Outputs:** created job row (dict).
- **Caller responsibilities:** valid task, scheduler-class actor.
- **AXOS responsibilities:** job starts `PENDING`, no lease.

#### `transition_job(job_id, to_state, actor, reason=None) -> dict`
- **Purpose:** Move a job along the job graph.
- **State transitions:** exactly `JOB_TRANSITIONS` (PENDING→CLAIMED;
  CLAIMED→RUNNING/PENDING/FAILED; RUNNING→COMMITTING/PENDING/UNCERTAIN/
  FAILED; COMMITTING→COMPLETE/UNCERTAIN/FAILED; UNCERTAIN→COMPLETE/PENDING/
  QUARANTINED; FAILED→PENDING/QUARANTINED/BLOCKED; QUARANTINED→PENDING;
  BLOCKED→PENDING; COMPLETE terminal).

### 1.2 Leases — the claim/renew/release contract

The lease is the triple `(owner_worker_id, lease_expires_at, fencing_token)`
on the job row, mutated only by atomic conditional updates; all lease
timestamps are store time.

#### `claim_job(job_id, worker_id, ttl_s, actor, worker_reported_ts=None) -> bool`
- **Purpose:** PENDING → CLAIMED with a lease triple.
- **Inputs:** job id, worker id, `ttl_s` (F2: must be a positive number —
  bool/NaN/zero/negative raise `TransitionRejected` BEFORE the transaction
  opens, so stillborn leases are impossible), `actor`,
  `worker_reported_ts` (informational metadata only).
- **Outputs:** `True` on success; `False` if the job was not claimable
  (already claimed, conflicting lease, wrong state) — no exception on the
  lost race.
- **State transition:** PENDING → CLAIMED; sets owner + `lease_expires_at =
  acquired_at + ttl_s` + fencing token.
- **Invariants:** atomic — exactly one claimant wins under concurrency;
  TTL validated pre-transaction.
- **Caller responsibilities:** pass a positive TTL; treat `False` as "lost the
  race, do not execute".
- **AXOS responsibilities:** atomic conditional claim; ledger event.

#### `claim_job_bounded(job_id, worker_id, ttl_s, actor, max_concurrent_jobs, scheduler_id=None) -> bool`
- **Purpose:** PENDING → CLAIMED with R10 admission control (max concurrent
  jobs per scheduler) on top of `claim_job` semantics.

#### `claim_job_resilient(job_id, worker_id, ttl_s, actor, max_concurrent_jobs, scheduler_id=None, *, probe_limit=1) -> bool`
- **Purpose:** R12 circuit-breaker-aware claim: evaluates breaker scopes
  (GLOBAL → TASK → DESIRED → JOB); an OPEN breaker denies the claim
  fail-closed (an OPEN row with elapsed cooldown still denies until the
  resilience controller transitions it).

#### `renew_lease(job_id, worker_id, fencing_token, ttl_s, actor) -> bool`
- **Purpose:** Extend the lease.
- **Inputs:** must name the CURRENT owner and valid fencing token; `ttl_s`
  re-validated by F2.
- **Outputs:** `False` instead of raising on conflict/staleness.
- **Invariants:** only the live owner with a live lease can renew; renewal
  moves `lease_expires_at` only while the lease is still live.
- **Caller responsibilities:** renew on a daemon thread well inside the TTL;
  stop executing immediately on `False`/`LeaseError` (fenced — only a fresh
  claim restores authority).
- **AXOS responsibilities:** atomic conditional update.

#### `release_lease(job_id, worker_id, fencing_token, actor) -> bool`
- **Purpose:** Voluntary release by the owner (valid token). Clears the
  triple. Returns False on mismatch.

#### `observe_expired_leases() -> list[dict]`
- **Purpose:** Deterministic, authoritative expiry observation (R4).
- **Outputs:** one evidence record per job whose durable lease actually
  expired: `job_id, owner_worker_id, fencing_token, status,
  lease_acquired_at, lease_expires_at, observed_at, predicate, reason`.
- **Invariants:** SELECT-only (runs on the gate's read-only connection —
  idempotent by construction; mutates nothing, bumps no token, appends no
  events). Advisory evidence, NOT authorization to mutate. Expiry predicate:
  owner exists AND lease granted AND `lease_expires_at <= store_now` AND
  status in (CLAIMED, RUNNING, COMMITTING). Never reports PENDING/terminal/
  BLOCKED/QUARANTINED/UNCERTAIN or ownerless jobs.
- **Caller responsibilities:** never cache owner/token across a mutation
  boundary; hand evidence to `reclaim_lease`, which revalidates atomically.
- **AXOS responsibilities:** evaluation against the current durable row with
  the authoritative store clock; can only under-report relative to R1, never
  over-report (a lease observed expired can never be seen as live by
  `reclaim_lease` unless a renewal committed in between — itself durable
  state).

#### `reclaim_lease(job_id, *, actor, reason, expected_owner, expected_token, force=False, verdict=None, incident_id=None) -> dict`
- **Purpose:** Atomically revoke a lease: verify → bump fencing token →
  clear owner → transition job → ledger, in ONE transaction.
- **Inputs:** caller must be in `RECLAIM_ACTORS`
  (recovery-controller/reconciler/system/operator/test — supervisor
  deliberately absent); must name the exact lease (`expected_owner`,
  `expected_token`) — compare-and-swap on the lease triple.
- **State transitions:** CLAIMED → PENDING, RUNNING → UNCERTAIN,
  COMMITTING → UNCERTAIN. Any other state (terminal, PENDING, UNCERTAIN…)
  or ownerless job → `TransitionRejected`, zero mutation.
- **Invariants:** routine reclaim (`force=False`) requires the lease expired
  per store time; forced reclaim requires a watchdog verdict (DEAD/STALLED)
  + incident identity. Token N → N+1 and owner → NULL commit atomically with
  the state transition and ledger events, so the old owner cannot commit,
  renew, heartbeat, or update progress with the old token afterwards.
  Reclaim revokes a lease; it never invents one.
- **Caller responsibilities:** re-observe (or accept `LeaseError` on stale
  evidence); name the exact lease; supply a real reason and, for forced
  reclaim, the verdict + incident evidence (R1 validates evidence shape; it
  produces no verdicts and implements no recovery policy).
- **AXOS responsibilities:** atomic verify-and-revoke; every rejection rolls
  the transaction back — no partial state, no half-reclaimed lease.

### 1.3 Progress & heartbeats

#### `update_job_progress(job_id, worker_id, fencing_token, progress_done, progress_total, actor) -> dict`
- **Purpose:** Record progress. Informational only — never completion evidence.
- **Invariants:** only the current lease owner with a valid fencing token,
  and only while the job is actively executing (CLAIMED/RUNNING/COMMITTING).
  F3: terminal jobs (COMPLETE/FAILED/QUARANTINED/…) reject progress mutations
  — terminal state must not contradict itself; rejection leaves the row
  byte-identical. R3: stamps `jobs.progress_updated_at` with store time.
- **Caller responsibilities:** valid token, active state.
- **AXOS responsibilities:** atomic check-and-stamp.

#### `ingest_heartbeat(worker_id, proc_id, job_id, fencing_token, hb_seq, worker_state, current_operation, actor, worker_reported_ts=None, progress_done=..., progress_total=...) -> dict`
- **Purpose:** Durable heartbeat ingestion (R3/D9). The heartbeat is a
  time-series evidence record, NOT authority.
- **Invariants:** `ts` stamped with store time on ingest; worker-supplied
  timestamp is informational only. `hb_seq` must strictly increase per
  (worker_id, proc_id) — duplicates/out-of-order rejected WITHOUT mutation.
  Heartbeats from stale/fenced workers rejected: the heartbeat's fencing
  token must equal the job's current token and the worker must be the current
  owner — a fencing rejection journals a `worker.heartbeat_fenced` milestone
  and raises `LeaseError`. A heartbeat never extends a lease, never renews
  authority, never reclaims, never bumps the fencing token, is never evidence
  of job success. No ledger event per accepted heartbeat (summaries and
  milestones only). Optional progress is routed to the same
  `_apply_progress` path as `update_job_progress` — identical checks, one
  atomic transaction.
- **Caller responsibilities:** monotonic `hb_seq`; current fencing token;
  never rely on a heartbeat for authority.
- **AXOS responsibilities:** evidence record with store time; fencing
  enforcement.

### 1.4 R5 artifact pipeline — the only completion path

There is no artifact-free success path. Completion is:
`stage_artifact` (STAGING) → `begin_commit` (RUNNING→COMMITTING) →
`verify_artifact` (STAGING→VALIDATED) → `commit_artifact` (→COMPLETE).
On failure: `fail_job_execution` (→FAILED).

#### `stage_artifact(*, job_id, worker_id, fencing_token, task_id, kind, data, actor, attempt=None, claimed_hash=None, producer=None) -> dict`
- **Purpose:** Stage worker-produced bytes as a content-addressed artifact.
- **Inputs:** `data: bytes` (required); `claimed_hash` is informational — the
  GATE computes `sha256(data)` itself and rejects on mismatch.
- **State transition:** creates artifact row in `STAGING` (idempotent:
  re-staging identical bytes returns the existing row via INSERT … ON
  CONFLICT DO NOTHING — the same bytes always produce the same
  `artifact_id = sha256(bytes)`).
- **Invariants:** requires the job RUNNING under the caller's live lease
  (owner + token + unexpired, checked inside the transaction). Bytes written
  with file+directory fsync before the row is registered. R14: per-job
  staging provenance recorded (`artifact_stagings`) — two jobs producing
  identical bytes share one row, each with its own provenance.
- **Caller responsibilities:** live lease; real bytes; fencing token.
- **AXOS responsibilities:** gate-computed content hash; fsync'd durable
  bytes; staging is NOT verification — a staged artifact alone never
  completes a job.

#### `get_artifact(artifact_id) -> dict`
- **Purpose:** Read an artifact row (read-only connection).

#### `begin_commit(job_id, worker_id, fencing_token, *, artifact_id, actor) -> dict`
- **Purpose:** RUNNING → COMMITTING: open the fenced commit phase.
- **Invariants:** asserts the caller still holds the live lease (fencing
  identity checked before state) and that the artifact was staged for THIS
  job with bytes present (R14 provenance: `_require_staging_provenance`).
  The job is still not COMPLETE — verification and the atomic commit follow.
- **Caller responsibilities:** live lease; artifact staged by this job.

#### `verify_artifact(artifact_id, *, actor, worker_id=None, fencing_token=None, job_id=None, required_validators=None) -> dict`
- **Purpose:** Authoritative verification: STAGING → VALIDATED.
- **Invariants:** THE GATE performs verification — re-reads staged bytes,
  recomputes the hash, runs structural checks, records gate-computed
  validator receipts. Nobody can set VERIFIED by assertion: no setter, only
  this predicate. `required_validators` defaults to `REQUIRED_VALIDATORS =
  (("axos-structural","1"),)`; every required validator must hold a PASS
  receipt bound to the EXACT content hash. Fencing: a worker-owned actor must
  name its token and hold the live lease; authority actors
  (supervisor/system/test/…) verify without a lease. Corrupt bytes →
  STAGING→QUARANTINED (retained for forensics, never silently deleted) and
  raise; quarantine recorded in a second, forensic transaction only if the
  bytes still fail on re-read.
- **Caller responsibilities:** for worker path, current token + live lease.
- **AXOS responsibilities:** gate-computed receipts; forensic quarantine on
  corruption.

#### `commit_artifact(job_id, worker_id, fencing_token, *, artifact_id, actor, evidence=None) -> dict`
- **Purpose:** THE authoritative completion transaction (R5/I-4). One
  `write_txn`: re-read job, artifact, verification receipts; verify
  owner/token/lease, artifact↔job linkage, and the full VERIFIED predicate
  recomputed against the bytes on disk right now; then write job COMPLETE +
  `job.result_artifact_id` + `job.content_hash` + completion ledger event,
  atomically.
- **Two entry paths, one contract:**
  - Worker path: job in COMMITTING; caller holds the live lease. Fencing
    checked BEFORE any terminal handling — a stale token is rejected even
    against an already-COMPLETE job; a fenced worker never earns a success
    signal.
  - Resolver path: job in UNCERTAIN (post-reclaim); actor in
    `RECLAIM_ACTORS`; the artifact's recorded fencing token must predate the
    reclaim (token lineage), proving the bytes were produced under the
    superseded epoch.
- **Invariants:** idempotent — repeating the commit for an already-COMPLETE
  job with the SAME verified artifact is a no-op (no duplicate ledger
  event); a different artifact is a contradiction and is rejected. There is
  never durable state where the job says COMPLETE but the referenced
  verified artifact is missing or its required verification evidence is
  absent.
- **Caller responsibilities:** commit only the artifact this job staged and
  that passed `verify_artifact`; hold the live lease (worker path).
- **AXOS responsibilities:** atomicity; no COMPLETE without verified
  artifact; no duplicate completion events.

#### `fail_job_execution(job_id, worker_id, fencing_token, *, actor, reason, evidence=None) -> dict`
- **Purpose:** (CLAIMED/RUNNING/COMMITTING) → FAILED. The failure path —
  FAILED is not COMPLETE and never references an artifact.
- **Invariants:** fencing (owner/token/live lease) enforced identically;
  idempotent (repeating for an already-FAILED job is a no-op); failing a
  COMPLETE job is a contradiction and is rejected.

### 1.5 Checkpoints (R5)

#### `stage_checkpoint(task_id, *, actor, manifest, trigger, stage_id=None, ledger_tip_seq=None, supersedes=None, versions=None) -> dict`
- **Purpose:** Stage a checkpoint candidate with deterministic identity.
- **State transition:** creates checkpoint row in `UNVERIFIED`.

#### `verify_checkpoint(checkpoint_id, *, actor, release=False) -> dict`
- **Purpose:** UNVERIFIED → VERIFYING → VERIFIED (or CORRUPT).
- **Invariants:** VERIFIED requires a receipt with full manifest + ledger-chain
  verification — no receipt, no trust.

#### `invalidate_checkpoint(checkpoint_id, *, actor, evidence) -> dict`
- **Purpose:** Invalidate a VERIFIED checkpoint whose manifest/artifact bytes
  no longer verify (→ CORRUPT lineage handling).

#### `latest_known_good(task_id) -> dict | None`
- **Purpose:** Read-only: the latest verified checkpoint safe to resume from.

### 1.6 Workers, approvals, ledger

#### `create_worker(worker_id: str | None, actor: str, task_id=None, capabilities=None) -> dict`
- **Purpose:** Register a worker identity (`PROVISIONING`).

#### `transition_worker(worker_id, to_state, actor) -> dict`
- **State transitions:** exactly `WORKER_TRANSITIONS`
  (PROVISIONING→IDLE/DEAD; IDLE→ASSIGNED/DRAINING; ASSIGNED→RUNNING/SUSPECT;
  RUNNING→IDLE/DRAINING/SUSPECT; SUSPECT→RUNNING/DEAD; DRAINING→RETIRED;
  DEAD/RETIRED terminal).

#### `create_approval(approval_id, task_id, reason, requested_action, actor, job_id=None, stage_id=None, evidence_refs=None, risk=None, options=None, deadline=None, checkpoint_id=None, on_timeout=None) -> dict`
- **Purpose:** Create an approval request (`PENDING`). Used for human gates
  (I-17/ADR-005).

#### `decide_approval(approval_id, decision, decided_by, actor, decision_reason=None) -> dict`
- **Purpose:** PENDING → APPROVED / DENIED / EXPIRED.
- **Invariants:** only a genuine recorded `APPROVED` decision releases
  `PAUSED_FOR_HUMAN`.

#### `append_event(event_type: str, payload: dict, actor: str) -> int`
- **Purpose:** Append a hash-chained ledger event. Chain: `hash =
  sha256(prev_hash + canonical_json(seq, type, payload, actor, ts))`,
  genesis `prev_hash = "0"*64`. Raw heartbeats are never ledgered
  (summaries/milestones only — Phase 0 §20).
- **Outputs:** the event's sequence number.
- **Caller responsibilities:** payload must be JSON-canonicalizable.
- **AXOS responsibilities:** atomic append in the caller's transaction
  context (via `_append_event` inside `write_txn`).

#### `verify_ledger_chain() -> tuple[bool, str]`
- **Purpose:** Recompute the hash chain; returns `(ok, detail)`. Detects
  tamper/missing/reorder/duplicate/payload mutation. (Tamper-evident, not
  tamper-proof — a writer with raw DB access can rewrite the whole chain;
  this is the documented known limitation.)

### 1.7 R11 desired-state API (reconciler-facing)

#### `ensure_job_for_desired_state(*, desired_work_id, task_id, stage_id, max_attempts, policy, desired_version, actor="reconciler") -> tuple[dict, bool]`
- **Purpose:** The SINGLE idempotent op by which the reconciler materializes
  jobs for declared desired-state items.
- **Inputs:** `actor` defaults to the literal `"reconciler"`; the gate
  enforces `WORK_CREATOR_ACTORS`.
- **Outputs:** `(job_row, created: bool)`.
- **Invariants:** fail closed on identity collision — deterministic job_id
  from `_canonical_desired_job_id(desired_work_id)`; creation is idempotent
  ONLY when identity AND spec both match; on mismatch raises
  `DesiredStateConflict` and never adopts a foreign job, never deletes,
  never recreates.
- **Caller responsibilities:** only the reconciler calls this; contradictory
  actual state must be resolved by the operator.
- **AXOS responsibilities:** canonical identity; spec-hash comparison;
  idempotent creation.

Supporting read/write ops: `set_desired_item`, `retire_desired_item`,
`get_desired_head`, `get_desired_item`, `list_desired_items`,
`get_desired_job_map`, `get_desired_work_id_for_job`,
`begin_reconciliation_run` / `checkpoint_reconciliation_run` /
`finish_reconciliation_run` / `get_reconciliation_run` /
`open_reconciliation_runs`.

### 1.8 R12 breaker API (resilience-controller-facing)

`get_breaker_state`, `list_breaker_states`, `ensure_breaker_state`,
`record_breaker_signal`, `transition_breaker`, `claim_half_open_probe`,
`breaker_allows(scope_type, scope_id) -> (bool, reason)`. Only the resilience
controller transitions rows; the admission path (`claim_job_resilient`)
never moves a breaker itself. CAS losers raise `BreakerConflict` and must
re-read and reconcile.

### 1.9 R13 finalization API (finalizer-facing)

`begin_finalization_run(release_generation, desired_state_version, actor)`,
`evaluate_finalization(...)`, `publish_finalization(release_generation,
expected_version, expected_manifest_hash, actor)`, `get_finalization_run`,
`list_finalization_runs`. Module helpers: `canonical_release_generation()`,
`canonical_finalization_id(...)`, `build_finalization_manifest(...)`,
`finalization_manifest_hash(manifest)`. CAS losers raise
`FinalizationConflict`; a run that became FINALIZED under the loser returns
the verified finalized record idempotently instead of conflicting.

### 1.10 R7/R8/R9/R10 supporting API (controller-facing)

- Watchdog verdicts: `record_watchdog_verdict`, `latest_watchdog_verdict`,
  `watchdog_verdicts_for` (verdicts: HEALTHY/STALLED/DEAD).
- Recovery: `create_incident` / `set_incident_outcome` /
  `set_incident_escalated`; `find_or_create_recovery_incident`;
  `create_recovery_attempt` / `claim_recovery_attempt` /
  `transition_recovery_attempt` / `complete_recovery_attempt` /
  `mark_recovery_attempt_uncertain` / `record_recovery_attempt` /
  `recovery_attempts_for` / `open_recovery_incidents` /
  `open_recovery_attempts` / `get_recovery_incident` /
  `get_recovery_attempt` / `set_attempt_verify_after` /
  `note_attempt_dispatch` / `set_attempt_budget_context` /
  `escalated_recovery_incidents` / `recovery_incidents_with_open_policy`.
- R9 policy: `get_recovery_policy`, `ensure_recovery_policy`,
  `cas_update_recovery_policy` (CAS; loser gets `PolicyConflict`),
  `consume_policy_attempts`.
- Scheduler liveness: `refresh_scheduler_claim_beats`,
  `withdraw_scheduler_claim_beat`, `clear_scheduler_claim_beats`,
  `scheduler_claim_live`, `sched_claimed_orphans`.
- Fencing/process evidence: `fencing_ledger_for_job`, `unreaped_proc_spawns`,
  `latest_spawn_generation`, `progress_evidence_for_job`,
  `mark_worker_seen`, `heartbeats_for`, `owned_active_jobs`,
  `jobs_in_states`, `pending_jobs`, `get_task`, `get_job`, `get_worker`.

## 2. Execution entry points a workload/operator touches

### `exec/worker.py` — `python -m axos.exec.worker`
CLI: `--db PATH --worker-id W --proc-id P --job-id J --ttl-s 60
--behavior '{...}' [--hb-interval-s 0.5] [--no-renew] [--expect-token T]`.
Runs the claim → RUNNING → synthetic behavior (heartbeat + progress, daemon
lease-renewal thread) → R5 commit/fail protocol. Exit codes: 0 committed
SUCCESS; 10 committed FAILURE; 3 claim failed; 4 fenced mid-execution; 5
commit rejected; 6 fatal store error; 7 stale token (pre-claimed dispatch
mismatch — no claim, no transition, no execution); 143 SIGTERM. Exit codes
are process evidence only — NEVER job success. Note: `exec/worker.py` runs
the *synthetic* executor; a real workload would replace `SyntheticExecutor`
with its own executor but keep the identical gate protocol (claim → renew →
stage → begin_commit → verify → commit/fail), since the gate contract is
executor-agnostic.

### `exec/supervisor.py` — `Supervisor`
`start_worker(...)`, `stop_worker(...)`, `kill_worker(...)`, `reap()`,
`observe()`, `fence_sweep()`, `restart_worker(...)`, `reconstruct()`,
`close()` (+ context manager). Owns process lifecycle only — never job
authority; R2 fencing (process-group kill) on lease revocation. `_FenceRefused`
raised when a fence cannot be enforced.

### `exec/boot.py` — `boot_recover(supervisor, actor, boot_id) -> dict`
Idempotent boot-recovery pass; returns the boot report (`phase` ∈
STARTING/RECOVERING/READY/BLOCKED; per-record `disposition` ∈ ADOPT/FENCE/
ALREADY_DEAD/PID_REUSE/ORPHAN/UNCERTAIN). ADOPT only when process identity
matches AND durable job authority is current (owner, token lineage, live
lease, active state).

### Controllers (each: `<X>Config` dataclass + `<X>Controller` with `evaluate_once()` / `evaluate(...)`, `start()`, `_loop()`, `stop()`, `close()`)
- `exec/scheduler.py` — `SchedulerConfig`, `Scheduler.evaluate_once()`:
  admits PENDING jobs (skips BLOCKED and task-level PAUSED_FOR_HUMAN),
  atomic bounded claim, dispatches worker processes. Narrow by design.
- `exec/watchdog.py` — `WatchdogConfig`, `Watchdog.evaluate(...)`:
  detects/classifies STALLED/DEAD; never reclaims/fences/schedules.
- `exec/recovery.py` — `RecoveryConfig`, `RecoveryController.evaluate()`:
  consumes watchdog verdicts; creates incidents/attempts; dispatches the
  smallest authorized action (reclaim via gate, fence via supervisor,
  restart via scheduler path); verifies attempts against progress deltas;
  escalates to `r9-policy` after two consecutive non-success attempts.
  `dispatch_restart(...)` is the public restart entry.
- `exec/policy.py` — `PolicyConfig`, `PolicyController.evaluate()`:
  chooses the recovery rung from `CANONICAL_LADDER` (via `select_rung` /
  `PolicyRungProvider`), enforces durable per-rung/per-incident budgets via
  CAS (`PolicyConflict` on lost race), decides terminal escalation
  (`T_RECOVERY_COMPLETE`, `T_BUDGET_EXHAUSTED`, `T_R5_TERMINAL`,
  `T_BLOCKED_CONTRADICTORY`, `T_BLOCKED_STATE_UNAVAILABLE`, `T_SUPERSEDED`),
  converges human-gated pauses, blocks on corrupt policy (`PolicyCorrupt`).
- `exec/reconciler.py` — `ReconcilerConfig`, `Reconciler.reconcile(...)` /
  `evaluate_once()`: observe-and-converge desired→actual via the single
  `ensure_job_for_desired_state` op.
- `exec/resilience.py` — `ResilienceConfig`,
  `ResilienceController.evaluate_once()` + `resilience_allows(...)`:
  evaluates failure signals against thresholds/cooldowns, transitions
  breaker rows (CLOSED→OPEN→HALF_OPEN→CLOSED|OPEN), admits half-open probes.
- `exec/finalizer.py` — `FinalizationConfig`, `Finalizer.evaluate_once()`:
  evaluates release readiness blockers and publishes finalizations through
  the gate's finalization API. No execution authority whatsoever.

### `exec/identity.py`
`new_proc_id()`, dataclasses `WorkerIdentity`, `ProcessInstance`, `LeaseRef`
(`lease_id = (job_id, fencing_token)` — one lease epoch).

### `exec/synthetic.py`
`BehaviorSpec.from_dict(spec)` + `SyntheticExecutor(gate, spec, …).run()` —
the deterministic fault-injection executor the shipped worker uses; raises
`FencedError` on lost authority, `InterruptedExecution` on stop.

## 3. Invariants a workload author must internalize

1. **Completion = gate-verified artifact + COMPLETE job, atomically.**
   `commit_artifact` is the only path to COMPLETE; there is never durable
   state where the job says COMPLETE but the verified artifact is missing.
2. **The lease triple is the only authority.** Reads of job state are
   observations; only (owner, token, unexpired lease) authorizes mutation.
   After `LeaseError`/`False` from renew, stop — only a fresh claim restores
   authority.
3. **Worker timestamps are informational.** `worker_reported_ts` never affects
   expiry, ordering, or fencing. Store time is authoritative and monotonic.
4. **Fencing is checked before state.** A stale token is rejected even against
   an already-COMPLETE job — a fenced worker never earns a success signal.
5. **Heartbeats are evidence, not authority.** They never renew leases.
6. **Rejections are transactional.** Every `TransitionRejected`/`LeaseError`
   rolls back — no partial state, no half-reclaimed lease, no contradictory
   progress on terminal jobs (F3 leaves the row byte-identical).
7. **Idempotency where it matters:** re-claim is atomic (one winner);
   re-staging identical bytes returns the existing row; re-committing the
   same verified artifact to a COMPLETE job is a no-op; re-failing a FAILED
   job is a no-op; re-running `migrate()` is a no-op; `boot_recover` is
   idempotent.
8. **Humans gate explicitly.** `PAUSED_FOR_HUMAN` exits only via a recorded
   human `APPROVED` approval — across restarts, unconditionally (I-17).
9. **Workers never create work** (I-16); **the supervisor never reclaims
   leases** (RECLAIM_ACTORS excludes it by design).
