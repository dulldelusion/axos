# Data Flow

All authoritative state flows through one path: the gate writes, the
ledger records, controllers read. There are no side channels in the
implementation — `exec/` never calls `write_txn()`, never opens its
own `sqlite3` connections, never issues SQL writes (enforced by
`src/axos/audit/09_authority_audit.py`).

## The write path: the gate as the single narrow API

`TransitionGate` (`src/axos/store/gate.py`) is the single narrow API
through which authoritative state transitions occur. A gate mutation
generally combines four steps in one `write_txn()` transaction:

```
read state → validate transition → begin transaction
→ verify preconditions → apply transition
→ record authoritative transaction time
→ record ledger event → commit
```

`Store.write_txn()` opens `BEGIN IMMEDIATE` (serializing writers)
and assigns **one authoritative store timestamp per transaction**,
monotonic against the persisted `axos_meta.last_commit_ts`. Every
timestamped row carries the transaction's authoritative time;
wall-clock time never enters durable state directly, and worker
clocks are never trusted. An invalid transition raises
`TransitionRejected` with state left unchanged; lost fencing
authority raises `LeaseError`; lost compare-and-swap races raise
`PolicyConflict`, `BreakerConflict`, `FinalizationConflict`, or
`DesiredStateConflict`.

The gate's method surface (all observed public methods in
`store/gate.py`):

- **Tasks:** `create_task`, `transition_task`, `get_task`.
- **Jobs:** `create_job`, `transition_job`, `get_job`,
  `jobs_in_states`, `pending_jobs`, `owned_active_jobs`,
  `sched_claimed_orphans`, claim-beat bookkeeping
  (`refresh_scheduler_claim_beats`, `withdraw_scheduler_claim_beat`,
  `clear_scheduler_claim_beats`, `scheduler_claim_live`).
- **Leases:** `claim_job`, `claim_job_bounded`,
  `claim_job_resilient`, `renew_lease`, `release_lease`,
  `observe_expired_leases`, `expired_leases` (legacy),
  `reclaim_lease`; fencing evidence `fencing_ledger_for_job`,
  `unreaped_proc_spawns`, `latest_spawn_generation`.
- **Progress:** `update_job_progress`, `progress_evidence_for_job`.
- **Workers:** `create_worker`, `transition_worker`, `get_worker`,
  `mark_worker_seen`, `ingest_heartbeat`, `heartbeats_for`.
- **R5 artifacts:** `stage_artifact`, `get_artifact`,
  `verify_artifact`, `commit_artifact`, `create_artifact`,
  `transition_artifact`, `record_validation`,
  `quarantine_validator`, `validations_for_validator`,
  `classify_staging_orphans`, `commit_job_result` (rejecting stub).
- **Checkpoints:** `stage_checkpoint`, `verify_checkpoint`,
  `invalidate_checkpoint`, `create_checkpoint`,
  `set_checkpoint_verification`, `latest_known_good`,
  `all_latest_known_good`.
- **Approvals/incidents:** `create_approval`, `decide_approval`,
  `create_incident`, `set_incident_outcome`,
  `set_incident_escalated`.
- **R8 recovery:** incident/attempt find-or-create, create, claim,
  `transition_recovery_attempt` (caller-supplied `from_states`),
  `complete_recovery_attempt`, `mark_recovery_attempt_uncertain`,
  open/escalated incident queries.
- **R9 policy:** `get_recovery_policy`,
  `ensure_recovery_policy`, `cas_update_recovery_policy`,
  `consume_policy_attempts`.
- **R10:** `claim_job_resilient()` — capacity predicate, breaker
  revalidation, claim, and scheduler-beat planting in **one**
  `write_txn()`.
- **R11:** `get_desired_head`, `get_desired_job_map`,
  `ensure_job_for_desired_state`, `begin` / `checkpoint` /
  `finish_reconciliation_run`.
- **R12:** `record_breaker_signal`, `get_breaker_state`,
  `list_breaker_states`, `transition_breaker`,
  `claim_half_open_probe`, `breaker_allows`.
- **R13:** `begin_finalization_run`, `evaluate_finalization`,
  `publish_finalization`, `list_finalization_runs`.
- **R7:** `record_watchdog_verdict`, `watchdog_verdicts_for`.
- **Ledger:** `append_event`, `verify_ledger_chain`.

Process evidence is also written through the gate, but as *events*
rather than methods: durable spawn/reap/fence facts are ledger
milestones appended via `gate.append_event("worker.proc_spawned" |
"worker.proc_reaped" | "worker.fence_enforced", …)` by the
supervisor.

## The hash-chained ledger

Every gate mutation appends to the ledger through
`TransitionGate._append_event()`. The ledger is **SHA-256 hash
chained**: each event binds to its predecessor, so any tampering,
reordering, deletion, or payload mutation breaks the chain, and
`verify_ledger_chain()` detects it (attacked directly by
`src/axos/audit/05_ledger.py`).

The ledger is tamper-**evident**, not tamper-**proof**: the hash
chain lives inside the same database it protects, and there is no
external anchoring. This is a known, recorded limitation, not a
defect.

Authoritative-time semantics: the ledger's timestamps are the
transaction's authoritative store time, not the moment a controller
acted. Actor attribution is recorded per event.

## Schema: migrations v1–v12

`src/axos/store/migrations.py` defines 12 ordered, deterministic,
atomic migrations (a failing migration leaves the schema untouched;
re-running `migrate()` on a current DB is a no-op; state recorded in
`schema_migrations`). Tables owned, from the DDL: `tasks`, `jobs`,
`workers`, `artifacts`, `artifact_stagings` (v12: per-job staging
provenance preserving identity when identical bytes deduplicate to
one artifact row), `checkpoints`, `checkpoint_pointers`, `approvals`,
`incidents`, `recovery_attempts`, `recovery_policy`, `heartbeats`,
`validations`, `desired_state`, `desired_state_head`,
`desired_job_map`, `reconciliation_runs`, `breaker_state`,
`breaker_signals`, `finalization_runs`, `scheduler_claim_beats`,
`watchdog_verdicts`, `ledger`, `axos_meta`, `schema_migrations`.

Pragmas (verified in `store/db.py`): WAL mode enforced,
`synchronous=NORMAL`, `foreign_keys=ON`. `Store` also exposes
`backup_to()` (consistent backup via the SQLite online-backup API)
and `integrity_check()`.

## Checkpoints and artifacts

**Artifacts** are content-addressed: the gate computes SHA-256 over
the bytes itself in `stage_artifact()`; bytes are `fsync`'d before
row registration. Lifecycle: `STAGING → VALIDATED → RELEASED`, with
`QUARANTINED` reachable from each (forensic retention, never silent
deletion). The required validator tuple currently contains only
`("axos-structural", "1")`; receipts are bound to the exact content
hash. Staged artifact bytes live in a sibling `axos-staging/`
directory of the DB file (`staging_root`).

**Checkpoints:** identity is SHA-256 of the canonical artifact
manifest (`stage_checkpoint()`); `verify_checkpoint()` fully
revalidates manifest identity, artifact rows, bytes, hashes,
validator receipts, and ledger sequence contiguity. Successful
verification advances the task's latest-known-good pointer, and
`latest_known_good()` returns only a checkpoint still in literal
`VERIFIED` state. `invalidate_checkpoint()` is the authorized
post-verification `VERIFIED → CORRUPT` path for `system`,
`operator`, `test`. Boot only observes checkpoints; it never
repairs or advances pointers.

## The completion flow end to end

The only path from work to `COMPLETE`, in write order:

1. `ensure_job_for_desired_state()` → `PENDING` job row
   (reconciler, actor `"reconciler"`).
2. `claim_job_resilient()` → `PENDING → CLAIMED` + owner + token
   increment + lease timestamps + scheduler beat, one transaction.
3. Worker: `claim_job()` (or `--expect-token` triple verification)
   → read triple back → `transition_job(RUNNING)` →
   `ingest_heartbeat()` / `update_job_progress()` (under the live
   triple) → daemon thread `renew_lease()`.
4. `stage_artifact()` (bytes `fsync`'d, hash computed by the gate) →
   `begin_commit()` (`RUNNING → COMMITTING`, fenced) →
   `verify_artifact()` → `commit_artifact()` (`COMMITTING →
   COMPLETE`, atomic).
5. Each step appends hash-chained ledger events; the completion
   transaction re-reads bytes and validator receipts before
   flipping to `COMPLETE`.

Failure paths write through the same API: `fail_job_execution()`
(active states → `FAILED`, fenced), `reclaim_lease()` (`CLAIMED →
PENDING`; `RUNNING | COMMITTING → UNCERTAIN`, token incremented,
ledger evidence), and the `UNCERTAIN` resolver's
`commit_artifact()` (adopt if staged bytes verify, requeue if not —
token lineage must predate the reclaim).

## Reconciliation reads

Controllers read; the store decides. The read contract:

- Non-gate components get `ReadOnlyStore` (via `Store.read_only()`
  or `open_readonly_store()`): read queries only; any mutation
  attempt raises `sqlite3.OperationalError`. The gate is the only
  holder of a writable `Store`.
- `observe_expired_leases()` is explicitly read-only (expiry is
  observed; the reclaim authority moves state).
- Controllers consume each other's outputs only as durable rows:
  the watchdog writes verdicts that R8 reads; R8 writes attempts
  that R9 reads; the resilience controller moves breaker rows that
  the gate's `claim_job_resilient()` enforces transactionally;
  the reconciler pins the desired head and re-pins before every
  batch. There are no callbacks, queues, or signals between
  controllers.
- Reconciliation runs keep a durable cursor (`last_item_id`,
  counts) checkpointed after every batch; results
  `CONVERGED / CHANGED / BLOCKED / CONFLICT / FAILED` with
  precedence `FAILED > CONFLICT > BLOCKED > CHANGED > CONVERGED`.
  Refusals perform zero mutation — not even a run row.
- Finalization is read-evaluate then atomically publish: the
  finalizer re-evaluates every non-finalized run from durable
  evidence with CAS on version, and the gate's `publish_finalization`
  re-validates everything authoritatively inside one `write_txn`
  and flips `READY → FINALIZED` — or refuses. A pass over an
  already-`FINALIZED` generation performs zero writes.

## Known boundaries

- The store is the single point of failure. No replication,
  failover, backup automation, or tamper-proofing of the ledger —
  known boundaries, not oversights.
- The ledger's tamper-evidence rests on a hash chain inside the
  same database it protects; there is no external anchoring.
- Liveness properties (e.g. "the renewal thread keeps a live worker
  unfenced") are the code's stated intent; the export pass executed
  no runtime behavior.
- Reconciler `diff_item` lists `"VERIFYING"` among mapped job
  statuses, but `VERIFYING` is not a legal job status in
  `JOB_TRANSITIONS` (observed discrepancy; nil practical effect —
  no job row can carry that status).

See [overview.md](overview.md) for the loop these flows implement,
[control-plane.md](control-plane.md) for the components that run
them, [state-machine.md](state-machine.md) for the transition graphs
they move through, and [ownership-model.md](ownership-model.md) for
the lease triple that gates every write.
