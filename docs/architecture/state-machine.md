# State Machines

Every graph below is the literal content of
`src/axos/store/transitions.py` (verified by direct import of each
`*_TRANSITIONS` table). Guards, actors, and special paths were observed
in `src/axos/store/gate.py`. Where a transition has an authorized path
outside the generic graph, that path is named explicitly — it is not
folded into the graph.

Convention: every unlisted transition raises `TransitionRejected` with
zero mutation.

## 1. Task lifecycle (`TASK_TRANSITIONS`)

Edge list, verbatim:

- `PROPOSED → AUTHORIZED | REJECTED`
- `AUTHORIZED → PLANNED | PAUSED_FOR_HUMAN`
- `PLANNED → EXECUTING`
- `EXECUTING → VERIFYING | PAUSED_FOR_HUMAN | FAILED | CANCELLED`
- `VERIFYING → FINALIZED | PAUSED_FOR_HUMAN`
- `BLOCKED → EXECUTING`
- `PAUSED_FOR_HUMAN → EXECUTING | CANCELLED`
- Terminal: `REJECTED`, `FAILED`, `CANCELLED`, `FINALIZED`

Gate: `TransitionGate.transition_task()`.

**Guard I-17 (human gate):** leaving `PAUSED_FOR_HUMAN` requires an
explicit recorded `APPROVED` approval for the same task. The gate is
sticky across restarts: the scheduler never admits new work under a
paused task (`Scheduler._paused_task_ids`), the reconciler never
creates work under one (`Reconciler._create_for_missing`), and R9
stands down rather than working around one
(`PolicyController._reconcile_incident`). R9's terminal rung 5 may
*enter* `PAUSED_FOR_HUMAN` through
`PolicyController._pause_task_for_human()` — best-effort, journaled,
never auto-cleared.

**Note:** the task graph's `BLOCKED` state has an outgoing edge to
`EXECUTING` but no verified incoming edge — treat task-`BLOCKED` as
defined-but-unreached unless a caller is found. `PROPOSED` has no
cancel edge (only `REJECTED`).

## 2. Job lifecycle (`JOB_TRANSITIONS`)

Edge list, verbatim:

- `PENDING → CLAIMED`
- `CLAIMED → RUNNING | PENDING | FAILED`
- `RUNNING → COMMITTING | PENDING | UNCERTAIN | FAILED`
- `COMMITTING → COMPLETE | UNCERTAIN | FAILED`
- `UNCERTAIN → COMPLETE | PENDING | QUARANTINED`
- `FAILED → PENDING | QUARANTINED | BLOCKED`
- `QUARANTINED → PENDING`
- `BLOCKED → PENDING`
- Terminal: `COMPLETE` (no outgoing edges)

**Gates:**

- `TransitionGate.transition_job()` — **always rejects the
  `COMPLETE` target**; there is no legal direct transition to
  `COMPLETE`.
- The **sole normal completion path** is the R5 protocol
  `stage_artifact()` → `begin_commit()` (RUNNING→COMMITTING, fenced)
  → `verify_artifact()` → `commit_artifact()`
  (COMMITTING→COMPLETE, atomic). `commit_job_result()` exists only
  as a rejecting compatibility stub — the direct transition path is
  dead code by design.
- `UNCERTAIN → COMPLETE` via `commit_artifact()` is the
  crash-after-stage adoption path: available only to reclaim-authority
  actors, requiring artifact token lineage that predates the reclaim.
  The resolver adopts when the staged bytes verify, requeues when
  they do not.

**Guards:**

- Claims use the shared `_claim_job_txn()`: `PENDING → CLAIMED`,
  owner set, fencing token incremented, lease timestamps set — one
  atomic transaction. TTL must be a positive number before the
  transaction opens (F2); non-positive or malformed TTL is rejected
  with no mutation.
- `fail_job_execution()` moves active execution states to `FAILED`,
  fenced by owner/fencing-token/live-lease.
- `reclaim_lease()`: `CLAIMED → PENDING`,
  `RUNNING | COMMITTING → UNCERTAIN` — atomically verifying evidence,
  incrementing the fencing token, clearing owner/lease, and appending
  ledger evidence. Routine reclaim requires actual expiry; forced
  reclaim requires a `DEAD` or `STALLED` verdict plus `incident_id`.
  Reclaim actors (`RECLAIM_ACTORS` in `transitions.py`):
  `recovery-controller`, `reconciler`, `system`, `operator`, `test` —
  the supervisor is deliberately excluded.
- **I-16:** work creators allowed (`WORK_CREATOR_ACTORS`):
  `human`, `scheduler`, `system`, `test`, `reconciler`. Actors
  starting `worker:` are refused at creation. Direct artifact
  lifecycle transitions are denied to `worker:*` actors; checkpoint
  staging/verification reject worker actors.

**Note:** the graph has no `REJECTED`, `CANCELLED`, or `FINALIZED` job
states. Some docstrings mention wider state sets, but the graph in
`transitions.py` governs.

## 3. Worker lifecycle (`WORKER_TRANSITIONS`)

Edge list, verbatim:

- `PROVISIONING → IDLE | DEAD`
- `IDLE → ASSIGNED | DRAINING`
- `ASSIGNED → RUNNING | SUSPECT`
- `RUNNING → IDLE | DRAINING | SUSPECT`
- `SUSPECT → RUNNING | DEAD`
- `DRAINING → RETIRED`
- Terminal: `DEAD`, `RETIRED`

**Driver:** the supervisor drives worker-state transitions while only
reading job state; the worker process itself never transitions its
own worker row. Spawned workers start `PROVISIONING`; durable evidence
pairs `worker.proc_spawned` / `worker.proc_reaped` ledger events with
the row. `SUSPECT → DEAD` is the terminal suspicion path.

## 4. Artifact lifecycle (`ARTIFACT_TRANSITIONS`)

Edge list, verbatim:

- `STAGING → VALIDATED | QUARANTINED`
- `VALIDATED → RELEASED | QUARANTINED`
- `RELEASED → QUARANTINED`
- Terminal: `QUARANTINED`

**Guards:** artifacts are content-addressed — the gate computes the
SHA-256 content hash itself in `stage_artifact()`; bytes are `fsync`'d
before row registration. The required validator tuple currently
contains only `("axos-structural", "1")`. `commit_artifact()`
re-reads bytes and validator receipts inside the completion
transaction. Worker-owned artifact operations require current owner,
matching fencing token, and a live lease. Corrupt bytes go to
`QUARANTINED` for forensic retention — never silent deletion.

**Naming note:** the literal artifact states are `STAGING`,
`VALIDATED`, `RELEASED`, `QUARANTINED` — there is no `VERIFIED`
artifact state. Checkpoints (§6) *do* use `VERIFIED`.

## 5. Approval lifecycle (`APPROVAL_TRANSITIONS`)

Edge list, verbatim:

- `PENDING → APPROVED | DENIED | EXPIRED`
- Terminal: `APPROVED`, `DENIED`, `EXPIRED`

An explicit recorded `APPROVED` approval for the task is the I-17 key
that unlocks `PAUSED_FOR_HUMAN → EXECUTING`.

## 6. Checkpoint lifecycle (`CHECKPOINT_TRANSITIONS`)

Edge list, verbatim:

- `UNVERIFIED → VERIFYING`
- `VERIFYING → VERIFIED | CORRUPT`
- Terminal in the generic graph: `VERIFIED`, `CORRUPT`

**Special authorized path (outside the generic graph):**
`TransitionGate.invalidate_checkpoint()` implements an authorized
`VERIFIED → CORRUPT` post-verification invalidation for actors
`system`, `operator`, or `test`. This is a real, implemented edge
that the generic table does not show.

**Guards:** checkpoint identity is SHA-256 of the canonical artifact
manifest. Allowed triggers: `policy`, `pre_risky_operation`,
`operator_request`. `verify_checkpoint()` fully revalidates manifest
identity, artifact rows, bytes, hashes, validator receipts, and
ledger sequence contiguity. Successful verification advances the
task's latest-known-good pointer; `latest_known_good()` returns only
a checkpoint still in literal `VERIFIED` state. Boot only observes;
it never repairs or advances pointers.

## 7. Recovery attempt lifecycle

States (observed in `store/gate.py`; `gate._ATTEMPT_OPEN` is asserted
verbatim by the resilience controller):

```
CREATED → RUNNING → VERIFYING → SUCCEEDED | FAILED | BLOCKED | UNCERTAIN
```

- **Open** (still owned by R8): `CREATED`, `RUNNING`, `VERIFYING`,
  `UNCERTAIN`.
- **Result** (consumable by R9): `SUCCEEDED`, `FAILED`, `BLOCKED`
  (`PolicyController._R8_RESULT_STATES`).

`TransitionGate.transition_recovery_attempt()` takes caller-supplied
allowed `from_states` — there is **no single centralized attempt edge
table** in the gate. The graph above is reconstructed from the open/
result state sets the controllers actually use; any caller passing a
different `from_states` set would be authoritative over its own edges.
No caller doing so was observed in this snapshot.

**Rung binding:** every attempt records a rung; the canonical rung
mapping observed in `exec/recovery.py` is `reclaim` → rung 2
("re-claim/requeue"), `fence` / `restart` → rung 3
("replace/reassign").

## 8. Circuit breaker lifecycle (R12)

`CLOSED → OPEN → HALF_OPEN → CLOSED | OPEN`.

- `CLOSED → OPEN`: windowed `failure_count` reaches the per-scope
  threshold (`ResilienceController._apply_thresholds`; CAS on
  version — loser re-reads).
- `OPEN → HALF_OPEN`: `cooldown_until` elapsed
  (`ResilienceController._apply_cooldowns`; CAS on version).
- `HALF_OPEN → CLOSED`: probe success only — job `COMPLETE` (R5
  commit) or strictly positive `progress_done` delta over the probe
  baseline.
- `HALF_OPEN → OPEN`: counted failure evidence after the probe
  allocation, or corrupt probe state (fails closed — reopen, never
  silent reset).

Invariants observed in `exec/resilience.py`: `OPEN` never transitions
directly to `CLOSED`; heartbeats and process existence are never
success evidence; probe allocation is the gate's job
(`claim_half_open_probe()`, probe id `"<worker_id>:<job_id>"`);
scopes in fixed evaluation order: GLOBAL, TASK, DESIRED, JOB.

## 9. Recovery policy row lifecycle (R9)

**Terminal states** (`exec/policy.py`): `RECOVERY_COMPLETE`,
`BUDGET_EXHAUSTED`, `R5_TERMINAL`, `BLOCKED_CONTRADICTORY`,
`BLOCKED_STATE_UNAVAILABLE`, `SUPERSEDED`. Terminal is sticky: once
set, no further rung selection, budget consumption, or execution for
the incident.

**Row invariants** (`_validate_policy_row`): `current_rung` in 1..5;
`attempt_count + remaining_budget == incident_budget`;
`consumed_attempt_number` / `result_consumed_attempt_number` /
`zero_progress_count` non-negative; `per_rung_budgets` covers rungs
1..5; row `policy_version` must equal the controller's
(`r9-policy/v1`) or the row is refused as `PolicyCorrupt` — old
policy state is never silently reinterpreted.

**`select_rung()`** (pure deterministic function of durable state,
never rewinds). Rules in order: unavailable state → rung 5
`BLOCKED_STATE_UNAVAILABLE`; contradictory evidence → rung 5
`BLOCKED_CONTRADICTORY`; incident budget consumed → rung 5
`BUDGET_EXHAUSTED`; two consecutive zero-progress attempts →
escalate one rung; per-rung budget consumed → escalate one rung;
rung 4 (no R9 executor) → traverse to 5; R8 intake guard holding
with a non-executable rung → skip forward; otherwise continue at the
current rung.

## 10. Finalization run lifecycle (R13)

Runs progress through evaluation toward `READY`; `publish`
atomically flips a `READY` run to `FINALIZED` inside one `write_txn`
— or refuses. CAS on version (loser re-reads, never overwrites).
A pass over an already-`FINALIZED` generation performs zero writes.
The canonical release generation and finalization manifest hash are
gate-defined (`canonical_release_generation`,
`canonical_finalization_id`, `build_finalization_manifest`,
`finalization_manifest_hash`, re-exported by `exec/finalizer.py`).
The full enumerated pre-`READY` state list was not extracted in this
snapshot (observed at the API level only).

## 11. Desired-state and scheduler-beat bookkeeping

- **Reconciliation runs** (`gate.begin/checkpoint/finish_reconciliation_run`):
  a durable cursor (`last_item_id`, counts) is checkpointed after
  every batch; results `CONVERGED` / `CHANGED` / `BLOCKED` /
  `CONFLICT` / `FAILED` with precedence
  `FAILED > CONFLICT > BLOCKED > CHANGED > CONVERGED`. Refusals
  (stale pin, contradictory desired state) perform zero mutation —
  not even a run row.
- **Scheduler claim beats:** planted atomically with the claim
  (`claim ⟹ beat`), refreshed every `evaluate_once()`, withdrawn on
  dispatch failure (`withdraw_scheduler_claim_beat`), cleared on
  graceful shutdown (`clear_scheduler_claim_beats`). Staleness bound
  is `scheduler_stale_after_s` (default 2.0s).

## 12. Lease, fencing, heartbeat, and progress rules

- Lease authority is the **job-row triple**
  `(owner_worker_id, lease_expires_at, fencing_token)`.
- Claims use shared `_claim_job_txn()`: `PENDING → CLAIMED`, owner
  set, token incremented, lease timestamps set. TTL must be a
  positive number before the transaction opens.
- Renewal (`gate.renew_lease`) requires owner, matching token,
  active status, and an **unexpired** lease. Renewal extends only
  the current triple; it can never resurrect a superseded token or
  steal another lease.
- **Heartbeats do not renew leases.** Heartbeats are sequenced per
  `(worker_id, proc_id)` and reject duplicate/out-of-order `hb_seq`.
  A heartbeat naming a job requires current owner and matching
  fencing token. Heartbeat progress, when present, flows through the
  same `_apply_progress()` path as explicit progress updates.
- Progress is accepted only in `CLAIMED` or `RUNNING`, with current
  owner/token/live lease (F3).
- `observe_expired_leases()` is read-only and uses authoritative
  store time with inclusive expiry (`lease_expires_at <= now`).
- `reclaim_lease()` atomically verifies evidence, increments the
  fencing token, clears owner/lease, changes state (`CLAIMED →
  PENDING`; `RUNNING | COMMITTING → UNCERTAIN`), and appends ledger
  evidence.

## Authority summary

| Mutation | Who may call | Who is excluded |
|----------|--------------|-----------------|
| Create tasks/jobs | `human`, `scheduler`, `system`, `test`, `reconciler` | `worker:*` (I-16) |
| Reclaim leases | `recovery-controller`, `reconciler`, `system`, `operator`, `test` | supervisor |
| Exit `PAUSED_FOR_HUMAN` | human with recorded `APPROVED` approval | all automation (I-17) |
| Complete a job | worker under live lease via R5; reclaim actors via UNCERTAIN resolver | everyone via direct transition |
| Direct artifact transitions | authority actors | `worker:*` |
| Invalidate checkpoint | `system`, `operator`, `test` | workers |

## Evidence gaps (as exported)

- Recovery attempt edges are reconstructed from the state sets the
  controllers use; the gate defines no single authoritative edge
  table.
- Pre-`READY` finalization run states were observed at the API level;
  the full enumerated list was not extracted.
- Task `BLOCKED` has no observed incoming edge.
- All guards are static-source observations; no transition was
  executed at runtime in the export pass.
