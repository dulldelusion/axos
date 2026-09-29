# AXOS State Machines — As Implemented

**Release:** `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
**Migration version:** 12 · **Source:** `~/workspace/axos/`
**Exported:** 2026-09-27 · **Method:** read-only inspection; no source modified.

> Every graph below is the literal content of `store/transitions.py`,
> verified by direct import of each `*_TRANSITIONS` table on 2026-09-27.
> Guards, actors, and special paths were observed in `store/gate.py`
> (function names and line numbers given). Where a transition has an
> authorized path outside the generic graph, that path is named explicitly
> — it is not folded into the graph.

---

## 1. Task lifecycle

**Graph** (`TASK_TRANSITIONS`):

```
PROPOSED ──► AUTHORIZED ──► PLANNED ──► EXECUTING ──► VERIFYING ──► FINALIZED
    │              │                         │              │
    ▼              ▼                         ▼              ▼
 REJECTED   PAUSED_FOR_HUMAN ◄──────────────┴──────────────┘
    │              │
    ▼              ▼
 (terminal)   EXECUTING | CANCELLED

EXECUTING ──► FAILED | CANCELLED          BLOCKED ──► EXECUTING
FAILED, CANCELLED, FINALIZED, REJECTED: terminal (no outgoing edges)
```

Edge list, verbatim:

- `PROPOSED -> AUTHORIZED | REJECTED`
- `AUTHORIZED -> PLANNED | PAUSED_FOR_HUMAN`
- `PLANNED -> EXECUTING`
- `EXECUTING -> VERIFYING | PAUSED_FOR_HUMAN | FAILED | CANCELLED`
- `VERIFYING -> FINALIZED | PAUSED_FOR_HUMAN`
- `BLOCKED -> EXECUTING`
- `PAUSED_FOR_HUMAN -> EXECUTING | CANCELLED`
- `REJECTED`, `FAILED`, `CANCELLED`, `FINALIZED` terminal

**Gate:** `TransitionGate.transition_task()` (`store/gate.py:399`).

**Guards:**

- **I-17 (human gate):** leaving `PAUSED_FOR_HUMAN` requires an explicit
  recorded `APPROVED` approval for the same task. Human-gated states are
  sticky across restarts: the scheduler never admits new work under a
  paused task (`Scheduler._paused_task_ids`, `exec/scheduler.py`), the
  reconciler never creates work under one (`Reconciler._create_for_missing`,
  `exec/reconciler.py`), and R9 stands down rather than working around one
  (`PolicyController._reconcile_incident`, `exec/policy.py`).
- R9's terminal rung 5 may enter `PAUSED_FOR_HUMAN` through
  `PolicyController._pause_task_for_human()` — best-effort, journaled, never
  auto-cleared.

**Notes:**

- There is no listed edge *into* `BLOCKED` in the task graph. The task
  graph's `BLOCKED` state is reachable only if some gate path outside the
  generic table writes it — **no such path was verified in this pass**;
  treat task-`BLOCKED` as defined-but-unreached unless a caller is found.
- `transition_task` to `CANCELLED` from `EXECUTING`/`PAUSED_FOR_HUMAN` is the
  only cancellation edge; `PROPOSED` has no cancel edge (only `REJECTED`).

---

## 2. Job lifecycle

**Graph** (`JOB_TRANSITIONS`):

```
PENDING ──► CLAIMED ──► RUNNING ──► COMMITTING ──► COMPLETE (terminal)
                │           │             │
                │           │             ▼
                │           │        UNCERTAIN ──► COMPLETE | PENDING | QUARANTINED
                │           │             ▲
                │           ▼             │
                │        PENDING ◄────────┘
                │           ▲
                ▼           │
              FAILED ──► PENDING | QUARANTINED | BLOCKED ──► PENDING
                │
                ▼
            (terminal: COMPLETE only)
```

Edge list, verbatim:

- `PENDING -> CLAIMED`
- `CLAIMED -> RUNNING | PENDING | FAILED`
- `RUNNING -> COMMITTING | PENDING | UNCERTAIN | FAILED`
- `COMMITTING -> COMPLETE | UNCERTAIN | FAILED`
- `UNCERTAIN -> COMPLETE | PENDING | QUARANTINED`
- `FAILED -> PENDING | QUARANTINED | BLOCKED`
- `QUARANTINED -> PENDING`
- `BLOCKED -> PENDING`
- `COMPLETE` terminal (no outgoing edges)

**Gates:**

- `TransitionGate.transition_job()` — **always rejects the `COMPLETE`
  target**; there is no legal direct transition to `COMPLETE`.
- The **sole normal completion path** is the R5 protocol
  (`store/gate.py:1137–1670`):
  `stage_artifact()` → `begin_commit()` (RUNNING→COMMITTING, fenced) →
  `verify_artifact()` → `commit_artifact()` (COMMITTING→COMPLETE, atomic).
  `commit_job_result()` exists only as a rejecting compatibility stub.
- `UNCERTAIN` resolution (`commit_artifact()` from `UNCERTAIN`) is available
  only to reclaim-authority actors and requires artifact token lineage
  predating the reclaim.

**Guards:**

- Claims use the shared `_claim_job_txn()` (`store/gate.py:673`):
  `PENDING -> CLAIMED`, owner set, fencing token incremented, lease
  timestamps set — one atomic transaction.
- TTL must be a positive number before the transaction opens; non-positive
  or malformed TTL is rejected with no mutation.
- `fail_job_execution()` moves active execution states to `FAILED`, fenced
  by owner/fencing-token/live-lease.
- `reclaim_lease()` (`store/gate.py:927`): `CLAIMED -> PENDING`,
  `RUNNING | COMMITTING -> UNCERTAIN`, atomically verifying evidence,
  incrementing the fencing token, clearing owner/lease, and appending
  ledger evidence. Routine reclaim requires actual expiry; forced reclaim
  requires a `DEAD` or `STALLED` verdict plus `incident_id`.
- Work creators allowed (`WORK_CREATOR_ACTORS`): `human`, `scheduler`,
  `system`, `test`, `reconciler`. Worker actors are prohibited from creating
  tasks/jobs (I-16). Direct artifact lifecycle transitions are denied to
  `worker:*` actors; checkpoint staging/verification reject worker actors.
- Reclaim actors: `recovery-controller`, `reconciler`, `system`, `operator`,
  `test` — the supervisor is deliberately excluded.

**Notes:**

- The graph has **no** `REJECTED`, `CANCELLED`, or `FINALIZED` job states,
  despite `exec/scheduler.py`'s docstring listing them as never-admitted
  states. The docstring is wider than the graph; the graph governs.
- `UNCERTAIN -> COMPLETE` is the crash-after-stage adoption path
  (synthetic behavior `crash_after_stage` stages and verifies bytes, then
  dies before commit); the resolver adopts when the staged bytes verify,
  requeues when they do not.

---

## 3. Worker lifecycle

**Graph** (`WORKER_TRANSITIONS`):

```
PROVISIONING ──► IDLE ──► ASSIGNED ──► RUNNING ──► IDLE
     │                       │              │
     ▼                       ▼              ▼
   DEAD (terminal)        SUSPECT ◄────────┘
                             │  │
                             ▼  ▼
                           RUNNING  DEAD (terminal)

IDLE ──► DRAINING ──► RETIRED (terminal)      RUNNING ──► DRAINING ──► RETIRED
```

Edge list, verbatim:

- `PROVISIONING -> IDLE | DEAD`
- `IDLE -> ASSIGNED | DRAINING`
- `ASSIGNED -> RUNNING | SUSPECT`
- `RUNNING -> IDLE | DRAINING | SUSPECT`
- `SUSPECT -> RUNNING | DEAD`
- `DRAINING -> RETIRED`
- `DEAD`, `RETIRED` terminal

**Driver:** the supervisor (`exec/supervisor.py`) drives worker-state
transitions while only reading job state. Spawned workers start
`PROVISIONING`; durable evidence pairs `worker.proc_spawned` /
`worker.proc_reaped` ledger events with the row.

**Notes:**

- `SUSPECT` is the only non-terminal state reachable from `RUNNING` other
  than `IDLE`/`DRAINING`; `SUSPECT -> DEAD` is the terminal suspicion path.
- The worker process itself never transitions its own worker row; the
  supervisor does.

---

## 4. Artifact lifecycle

**Graph** (`ARTIFACT_TRANSITIONS`):

```
STAGING ──► VALIDATED ──► RELEASED
    │             │             │
    ▼             ▼             ▼
 QUARANTINED ◄───┴─────────────┘   (QUARANTINED terminal)
```

Edge list, verbatim:

- `STAGING -> VALIDATED | QUARANTINED`
- `VALIDATED -> RELEASED | QUARANTINED`
- `RELEASED -> QUARANTINED`
- `QUARANTINED` terminal

**Guards:**

- Artifacts are content-addressed: the gate computes SHA-256 itself in
  `stage_artifact()`; bytes are `fsync`'d before row registration.
- The required validator tuple currently contains only
  `("axos-structural", "1")`.
- `commit_artifact()` re-reads bytes and validator receipts inside the
  completion transaction.
- Worker-owned artifact operations require current owner, matching fencing
  token, and a live lease; direct artifact lifecycle transitions are denied
  to `worker:*` actors.

**Note on naming:** comments in `store/gate.py` sometimes use "VERIFIED" as
a predicate/lifecycle description for artifacts, but the literal artifact
status names are `STAGING`, `VALIDATED`, `RELEASED`, `QUARANTINED` — there
is no `VERIFIED` artifact state. (Checkpoint states, §6, *do* use
`VERIFIED`.)

---

## 5. Approval lifecycle

**Graph** (`APPROVAL_TRANSITIONS`):

```
PENDING ──► APPROVED | DENIED | EXPIRED      (all three terminal)
```

Edge list, verbatim:

- `PENDING -> APPROVED | DENIED | EXPIRED`
- `APPROVED`, `DENIED`, `EXPIRED` terminal

**Role:** an explicit recorded `APPROVED` approval for the task is the I-17
key that unlocks `PAUSED_FOR_HUMAN -> EXECUTING`.

---

## 6. Checkpoint lifecycle

**Graph** (`CHECKPOINT_TRANSITIONS`):

```
UNVERIFIED ──► VERIFYING ──► VERIFIED | CORRUPT
(VERIFIED, CORRUPT terminal in the generic graph)
```

Edge list, verbatim:

- `UNVERIFIED -> VERIFYING`
- `VERIFYING -> VERIFIED | CORRUPT`
- `VERIFIED`, `CORRUPT` terminal

**Special authorized path (outside the generic graph):**
`TransitionGate.invalidate_checkpoint()` implements an authorized
`VERIFIED -> CORRUPT` post-verification invalidation for actors `system`,
`operator`, or `test`. This is a real, implemented edge that the generic
table does not show.

**Guards:**

- Checkpoint identity is SHA-256 of the canonical artifact manifest.
- Allowed triggers: `policy`, `pre_risky_operation`, `operator_request`.
- `verify_checkpoint()` fully revalidates manifest identity, artifact rows,
  bytes, hashes, validator receipts, and ledger sequence contiguity.
- Successful verification advances the task's latest-known-good pointer;
  `latest_known_good()` returns only a checkpoint still in literal
  `VERIFIED` state. Boot only observes; it never repairs or advances
  pointers.

---

## 7. Recovery attempt lifecycle

**States** (observed in `store/gate.py` and asserted by
`exec/resilience.py::_validate_attempt_states` against
`gate._ATTEMPT_OPEN`):

```
CREATED ──► RUNNING ──► VERIFYING ──► SUCCEEDED | FAILED | BLOCKED | UNCERTAIN
```

- Open (still owned by R8): `CREATED`, `RUNNING`, `VERIFYING`, `UNCERTAIN`
  (`gate._ATTEMPT_OPEN`, asserted verbatim by the resilience controller).
- Result (consumable by R9): `SUCCEEDED`, `FAILED`, `BLOCKED`
  (`PolicyController._R8_RESULT_STATES`).

**Gate:** `TransitionGate.transition_recovery_attempt()` takes
caller-supplied allowed `from_states` — there is **no single centralized
attempt edge table** in the gate. The graph above is reconstructed from the
open/result state sets the controllers actually use; any caller passing a
different `from_states` set would be authoritative over its own edges.

**Rung binding:** every attempt records a rung; the canonical rung mapping
observed in `exec/recovery.py` is `reclaim` → rung 2 `"re-claim/requeue"`,
`fence`/`restart` → rung 3 `"replace/reassign"`.

---

## 8. Circuit breaker lifecycle (R12)

**States:** `CLOSED -> OPEN -> HALF_OPEN -> CLOSED | OPEN`.

- `CLOSED -> OPEN`: windowed `failure_count` reaches the per-scope threshold
  (`ResilienceController._apply_thresholds`; CAS on version).
- `OPEN -> HALF_OPEN`: `cooldown_until` elapsed
  (`ResilienceController._apply_cooldowns`; CAS on version).
- `HALF_OPEN -> CLOSED`: probe success only — job `COMPLETE` (R5 commit) or
  strictly positive `progress_done` delta over the probe baseline.
- `HALF_OPEN -> OPEN`: counted failure evidence after the probe allocation,
  or corrupt probe state (fails closed — reopen, never silent reset).

**Invariants observed in `exec/resilience.py`:**

- `OPEN` never transitions directly to `CLOSED`; the only closing path is
  via `HALF_OPEN` probe success.
- Heartbeats and process existence are never success evidence.
- Probe allocation itself is the gate's job: `claim_half_open_probe()` runs
  inside the claim transaction with probe id `"<worker_id>:<job_id>"`.
- Scopes, fixed evaluation order: `GLOBAL`, `TASK`, `DESIRED`, `JOB`.

---

## 9. Recovery policy row lifecycle (R9)

**Terminal states** (`exec/policy.py`): `RECOVERY_COMPLETE`,
`BUDGET_EXHAUSTED`, `R5_TERMINAL`, `BLOCKED_CONTRADICTORY`,
`BLOCKED_STATE_UNAVAILABLE`, `SUPERSEDED`. Terminal is sticky: once set, no
further rung selection, budget consumption, or execution for the incident.

**Row invariants** (`_validate_policy_row`): `current_rung` in 1..5;
`attempt_count + remaining_budget == incident_budget`;
`consumed_attempt_number` / `result_consumed_attempt_number` /
`zero_progress_count` non-negative; `per_rung_budgets` covers rungs 1..5;
row `policy_version` must equal the controller's (`r9-policy/v1`) or the
row is refused as `PolicyCorrupt` — old policy state is never silently
reinterpreted.

**Rung selection** (`select_rung`, pure function of durable state): the
returned rung is always ≥ `current_rung`; the ladder never rewinds. Order:
unavailable state → rung 5 `BLOCKED_STATE_UNAVAILABLE`; contradictory
evidence → rung 5 `BLOCKED_CONTRADICTORY`; incident budget consumed → rung 5
`BUDGET_EXHAUSTED`; two consecutive zero-progress attempts (D6) → escalate
one rung; per-rung budget consumed → escalate one rung; rung 4 (no R9
executor) → traverse to 5; R8 intake guard holding with a non-executable
rung → skip forward; otherwise continue at the current rung.

---

## 10. Finalization run lifecycle (R13)

**States** (observed in `exec/finalizer.py` / gate finalization API):
run states progress through evaluation toward `READY`, then `publish`
atomically flips a `READY` run to `FINALIZED` inside one `write_txn` — or
refuses. CAS on version: the loser re-reads, never overwrites. No-op
discipline: a pass over an already-`FINALIZED` generation performs zero
writes. The canonical release generation and the finalization manifest hash
are gate-defined (`canonical_release_generation`,
`canonical_finalization_id`, `build_finalization_manifest`,
`finalization_manifest_hash`, re-exported by `exec/finalizer.py`).

---

## 11. Desired-state and scheduler-beat bookkeeping

- **Reconciliation runs** (`gate.begin/checkpoint/finish_reconciliation_run`):
  a durable cursor (`last_item_id`, counts) is checkpointed after every
  batch; results are `CONVERGED` / `CHANGED` / `BLOCKED` / `CONFLICT` /
  `FAILED` with precedence `FAILED > CONFLICT > BLOCKED > CHANGED >
  CONVERGED`. Refusals (stale pin, contradictory desired state) perform zero
  mutation — not even a run row.
- **Scheduler claim beats:** planted atomically with the claim
  (`claim ⟹ beat`), refreshed every `evaluate_once()`, withdrawn on
  dispatch failure (`withdraw_scheduler_claim_beat`), cleared on graceful
  shutdown (`clear_scheduler_claim_beats`). Staleness bound is
  `scheduler_stale_after_s` (default 2.0s). **Documented residual:**
  two schedulers racing one orphaned CLAIMED dispatch of a DEAD scheduler
  may briefly spawn two processes; exactly one wins the atomic
  `CLAIMED -> RUNNING` transition (serialized by `BEGIN IMMEDIATE`) and the
  loser gets `TransitionRejected` before executing, after which R2's fence
  sweep contains its process group. No duplicate execution.

---

## 12. Lease, fencing, heartbeat, and progress rules

- Lease authority is the **job-row triple**
  `(owner_worker_id, lease_expires_at, fencing_token)`.
- Claims use shared `_claim_job_txn()`: `PENDING -> CLAIMED`, owner set,
  token incremented, lease timestamps set. TTL must be a positive number
  before the transaction opens.
- Renewal (`gate.renew_lease`, `store/gate.py:772`) requires owner, matching
  token, active status, and an **unexpired** lease. Renewal extends only the
  current triple; it can never resurrect a superseded token or steal
  another lease.
- **Heartbeats do not renew leases.** Heartbeats are sequenced per
  `(worker_id, proc_id)` and reject duplicate/out-of-order `hb_seq`. A
  heartbeat naming a job requires current owner and matching fencing token.
  Heartbeat progress, when present, flows through the same `_apply_progress()`
  path as explicit progress updates.
- Progress is accepted only in `CLAIMED` or `RUNNING`, with current
  owner/token/live lease.
- `observe_expired_leases()` is read-only and uses authoritative store time
  with inclusive expiry (`lease_expires_at <= now`).
- `reclaim_lease()` atomically verifies evidence, increments the fencing
  token, clears owner/lease, changes state (`CLAIMED -> PENDING`;
  `RUNNING | COMMITTING -> UNCERTAIN`), and appends ledger evidence.

---

## 13. Evidence gaps

- **Recovery attempt edges:** the attempt graph in §7 is reconstructed from
  the state sets the controllers use (`gate._ATTEMPT_OPEN`,
  `_R8_RESULT_STATES`); the gate accepts caller-supplied `from_states` and
  defines no single authoritative edge table, so a caller could in principle
  use different edges. No caller doing so was observed.
- **Finalization run states:** the pre-`READY` evaluation states were
  observed at the API level (`evaluate_finalization` returns `state` and
  `blockers`); the full enumerated state list was not extracted in this
  pass.
- **Task `BLOCKED`:** the state exists in the task graph with an outgoing
  edge to `EXECUTING` but no observed incoming edge (see §1).
- All guards are static-source observations; no transition was executed at
  runtime in this export.
