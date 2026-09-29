# AXOS Overview

**AXOS** — Autonomous Execution OS.

## What is AXOS?

AXOS is an execution OS for running work under machine enforcement
rather than convention. Its core idea is that **durable state is the
only authority**: who may execute what, in which lease epoch, with
which fencing token, is decided by rows in one SQLite database, not by
what any controller claims. AI reasoning, operator intent, and process
existence are *evidence*, never authority — no component of AXOS may
advance, complete, or recover work on the basis of reasoning alone; a
decision only becomes real when a governed transition lands in the
store.

The authoritative state is a single SQLite database in WAL mode
(`store/db.py`). All authority lives in the store layer (`store/`):
lease/fencing authority, the explicit transition graphs, checkpoints,
artifacts, incidents, recovery attempts and policies, breakers, and a
SHA-256 hash-chained tamper-evident ledger. Everything else is a
controller that **reads** durable evidence and **requests** state
changes through one narrow mutation bottleneck, `TransitionGate`
(`store/gate.py`). No execution component writes SQL directly; that
separation is machine-checked by the authority audit
(`src/axos/audit/09_authority_audit.py`).

AXOS ships with a deterministic synthetic executor
(`exec/synthetic.py` — no LLMs, no network, no randomness, no real
side effects) so the full lifecycle can be exercised under fault
injection. A real workload replaces the executor but keeps the
identical gate protocol: claim, renew, heartbeat, progress, stage,
verify, commit.

## The core loop

AXOS implements a closed control loop over declared work:

```
desired state
    ↓
actual state
    ↓
difference
    ↓
diagnosis
    ↓
repair / recovery
    ↓
verification
    ↓
reconciliation
    ↓
continue / replan / safely stop
```

Concretely:

- **Desired state** is declared as versioned `desired_state` items with
  a pinned head hash (`exec/reconciler.py`, gate methods
  `get_desired_head` / `ensure_job_for_desired_state`). The reconciler
  is deliberately the narrowest authority in the system: it creates
  `PENDING` jobs only through the gate's single idempotent op, never
  deletes, never moves a job between states, never completes or fails
  anything.
- **Actual state** is whatever the store rows say: job statuses,
  worker rows, lease triples, watchdog verdicts, ledger events.
- **Difference** is computed by `Reconciler.diff_item()` per desired
  item (`DESIRED_MISSING`, discrepancies, retired tombstones) and by
  the watchdog, which classifies execution as `HEALTHY` / `STALLED` /
  `DEAD` from heartbeats, progress evidence, and process identity.
- **Diagnosis** binds a failure to an execution identity
  (`job_id`, `fencing_token`) and a failure class (`STALLED`,
  `DEAD`, `LEASE_EXPIRED`, `STALE_AUTHORITY`, `UNCERTAIN`).
- **Repair / recovery** is the smallest authorized action through
  existing authorities only: R8 `reclaim` (rung 2, "re-claim/requeue")
  via `gate.reclaim_lease()`; `fence` / `restart` (rung 3,
  "replace/reassign") via `Supervisor.fence_sweep()` /
  `Supervisor.start_worker()`. R8 never signals processes itself and
  performs no budget arithmetic. R9 chooses the rung from the canonical
  5-rung ladder, bounds recovery with durable per-rung and per-incident
  budgets (consumed exactly once via a CAS watermark), and decides
  terminal escalation — including the human-gated `PAUSED_FOR_HUMAN`
  task state, which is sticky and never auto-cleared.
- **Verification** of recovery is evidence-based: an attempt succeeds
  only on **positive authoritative progress** (I-18); heartbeats never
  count, and "process restarted" never counts as recovery. The only
  completion path for a job is the R5 artifact protocol
  (`stage_artifact()` → `begin_commit()` → `verify_artifact()` →
  `commit_artifact()`); `TransitionGate.transition_job()` always
  rejects the `COMPLETE` target, and `commit_job_result()` exists only
  as a rejecting stub.
- **Reconciliation** closes the loop: the reconciler re-pins the
  desired head before every batch, refuses internally contradictory
  desired state with zero mutation (not even a run row), and stops
  creation on a mid-pass desired-state move.
- **Continue / replan / safely stop** is governed by the policy row
  lifecycle (R9 terminal states: `RECOVERY_COMPLETE`,
  `BUDGET_EXHAUSTED`, `R5_TERMINAL`, `BLOCKED_CONTRADICTORY`,
  `BLOCKED_STATE_UNAVAILABLE`, `SUPERSEDED` — terminal is sticky) and
  by R13 finalization, which flips a `READY` run to `FINALIZED`
  atomically inside one `write_txn` or refuses.

## Key facts

- **Release:** `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
  (frozen; see `src/axos/release_manifest.json`). Frozen means the
  implementation is fixed — it is the record, not a moving target.
- **Migration version:** 12 (`store/migrations.py`, 12 ordered atomic
  migrations; a failing migration leaves the schema untouched).
- **Policy version:** `r9-policy/v1` — policy rows carrying any other
  version are refused as `PolicyCorrupt`, never silently reinterpreted.
- **Platform:** stdlib-only Python 3.12.3, SQLite 3.45.1, Linux x86_64
  (from `release_manifest.json` identity block).
- **No configuration via environment variables.** The only `os.environ`
  touch in the tree is the supervisor copying the process environment
  to prepend `PYTHONPATH` when spawning a worker subprocess
  (`exec/supervisor.py`); there is no env-var configuration surface.
- **Source layout:** `src/axos/store/` (authority core), `src/axos/exec/`
  (control plane), `src/axos/audit/` (authority audit harnesses),
  `src/axos/tests/` (unit/integration/fault-injection suites).

## Status — stated honestly

- **Implemented:** the full authority core (store, gate, transitions,
  migrations v1–v12), execution substrate (supervisor, worker,
  synthetic executor, boot recovery), work intake (scheduler R10,
  reconciler R11), observation and recovery (watchdog R7, recovery R8,
  policy R9, circuit breakers R12), and release finalization (R13).
- **Tested:** 17 test files under `src/axos/tests/` covering the store,
  the F1/F2/F3 remediation regressions, each R-series (R5 artifacts,
  R6 boot, R7 watchdog, R8 recovery, R9 policy, R10 scheduler, R11
  reconciliation, R12 resilience, R13 finalization), fencing, reclaim,
  expiry, heartbeats, and the R14 hardening suite (R14-A…E, FI-01…FI-12).
- **Audited:** the `src/axos/audit/` harness suite (01–09) statically
  enforces the authority boundary — `exec/` never calls `write_txn()`,
  never opens its own `sqlite3` connections, never issues SQL writes —
  plus lease attacks, transition attacks, ledger tamper attacks, and
  crash/kill-point injection.
- **Verified:** internal failure-injection evidence (FI-01…FI-12) was
  gathered with real SIGKILLs of all AXOS processes mid-flight and
  boot-from-disk recovery; the release manifest's per-file SHA-256
  hashes pin the frozen tree. All R-series evidence is preserved under
  `reports/historical/`.
- **Blocked:** R14 external real-VM lifecycle proof is **BLOCKED** —
  hypervisor-level power-cycle proof could not be performed in this
  environment (no hypervisor tools, socket, or metadata service), and
  no substitute evidence was manufactured. This limitation is recorded
  as-is; it is not converted into a success here.
- **Historical:** Phase 0 design documents, R-series reports, and audit
  evidence under `reports/historical/` are snapshots of what was done
  at export time. They are preserved verbatim and are not re-proven
  by this repository.

## Core principles (as implemented)

- **Durable state is the only authority.** Reasoning, heartbeats, and
  process existence are evidence, never authority.
- **Reconciliation over assumptions.** Controllers read the store and
  converge; they never assume a prior state survived.
- **Replaceable workers.** A worker is a `worker_id` plus a disposable
  `proc_id`; a restart always mints a new process instance and must
  claim a fresh lease. Terminal worker identities are never resurrected.
- **Explicit ownership.** The lease triple `(owner_worker_id,
  lease_expires_at, fencing_token)` on the job row is the whole
  authority story; see [ownership-model.md](ownership-model.md).
- **Fencing.** Stale holders are fenced by token first (every write
  re-checks owner/token/live-lease) and physically by the supervisor's
  process-group fence sweep when durable authority says so.
- **Idempotency.** Claims, desired-job creation
  (`ensure_job_for_desired_state`), policy ensures, finalization
  re-begin, and scheduler-beat refreshes are all idempotent; replays
  are no-ops or lose compare-and-swap races cleanly.
- **Bounded recovery.** Per-rung and per-incident budgets, exactly-once
  consumption via a durable CAS watermark, a ladder that never rewinds,
  and sticky terminal states.
- **Progress-based recovery.** R8 attempts succeed only on positive
  authoritative progress; two consecutive zero-progress attempts
  escalate to `r9-policy`.
- **Verified checkpoints.** A checkpoint is identity-hashed (SHA-256 of
  the canonical artifact manifest), fully revalidated on verification,
  and only a checkpoint still in literal `VERIFIED` state is returned
  by `latest_known_good()`. Boot observes; it never repairs or advances
  pointers.
- **Safe stopping.** Terminal policy states are sticky; `PAUSED_FOR_HUMAN`
  is never auto-cleared; `Supervisor.close()` deliberately does not
  terminate worker OS processes; the finalizer performs zero writes
  over an already-`FINALIZED` generation.
- **Observable recovery.** Every attempt binds `(job_id,
  fencing_token)` and records its rung, budget context, and result in
  durable rows; process evidence (spawn/reap/fence) is recorded as
  hash-chained ledger milestones.
- **Actual failure testing.** `exec/synthetic.py` provides 12
  deterministic fault-injection behaviors (crash, hang, wedge, SIGKILL
  self, crash-after-stage, expire-then-commit, …) and the R14 suite
  kills real processes mid-transaction.
- **AI reasoning is not authoritative state.** No transition, verdict,
  or recovery outcome is valid because a model concluded it; it is
  valid only when the gate accepted it and the ledger records it.

## Further reading

- [control-plane.md](control-plane.md) — components and the authority
  boundary.
- [state-machine.md](state-machine.md) — the explicit transition graphs.
- [ownership-model.md](ownership-model.md) — the ownership chain.
- [data-flow.md](data-flow.md) — write path, ledger, checkpoints,
  reconciliation reads.
- `docs/contracts/` — the per-subsystem contracts.
- `docs/invariants/` — the invariant registry.
- `docs/recovery/` — the recovery model in depth.
