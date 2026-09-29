# Fencing Contract

## Purpose

Fencing excludes superseded workers from all authority — in the store,
where stale authority is rejected, and in the OS, where stale processes
are contained. Together, the two halves guarantee that only the current
lease epoch can act, and that a worker from a previous epoch cannot
interfere with execution it no longer owns.

## Authoritative state

- The **`fencing_token`** on the `jobs` row: the durable fencing identity.
  Every claim and every reclaim increments it; it is monotonic.
- The **`worker.fence_enforced`** ledger event: exactly one per enforced
  physical fencing, per `(worker_id, proc_id)`.
- The **`worker.heartbeat_fenced`** ledger milestone: the journaled
  evidence of a fencing rejection at the heartbeat path (observed vs
  authoritative token/owner).

## Allowed transitions

Fencing itself is not a state machine with branches — it is a monotonic
token and a set of checks. The transitions it governs:

- Token N -> N+1 on reclaim (`reclaim_lease()`), atomically with owner
  clearing and the job-state transition. The old token can never become
  valid again.
- Physical fencing: a stale process (the store says the lease moved; a
  process still runs under the old identity) is signaled and reaped by
  the supervisor's `fence_sweep()`; exactly one `worker.fence_enforced`
  ledger event is recorded per enforcement.

## Safety invariant

**Fencing is checked before state, inside the transaction.** The
worker-side fencing triple (current owner, current token, live lease) is
enforced by `_check_fenced_execution()` in `src/axos/store/gate.py`,
called *inside* the caller's write transaction so the checks serialize
with the mutation (no check-then-act gap). It applies identically to
`stage_artifact`, `begin_commit`, `verify_artifact`, `commit_artifact`,
`fail_job_execution`, `update_job_progress`, and `ingest_heartbeat`.

A stale token is rejected **even against an already-COMPLETE job** — a
fenced worker never earns a success signal. Authority questions outrank
state questions: a fenced worker is rejected as fenced (`LeaseError`)
even when the job has moved on.

## Failure behavior

- Stale worker calls -> `LeaseError` with zero mutation. A fencing
  rejection at the heartbeat path journals a `worker.heartbeat_fenced`
  milestone in a *separate* best-effort transaction (the evidence
  survives the rollback); journaling must never mask the `LeaseError`.
- **R2 — physical fencing** (`Supervisor.fence_sweep()` in
  `src/axos/exec/supervisor.py`): detects durable owner/token divergence
  and contains it by signaling the **process group** with `SIGTERM`,
  then `SIGKILL` if required, reaping, and recording exactly one
  `worker.fence_enforced` ledger event per enforcement. Process-group
  (not process) signaling is what contains a wedged worker that spawns
  children and grandchildren — the whole tree shares the worker's
  process group. On unreadable authority the sweep fails closed and
  signals nothing.
- Authority boundaries: R8 dispatches fencing only through the R2
  mechanism (`supervisor.fence_sweep()`); it never signals a process
  itself. Boot uses the same mechanism; boot never signals processes
  either. The watchdog never fences. R1 never touches processes:
  revocation is a store fact; physical containment is R2's job.

## Verification mechanism

- `TransitionGate.fencing_ledger_for_job()` returns the
  authority-changing ledger events (`job.claimed`, `job.lease_reclaimed`)
  for one job in sequence order — the durable revocation evidence used
  to judge whether a worker's authority epoch provably ended. It performs
  no authority judgment itself and never mutates state.
- `TransitionGate.unreaped_proc_spawns()` and
  `latest_spawn_generation()` provide the durable process-identity
  evidence (spawn/reap milestones, exact start identity, process group)
  the fence path and the R6 classifier read.
- The enforced-fencing path is measured: fence-to-process-group-death
  latency was measured below the sweep heartbeat at freeze
  (0.1075–0.1274s vs H=0.4s).

## Relevant implementation

- `src/axos/store/gate.py` — `_check_fenced_execution` (the triple
  check), `fencing_ledger_for_job` (durable revocation evidence),
  `unreaped_proc_spawns`, `latest_spawn_generation`, `reclaim_lease`
  (the token increment that fences out the old owner).
- `src/axos/exec/supervisor.py` — `fence_sweep()` (R2 physical fencing;
  one enforced pass), the background sweep loop.
- `src/axos/exec/recovery.py` — `_dispatch_fence` (dispatches fencing
  only through the R2 mechanism).

## Relevant tests

- `src/axos/tests/test_fence_enforce.py` — R2 enforced process-group
  fencing: external authorized R1 reclaim triggers automatic sweep
  enforcement; process-group death kills parent, child, and grandchild;
  graceful SIGTERM without SIGKILL; SIGKILL escalation when SIGTERM is
  ignored; measured fencing-transaction -> group-death latency under H;
  continuous sweep cadence; owner divergence detected after restart.
