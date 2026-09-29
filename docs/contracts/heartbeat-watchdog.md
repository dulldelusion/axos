# Heartbeat and Watchdog Contracts

This document covers two contracts that are deliberately asymmetric:
heartbeats (R3/D9) are evidence that is never authority; the watchdog
(R7) is classification that is never action.

## Heartbeat (R3 / D9)

### Purpose

A heartbeat is a durable time-series liveness evidence record — proof
that a worker process was alive and willing to speak at a moment in
time. It is **never** authority: it cannot grant, extend, or restore any
execution right.

### Authoritative state

The **`heartbeats`** rows, one per accepted heartbeat, keyed by
`(worker_id, proc_id, hb_seq)`. `ts` is stamped with authoritative store
time on ingest; the worker-supplied timestamp is stored as informational
metadata only and never influences expiry, ordering, or fencing.

### Rules (the contract)

- `hb_seq` must strictly increase per `(worker_id, proc_id)`. A duplicate
  or out-of-order heartbeat is rejected **without mutating state**, so
  duplicate delivery can never create contradictory state.
- A heartbeat that names a job requires the current owner and the
  matching fencing token. Stale/fenced heartbeats are rejected: the
  rejection journals a `worker.heartbeat_fenced` milestone (observed vs
  authoritative token/owner) and raises `LeaseError`. The rejection
  mutates nothing else.
- The heartbeat may carry informational progress
  (`progress_done`/`progress_total`). When present it is routed to the
  same `_apply_progress` path as `update_job_progress` — identical
  checks, one atomic transaction, `progress_updated_at` stamped with
  store time. A heartbeat **without** progress never touches
  `progress_updated_at`. A heartbeat that names no job cannot carry
  progress.
- A heartbeat never extends a lease, never renews authority, never
  reclaims, never bumps the fencing token, and is never evidence of job
  success. No ledger event is recorded per accepted heartbeat
  (summaries and milestones only).

### Safety invariant

**D9:** heartbeat is liveness evidence, never progress. The watchdog
treats it accordingly: fresh heartbeats do not mask stale durable
progress (see the watchdog contract below). **R3:** every accepted
progress write stamps `jobs.progress_updated_at` with authoritative
store time; progress is informational only — never recovery evidence.

### Failure behavior

- Duplicate/out-of-order sequence -> `TransitionRejected`, zero mutation.
- Stale token or non-owner heartbeat naming a job -> `LeaseError`, zero
  mutation (plus the best-effort `worker.heartbeat_fenced` journal).
- Progress for a job outside the active execution window (not
  `CLAIMED`/`RUNNING`) -> rejected; terminal state cannot acquire
  further progress mutations (the rejection leaves the row byte-identical).

### Verification mechanism

- `TransitionGate.heartbeats_for(worker_id, proc_id=None)` — read-only
  access to the durable heartbeat evidence (newest first), consumed by
  the watchdog and by recovery verification snapshots.
- `update_job_progress()` / `_apply_progress()` — the single progress
  authority path, gated on current owner + valid token + live lease +
  active execution status.

### Relevant implementation

- `src/axos/store/gate.py` — `ingest_heartbeat`, `heartbeats_for`,
  `update_job_progress`, `_apply_progress`, `mark_worker_seen`
  (store-time `last_seen_at`).

---

## Watchdog (R7)

### Purpose

The watchdog answers: "is this execution still making authoritative
progress, or should it be considered STALLED/DEAD?" It **detects and
classifies only** — it never recovers, never reclaims, never fences,
never schedules, never completes.

### Authoritative state

The **`watchdog_verdicts`** rows, keyed by execution identity
`(job_id, fencing_token)`, plus the `watchdog.verdict` ledger events.
A fencing-token bump (reclaim) starts a new identity with no verdict
history.

Verdicts: `HEALTHY | STALLED | DEAD`.

- **HEALTHY** — sufficient authoritative evidence of continued progress:
  progress fresh within the configured threshold, valid ownership, live
  lease.
- **STALLED** — execution is still associated with an active job/owner,
  but authoritative progress (`jobs.progress_updated_at`, or the
  claim-time baseline when no progress was ever recorded) is stale beyond
  the configured threshold. Fresh heartbeats do **not** prevent STALLED.
  An expired lease (R4 observation) is reported as STALLED with
  lease-expired evidence — the watchdog never reclaims.
- **DEAD** — definitive death evidence only: the owner's worker row is in
  a terminal state, the owner's latest unreaped spawn record classifies
  as dead/reused under R6 process-identity rules (ESRCH/zombie, or the
  PID provably belongs to another process), or the supervisor's durable
  `worker.proc_reaped` milestone shows the owner's latest spawn
  generation was observed dead and reaped. A late heartbeat alone is
  never DEAD; stale progress alone is never DEAD; ambiguity resolves to
  the safer non-DEAD outcome.

### Safety invariant

- **DEAD is terminal for the execution identity.** Once DEAD, that
  identity stays DEAD: no observation resurrects it, and no verdict ever
  restores revoked leases, resets tokens, or changes ownership.
- Same-verdict re-evaluations are **zero-mutation no-ops** — the fast
  path does not even open a write transaction, so not even the store's
  commit timestamp is touched.
- The watchdog never redefines verdict semantics, never performs
  staleness math on worker-supplied timestamps, and never evaluates
  before the runtime reports boot READY (`WatchdogNotReady` otherwise).

### Failure behavior

The watchdog is fail-closed:

- If the store clock is unavailable or authoritative state cannot be
  safely read, `evaluate()` raises `WatchdogError` and writes nothing —
  a verdict is never invented from incomplete state.
- The scan is two-phase: **all** jobs' verdicts are computed from
  read-only evidence first; only then are transitions recorded. If any
  job's evidence is unreadable, the scan fails closed before anything is
  written — a scan never writes one job and then discovers another is
  unreadable.
- Staleness is strict: stale means `(now - ts) > threshold` — equal to
  the threshold is *not* stale. A missing timestamp is never stale on
  its own: absence of evidence is not evidence.

### Verification mechanism

`TransitionGate.record_watchdog_verdict()` persists a verdict through an
atomic compare-and-swap inside one write transaction: the latest verdict
for `(job_id, fencing_token)` is re-read inside the transaction, so two
concurrent watchdog instances produce exactly one authoritative
transition (BEGIN IMMEDIATE serializes the writers). Same-verdict and
post-DEAD re-evaluations roll back with true zero mutation. Each recorded
verdict carries its full evidence bundle (progress ages, heartbeat ages,
lease state, death evidence, process identity, thresholds used, previous
verdict) so any verdict is reconstructible from durable state.

The handoff to recovery is durable rows only: R8 consumes
`latest_watchdog_verdict()` and R12 consumes verdicts in its evidence
scan. There is no callback, queue, or signal from the watchdog to
recovery.

### Relevant implementation

- `src/axos/exec/watchdog.py` — `Watchdog` (two-phase `evaluate()`),
  `WatchdogConfig` (explicit, validated thresholds;
  `heartbeat_stale_s`, `progress_stale_s`, `evaluation_interval_s`),
  `WATCHDOG_VERDICTS`, `_is_stale`, `WatchdogError`, `WatchdogNotReady`.
- `src/axos/store/gate.py` — `record_watchdog_verdict` (atomic CAS),
  `latest_watchdog_verdict` (read-only), `_verdict_noop_result`.
- `src/axos/exec/boot.py` — `_classify_spawn` (the R6 process-identity
  rules the watchdog reads for death evidence; signal-0 existence probe
  only, no process signaling).

### Relevant tests

- `src/axos/tests/test_heartbeat_r3.py` — R3 durable heartbeat ingestion:
  durable row with store-time ts and informational worker ts; sequence
  monotonicity; duplicate rejection with zero mutation; stale fencing
  token rejected and journaled; owner-cleared heartbeat cannot restore
  ownership; replacement worker's new token accepted, old rejected.
- `src/axos/tests/test_watchdog_r7.py` — R7 deterministic detection:
  healthy worker with recent progress -> HEALTHY; stale progress with
  fresh heartbeats -> STALLED; heartbeat alone never counts as progress;
  fresh progress prevents STALLED even with zero heartbeats; definitively
  dead worker/process -> DEAD; missing heartbeat alone or stale progress
  alone never implies DEAD.
