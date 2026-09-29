# Ownership Model

Ownership in AXOS is explicit, durable, and checked on every write.
A piece of work is never "owned" by a process that exists; it is
owned by a row triple that the store accepted. The chain is:

```
worker
  ↓
attempt
  ↓
claim
  ↓
fencing
  ↓
artifact
  ↓
verification
  ↓
authoritative completion
```

Each link is a separate, separately-checked authority. This document
explains each link and the rules that bind them.

## The five identities (`exec/identity.py`)

Five identities, never collapsed:

- **`worker_id`** — the durable logical worker.
- **`proc_id`** — one OS process instance. A restart always mints a
  fresh one via `new_proc_id()`; a worker with the same `worker_id`
  but a new `proc_id` inherits nothing.
- **`job_id`** — the unit of work; the store's truth.
- **`lease_id`** — `job_id#fencing_token`: one lease epoch
  (`LeaseRef.lease_id`).
- **`fencing_token`** — a monotonic authority counter; higher means
  newer. An old token can never become valid again.

The worker process itself never transitions its own worker row; the
supervisor does. The worker never manufactures authority: with
`--expect-token` it *verifies* the durable
`(owner_worker_id, fencing_token)` triple and exits
`EXIT_STALE_TOKEN` (7) with zero side effects on mismatch.

## The lease triple

Lease authority is the **job-row triple**

```
(owner_worker_id, lease_expires_at, fencing_token)
```

stored on the job row and owned by the gate. Every gate write that
acts on an executing job re-checks all three: current owner,
matching token, live lease (`lease_expires_at > now` against the
authoritative store clock). A write with any of the three wrong is
refused — either `TransitionRejected` for a legal-but-refused
transition, or `LeaseError` for lost fencing authority.

- **Claim** (`_claim_job_txn`, called by `claim_job()` /
  `claim_job_resilient()`): atomic `PENDING → CLAIMED` — owner set,
  fencing token incremented, lease timestamps set — in one
  transaction. TTL must be a positive number before the transaction
  opens (F2).
- **Renewal** (`gate.renew_lease`): requires owner, matching token,
  active status, and an **unexpired** lease. It extends only the
  current triple — never resurrects a superseded token, never steals
  another lease. Renewal is not a heartbeat and confers no new
  authority; it is executed by the worker's daemon renewal thread on
  its own store connection via the sanctioned path.
- **Release** (`gate.release_lease`): the owner may release its own
  lease.
- **Expiry** (`gate.observe_expired_leases`): read-only observation
  with inclusive expiry (`lease_expires_at <= now`) against the
  authoritative store clock. Expiry alone moves nothing; the reclaim
  authority moves it.
- **Reclaim** (`gate.reclaim_lease`): atomically verifies evidence,
  increments the fencing token, clears owner/lease, changes state
  (`CLAIMED → PENDING`; `RUNNING | COMMITTING → UNCERTAIN`), and
  appends ledger evidence. Routine reclaim requires actual expiry;
  forced reclaim requires a `DEAD` or `STALLED` watchdog verdict plus
  `incident_id`. Reclaim actors are `recovery-controller`,
  `reconciler`, `system`, `operator`, `test` — the supervisor is
  deliberately excluded.

## Fencing token semantics

The `fencing_token` is the generation counter of authority over a
job. Every successful claim or reclaim increments it. Semantics:

1. **Monotonic.** A higher token always means a newer, superseding
   authority. The gate never decrements it.
2. **Self-invalidating.** Any write presenting an older token is
   refused: the old holder discovers its loss not by notification
   but by the store refusing its next write (`LeaseError` →
   `FencedError` → the worker stops immediately; on SIGTERM it
   stops promptly and commits nothing).
3. **Bound to the attempt identity.** Recovery attempts bind
   `(job_id, fencing_token)`; a changed token aborts stale attempts
   unless durable dispatch evidence proves the attempt caused the
   change. Watchdog verdict identity is likewise
   `(job_id, fencing_token)`.
4. **Lineage gates the uncertain path.** `commit_artifact()` from
   `UNCERTAIN` (the crash-after-stage adoption path) is available
   only to reclaim-authority actors and requires the artifact's
   token lineage to predate the reclaim.

Physical fencing backs the token: when durable authority says a
holder is stale, `Supervisor.fence_sweep()` detects the owner/token
divergence and signals the process **group** (`SIGTERM`, then
`SIGKILL` if required), reaps it, and records `worker.fence_enforced`
as a ledger milestone. On unreadable authority the sweep fails closed
and signals nothing. Boot uses the same sweep for force-reclaims of
live leases whose owner process is definitively dead or whose PID was
reused (in the `PID_REUSE` disposition it never kills).

## Claim → artifact → verification → completion

Ownership of *execution* (the lease triple) is distinct from the
authority to *complete*. The chain from claim to authoritative
completion is the R5 protocol, and it is the only path to
`COMPLETE`:

1. **Claim.** `PENDING → CLAIMED` under the lease triple (scheduler
   dispatch via `claim_job_resilient()` or worker claim via
   `claim_job()`).
2. **Execution.** `CLAIMED → RUNNING` (informational; fencing is
   enforced at commit). Heartbeats and progress flow through the
   gate under the live triple; progress is accepted only in
   `CLAIMED`/`RUNNING` (F3).
3. **Fencing.** Any owner/token/live-lease mismatch at any step
   refuses the write. A superseded holder cannot proceed: its next
   commit attempt is the fencing test.
4. **Artifact.** `stage_artifact()` computes the SHA-256 content
   hash itself over `fsync`'d bytes (content-addressed), then
   `begin_commit()` moves the job `RUNNING → COMMITTING` under
   fencing.
5. **Verification.** `verify_artifact()` revalidates bytes, hashes,
   and validator receipts; the required validator tuple currently
   contains only `("axos-structural", "1")`.
6. **Authoritative completion.** `commit_artifact()` re-reads bytes
   and validator receipts inside the completion transaction and
   moves `COMMITTING → COMPLETE` atomically.

`TransitionGate.transition_job()` always rejects the `COMPLETE`
target, and `commit_job_result()` exists only as a rejecting stub —
there is no shortcut from claim to completion. If the worker dies
after staging but before commit (`crash_after_stage` in
`exec/synthetic.py`), the job moves to `UNCERTAIN` on reclaim and the
durable staged bytes are adopted only if they verify (otherwise
requeued) — the uncertain-completion path, resolved from evidence,
not from assumption.

## PAUSED_FOR_HUMAN — the ownership stop

`PAUSED_FOR_HUMAN` is the task-level ownership stop (I-17): the task
exits it only through a recorded human `APPROVED` approval. While
paused: the scheduler never admits new work under the task, the
reconciler never creates work under it, and R9 stands down rather
than working around it. R9's terminal rung 5 may *enter* the state
via `TransitionGate` — best-effort, journaled, never auto-cleared.
No automated component holds the key out of this state.

## Summary of the invariant

> Only the holder of the current `(owner_worker_id,
> fencing_token)` triple with a live `lease_expires_at` may write
> execution evidence for a job; only the R5 protocol may move a job
> to `COMPLETE`; only a recorded human approval may release
> `PAUSED_FOR_HUMAN`; and only reclaim-authority actors may adopt
> uncertain work, and only when the token lineage proves it predates
> the reclaim.

See [state-machine.md](state-machine.md) for the transition graphs
that encode these rules, [control-plane.md](control-plane.md) for
which components hold which link, and [data-flow.md](data-flow.md)
for how every link is written through the gate and recorded in the
ledger.
