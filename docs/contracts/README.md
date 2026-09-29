# AXOS Contracts

A contract in AXOS is an enforceable guarantee, not a style guide. Each
contract below names the authoritative state it governs, the transitions
that state may take, the safety invariant the gate enforces
transactionally, and the tests that prove the mechanism works. Where a
contract says "never", the implementation rejects the forbidden operation
inside the transaction and leaves authoritative state unchanged.

## Index

1. [Recovery](recovery.md) — the recovery loop: detect a failure, take the
   smallest authorized recovery action, verify it against authoritative
   evidence, and escalate when it did not work. Recovery success is
   determined by verified progress, never by the mere performance of an
   action.
2. [Checkpoint](checkpoint.md) — staged resume points: how checkpoints are
   created, what it takes for one to become trusted, and why creation
   without verification is never a checkpoint.
3. [Artifact](artifact.md) — verified result artifacts: the stage, verify,
   and commit protocol that makes an artifact the only evidence able to
   complete a job.
4. [Lease](lease.md) — the lease triple
   `(owner_worker_id, lease_expires_at, fencing_token)` on the job row:
   the exclusive execution authority for a job, and the single primitive
   that revokes it.
5. [Fencing](fencing.md) — how superseded workers are excluded from all
   authority, both in the store (fencing tokens) and in the OS
   (process-group fencing).
6. [Heartbeat and Watchdog](heartbeat-watchdog.md) — heartbeats as durable
   liveness evidence that is never authority, and the watchdog's
   deterministic HEALTHY/STALLED/DEAD classification.
7. [Reconciliation](reconciliation.md) — converging actual state toward
   declared desired state (R11), orphan redispatch (R10), and
   post-crash reconciliation of recovery actions (R8): observe and
   converge, never assume.
8. [Finalization](finalization.md) — releasing a generation only when
   durable evidence proves it is safe; the read-and-evaluate half of the
   release process.
9. [Uncertain Completion](uncertain-completion.md) — what happens when a
   job's outcome cannot be determined: inspect the durable evidence,
   adopt only what verifies, and never assume the work was done.

## How contracts relate to invariants

The contracts above are the operational form of the invariant registry in
`../invariants/invariants.md`. Every contract's safety-invariant section
names the invariant IDs it preserves (for example, I-4: no artifact-free
path to COMPLETE; I-18: recovery success requires positive authoritative
progress).
