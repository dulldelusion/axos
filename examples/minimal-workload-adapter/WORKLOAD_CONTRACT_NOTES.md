# WORKLOAD CONTRACT NOTES — "uppercase-jobs" example adapter

What the two sides of the adapter boundary owe each other, grounded in the
actual `TransitionGate` API used by `adapter_example.py`. Method names below
are the real public methods from `axos/store/gate.py` (exported via
`axos/store/__init__.py`).

## What the workload provides TO AXOS

| Obligation | In this example | Gate method(s) that consume it |
|---|---|---|
| Task definition | `create_task(..., {"objective": "uppercase payload", "payload": ...}, budgets, "scheduler")` | `create_task` |
| Job definition | `create_job(job_id, task_id, "stage-1", "scheduler")` | `create_job` |
| Execution function | `transform(payload) -> payload.upper()` — a pure function, run **outside** the gate; AXOS never inspects it | (none — deliberately) |
| Artifact spec | Bytes produced by the execution function, staged with a `kind` label (`"uppercase-result"`); the gate computes the content hash itself | `stage_artifact` |
| Success condition | Gate verification passes (byte re-read + recomputed sha256 + structural checks) and the atomic commit binds the verified artifact as the job's result | `verify_artifact`, `commit_artifact` |

## What AXOS provides TO the workload

| Guarantee | Gate method(s) | Notes |
|---|---|---|
| Durable state | `open_store`, `migrate`, `TransitionGate(store, staging_root)` | SQLite-backed; every mutation goes through a write transaction |
| Leased claims | `claim_job(job_id, worker_id, ttl_s, actor)` | PENDING → CLAIMED atomically; exactly one claimant wins; mints a fencing token |
| Fencing | fencing token read from `get_job(...)["fencing_token"]`, presented on every fenced call (`stage_artifact`, `begin_commit`, `verify_artifact`, `commit_artifact`) | Stale tokens are rejected before any terminal handling — a fenced worker never earns a success signal |
| Lease management | `renew_lease`, `release_lease` (public; unused by this toy, which finishes inside its TTL) | `claim_job` rejects non-positive/malformed TTLs before mutating |
| Checkpointing | `stage_checkpoint`, `verify_checkpoint`, `invalidate_checkpoint`, `latest_known_good` (public; unused by this toy) | Available through the same gate, same actor model |
| Recovery | `reclaim_lease`, `observe_expired_leases`, recovery-incident API (public; unused by this toy) | A reclaimed job's commit is only accepted via the resolver path with token lineage |
| Provenance | Ledger events appended on every transition (`job.claimed`, `job.transition`, `lease.renewed`, …) readable via `fencing_ledger_for_job`, `progress_evidence_for_job` | Every state change is an append-only, attributed event |
| Finalization | `commit_artifact` is the **sole** route to COMPLETE (`transition_job(..., "COMPLETE", ...)` is refused per I-4) | Completion is atomic: job COMPLETE + result artifact + content hash + completion ledger event in one transaction, with the full VERIFIED predicate recomputed against bytes on disk |

## Boundary invariants this example demonstrates

1. **One-way dependency.** The adapter imports from AXOS (`from axos.store
   import TransitionGate, open_store, migrate, TransitionRejected`). AXOS
   imports nothing from the adapter and knows nothing about "uppercase" —
   the workload's logic stays on the workload's side.
2. **No self-attestation.** The worker never declares its own hash valid
   (`stage_artifact` recomputes sha256 and rejects a mismatched
   `claimed_hash`); never declares its own artifact verified
   (`verify_artifact` re-reads the bytes and runs the checks itself); never
   declares its own job complete (only `commit_artifact` reaches COMPLETE).
3. **Fencing before state.** Every mutating call after the claim presents
   the fencing token first; identity is checked before state transitions.
4. **Skippable.** This directory adds no AXOS capability. Delete it and the
   core, its tests, and any other workload adapter behave identically.
