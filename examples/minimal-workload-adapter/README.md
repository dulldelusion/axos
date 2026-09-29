# Minimal workload adapter — example only

> **Toy/example only. NOT production code. NOT part of AXOS core.**
> Demonstrates the TransitionGate workload contract. Requires no
> credentials. Runs against a temporary SQLite database.

This directory is a pedagogical example of the **workload adapter
pattern**: a thin layer that sits *between* the AXOS core and a
workload's own logic, using only the public **TransitionGate** API. AXOS
runs fully without this directory; deleting it changes nothing about
the core. Skip it entirely if you bring your own workload adapter.

## What it is

A deliberately trivial toy workload (`adapter_example.py`,
"uppercase-jobs"): claim a task carrying a string payload, uppercase it,
stage the result as a content-addressed artifact, and commit it through
the gate. The protocol — not the domain — is what you read. It contains
no production data, prompts, canonical datasets, or application business
logic of any kind.

## What the adapter demonstrates (read-only from the adapter's side)

The adapter never touches private internals. Everything it needs comes
from the public TransitionGate surface:

- Durable tasks/jobs with a state machine (`create_task`, `create_job`,
  `transition_task`, `transition_job`)
- Leased claims with fencing tokens (`claim_job`, `renew_lease`,
  `release_lease`) so at most one worker owns a job at a time
- Content-addressed artifact staging where the gate computes the hash
  itself (`stage_artifact`)
- Gate-performed verification: the gate re-reads the bytes and runs the
  structural checks — no self-attestation (`verify_artifact`)
- A single atomic completion transaction (`commit_artifact`) — `COMPLETE`
  is *not* reachable through the generic transition API
- Checkpointing (`stage_checkpoint`, `verify_checkpoint`) and recovery
  machinery, unused by this toy but available through the same gate

## Run it

```
python3 adapter_example.py
```

It bootstraps an ephemeral, throwaway SQLite store in a temporary
directory, runs one uppercase job end-to-end through the claim→commit
protocol, prints each step, and exits. It needs no credentials, never
touches any production store, and writes nothing outside its own temp
files.

## The claim→commit protocol (mapped to `TransitionGate` methods)

1. **Define work** — `create_task` + `create_job` (scheduler-class actor;
   the job carries the payload in its task objective).
2. **Claim** — `claim_job(job_id, worker_id, ttl_s, actor)` atomically
   moves PENDING → CLAIMED and mints a fencing token. Exactly one
   claimant wins.
3. **Start** — `transition_job(..., "RUNNING", actor)`.
4. **Execute** — the workload's own pure function runs *outside* the gate
   (here: `payload.upper()`). The gate never sees your business logic.
5. **Stage** — `stage_artifact(job_id, worker_id, fencing_token, ...)`
   writes the bytes and registers a content-addressed artifact in STAGING.
   The gate computes `sha256` itself; a worker-claimed hash that
   mismatches is rejected.
6. **Commit phase** — `begin_commit(...)` moves the job RUNNING →
   COMMITTING.
7. **Verify** — `verify_artifact(artifact_id, ...)` has the gate re-read
   the bytes, recompute the hash, and run structural checks (STAGING →
   VALIDATED).
8. **Complete** — `commit_artifact(...)` atomically moves the job to
   COMPLETE with the verified artifact bound as the result. This is the
   *only* route to COMPLETE.

## Related reading

- `WORKLOAD_CONTRACT_NOTES.md` — the TransitionGate workload contract
  this adapter implements against
- `../../docs/contracts/` — the AXOS contracts this adapter exercises
