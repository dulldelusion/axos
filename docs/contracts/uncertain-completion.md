# Uncertain Completion Contract

## Purpose

Uncertain completion is what happens when a job's outcome cannot be
determined: the worker may have finished and staged bytes, but the crash
or reclaim cut the protocol off before the outcome was durably recorded.
The contract's single rule: **jobs whose outcome cannot be determined
are reconciled, never assumed.** Neither "the work finished" nor "the
work did not finish" may be manufactured from incomplete evidence.

## Authoritative state

- The **`jobs`** row in `UNCERTAIN` — the state that says "this execution
  was interrupted before its outcome was determined".
- The **`artifacts`** rows and **`validations`** receipts for the bytes
  the interrupted execution may have staged.
- The job's fencing token and the artifact's staging fencing token
  (token lineage).

## Allowed transitions

`UNCERTAIN` arises when a lease is reclaimed while execution was active:
`reclaim_lease()` moves `RUNNING | COMMITTING -> UNCERTAIN` (while
`CLAIMED -> PENDING` is a plain requeue, since nothing may have been
produced yet). From `UNCERTAIN`, the job may go:

- `UNCERTAIN -> COMPLETE` — only through the resolver path of
  `commit_artifact()`: a recovery authority **adopts** the staged bytes
  when they verify.
- `UNCERTAIN -> PENDING` — requeue when no adoptable evidence exists.
- `UNCERTAIN -> QUARANTINED` — the standard job-graph edge.

No transition *into* `UNCERTAIN` other than reclaim exists, and no
completion path from `UNCERTAIN` other than the resolver exists.

## Safety invariant

**Never trust pre-crash verification alone.** The gate re-reads the
bytes and recomputes the *full* VERIFIED predicate inside the completion
transaction — bytes present, hash matches the content hash, required
validator PASS receipts bound to the exact content hash — at commit
time. Adopt when the staged bytes verify; requeue when they do not.

**Token lineage (contract D5):** the resolver path requires the
artifact's recorded staging fencing token to *predate* the reclaim that
opened the UNCERTAIN state (`staging fencing_token < job fencing_token`),
proving the bytes were produced under the superseded epoch. Token
lineage comes from this job's own `artifact_stagings` record when one
exists (the deduplicated row's token may belong to another job's staging
epoch); legacy rows without a staging record fall back to the artifact
row.

**Resolver authority:** only actors in `RECLAIM_ACTORS` may run the
resolver path, and only when the UNCERTAIN job is ownerless. The
recovery controller does not complete jobs through any other route.

## Failure behavior

- The inspector never guesses. `inspect_uncertain_completion()` is a
  deterministic, read-only classification:
  - **COMMITTED** — job is `COMPLETE` and the referenced artifact is
    intact (bytes exist, hash matches, required receipts present).
  - **UNCERTAIN** — job is `COMPLETE` with an integrity contradiction,
    **or** the job is not `COMPLETE` but a fully verified artifact
    exists that the recovery authority could adopt via
    `commit_artifact()`.
  - **NOT_COMMITTED** — job not `COMPLETE` and no adoptable artifact
    exists.
- An artifact existing on disk does **not** itself prove completion,
  and a `COMPLETE` job with missing or corrupt artifact bytes is
  reported as an integrity contradiction — never silently manufactured.
- The `RUNNING -> UNCERTAIN` attempt transition
  (`mark_recovery_attempt_uncertain`) applies the same rule to recovery
  attempts: when the controller cannot determine whether the dispatched
  action completed, the attempt is marked UNCERTAIN and the next
  evaluation reconciles from durable evidence only.
- A resolver commit whose artifact fails verification at commit time is
  refused; the job stays `UNCERTAIN`.

## Verification mechanism

`inspect_uncertain_completion()` checks both directions:

- For a `COMPLETE` job: the referenced artifact row exists, the job's
  content hash equals the artifact's, the bytes re-hash to the expected
  hash, and every required validator holds a PASS receipt for the exact
  content hash.
- For a non-`COMPLETE` job: candidates are the artifacts this job
  staged (via the deduplicated row's `job_id` *or* a per-job staging
  record). An artifact is adoptable only when it satisfies the full
  VERIFIED predicate (status `VALIDATED`/`RELEASED`, bytes present and
  hash-matching, receipts bound to the content hash). Adoptable
  artifact IDs are listed for the recovery authority; everything else is
  reported as evidence, never acted on.

## Relevant implementation

- `src/axos/store/gate.py` — `commit_artifact()` (resolver path:
  `UNCERTAIN` adoption with token lineage), `inspect_uncertain_completion()`
  (deterministic read-only inspection),
  `_completion_integrity_problems()` (both-directions integrity check),
  `_require_staging_provenance()`, `_assert_artifact_verified()`,
  `mark_recovery_attempt_uncertain`, `reclaim_lease()` (the
  `RUNNING | COMMITTING -> UNCERTAIN` transition).
- `src/axos/store/transitions.py` — `JOB_TRANSITIONS` (`UNCERTAIN` edges),
  `RECLAIM_ACTORS`.
- `src/axos/exec/recovery.py` — `_reconcile()` (attempt-level uncertain
  resolution from durable evidence).

## Relevant tests

- `src/axos/tests/test_artifacts_r5.py` — R5-15 uncertain-completion
  inspection: COMMITTED / UNCERTAIN / NOT_COMMITTED dispositions over
  real bytes and rows.
- `src/axos/tests/test_recovery_r8.py` — UNCERTAIN attempt handling in
  the recovery drive loop (reconcile from durable evidence; never
  re-dispatch blindly).
