# Artifact Contract

## Purpose

An artifact is worker-produced result bytes that have survived
gate-performed verification. Artifacts are the only evidence that can
complete a job: there is no path from execution to `COMPLETE` that does
not pass through a verified artifact (I-4). The artifact contract
defines the stage -> verify -> commit pipeline and the guarantees each
step carries.

## Authoritative state

- **`artifacts`** — one row per artifact, content-addressed:
  `artifact_id = sha256(bytes)`. The same bytes always produce the same
  artifact identity; different bytes can never share one. Columns carry
  the gate-computed `content_hash`, `size`, `uri`, `status`, producer
  provenance, and the fencing token of the staging epoch.
- **`artifact_stagings`** — per-job staging provenance: the row proving
  that a given job staged a given artifact under a live lease. Two jobs
  producing identical bytes share one deduplicated `artifacts` row, but
  each job gets its own staging record — and the *record*, not the row's
  first-writer `job_id`, is the authority for "this job staged these
  bytes" (R14).
- **`validations`** — validator receipts bound to exact content hashes.

Artifact status: `STAGING | VALIDATED | QUARANTINED | RELEASED`
(see `ARTIFACT_TRANSITIONS` in `src/axos/store/transitions.py`).

## Allowed transitions

- `stage_artifact()`: job must be `RUNNING` under the caller's live lease
  (owner + token + unexpired, checked inside the transaction). The gate
  computes `sha256(data)` itself; a worker-supplied `claimed_hash` that
  differs is rejected. Bytes are written with file and directory fsync
  *before* the artifact row is registered. Row is created in `STAGING`.
  Idempotent: re-staging identical bytes returns the existing row via
  `ON CONFLICT DO NOTHING` (never contradictory identities).
- `begin_commit()`: `RUNNING -> COMMITTING` — opens the fenced commit
  phase, asserts the live lease and that the named artifact was staged
  for this job with bytes present. The job is still not COMPLETE.
- `verify_artifact()`: `STAGING -> VALIDATED` — the gate re-reads the
  staged bytes, recomputes the hash, runs the structural checks, and
  records validator receipts with gate-computed results. Nobody can set
  VERIFIED by assertion: there is no setter, only this predicate. When
  the caller is a worker-owned actor, it must present its fencing token
  and still hold the live lease; a stale worker cannot mark artifacts
  verified. Authority actors (supervisor/system/test) verify without a
  lease.
- `commit_artifact()`: the authoritative completion transaction —
  `COMMITTING -> COMPLETE` (worker path) or `UNCERTAIN -> COMPLETE`
  (resolver path; see [Uncertain Completion](uncertain-completion.md)).
- `fail_job_execution()`: `CLAIMED | RUNNING | COMMITTING -> FAILED`
  (failure path; FAILED references no artifact). Idempotent; failing an
  already-COMPLETE job is rejected as a contradiction.
- Corrupt bytes quarantine the artifact: `STAGING -> QUARANTINED`
  (retained for forensics, never silently deleted).

The old `commit_job_result()` direct-completion path (CLAIMED/RUNNING ->
COMPLETE on a result string, no artifact, no hash, no validation) was
removed in the I-4 correction. Only a stub remains, and it raises
`TransitionRejected` to fail loudly if any caller was missed.

## Safety invariant

**I-4: no artifact-free path to `COMPLETE`.** The generic
`transition_job()` API *refuses* any transition to COMPLETE with the
message that COMPLETE is reachable only through `commit_artifact()`
with a verified artifact. In the completion transaction,
`commit_artifact()` re-reads the job, the artifact, and the verification
receipts, recomputes the full VERIFIED predicate against the bytes on
disk *right now*, and only then writes `COMPLETE` +
`result_artifact_id` + `content_hash` + the `job.committed` ledger
event, atomically.

**Fencing before state:** `_check_fenced_execution()` (current owner,
current token, live lease) runs inside the transaction before any
terminal handling — a stale token is rejected even against an
already-COMPLETE job, so a fenced worker never earns a success signal.
The worker path requires the owning worker's identity; a job in
COMMITTING cannot be completed by an anonymous authority.

**Linkage is per-job staging provenance:** `begin_commit`,
`verify_artifact`, and `commit_artifact` resolve "this job staged these
bytes" through the `artifact_stagings` record, never through the
deduplicated row's first-writer `job_id`.

## Failure behavior

- Staging with a mismatched worker-claimed hash is rejected.
- Staging when the job is not `RUNNING`, the task does not own the job,
  or the lease is not live is rejected with zero mutation.
- Structural verification failure quarantines the artifact. The
  evaluation transaction performs **zero writes** on the failure path
  (it rolls back cleanly); a second forensic transaction re-reads the
  bytes and records the FAIL receipt + quarantine only if the bytes
  still fail — a quarantine is never written on stale evidence.
- Re-committing an already-COMPLETE job with the *same* verified artifact
  is a no-op returning the current row (no duplicate ledger event); a
  *different* artifact is a contradictory duplicate completion and is
  rejected. There is never a durable state where the job says COMPLETE
  but the referenced verified artifact does not exist or lacks required
  verification evidence.
- Bytes that hit durable storage without a registered row are inert
  orphan files: `classify_staging_orphans()` reports them, and they can
  never complete a job.

## Verification mechanism

- The gate computes `sha256(data)` itself on staging and recomputes it
  on verification and on commit, each time from the bytes on disk.
- `REQUIRED_VALIDATORS = (("axos-structural", "1"),)` — the structural
  bar. Every required validator must hold a PASS receipt bound to the
  exact content hash; a receipt for another hash never validates this
  artifact. Semantic validators plug in later via the same receipts
  table.
- `_assert_artifact_verified()` re-asserts the full VERIFIED predicate
  (status, bytes present, hash match, receipts) inside the caller's
  transaction — used by `commit_artifact()` so the completion transaction
  never trusts an earlier predicate alone.
- `get_artifact()` is the read-only read path; ledger events
  `artifact.staged`, `artifact.verified`, `artifact.quarantined`, and
  `job.committed` provide the tamper-evident trail.

## Relevant implementation

- `src/axos/store/gate.py` — `stage_artifact` (gate-computed hash, fsync,
  idempotent dedup, per-job staging provenance), `begin_commit`
  (RUNNING -> COMMITTING), `verify_artifact` (the VERIFIED predicate;
  forensic quarantine path), `commit_artifact` (the authoritative
  completion transaction), `fail_job_execution`, `get_artifact`,
  `classify_staging_orphans`, `_assert_artifact_verified`,
  `_check_fenced_execution`, `REQUIRED_VALIDATORS`, `commit_job_result`
  (removed-path stub).
- `src/axos/store/transitions.py` — `ARTIFACT_TRANSITIONS`.

## Relevant tests

- `src/axos/tests/test_artifacts_r5.py` — R5 verified artifacts and
  artifact integrity: deterministic content addressing; worker-claimed
  checksum mismatch rejected; real staging with fsync'd bytes and
  gate-computed hash; validator provenance and hash binding; real
  corruption -> quarantine with a durable FAIL receipt; VERIFIED requires
  validators (no setter, no bypass); uncertain-completion inspection
  (R5-15).
