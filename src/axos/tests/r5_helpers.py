"""Phase 1C R5 test helper: the full artifact completion flow.

Replaces the removed commit_job_result() in pre-R5 tests. Runs the single
authoritative completion contract:

    stage_artifact -> begin_commit -> verify_artifact -> commit_artifact

for SUCCESS, and fail_job_execution() for FAILURE — exactly what a real
worker process does. Deterministic test bytes (content-addressed) unless
the caller supplies its own.

Concurrency note: a per-job lock serializes the multi-step protocol
within one test process so the duplicate-same-token test can assert zero
spurious errors. The lock does NOT weaken the gate: every step still
re-validates the fencing triple inside its own transaction, and stale
tokens are still rejected there (the TOCTOU property under test).
"""
from __future__ import annotations

import hashlib
import threading

from axos.store import TransitionRejected

_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def _job_lock(job_id: str) -> threading.Lock:
    with _locks_guard:
        return _locks.setdefault(job_id, threading.Lock())


def r5_complete(gate, *, job_id: str, worker_id: str, fencing_token,
                task_id: str, outcome: str, evidence, actor: str,
                data: bytes | None = None, kind: str = "test"):
    """Complete a job through the R5 artifact protocol (test-only).

    SUCCESS: stage real bytes -> begin_commit -> verify -> atomic commit.
    The begin_commit/verify steps are duplicate-delivery tolerant: when
    the job already left RUNNING (another delivery committed or is
    committing), commit_artifact's own idempotent path decides — same
    artifact is a no-op, contradiction is rejected. FAILURE: explicit
    fail_job_execution.

    Returns the final job row.
    """
    if outcome not in ("SUCCESS", "FAILURE"):
        raise TransitionRejected(f"unknown commit outcome {outcome!r}")
    if outcome == "FAILURE":
        reason = str((evidence or {}).get("reason", "test failure"))
        return gate.fail_job_execution(
            job_id, worker_id, fencing_token, actor=actor,
            reason=reason, evidence=evidence)
    data = b"test-artifact-" + job_id.encode("utf-8") if data is None \
        else data
    # Content addressing: the artifact id is computable from the bytes
    # alone, so duplicate delivery can fall through to the atomic commit
    # even when staging itself is past (the job already left RUNNING).
    artifact_id = hashlib.sha256(bytes(data)).hexdigest()
    with _job_lock(job_id):
        try:
            artifact = gate.stage_artifact(
                job_id=job_id, worker_id=worker_id,
                fencing_token=fencing_token, task_id=task_id, kind=kind,
                data=data, actor=actor)
            artifact_id = artifact["artifact_id"]
            try:
                gate.begin_commit(job_id, worker_id, fencing_token,
                                  artifact_id=artifact_id, actor=actor)
                gate.verify_artifact(artifact_id, actor=actor,
                                     worker_id=worker_id,
                                     fencing_token=fencing_token,
                                     job_id=job_id)
            except TransitionRejected:
                # Duplicate delivery at an intermediate protocol step (or
                # an artifact the gate already verified): the atomic
                # commit is the single decider.
                pass
        except TransitionRejected:
            # Staging itself is past (duplicate delivery after the job
            # left RUNNING): commit_artifact's idempotent path decides —
            # same artifact is a no-op, contradiction is rejected.
            # LeaseError is never swallowed: fencing failures propagate.
            pass
        return gate.commit_artifact(
            job_id, worker_id, fencing_token, artifact_id=artifact_id,
            actor=actor, evidence=evidence)
