"""Phase 1A — the transition-gate API.

The single narrow API through which authoritative state transitions occur:

    read state -> validate transition -> begin transaction ->
    verify preconditions -> apply transition ->
    record authoritative transaction time -> record ledger event -> commit

No kernel component may mutate authoritative state behind this API; the gate
is the enforcement boundary. Invalid transitions are rejected transactionally
and leave authoritative state unchanged.

The boundary is structural, not conventional (F1 remediation): the gate holds
the writable Store; every other component receives a ReadOnlyStore, which
exposes no writable connection and no transaction capability. Store.conn is
read-only (PRAGMA query_only=ON); Store.write_txn() is the gate-owned
transaction capability below.

Authoritative time (Phase 0 Q8/ADR-008): every mutation stamps rows with the
store's own monotonic transaction time from Store.write_txn. A worker-supplied
timestamp, if present, is stored in `worker_reported_ts` as informational
metadata and is NEVER used for lease, expiry, ordering, or fencing semantics.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid

from .db import (Store, TransitionRejected, LeaseError, PolicyConflict,
                 DesiredStateConflict, BreakerConflict, FinalizationConflict)
from . import transitions as T


def _canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


# ------------------------------------------------- R11 desired-state helpers
# Canonical identity for a desired-work item: the deterministic job_id the
# gate's idempotent creation op mints. The domain tag pins the hash
# construction; the 32-hex-char suffix keeps job_ids compact while the
# full desired_work_id remains recoverable from the desired_job_map row.
def _canonical_desired_job_id(desired_work_id: str) -> str:
    return ("dj-"
            + hashlib.sha256(
                ("axos-desired-job:v1:" + desired_work_id).encode()
            ).hexdigest()[:32])


_DESIRED_SPEC_FIELDS = ("task_id", "stage_id", "max_attempts", "policy")


def _normalize_desired_spec(spec: dict) -> dict:
    """Validate and normalize a desired-work spec to the canonical four
    fields. Unknown keys are rejected (a spec is exactly this shape —
    anything else is a caller bug, never silently carried). Defaults:
    stage_id=None, max_attempts=3, policy={}."""
    if not isinstance(spec, dict):
        raise TransitionRejected(
            f"desired spec must be a dict, got {type(spec).__name__}")
    unknown = sorted(k for k in spec if k not in _DESIRED_SPEC_FIELDS)
    if unknown:
        raise TransitionRejected(
            f"unknown desired spec fields: {unknown}")
    task_id = spec.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        raise TransitionRejected(
            "desired spec 'task_id' must be a non-empty string")
    stage_id = spec.get("stage_id")
    if stage_id is not None and not isinstance(stage_id, str):
        raise TransitionRejected(
            "desired spec 'stage_id' must be a string or null")
    max_attempts = spec.get("max_attempts", 3)
    if (isinstance(max_attempts, bool) or not isinstance(max_attempts, int)
            or max_attempts < 1):
        raise TransitionRejected(
            "desired spec 'max_attempts' must be a positive int,"
            f" got {max_attempts!r}")
    policy = spec.get("policy", {})
    if not isinstance(policy, dict):
        raise TransitionRejected("desired spec 'policy' must be a dict")
    return {"task_id": task_id, "stage_id": stage_id,
            "max_attempts": max_attempts, "policy": policy}


def _desired_spec_hash(spec: dict) -> str:
    """Stable identity of a normalized desired spec: sha256 of its
    canonical JSON. The gate binds the created job's map row to this
    hash; a spec that drifts under an existing mapping is contradictory
    actual state (DesiredStateConflict)."""
    return hashlib.sha256(
        _canon(_normalize_desired_spec(spec)).encode()).hexdigest()


def _desired_snapshot_hash(items) -> str:
    """Stable identity of the whole desired set: sha256 of the canonical
    JSON of the sorted item list (retired items INCLUDED — a retire is a
    state change the head must observe). Each item is
    {"id": str, "spec": dict, "retired": int, "version": int}; `spec` must
    already be parsed (not the raw JSON string). The reconciler reuses
    this exact function to verify the head's snapshot_hash."""
    canon_items = sorted(
        ({"id": it["id"], "spec": it["spec"],
          "retired": int(it["retired"]), "version": int(it["version"])}
         for it in items),
        key=lambda d: d["id"])
    return hashlib.sha256(_canon(canon_items).encode()).hexdigest()




# Phase 1C R5 — the Phase 1C validation bar (contract D5). Structural only:
# bytes present and readable, gate-recomputed hash matches, size and
# provenance complete. Semantic validators (M6) plug in later via
# record_validation; they join this tuple when they exist.
REQUIRED_VALIDATORS: tuple[tuple[str, str], ...] = (("axos-structural", "1"),)# Phase 1C R5 — checkpoint creation triggers (contract D8). Only these.
# Graph CHECKPOINT_NODE is deferred with multi-stage graphs; a bare timer
# without verification capacity is never a trigger (doc 13.2).
CHECKPOINT_TRIGGERS: tuple[str, ...] = (
    "policy",               # task/stage policy: every N units or M minutes
    "pre_risky_operation",  # before replan / breaker / safe stop / approval
    "operator_request",     # explicit operator request
)


def _is_worker_actor(actor: str) -> bool:
    return isinstance(actor, str) and actor.startswith("worker:")


class _ArtifactVerifyFailed(Exception):
    """Internal: the evaluation transaction of verify_artifact() decided
    the bytes fail structural verification. Carries the evidence for the
    forensic transaction; never escapes verify_artifact() (callers see
    TransitionRejected)."""
    def __init__(self, artifact_id: str, expected: str, details: dict):
        super().__init__(artifact_id)
        self.artifact_id = artifact_id
        self.expected = expected
        self.details = details


def _check_ttl(ttl_s: float, op: str) -> None:
    """Validate a lease TTL before any mutation (F2).

    Phase 0's lease model defines ttl as a *duration* added to the
    acquisition time (lease_expires_at = acquired_at + ttl). A TTL that is
    not a positive number cannot produce a live lease: zero/negative values
    mint stillborn (immediately-expired) leases, and NaN poisons every
    expiry comparison. Reject before the transaction opens, so invalid
    input can never mutate state.
    """
    if isinstance(ttl_s, bool) or not isinstance(ttl_s, (int, float)) \
            or not ttl_s > 0:
        raise TransitionRejected(
            f"{op}: ttl_s must be a positive number of seconds, "
            f"got {ttl_s!r}")


# ------------------------------------------------- R12 breaker primitives
# Circuit-breaker admission (Phase 1C R12): one breaker row per scope
# guards the claim path. Scopes are hierarchical — GLOBAL, TASK, DESIRED
# (the R11 desired-work item, when the job was materialized from desired
# state), JOB — and claims evaluate them in that fixed order. The state
# machine is CLOSED -> OPEN -> HALF_OPEN -> CLOSED | OPEN; only the
# recovery controller transitions rows (the admission path never moves a
# breaker itself — an OPEN row with an elapsed cooldown still denies
# until the controller transitions it, fail closed).
_BREAKER_SCOPES = frozenset({"GLOBAL", "JOB", "TASK", "DESIRED"})
_BREAKER_STATES = frozenset({"CLOSED", "OPEN", "HALF_OPEN"})
_BREAKER_FAILURE_KINDS = frozenset(
    {"R7_DEAD", "R8_ATTEMPT_FAILED", "R9_ESCALATED", "RECOVERY_PRESSURE"})
_BREAKER_TRANSITIONS = {
    "CLOSED": ("OPEN",),
    "OPEN": ("HALF_OPEN",),
    "HALF_OPEN": ("CLOSED", "OPEN"),
}


def _check_breaker_scope(scope_type: str, scope_id: str) -> None:
    """Validate a breaker scope before any read or mutation. A malformed
    scope is a caller bug: fail fast rather than letting a typo'd scope
    silently resolve to "no row" (which the admission path reads as
    CLOSED/allow)."""
    if scope_type not in _BREAKER_SCOPES:
        raise TransitionRejected(
            "breaker scope_type must be one of"
            f" {sorted(_BREAKER_SCOPES)}, got {scope_type!r}")
    if not isinstance(scope_id, str) or not scope_id:
        raise TransitionRejected(
            "breaker scope_id must be a non-empty string,"
            f" got {scope_id!r}")
    if scope_type == "GLOBAL" and scope_id != "global":
        raise TransitionRejected(
            "breaker GLOBAL scope_id must be 'global',"
            f" got {scope_id!r}")


def _check_positive_duration(value: float, op: str, name: str) -> None:
    """Validate a positive duration in seconds before any mutation (F2
    discipline, same shape as _check_ttl). A non-positive cooldown or
    failure window cannot produce sane breaker timing: zero/negative
    windows would either never count or instantly expire every failure,
    and NaN poisons every window comparison."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) \
            or not value > 0:
        raise TransitionRejected(
            f"{op}: {name} must be a positive number of seconds,"
            f" got {value!r}")


def _classify_breaker_row(row) -> str:
    """Enforcement verdict for one breaker row: 'closed' | 'open' |
    'half_open' | 'corrupt'. 'corrupt' means the row cannot be read as a
    breaker at all — unknown state, or a NULL/non-integer version or
    probe counter. The definition is shared verbatim by breaker_allows()
    and the resilient claim path, so "corrupt" can never mean "allow" in
    one place and "deny" in another. Never raises."""
    state = row["state"]
    version = row["version"]
    probes_used = row["half_open_probes_used"]
    if (state not in _BREAKER_STATES
            or version is None or isinstance(version, bool)
            or not isinstance(version, int)
            or probes_used is None or isinstance(probes_used, bool)
            or not isinstance(probes_used, int)):
        return "corrupt"
    if state == "OPEN":
        return "open"
    if state == "HALF_OPEN":
        return "half_open"
    return "closed"


def _new_id(prefix: str = "") -> str:
    return prefix + uuid.uuid4().hex


class _FencedHeartbeat(Exception):
    """Internal: a heartbeat failed the fencing check inside the
    authoritative transaction.

    Raised instead of LeaseError inside the write transaction so the
    evidence (observed vs authoritative token/owner) survives the rollback
    and can be journaled as a worker.heartbeat_fenced milestone before the
    LeaseError is surfaced to the caller.
    """

    def __init__(self, worker_id: str, proc_id: str, job_id: str | None,
                 observed_token, current_token, current_owner,
                 reason: str) -> None:
        super().__init__(reason)
        self.worker_id = worker_id
        self.proc_id = proc_id
        self.job_id = job_id
        self.observed_token = observed_token
        self.current_token = current_token
        self.current_owner = current_owner
        self.reason = reason


class _VerdictNoOp(Exception):
    """Private control-flow signal: a watchdog verdict write lost the
    compare-and-swap race inside the transaction (the concurrent writer's
    verdict is now the latest, and it is the same verdict or DEAD).

    Raised instead of returning early so write_txn rolls the transaction
    back: even the last_commit_ts bump is undone, making the no-op a true
    zero-mutation path. The authoritative result travels on the exception.
    """

    def __init__(self, result: dict) -> None:
        super().__init__("watchdog verdict no-op")
        self.result = result


class _AdmissionDenied(Exception):
    """Private control-flow signal: claim_job_resilient() decided to deny
    admission AFTER it had already mutated breaker state inside the
    transaction (an allocated half-open probe, or a later scope/step
    denying). Raised instead of returning False so write_txn rolls the
    transaction back: the probe allocation is undone atomically and no
    probe slot ever leaks without an admitted claim. The authoritative
    denial travels on the exception; callers see a plain False. Never
    escapes claim_job_resilient()."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class TransitionGate:
    """Enforcement boundary for all authoritative state transitions."""

    def __init__(self, store: Store, staging_root: str | None = None) -> None:
        self.store = store
        # Phase 1C R5 — local artifact staging area (single VM, durable
        # filesystem; deliberately simple, no object storage). Defaults to
        # a sibling of the database file so every gate over the same DB —
        # including worker subprocesses constructing their own gate —
        # resolves the identical staging root.
        if staging_root is None:
            staging_root = os.path.join(
                os.path.dirname(os.path.abspath(store.path)),
                "axos-staging",
            )
        self.staging_root = staging_root

    # ------------------------------------------------------------ internals
    def _get(self, conn, table, id_col, entity_id) -> sqlite3.Row:
        row = conn.execute(
            f"SELECT * FROM {table} WHERE {id_col}=?", (entity_id,)
        ).fetchone()
        if row is None:
            raise TransitionRejected(f"{table}.{id_col}={entity_id!r} does not exist")
        return row

    def _transition(
        self,
        table: str,
        id_col: str,
        entity_id: str,
        to_state: str,
        allowed: dict,
        actor: str,
        extra: dict | None = None,
        ledger_event: str | None = None,
        ledger_payload: dict | None = None,
    ) -> dict:
        with self.store.write_txn() as (conn, now):
            row = self._get(conn, table, id_col, entity_id)
            from_state = row["status"]
            if to_state not in allowed.get(from_state, ()):
                raise TransitionRejected(
                    f"invalid transition {table} {entity_id}: "
                    f"{from_state} -> {to_state}"
                )
            sets = {"status": to_state, "updated_at": now}
            if extra:
                sets.update(extra)
            set_clause = ", ".join(f"{k}=?" for k in sets)
            conn.execute(
                f"UPDATE {table} SET {set_clause} WHERE {id_col}=?",
                (*sets.values(), entity_id),
            )
            if ledger_event:
                self._append_event(
                    conn, now, ledger_event,
                    {"entity": table, "id": entity_id,
                     "from": from_state, "to": to_state,
                     **(ledger_payload or {})},
                    actor,
                )
            out = dict(self._get(conn, table, id_col, entity_id))
        return out

    def _append_event(self, conn, now, event_type, payload, actor) -> int:
        last = conn.execute(
            "SELECT seq, hash FROM ledger ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        prev_seq = last["seq"] if last else 0
        prev_hash = last["hash"] if last else "0" * 64
        body = _canon({"seq": prev_seq + 1, "type": event_type,
                       "payload": payload, "actor": actor, "ts": now})
        h = hashlib.sha256(f"{prev_hash}:{body}".encode()).hexdigest()
        cur = conn.execute(
            "INSERT INTO ledger(event_type, payload, actor, ts, prev_hash, hash)"
            " VALUES(?,?,?,?,?,?)",
            (event_type, _canon(payload), actor, now, prev_hash, h),
        )
        return cur.lastrowid

    def _check_work_creator(self, actor: str, what: str) -> None:
        # I-16: workers cannot create work. Only scheduler-class actors
        # may materialize tasks/jobs.
        if actor.startswith("worker:") or actor not in T.WORK_CREATOR_ACTORS:
            raise TransitionRejected(
                f"I-16: actor {actor!r} may not create {what}"
            )

    # ----------------------------------------------------------------- tasks
    def create_task(self, task_id: str | None, objective: dict,
                    budgets: dict, actor: str) -> dict:
        self._check_work_creator(actor, "tasks")
        task_id = task_id or _new_id("task-")
        with self.store.write_txn() as (conn, now):
            conn.execute(
                "INSERT INTO tasks(task_id, status, objective, budgets,"
                " created_at, updated_at)"
                " VALUES(?, 'PROPOSED', ?, ?, ?, ?)",
                (task_id, _canon(objective), _canon(budgets), now, now),
            )
            self._append_event(conn, now, "task.created",
                               {"task_id": task_id}, actor)
            return dict(self._get(conn, "tasks", "task_id", task_id))

    def transition_task(self, task_id: str, to_state: str, actor: str,
                        reason: str | None = None,
                        approval_ref: str | None = None,
                        pause_reason: str | None = None,
                        pause_diagnostic: dict | None = None) -> dict:
        with self.store.write_txn() as (conn, now):
            row = self._get(conn, "tasks", "task_id", task_id)
            from_state = row["status"]
            if to_state not in T.TASK_TRANSITIONS.get(from_state, ()):
                raise TransitionRejected(
                    f"invalid transition task {task_id}: {from_state} -> {to_state}"
                )
            if from_state in T.HUMAN_GATED_FROM:
                # I-17: human-gated states are sticky. Leaving one requires
                # an explicit, recorded APPROVED decision — never a restart,
                # never a timeout, never silence.
                if not approval_ref:
                    raise TransitionRejected(
                        f"I-17: task {task_id} is in human-gated state "
                        f"{from_state}; an APPROVED approval is required to move it"
                    )
                ap = self._get(conn, "approvals", "approval_id", approval_ref)
                if ap["status"] != "APPROVED" or ap["task_id"] != task_id:
                    raise TransitionRejected(
                        f"I-17: approval {approval_ref} is not an APPROVED"
                        f" approval for task {task_id}"
                    )
            sets: dict = {"status": to_state, "updated_at": now}
            if to_state == "PAUSED_FOR_HUMAN":
                sets["pause_reason"] = pause_reason
                sets["pause_diagnostic"] = (
                    _canon(pause_diagnostic) if pause_diagnostic else None
                )
            set_clause = ", ".join(f"{k}=?" for k in sets)
            conn.execute(
                f"UPDATE tasks SET {set_clause} WHERE task_id=?",
                (*sets.values(), task_id),
            )
            self._append_event(
                conn, now, "task.transition",
                {"task_id": task_id, "from": from_state, "to": to_state,
                 "reason": reason, "approval_ref": approval_ref},
                actor,
            )
            return dict(self._get(conn, "tasks", "task_id", task_id))

    def get_task(self, task_id: str) -> dict:
        return dict(self._get(self.store.conn, "tasks", "task_id", task_id))

    # ------------------------------------------------------------------ jobs
    def create_job(self, job_id: str | None, task_id: str, stage_id: str | None,
                   actor: str, max_attempts: int = 3,
                   policy: dict | None = None) -> dict:
        self._check_work_creator(actor, "jobs")
        job_id = job_id or _new_id("job-")
        with self.store.write_txn() as (conn, now):
            self._get(conn, "tasks", "task_id", task_id)  # must exist
            conn.execute(
                "INSERT INTO jobs(job_id, task_id, stage_id, status,"
                " max_attempts, policy, created_at, updated_at)"
                " VALUES(?, ?, ?, 'PENDING', ?, ?, ?, ?)",
                (job_id, task_id, stage_id, max_attempts,
                 _canon(policy or {}), now, now),
            )
            self._append_event(conn, now, "job.created",
                               {"job_id": job_id, "task_id": task_id}, actor)
            return dict(self._get(conn, "jobs", "job_id", job_id))

    def transition_job(self, job_id: str, to_state: str, actor: str,
                       reason: str | None = None) -> dict:
        # Phase 1C R5 (I-4): COMPLETE is not reachable through the generic
        # transition API. The single authoritative completion contract is
        # commit_artifact(), which atomically verifies artifact bytes,
        # content hash, and validator evidence in the same transaction
        # that moves the job to COMPLETE. Any other route to COMPLETE —
        # including UNCERTAIN -> COMPLETE here — is refused.
        if to_state == "COMPLETE":
            raise TransitionRejected(
                f"job {job_id}: COMPLETE is reachable only through"
                f" commit_artifact() with a verified artifact (I-4);"
                f" the generic transition API cannot complete a job")
        return self._transition(
            "jobs", "job_id", job_id, to_state, T.JOB_TRANSITIONS, actor,
            ledger_event="job.transition",
            ledger_payload={"job_id": job_id, "reason": reason},
        )

    def get_job(self, job_id: str) -> dict:
        return dict(self._get(self.store.conn, "jobs", "job_id", job_id))

    def fencing_ledger_for_job(self, job_id: str) -> list[dict]:
        """Read-only: authority-changing ledger events for one job, in order.

        Returns the job.claimed and job.lease_reclaimed events whose payload
        names this job_id, ordered by seq. The supervisor's fence sweep uses
        these as *durable revocation evidence* (did this worker's authority
        epoch provably end after its process spawned?). This method performs
        no authority judgment itself and never mutates state.
        """
        rows = self.store.conn.execute(
            "SELECT seq, ts, event_type, payload FROM ledger "
            "WHERE event_type IN ('job.claimed', 'job.lease_reclaimed') "
            "AND json_extract(payload, '$.job_id') = ? "
            "ORDER BY seq",
            (job_id,),
        ).fetchall()
        return [
            {"seq": r["seq"], "ts": r["ts"],
             "event_type": r["event_type"],
             "payload": json.loads(r["payload"])}
            for r in rows
        ]

    def unreaped_proc_spawns(self) -> list[dict]:
        """Read-only: proc_spawned milestones with no matching proc_reaped.

        Returns one entry per (worker_id, proc_id) that was spawned but never
        reaped, ordered by spawn seq. A restarted supervisor uses this to
        discover possibly-live worker processes from durable state. The
        caller must still verify liveness and process identity before
        taking any action — a missing reap is not proof of life.

        Each entry also carries the durable spawn-time process identity
        (start_jiffies, pgid — None on records that predate them) for the
        R6 exact-identity PID-reuse guard.
        """
        return self._unreaped_proc_spawns_conn(self.store.conn)

    def latest_spawn_generation(self, worker_id: str) -> dict | None:
        """Read-only: the worker's latest worker.proc_spawned milestone
        and its matching worker.proc_reaped milestone (if any).

        Returns None when the worker has no spawn record. Otherwise
        returns {"proc_id": str, "spawn": {...}, "reap": {...} | None}.
        The spawn dict carries pid/start_jiffies/pgid/spawn_ts; the reap
        dict carries pid/returncode/signal/how/reaped_at. A present reap
        means the supervisor observed that exact process instance dead:
        every supervisor _reap caller passes a non-None poll() (the
        process had exited). The (worker_id, proc_id) key pins the reap
        to one generation — a respawn supersedes it with a newer proc_id,
        so only the latest generation's reap is returned.
        """
        rows = self.store.conn.execute(
            "SELECT ts, payload FROM ledger "
            "WHERE event_type = 'worker.proc_spawned' "
            "ORDER BY seq DESC"
        ).fetchall()
        latest = None
        latest_ts = None
        for r in rows:
            p = json.loads(r["payload"])
            if p.get("worker_id") == worker_id:
                latest = p
                latest_ts = r["ts"]
                break
        if latest is None:
            return None
        proc_id = latest.get("proc_id")
        reap = None
        for r in self.store.conn.execute(
                "SELECT payload FROM ledger "
                "WHERE event_type = 'worker.proc_reaped' "
                "ORDER BY seq DESC").fetchall():
            p = json.loads(r["payload"])
            if (p.get("worker_id") == worker_id
                    and p.get("proc_id") == proc_id):
                reap = p
                break
        return {
            "proc_id": proc_id,
            "spawn": {"pid": latest.get("pid"),
                      "job_id": latest.get("job_id"),
                      "start_jiffies": latest.get("start_jiffies"),
                      "pgid": latest.get("pgid"),
                      "spawn_ts": latest_ts},
            "reap": ({"pid": reap.get("pid"),
                      "returncode": reap.get("returncode"),
                      "signal": reap.get("signal"),
                      "how": reap.get("how"),
                      "reaped_at": reap.get("reaped_at")}
                     if reap is not None else None),
        }

    def _unreaped_proc_spawns_conn(self, conn) -> list[dict]:
        """Conn-parameterized core of unreaped_proc_spawns: the R13
        finalizer re-reads this evidence inside its publish transaction,
        so the scan must run on the caller's connection."""
        spawns = conn.execute(
            "SELECT ts, payload FROM ledger "
            "WHERE event_type = 'worker.proc_spawned' ORDER BY seq"
        ).fetchall()
        reaped: set[tuple] = set()
        for r in conn.execute(
                "SELECT payload FROM ledger "
                "WHERE event_type = 'worker.proc_reaped'"):
            p = json.loads(r["payload"])
            reaped.add((p.get("worker_id"), p.get("proc_id")))
        out = []
        for r in spawns:
            p = json.loads(r["payload"])
            key = (p.get("worker_id"), p.get("proc_id"))
            if key not in reaped:
                out.append({"worker_id": p.get("worker_id"),
                            "proc_id": p.get("proc_id"),
                            "job_id": p.get("job_id"),
                            "pid": p.get("pid"),
                            # R6: exact process-start identity + process-group
                            # identity persisted at spawn (None on legacy
                            # records, which predate these fields).
                            "start_jiffies": p.get("start_jiffies"),
                            "pgid": p.get("pgid"),
                            "spawn_ts": r["ts"]})
        return out

    def _apply_progress(self, conn, now: float, job_id: str, worker_id: str,
                        fencing_token: int, progress_done: float,
                        progress_total: float | None) -> dict:
        """Shared informational-progress write (R3 / D9).

        The single progress authority path, used both by update_job_progress
        and by heartbeat ingestion when the heartbeat carries progress.
        Identical checks in both callers — there is no second progress
        authority. Must be called inside the caller's write_txn so the
        progress write commits atomically with whatever else the
        transaction carries.

        R3: every accepted write stamps jobs.progress_updated_at with
        authoritative store time. Informational only — never recovery
        evidence (D6); the recovery controller computes observed progress
        from durable diffs.
        """
        row = self._get(conn, "jobs", "job_id", job_id)
        if row["status"] not in ("CLAIMED", "RUNNING"):
            raise TransitionRejected(
                f"cannot record progress for job {job_id} in "
                f"{row['status']}: job is not actively executing")
        if row["owner_worker_id"] != worker_id:
            raise LeaseError(f"{worker_id} does not own job {job_id}")
        if int(row["fencing_token"]) != int(fencing_token):
            raise LeaseError(f"stale fencing token for job {job_id}")
        if row["lease_expires_at"] is not None and \
                row["lease_expires_at"] <= now:
            raise LeaseError(f"lease for job {job_id} has expired")
        conn.execute(
            "UPDATE jobs SET progress_done=?, progress_total=?,"
            " progress_updated_at=?, updated_at=? WHERE job_id=?",
            (progress_done, progress_total, now, now, job_id),
        )
        return dict(self._get(conn, "jobs", "job_id", job_id))

    def update_job_progress(self, job_id: str, worker_id: str,
                            fencing_token: int, progress_done: float,
                            progress_total: float | None, actor: str) -> dict:
        """Record progress; only the current lease owner with a valid fencing
        token may do so, and only while the job is actively executing.
        Informational only — never completion evidence.

        R3: stamps jobs.progress_updated_at with store time (D9).

        F3: jobs that have left the active execution window (COMPLETE, FAILED,
        QUARANTINED, ...) cannot acquire further progress mutations — terminal
        state must not contradict itself. Rejection rolls the transaction
        back, leaving the row byte-identical.
        """
        with self.store.write_txn() as (conn, now):
            return self._apply_progress(conn, now, job_id, worker_id,
                                        fencing_token, progress_done,
                                        progress_total)

    # ----------------------------------------------------------------- leases
    # Phase 0 (04-domain-model, Lease): the lease is the
    # (owner_worker_id, lease_expires_at, fencing_token) triple on the job
    # row, mutated only through atomic conditional updates. All timestamps
    # are authoritative store time; worker clocks are never consulted.
    def _claim_job_txn(self, conn, now: float, job_id: str, worker_id: str,
                       ttl_s: float, actor: str,
                       worker_reported_ts: float | None = None,
                       extra_payload: dict | None = None) -> bool:
        """Shared PENDING->CLAIMED claim core (R10).

        The single conditional UPDATE plus the `job.claimed` ledger event.
        There is no second lease implementation: `claim_job`,
        `claim_job_bounded`, and `claim_job_resilient` all run this exact
        core inside their own write_txn, so the triple, the fencing-token
        bump, and the event are identical on every claim path.

        `extra_payload` (R12) merges additional keys into the `job.claimed`
        event — e.g. the half-open probe allocation the resilient claim
        path made. Callers without admission context omit it and get the
        historical payload shape unchanged.
        """
        cur = conn.execute(
            "UPDATE jobs SET status='CLAIMED', owner_worker_id=?,"
            " fencing_token=fencing_token+1,"
            " lease_acquired_at=?, lease_expires_at=?,"
            " worker_reported_ts=?, updated_at=?"
            " WHERE job_id=? AND status='PENDING'"
            " AND (owner_worker_id IS NULL OR lease_expires_at IS NULL"
            "      OR lease_expires_at <= ?)",
            (worker_id, now, now + ttl_s, worker_reported_ts, now,
             job_id, now),
        )
        if cur.rowcount != 1:
            return False
        payload = {"job_id": job_id, "worker_id": worker_id,
                   "ttl_s": ttl_s}
        if extra_payload:
            payload.update(extra_payload)
        self._append_event(conn, now, "job.claimed", payload, actor)
        return True

    def claim_job(self, job_id: str, worker_id: str, ttl_s: float,
                  actor: str, worker_reported_ts: float | None = None) -> bool:
        """PENDING -> CLAIMED with lease triple. Atomic: exactly one claimant
        wins under concurrency. Returns True on success, False if the job was
        not claimable (already claimed, conflicting lease, wrong state)."""
        _check_ttl(ttl_s, "claim_job")
        with self.store.write_txn() as (conn, now):
            return self._claim_job_txn(conn, now, job_id, worker_id, ttl_s,
                                       actor, worker_reported_ts)

    def claim_job_bounded(self, job_id: str, worker_id: str, ttl_s: float,
                          actor: str, max_concurrent_jobs: int,
                          scheduler_id: str | None = None) -> bool:
        """PENDING -> CLAIMED with admission control (Phase 1C R10).

        Atomically: admit at most `max_concurrent_jobs` jobs in the active
        execution window (statuses CLAIMED, RUNNING, COMMITTING), then run
        the shared `_claim_job_txn` core. Returns False without mutation
        when capacity is full (backpressure) or the job is not claimable
        (lost race / wrong state).

        WHY this extension is necessary, rather than a read-then-claim in
        the scheduler: the capacity predicate MUST be evaluated inside the
        same transaction as the claim UPDATE. BEGIN IMMEDIATE serializes
        concurrent writers on a single VM, so two schedulers racing both
        see the same committed count; a read-then-claim across two
        transactions races and over-admits. And exec/ is forbidden from
        SQL writes entirely (authority audit A1/A3), so the predicate +
        claim cannot live in the scheduler — it must be gate-owned.

        When `scheduler_id` is given, the scheduler's claim-liveness beat
        for `worker_id` is planted in the SAME write_txn (claim ⟹ beat,
        atomically): another scheduler's orphan-redispatch path can then
        never observe this claim without also observing a fresh beat, so
        a live scheduler's fresh claim is never stolen mid-dispatch
        (R10-04: exactly one dispatch under racing schedulers). Callers
        that are not schedulers omit it and get the historical behavior.
        """
        _check_ttl(ttl_s, "claim_job_bounded")
        if isinstance(max_concurrent_jobs, bool) \
                or not isinstance(max_concurrent_jobs, int) \
                or max_concurrent_jobs < 1:
            raise TransitionRejected(
                "claim_job_bounded: max_concurrent_jobs must be an integer"
                f" >= 1, got {max_concurrent_jobs!r}")
        with self.store.write_txn() as (conn, now):
            active = conn.execute(
                "SELECT COUNT(*) FROM jobs"
                " WHERE status IN ('CLAIMED','RUNNING','COMMITTING')"
            ).fetchone()[0]
            if active >= max_concurrent_jobs:
                return False
            ok = self._claim_job_txn(conn, now, job_id, worker_id, ttl_s,
                                     actor)
            if ok and scheduler_id is not None:
                conn.execute(
                    "INSERT OR REPLACE INTO scheduler_claim_beats"
                    "(worker_id, scheduler_id, last_beat)"
                    " VALUES (?,?,?)",
                    (worker_id, scheduler_id, now))
            return ok

    def renew_lease(self, job_id: str, worker_id: str, fencing_token: int,
                    ttl_s: float, actor: str) -> bool:
        """Extend the lease; requires current ownership and a valid fencing
        token. Returns False instead of raising on conflict/staleness."""
        _check_ttl(ttl_s, "renew_lease")
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                "UPDATE jobs SET lease_expires_at=?, updated_at=?"
                " WHERE job_id=? AND owner_worker_id=?"
                " AND fencing_token=? AND status IN ('CLAIMED','RUNNING')"
                " AND (lease_expires_at IS NULL OR lease_expires_at > ?)",
                (now + ttl_s, now, job_id, worker_id, fencing_token, now),
            )
            if cur.rowcount != 1:
                return False
            self._append_event(conn, now, "lease.renewed",
                               {"job_id": job_id, "worker_id": worker_id}, actor)
            return True

    def release_lease(self, job_id: str, worker_id: str, fencing_token: int,
                      actor: str) -> bool:
        """Voluntary release by the owner (valid token). Clears the triple."""
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                "UPDATE jobs SET owner_worker_id=NULL, lease_acquired_at=NULL,"
                " lease_expires_at=NULL, updated_at=?"
                " WHERE job_id=? AND owner_worker_id=?"
                " AND fencing_token=? AND status IN ('CLAIMED','RUNNING')",
                (now, job_id, worker_id, fencing_token),
            )
            if cur.rowcount != 1:
                return False
            self._append_event(conn, now, "lease.released",
                               {"job_id": job_id, "worker_id": worker_id}, actor)
            return True

    # Phase 1C R4 — authoritative lease-expiry detection. Read-only: the
    # detector answers "has the durable lease actually expired?" and hands
    # the resulting evidence to the existing R1 reclaim primitive. It is
    # NOT a reclaim authority and performs no mutation of any kind.
    _EXPIRY_STATES = ("CLAIMED", "RUNNING", "COMMITTING")
    _EXPIRY_PREDICATE = (
        "owner_worker_id IS NOT NULL"
        " AND lease_expires_at IS NOT NULL"
        " AND lease_expires_at <= :now"
        " AND status IN ('CLAIMED','RUNNING','COMMITTING')"
    )

    def observe_expired_leases(self) -> list[dict]:
        """Deterministic, authoritative expiry observation (R4 evidence).

        Returns one evidence record per job whose durable lease has
        actually expired, evaluated against the CURRENT durable row with
        the authoritative store clock::

            owner_worker_id IS NOT NULL            -- an actual owner exists
            AND lease_expires_at IS NOT NULL       -- a lease was granted
            AND lease_expires_at <= store_now      -- expired (boundary
                                                      inclusive: == is expired)
            AND status IN (CLAIMED, RUNNING, COMMITTING)

        PENDING, terminal, BLOCKED, QUARANTINED, and UNCERTAIN jobs are
        never reported, nor are ownerless jobs or jobs without a lease —
        an old timestamp alone is not an expired worker lease.

        Each record carries everything needed to reconstruct the
        observation and to hand it to R1::

            job_id, owner_worker_id, fencing_token, status,
            lease_acquired_at, lease_expires_at,
            observed_at,   # the authoritative store now the predicate used
            predicate,     # the predicate string above
            reason,        # human-readable: why this job qualified

        Advisory evidence, NOT authorization to mutate:

        - This method is SELECT-only (it runs on the gate's read-only
          connection, which cannot write). Repeated observation is
          therefore idempotent by construction: it mutates no durable
          state, bumps no fencing token, and appends no ledger events.
        - The ONLY mutation path for an expired lease is
          reclaim_lease(job_id, expected_owner=..., expected_token=...),
          which revalidates owner, token, AND expiry atomically inside
          its own transaction. Never cache owner/token across a mutation
          boundary: re-observe, or let reclaim_lease reject stale
          evidence with LeaseError and zero mutation.
        - A heartbeat never renews a lease (ingest_heartbeat cannot touch
          lease fields), so heartbeat activity — whatever worker
          timestamps it carries — cannot make an expired lease unexpired
          and cannot prevent expiry. Only renew_lease moves
          lease_expires_at, and only while the lease is still live.

        Clock safety: observed_at = store.current_time() =
        max(clock, last_commit_ts). Any write_txn started afterwards
        stamps now >= observed_at (its now is floored at
        last_commit_ts + 0.001), so a lease observed expired here can
        never be seen as live by reclaim_lease's in-transaction expiry
        check — unless a renewal committed in between, which is itself
        authoritative durable state that reclaim_lease revalidates.
        Detection therefore can only under-report relative to R1, never
        over-report: R4 never flags a lease that R1 would consider live.

        No STALLED/DEAD verdicts, no recovery policy, no process control:
        those belong to the later watchdog/recovery layers. This method
        reports the deterministic fact "the authoritative lease is
        expired" and nothing more.
        """
        now = self.store.current_time()
        rows = self.store.conn.execute(
            "SELECT job_id, owner_worker_id, fencing_token, status,"
            " lease_acquired_at, lease_expires_at"
            " FROM jobs WHERE " + self._EXPIRY_PREDICATE,
            {"now": now},
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["observed_at"] = now
            d["predicate"] = self._EXPIRY_PREDICATE
            d["reason"] = (
                f"authoritative lease expired for job {d['job_id']}:"
                f" lease_expires_at={d['lease_expires_at']}"
                f" <= observed_at={now};"
                f" owner={d['owner_worker_id']!r} status={d['status']}"
                f" fencing_token={d['fencing_token']}"
            )
            out.append(d)
        return out

    def expired_leases(self) -> list[dict]:
        """Legacy projection over the R4-proven expiry predicate.

        Same authoritative detection as observe_expired_leases(), trimmed
        to the historical {job_id, owner_worker_id, fencing_token,
        lease_expires_at} shape. New consumers should prefer
        observe_expired_leases(), whose records carry the observation
        timestamp, predicate, and reason needed to hand evidence to
        reclaim_lease safely."""
        return [
            {k: ev[k] for k in ("job_id", "owner_worker_id",
                               "fencing_token", "lease_expires_at")}
            for ev in self.observe_expired_leases()
        ]

    # K4 lease reclaim — Phase 1C R1. The atomic ownership-revocation
    # primitive consumed later by the recovery controller, the reconciler,
    # boot/system recovery, and operator-manual action. The supervisor is
    # NOT a reclaim authority and is refused.
    #
    # D1 semantics: in ONE transaction — verify evidence and owner/token,
    # increment the fencing token, clear the owner/lease, transition the
    # job (CLAIMED->PENDING, RUNNING/COMMITTING->UNCERTAIN), append ledger
    # evidence. The old owner is fenced out by construction: its token no
    # longer matches the durable row, so commit/renew/heartbeat/progress
    # with the old token all fail.
    def reclaim_lease(self, job_id: str, *, actor: str, reason: str,
                      expected_owner: str, expected_token: int,
                      force: bool = False, verdict: str | None = None,
                      incident_id: str | None = None) -> dict:
        """Atomically revoke a lease: verify -> bump fencing token ->
        clear owner -> transition job -> ledger, in ONE transaction.

        Routine reclaim (force=False): the lease must be expired per
        authoritative store time. Forced reclaim (force=True): the lease may
        still be live, but requires a watchdog verdict (DEAD/STALLED) plus
        the incident identity — the evidence the recovery controller will
        supply later. R1 validates the evidence shape; it does not produce
        verdicts and implements no recovery policy.

        State outcomes (Phase 1C D1): CLAIMED -> PENDING, RUNNING ->
        UNCERTAIN, COMMITTING -> UNCERTAIN. Any other state (terminal,
        PENDING, UNCERTAIN, ...) and any ownerless job are rejected with
        zero mutation: reclaim revokes a lease, it never invents one.

        The caller must name the exact lease being revoked: expected_owner
        and expected_token are required and compared inside the transaction
        (compare-and-swap on the lease triple). A second reclaim holding
        the old token is rejected by the token mismatch; a reclaim after a
        successful one is rejected as ownerless. Reclaim never silently
        increments the token.

        Safety: token N -> N+1 and owner -> NULL commit in the same
        transaction as the state transition and the ledger events, so the
        old owner cannot commit, renew, heartbeat, or update progress with
        the old token afterwards.

        Raises TransitionRejected (invalid actor/reason/evidence, unknown
        job, invalid state, ownerless job) or LeaseError (owner/token
        mismatch, lease not expired). Every rejection rolls the transaction
        back: no partial state, no half-reclaimed lease.
        """
        # ---- input validation BEFORE the transaction (no row reference:
        # pure caller/evidence shape checks, per the A6 structural rule).
        if actor not in T.RECLAIM_ACTORS:
            raise TransitionRejected(
                f"reclaim_lease: actor {actor!r} is not a reclaim authority")
        if not isinstance(reason, str) or not reason.strip():
            raise TransitionRejected(
                "reclaim_lease: a non-empty reason is required")
        if not isinstance(expected_owner, str) or not expected_owner:
            raise TransitionRejected(
                "reclaim_lease: expected_owner is required")
        if isinstance(expected_token, bool):
            raise TransitionRejected(
                f"reclaim_lease: invalid fencing token {expected_token!r}")
        try:
            exp_tok = int(expected_token)
        except (TypeError, ValueError):
            raise TransitionRejected(
                f"reclaim_lease: invalid fencing token {expected_token!r}")
        if force:
            if verdict not in ("DEAD", "STALLED"):
                raise TransitionRejected(
                    "reclaim_lease: forced reclaim requires a DEAD/STALLED"
                    f" verdict, got {verdict!r}")
            if not isinstance(incident_id, str) or not incident_id:
                raise TransitionRejected(
                    "reclaim_lease: forced reclaim requires an incident_id")
        with self.store.write_txn() as (conn, now):
            row = self._get(conn, "jobs", "job_id", job_id)
            from_state = row["status"]
            if from_state == "CLAIMED":
                to_state = "PENDING"
            elif from_state in ("RUNNING", "COMMITTING"):
                to_state = "UNCERTAIN"
            else:
                raise TransitionRejected(
                    f"reclaim_lease: job {job_id} in {from_state} holds no"
                    f" reclaimable lease")
            if to_state not in T.JOB_TRANSITIONS.get(from_state, ()):
                # Defensive: the contract's reclaim outcomes must stay
                # inside the authoritative transition graph.
                raise TransitionRejected(
                    f"reclaim_lease: {from_state} -> {to_state} is not in"
                    f" the job transition graph")
            owner = row["owner_worker_id"]
            if owner is None:
                raise TransitionRejected(
                    f"reclaim_lease: job {job_id} has no owner to revoke")
            if owner != expected_owner:
                raise LeaseError(
                    f"reclaim_lease: expected owner {expected_owner!r},"
                    f" current owner is {owner!r}")
            cur_tok = row["fencing_token"]
            if cur_tok is None or int(cur_tok) != exp_tok:
                raise LeaseError(
                    f"reclaim_lease: stale fencing token {expected_token!r}"
                    f" for job {job_id} (current {cur_tok!r})")
            if not force:
                exp_at = row["lease_expires_at"]
                if exp_at is None or exp_at > now:
                    raise LeaseError(
                        f"reclaim_lease: lease for job {job_id} is not"
                        f" expired (expires_at={exp_at}, now={now})")
            new_token = int(cur_tok) + 1
            conn.execute(
                "UPDATE jobs SET owner_worker_id=NULL,"
                " lease_acquired_at=NULL, lease_expires_at=NULL,"
                " fencing_token=?, status=?, updated_at=? WHERE job_id=?",
                (new_token, to_state, now, job_id),
            )
            self._append_event(
                conn, now, "job.lease_reclaimed",
                {"job_id": job_id, "prev_owner": owner,
                 "prev_token": int(cur_tok), "new_token": new_token,
                 "prev_state": from_state, "new_state": to_state,
                 "reason": reason, "forced": force,
                 "verdict": verdict, "incident_id": incident_id},
                actor)
            if to_state == "UNCERTAIN":
                self._append_event(
                    conn, now, "job.uncertain_opened",
                    {"job_id": job_id, "from": from_state,
                     "cause": "lease_reclaimed", "prev_owner": owner},
                    actor)
            return dict(self._get(conn, "jobs", "job_id", job_id))

    # ================================== Phase 1C R5 — verified artifacts
    # Verified checkpoint & artifact integrity (I-4 correction).
    #
    # The old direct-completion path (commit_job_result: CLAIMED/RUNNING ->
    # COMPLETE on a result string, with no artifact, no hash, no validation)
    # is REMOVED — it no longer exists on this class. The single
    # authoritative completion contract is:
    #
    #   stage_artifact()    worker bytes -> fsync -> gate-computed sha256
    #   begin_commit()      RUNNING -> COMMITTING (fenced)
    #   verify_artifact()   gate recomputes the hash, runs the structural
    #                       checks, records validator receipts,
    #                       STAGING -> VALIDATED
    #   commit_artifact()   ONE atomic transaction: re-read job, re-read
    #                       artifact, re-read receipts, re-verify the full
    #                       predicate -> job COMPLETE + content_hash
    #
    # A job may enter COMPLETE only when artifact bytes are durably staged
    # and identified by content hash, required validators have passed, and
    # one transaction commits the artifact record plus the job state
    # referencing that verified content hash. Anything less is not COMPLETE.
    # transition_job() refuses COMPLETE; there is no second path.
    # ------------------------------------------------------------ staging
    def _stage_path(self, task_id: str, job_id: str, attempt: int,
                    content_hash: str) -> str:
        return os.path.join(self.staging_root, task_id, job_id,
                            str(attempt), content_hash)

    @staticmethod
    def _write_staged_bytes(path: str, data: bytes) -> None:
        """Write artifact bytes durably: file fsync + directory fsync."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        dirfd = os.open(os.path.dirname(path), os.O_DIRECTORY)
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)

    @staticmethod
    def _read_staged_bytes(uri: str | None) -> bytes | None:
        """Read staged bytes. Returns None when the bytes are absent —
        absence is an integrity fact, never an exception here."""
        if not uri:
            return None
        try:
            with open(uri, "rb") as f:
                return f.read()
        except (FileNotFoundError, NotADirectoryError, OSError):
            return None

    @staticmethod
    def _token_int(fencing_token, op: str, job_id: str) -> int:
        # A fencing token is an integer epoch. bool is rejected explicitly
        # (int(True) == 1 would otherwise smuggle a fake epoch through).
        if isinstance(fencing_token, bool):
            raise LeaseError(
                f"{op} for job {job_id}: invalid fencing token"
                f" {fencing_token!r}")
        try:
            return int(fencing_token)
        except (TypeError, ValueError):
            raise LeaseError(
                f"{op} for job {job_id}: invalid fencing token"
                f" {fencing_token!r}")

    def _check_fenced_execution(self, conn, now: float, job: dict,
                                worker_id: str, tok: int, op: str) -> None:
        """The worker-side fencing triple: current owner, current token,
        live lease. Must be called inside the caller's write_txn so the
        checks serialize with the mutation (no check-then-act gap)."""
        if job["owner_worker_id"] != worker_id:
            raise LeaseError(
                f"{op} for job {job['job_id']}: {worker_id!r} is not the"
                f" lease owner")
        if job["fencing_token"] is None or int(job["fencing_token"]) != tok:
            raise LeaseError(
                f"{op} for job {job['job_id']}: stale fencing token"
                f" (current {job['fencing_token']!r})")
        if job["lease_expires_at"] is not None and \
                job["lease_expires_at"] <= now:
            raise LeaseError(
                f"{op} for job {job['job_id']}: lease expired at"
                f" {job['lease_expires_at']}, now {now}")

    def stage_artifact(self, *, job_id: str, worker_id: str, fencing_token: int,
                       task_id: str, kind: str | None, data: bytes,
                       actor: str, attempt: int | None = None,
                       claimed_hash: str | None = None,
                       producer: dict | None = None) -> dict:
        """Stage worker-produced bytes as a content-addressed artifact (R5).

        The GATE computes sha256(data) itself — a worker-supplied checksum
        is informational input only: when claimed_hash is given and differs
        from the independently computed hash, staging is rejected. The same
        bytes always produce the same artifact_id (content addressing);
        different bytes can never share one.

        Requires the job to be RUNNING under the caller's live lease
        (owner + token + unexpired lease, checked inside the transaction).
        Bytes are written with file+directory fsync before the artifact row
        is registered. Idempotent: re-staging identical bytes returns the
        existing row (INSERT ... ON CONFLICT DO NOTHING) without
        contradictory identities.

        Staging is NOT verification: the row is created in STAGING and a
        staged artifact alone never completes a job.
        """
        if not isinstance(data, (bytes, bytearray)) or len(data) == 0:
            raise TransitionRejected(
                "stage_artifact: data must be non-empty bytes")
        data = bytes(data)
        content_hash = hashlib.sha256(data).hexdigest()
        if claimed_hash is not None and claimed_hash != content_hash:
            raise TransitionRejected(
                f"stage_artifact: worker-claimed hash {claimed_hash!r} does"
                f" not match the independently computed content hash"
                f" {content_hash!r}")
        tok = self._token_int(fencing_token, "stage_artifact", job_id)
        path = None
        with self.store.write_txn() as (conn, now):
            job = self._get(conn, "jobs", "job_id", job_id)
            # Fencing identity before state: a fenced worker is rejected as
            # fenced (LeaseError) even when the job has moved on — authority
            # questions outrank state questions.
            self._check_fenced_execution(conn, now, job, worker_id, tok,
                                         "stage_artifact")
            if job["status"] != "RUNNING":
                raise TransitionRejected(
                    f"stage_artifact: job {job_id} is {job['status']},"
                    f" artifacts stage only from RUNNING")
            if task_id != job["task_id"]:
                raise TransitionRejected(
                    f"stage_artifact: task {task_id!r} does not own job"
                    f" {job_id}")
            att = job["attempt"] if attempt is None else int(attempt)
            path = self._stage_path(task_id, job_id, att, content_hash)
            # Bytes hit durable storage (fsync) before the row exists; a
            # crash between the two leaves an inert orphan file, never a
            # half-registered artifact (classify_staging_orphans() reports
            # such files; they can never complete a job).
            self._write_staged_bytes(path, data)
            prod = {"worker_id": worker_id, "job_id": job_id,
                    "actor": actor, **(producer or {})}
            cur = conn.execute(
                "INSERT INTO artifacts(artifact_id, task_id, job_id, kind,"
                " size, uri, status, producer, content_hash, attempt,"
                " owner_worker_id, fencing_token, created_at, updated_at)"
                " VALUES(?, ?, ?, ?, ?, ?, 'STAGING', ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(artifact_id) DO NOTHING",
                (content_hash, task_id, job_id, kind, len(data), path,
                 _canon(prod), content_hash, att, worker_id, tok, now, now),
            )
            if cur.rowcount == 1:
                self._append_event(
                    conn, now, "artifact.staged",
                    {"artifact_id": content_hash, "job_id": job_id,
                     "task_id": task_id, "size": len(data),
                     "content_hash": content_hash,
                     "fencing_token": tok}, actor)
            # R14: per-job staging provenance. On a content-address dedup
            # hit (row exists, owned by the first stager) this job still
            # gets its own staging record — the record, not the row's
            # job_id, is the authority for "this job staged these bytes"
            # in begin_commit / verify_artifact / commit_artifact.
            conn.execute(
                "INSERT OR IGNORE INTO artifact_stagings(artifact_id,"
                " job_id, worker_id, fencing_token, uri, staged_at)"
                " VALUES(?, ?, ?, ?, ?, ?)",
                (content_hash, job_id, worker_id, tok, path, now),
            )
            return dict(self._get(conn, "artifacts", "artifact_id",
                                  content_hash))

    def get_artifact(self, artifact_id: str) -> dict:
        """Read an artifact row (read-only connection)."""
        return dict(self._get(self.store.conn, "artifacts", "artifact_id",
                              artifact_id))

    def _staging_record(self, conn, artifact_id: str,
                        job_id: str) -> dict | None:
        """Per-job staging provenance (R14): the row proving that `job_id`
        staged `artifact_id` under a live lease, or None.

        The fencing triple was checked at stage time inside the staging
        transaction, so a record's existence is authoritative evidence
        that this job produced these bytes — independent of which job's
        id the deduplicated artifact row happens to carry."""
        row = conn.execute(
            "SELECT artifact_id, job_id, worker_id, fencing_token, uri,"
            " staged_at FROM artifact_stagings"
            " WHERE artifact_id=? AND job_id=?",
            (artifact_id, job_id)).fetchone()
        return dict(row) if row else None

    def _require_staging_provenance(self, conn, art: dict,
                                    artifact_id: str, job_id: str) -> dict | None:
        """Enforce that `job_id` staged `artifact_id`.

        Returns the staging record when one exists. Raises
        TransitionRejected when the job never staged the artifact. For
        rows that predate the v12 staging table (no record can exist),
        the legacy row-level job linkage (art.job_id == job_id) still
        applies."""
        staging = self._staging_record(conn, artifact_id, job_id)
        if staging is not None:
            return staging
        if art["job_id"] == job_id:
            return None
        raise TransitionRejected(
            f"artifact {artifact_id} was staged for"
            f" job {art['job_id']!r}, not {job_id!r}")

    def begin_commit(self, job_id: str, worker_id: str, fencing_token: int,
                     *, artifact_id: str, actor: str) -> dict:
        """RUNNING -> COMMITTING: open the fenced commit phase (contract D5).

        Asserts the caller still holds the live lease and that the named
        artifact was staged for this job with its bytes present. Records
        job.commit_started. The job still is not COMPLETE — verification
        and the atomic commit follow.
        """
        tok = self._token_int(fencing_token, "begin_commit", job_id)
        with self.store.write_txn() as (conn, now):
            job = self._get(conn, "jobs", "job_id", job_id)
            # Fencing identity before state (see stage_artifact).
            self._check_fenced_execution(conn, now, job, worker_id, tok,
                                         "begin_commit")
            if job["status"] != "RUNNING":
                raise TransitionRejected(
                    f"begin_commit: job {job_id} is {job['status']},"
                    f" expected RUNNING")
            art = self._get(conn, "artifacts", "artifact_id", artifact_id)
            # R14: linkage is per-job staging provenance, not the
            # deduplicated row's first-writer job_id (two jobs producing
            # identical bytes share one artifact row, and each staged it).
            self._require_staging_provenance(conn, art, artifact_id,
                                             job_id)
            # R14: a job may open its commit against an already-VALIDATED
            # artifact — verification receipts are bound to the content
            # hash, and a sharing job may have verified first. Completion
            # must never depend on another job's progress. QUARANTINED
            # (and any other non-committable state) is still rejected,
            # and commit_artifact re-asserts the full VERIFIED predicate.
            if art["status"] not in ("STAGING", "VALIDATED"):
                raise TransitionRejected(
                    f"begin_commit: artifact {artifact_id} is"
                    f" {art['status']}, expected STAGING")
            if self._read_staged_bytes(art["uri"]) is None:
                raise TransitionRejected(
                    f"begin_commit: staged bytes for artifact {artifact_id}"
                    f" are missing")
            conn.execute(
                "UPDATE jobs SET status='COMMITTING', updated_at=?"
                " WHERE job_id=?", (now, job_id),
            )
            self._append_event(
                conn, now, "job.commit_started",
                {"job_id": job_id, "worker_id": worker_id,
                 "fencing_token": tok, "artifact_id": artifact_id}, actor)
            return dict(self._get(conn, "jobs", "job_id", job_id))

    # ------------------------------------------------------- verification
    def _structural_checks(self, art: dict,
                           data: bytes | None) -> tuple[bool, dict]:
        """The Phase 1C structural validation bar (contract D5): bytes
        present and readable, gate-recomputed hash matches the recorded
        content hash AND the artifact identity, size consistent, provenance
        complete. Deterministic — the gate computes everything itself."""
        expected = art["content_hash"] or art["artifact_id"]
        if data is None:
            return False, {"bytes_present": False,
                           "expected_content_hash": expected}
        recomputed = hashlib.sha256(data).hexdigest()
        try:
            prod = json.loads(art["producer"] or "{}")
            provenance_ok = isinstance(prod, dict) and bool(prod)
        except (ValueError, TypeError):
            provenance_ok = False
        details = {
            "bytes_present": True,
            "byte_len": len(data),
            "recomputed_hash": recomputed,
            "expected_content_hash": expected,
            "hash_matches": recomputed == expected,
            "identity_matches": recomputed == art["artifact_id"],
            "size_matches": art["size"] is None or art["size"] == len(data),
            "provenance_complete": provenance_ok,
        }
        ok = (details["hash_matches"] and details["identity_matches"]
              and details["size_matches"] and details["provenance_complete"])
        return ok, details

    def _upsert_validation(self, conn, now: float, artifact_id: str,
                           content_hash: str, validator_id: str,
                           validator_version: str, result: str, actor: str,
                           receipt: dict) -> dict:
        """Record (or refresh) one validator receipt inside the caller's
        transaction. The receipt is bound to the exact content hash the
        validator saw."""
        validation_id = f"{validator_id}/{validator_version}:{artifact_id}"
        conn.execute(
            "INSERT INTO validations(validation_id, artifact_id,"
            " validator_id, validator_version, result, method, sampled,"
            " sample_desc, receipt_ref, notes, validated_at, content_hash)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
            " ON CONFLICT(validation_id) DO UPDATE SET"
            " result=excluded.result, receipt_ref=excluded.receipt_ref,"
            " validated_at=excluded.validated_at,"
            " content_hash=excluded.content_hash, quarantined=0",
            (validation_id, artifact_id, validator_id, validator_version,
             result, receipt.get("method"), 0, None,
             _canon(receipt), receipt.get("notes"), now, content_hash),
        )
        self._append_event(
            conn, now, "validation.recorded",
            {"validation_id": validation_id, "artifact_id": artifact_id,
             "content_hash": content_hash, "validator_id": validator_id,
             "validator_version": validator_version, "result": result},
            actor)
        return dict(conn.execute(
            "SELECT * FROM validations WHERE validation_id=?",
            (validation_id,)).fetchone())

    def verify_artifact(self, artifact_id: str, *, actor: str,
                        worker_id: str | None = None,
                        fencing_token: int | None = None,
                        job_id: str | None = None,
                        required_validators: tuple | None = None) -> dict:
        """Authoritative artifact verification (R5): STAGING -> VALIDATED.

        The GATE performs the verification — it re-reads the staged bytes,
        recomputes the content hash itself, runs the structural checks, and
        records the validator receipts with gate-computed results. Nobody
        can set VERIFIED by assertion: there is no setter, only this
        predicate.

        Fencing: when the caller is a worker-owned actor, it must name its
        fencing token and still hold the live lease on the artifact's job —
        a stale worker cannot mark artifacts verified. Authority actors
        (supervisor/system/test/...) verify without a lease.

        required_validators defaults to REQUIRED_VALIDATORS (the Phase 1C
        structural bar). Every required validator must hold a PASS receipt
        bound to the EXACT content hash — a receipt for another hash never
        validates this artifact.

        Corrupt bytes quarantine the artifact (STAGING -> QUARANTINED,
        retained for forensics, never silently deleted) and raise. The
        quarantine is recorded in a SECOND transaction: the evaluation
        transaction performs zero writes on the failure path (it rolls
        back cleanly), then the forensic transaction re-reads the bytes
        and records the FAIL receipt + quarantine only if the bytes still
        fail — a quarantine is never written on stale evidence.
        """
        required = (tuple(required_validators) if required_validators
                    is not None else REQUIRED_VALIDATORS)
        try:
            with self.store.write_txn() as (conn, now):
                art = self._get(conn, "artifacts", "artifact_id", artifact_id)
                staging = None
                if _is_worker_actor(actor):
                    wid = worker_id or actor.split(":", 1)[1]
                    if fencing_token is None:
                        raise LeaseError(
                            f"verify_artifact: worker {wid!r} must present its"
                            f" fencing token")
                    # R14: the caller's own job owns the fencing context —
                    # not the deduplicated row's first-writer job_id (two
                    # jobs producing identical bytes share one artifact
                    # row, and each staged it under its own lease).
                    jid = job_id or art["job_id"]
                    if jid is None:
                        raise TransitionRejected(
                            f"verify_artifact: artifact {artifact_id} is not"
                            f" linked to a job")
                    job = self._get(conn, "jobs", "job_id", jid)
                    tok = self._token_int(fencing_token, "verify_artifact",
                                          job["job_id"])
                    # Fencing identity before state (see stage_artifact).
                    self._check_fenced_execution(conn, now, job, wid, tok,
                                                 "verify_artifact")
                    if job["status"] not in ("RUNNING", "COMMITTING"):
                        raise TransitionRejected(
                            f"verify_artifact: job {job['job_id']} is"
                            f" {job['status']}")
                    staging = self._require_staging_provenance(
                        conn, art, artifact_id, job["job_id"])
                elif art["job_id"] is not None:
                    job = self._get(conn, "jobs", "job_id", art["job_id"])
                else:
                    job = None
                expected = art["content_hash"] or artifact_id
                # Verify the bytes THIS job staged (its own namespaced
                # staging path), not the first stager's copy.
                data = self._read_staged_bytes(
                    staging["uri"] if staging else art["uri"])
                ok, details = self._structural_checks(art, data)
                if not ok:
                    # Failure path: zero writes in this transaction. The
                    # forensic record is written by the second transaction
                    # below, after re-reading the bytes.
                    raise _ArtifactVerifyFailed(artifact_id, expected, details)
                for vid, ver in required:
                    if (vid, ver) == ("axos-structural", "1"):
                        # Performed inline above (ok is True on this path);
                        # its PASS receipt is recorded below with the
                        # success writes.
                        continue
                    row = conn.execute(
                        "SELECT * FROM validations WHERE artifact_id=?"
                        " AND validator_id=? AND validator_version=?"
                        " AND result='PASS' AND quarantined=0"
                        " ORDER BY validated_at DESC LIMIT 1",
                        (artifact_id, vid, ver)).fetchone()
                    if row is None or (row["content_hash"] or "") != expected:
                        raise TransitionRejected(
                            f"verify_artifact: artifact {artifact_id} lacks a"
                            f" PASS receipt from required validator"
                            f" {vid}/{ver} for content hash {expected}")
                self._upsert_validation(
                    conn, now, artifact_id, expected,
                    "axos-structural", "1", "PASS", actor,
                    {"method": "gate-structural-r5", "details": details})
                if art["status"] == "STAGING":
                    conn.execute(
                        "UPDATE artifacts SET status='VALIDATED', updated_at=?"
                        " WHERE artifact_id=?", (now, artifact_id))
                    self._append_event(
                        conn, now, "artifact.verified",
                        {"artifact_id": artifact_id,
                         "content_hash": expected,
                         "validators": [f"{v[0]}/{v[1]}" for v in required]},
                        actor)
                return dict(self._get(conn, "artifacts", "artifact_id",
                                      artifact_id))
        except _ArtifactVerifyFailed as exc:
            failed_id, failed_details = exc.artifact_id, exc.details
        # Forensic transaction: re-read the bytes inside this transaction
        # and quarantine only on current evidence. Durable by construction —
        # this transaction commits (the raise below happens after it).
        with self.store.write_txn() as (conn, now):
            art = self._get(conn, "artifacts", "artifact_id", artifact_id)
            data = self._read_staged_bytes(art["uri"])
            ok, details = self._structural_checks(art, data)
            if not ok:
                expected = art["content_hash"] or artifact_id
                self._upsert_validation(
                    conn, now, artifact_id, expected,
                    "axos-structural", "1", "FAIL", actor,
                    {"method": "gate-structural-r5", "details": details})
                if art["status"] == "STAGING":
                    conn.execute(
                        "UPDATE artifacts SET status='QUARANTINED',"
                        " updated_at=? WHERE artifact_id=?",
                        (now, artifact_id))
                    self._append_event(
                        conn, now, "artifact.quarantined",
                        {"artifact_id": artifact_id,
                         "content_hash": expected,
                         "reason": "structural verification failed",
                         "details": details}, actor)
        raise TransitionRejected(
            f"verify_artifact: artifact {failed_id} failed"
            f" structural verification: {_canon(failed_details)}")

    def _assert_artifact_verified(self, conn, art: dict) -> str:
        """Re-verify the full VERIFIED predicate inside the caller's
        transaction (used by commit_artifact): the artifact must be in a
        verified lifecycle state, its bytes must exist and hash to the
        recorded content hash right now, and every required validator must
        hold a PASS receipt bound to that exact hash. Any contradiction
        raises TransitionRejected with zero mutation."""
        artifact_id = art["artifact_id"]
        expected = art["content_hash"] or artifact_id
        if art["status"] not in ("VALIDATED", "RELEASED"):
            raise TransitionRejected(
                f"commit_artifact: artifact {artifact_id} is"
                f" {art['status']}, not VERIFIED")
        data = self._read_staged_bytes(art["uri"])
        if data is None:
            raise TransitionRejected(
                f"commit_artifact: artifact {artifact_id} bytes are"
                f" missing — integrity contradiction")
        recomputed = hashlib.sha256(data).hexdigest()
        if recomputed != expected or recomputed != art["artifact_id"]:
            raise TransitionRejected(
                f"commit_artifact: artifact {artifact_id} hash mismatch:"
                f" bytes hash to {recomputed}, recorded {expected}")
        if art["size"] is not None and art["size"] != len(data):
            raise TransitionRejected(
                f"commit_artifact: artifact {artifact_id} size mismatch")
        for vid, ver in REQUIRED_VALIDATORS:
            row = conn.execute(
                "SELECT * FROM validations WHERE artifact_id=?"
                " AND validator_id=? AND validator_version=?"
                " AND result='PASS' AND quarantined=0"
                " ORDER BY validated_at DESC LIMIT 1",
                (artifact_id, vid, ver)).fetchone()
            if row is None or (row["content_hash"] or "") != expected:
                raise TransitionRejected(
                    f"commit_artifact: artifact {artifact_id} lacks a PASS"
                    f" receipt from required validator {vid}/{ver} for"
                    f" content hash {expected}")
        return expected

    # ------------------------------------------------- atomic completion
    def commit_artifact(self, job_id: str, worker_id: str | None,
                        fencing_token: int | None, *, artifact_id: str,
                        actor: str, evidence: dict | None = None) -> dict:
        """THE authoritative completion transaction (R5 / I-4).

        One write_txn: re-read job, re-read artifact, re-read verification
        receipts; verify owner/token/lease (worker path), artifact/job
        linkage, and the full VERIFIED predicate recomputed against the
        bytes on disk right now; then write job COMPLETE +
        job.result_artifact_id + job.content_hash + the completion ledger
        event, atomically.

        Two entry paths, one contract:
        - Worker path: job in COMMITTING, caller holds the live lease
          (owner + token + unexpired). Fencing is checked BEFORE any
          terminal handling: a stale token is rejected even against an
          already-COMPLETE job — a fenced worker never earns a success
          signal.
        - Resolver path: job in UNCERTAIN (post-reclaim), actor in
          RECLAIM_ACTORS. The artifact's recorded fencing token must
          predate the reclaim (token lineage), proving the bytes were
          produced under the superseded epoch.

        Idempotency: repeating the commit for an already-COMPLETE job with
        the SAME verified artifact is a no-op returning the current row
        (no duplicate ledger event); a different artifact is a
        contradiction and is rejected. There is never a durable state where
        the job says COMPLETE but the referenced verified artifact does
        not exist or its required verification evidence is absent.
        """
        with self.store.write_txn() as (conn, now):
            job = self._get(conn, "jobs", "job_id", job_id)
            art = self._get(conn, "artifacts", "artifact_id", artifact_id)
            status = job["status"]
            tok = None
            resolver = False
            if status == "COMPLETE":
                # Duplicate completion: fencing first (stale workers never
                # get a success signal), then agreement with history.
                if worker_id is not None:
                    tok = self._token_int(fencing_token, "commit_artifact",
                                          job_id)
                    self._check_fenced_execution(conn, now, job, worker_id,
                                                 tok, "commit_artifact")
                expected = art["content_hash"] or artifact_id
                if job["result_artifact_id"] == artifact_id and \
                        job["content_hash"] == expected:
                    return dict(job)
                raise TransitionRejected(
                    f"commit_artifact: contradictory duplicate completion"
                    f" for job {job_id}")
            if status == "COMMITTING":
                if worker_id is None:
                    raise TransitionRejected(
                        "commit_artifact: COMMITTING completion requires"
                        " the owning worker's identity")
                tok = self._token_int(fencing_token, "commit_artifact",
                                      job_id)
                self._check_fenced_execution(conn, now, job, worker_id,
                                             tok, "commit_artifact")
            elif status == "UNCERTAIN":
                # Deterministic adoption by the recovery authority: the
                # artifact must provably predate the reclaim that opened
                # the UNCERTAIN state (contract D5 resolver).
                if actor not in T.RECLAIM_ACTORS:
                    raise TransitionRejected(
                        f"commit_artifact: UNCERTAIN adoption requires a"
                        f" recovery authority actor, got {actor!r}")
                if job["owner_worker_id"] is not None:
                    raise TransitionRejected(
                        f"commit_artifact: UNCERTAIN job {job_id} still"
                        f" has an owner")
                # R14: token lineage comes from this job's own staging
                # record when one exists (the deduplicated row's token
                # may belong to another job's staging epoch); legacy
                # rows without a staging record fall back to the row.
                staging = self._staging_record(conn, artifact_id, job_id)
                lineage_token = (staging["fencing_token"]
                                 if staging is not None
                                 else art["fencing_token"])
                if lineage_token is None or \
                        int(lineage_token) >= int(job["fencing_token"]):
                    raise TransitionRejected(
                        f"commit_artifact: artifact {artifact_id} token"
                        f" lineage does not predate the reclaim")
                resolver = True
            else:
                raise TransitionRejected(
                    f"commit_artifact: cannot complete job {job_id} in"
                    f" state {status}")
            # R14: linkage is per-job staging provenance, not the
            # deduplicated row's first-writer job_id (two jobs producing
            # identical bytes share one artifact row, and each staged it).
            self._require_staging_provenance(conn, art, artifact_id,
                                             job_id)
            content_hash = self._assert_artifact_verified(conn, art)
            conn.execute(
                "UPDATE jobs SET status='COMPLETE', commit_outcome='SUCCESS',"
                " commit_evidence=?, result_artifact_id=?, content_hash=?,"
                " updated_at=? WHERE job_id=?",
                (_canon(evidence or {}), artifact_id, content_hash,
                 now, job_id),
            )
            self._append_event(
                conn, now, "job.committed",
                {"job_id": job_id, "worker_id": worker_id,
                 "fencing_token": tok, "artifact_id": artifact_id,
                 "content_hash": content_hash, "outcome": "SUCCESS",
                 "evidence": evidence or {}, "resolver": resolver},
                actor)
            return dict(self._get(conn, "jobs", "job_id", job_id))

    def fail_job_execution(self, job_id: str, worker_id: str,
                           fencing_token: int, *, actor: str, reason: str,
                           evidence: dict | None = None) -> dict:
        """Record execution failure: (CLAIMED/RUNNING/COMMITTING) -> FAILED.

        The failure path of the old commit contract, kept as its own
        explicit operation. FAILED is not COMPLETE and never references an
        artifact; fencing (owner/token/live lease) is enforced identically.
        Idempotent: repeating the failure for an already-FAILED job is a
        no-op; failing a COMPLETE job is a contradiction and is rejected.
        """
        if not isinstance(reason, str) or not reason.strip():
            raise TransitionRejected(
                "fail_job_execution: a non-empty reason is required")
        tok = self._token_int(fencing_token, "fail_job_execution", job_id)
        with self.store.write_txn() as (conn, now):
            job = self._get(conn, "jobs", "job_id", job_id)
            # Fencing identity before terminal handling, as in commit.
            self._check_fenced_execution(conn, now, job, worker_id, tok,
                                         "fail_job_execution")
            status = job["status"]
            if status == "FAILED":
                return dict(job)
            if status == "COMPLETE":
                raise TransitionRejected(
                    f"fail_job_execution: job {job_id} already COMPLETE")
            if status not in ("CLAIMED", "RUNNING", "COMMITTING"):
                raise TransitionRejected(
                    f"fail_job_execution: cannot fail job {job_id} in"
                    f" state {status}")
            conn.execute(
                "UPDATE jobs SET status='FAILED', commit_outcome='FAILURE',"
                " commit_evidence=?, updated_at=? WHERE job_id=?",
                (_canon({"reason": reason, **(evidence or {})}),
                 now, job_id),
            )
            self._append_event(
                conn, now, "job.committed",
                {"job_id": job_id, "worker_id": worker_id,
                 "fencing_token": tok, "outcome": "FAILURE",
                 "reason": reason, "evidence": evidence or {}}, actor)
            return dict(self._get(conn, "jobs", "job_id", job_id))

    # ------------------------------------------- uncertain completion
    def inspect_uncertain_completion(self, job_id: str) -> dict:
        """Deterministically inspect an uncertain completion (R5, read-only).

        Distinguishes, without guessing:
        - COMMITTED: job COMPLETE and the referenced artifact is intact
          (bytes exist, hash matches, required receipts present).
        - UNCERTAIN: job COMPLETE with an integrity contradiction, OR job
          not COMPLETE but a fully verified artifact exists that the
          recovery authority could adopt via commit_artifact().
        - NOT_COMMITTED: job not COMPLETE and no adoptable artifact exists.

        An artifact existing on disk does NOT itself prove completion, and
        a COMPLETE job with missing/corrupt artifact bytes is reported as
        an integrity contradiction — never silently manufactured.
        """
        job = self._get(self.store.conn, "jobs", "job_id", job_id)
        status = job["status"]
        if status == "COMPLETE":
            problems = self._completion_integrity_problems(job)
            if not problems:
                return {"job_id": job_id, "job_status": status,
                        "disposition": "COMMITTED",
                        "integrity": "consistent", "problems": [],
                        "artifact_id": job["result_artifact_id"],
                        "content_hash": job["content_hash"]}
            return {"job_id": job_id, "job_status": status,
                    "disposition": "UNCERTAIN",
                    "integrity": "contradiction", "problems": problems,
                    "artifact_id": job["result_artifact_id"],
                    "content_hash": job["content_hash"]}
        candidates = []
        seen = set()
        # R14: candidates are artifacts this job staged — via the
        # deduplicated row's job_id OR via a per-job staging record (two
        # jobs producing identical bytes share one artifact row).
        cand_rows = self.store.conn.execute(
            "SELECT a.*, s.uri AS staging_uri,"
            " s.fencing_token AS staging_token FROM artifacts a"
            " JOIN artifact_stagings s ON s.artifact_id=a.artifact_id"
            " WHERE s.job_id=?",
            (job_id,)).fetchall()
        cand_rows += self.store.conn.execute(
            "SELECT a.*, NULL AS staging_uri, NULL AS staging_token"
            " FROM artifacts a WHERE a.job_id=?",
            (job_id,)).fetchall()
        for r in cand_rows:
            a = dict(r)
            if a["artifact_id"] in seen:
                continue
            seen.add(a["artifact_id"])
            expected = a["content_hash"] or a["artifact_id"]
            uri = a["staging_uri"] or a["uri"]
            data = self._read_staged_bytes(uri)
            h = hashlib.sha256(data).hexdigest() if data is not None else None
            receipts_ok = True
            for vid, ver in REQUIRED_VALIDATORS:
                rr = self.store.conn.execute(
                    "SELECT content_hash FROM validations WHERE artifact_id=?"
                    " AND validator_id=? AND validator_version=?"
                    " AND result='PASS' AND quarantined=0"
                    " ORDER BY validated_at DESC LIMIT 1",
                    (a["artifact_id"], vid, ver)).fetchone()
                if rr is None or (rr["content_hash"] or "") != expected:
                    receipts_ok = False
            verified = (a["status"] in ("VALIDATED", "RELEASED")
                        and data is not None and h == expected
                        and receipts_ok)
            candidates.append({
                "artifact_id": a["artifact_id"], "content_hash": expected,
                "status": a["status"], "bytes_present": data is not None,
                "hash_matches": h == expected, "receipts_ok": receipts_ok,
                "verified": verified,
                "fencing_token": (a["staging_token"]
                                  if a["staging_token"] is not None
                                  else a["fencing_token"])})
        adoptable = [c for c in candidates if c["verified"]]
        disposition = "UNCERTAIN" if adoptable else "NOT_COMMITTED"
        return {"job_id": job_id, "job_status": status,
                "disposition": disposition,
                "adoptable_artifacts": [c["artifact_id"] for c in adoptable],
                "candidates": candidates}

    def _completion_integrity_problems(self, job: dict) -> list[str]:
        """Both-directions integrity check for a COMPLETE job (read-only)."""
        problems = []
        artifact_id = job["result_artifact_id"]
        content_hash = job["content_hash"]
        if not artifact_id:
            return ["COMPLETE job references no artifact"]
        if not content_hash:
            problems.append("COMPLETE job references no content hash")
        row = self.store.conn.execute(
            "SELECT * FROM artifacts WHERE artifact_id=?",
            (artifact_id,)).fetchone()
        if row is None:
            problems.append(f"referenced artifact {artifact_id} row missing")
            return problems
        a = dict(row)
        expected = a["content_hash"] or artifact_id
        if content_hash and content_hash != expected:
            problems.append(
                f"job content_hash {content_hash} != artifact {expected}")
        data = self._read_staged_bytes(a["uri"])
        if data is None:
            problems.append(f"artifact {artifact_id} bytes missing")
        else:
            h = hashlib.sha256(data).hexdigest()
            if h != expected:
                problems.append(
                    f"artifact {artifact_id} bytes hash to {h},"
                    f" expected {expected}")
        for vid, ver in REQUIRED_VALIDATORS:
            rr = self.store.conn.execute(
                "SELECT content_hash FROM validations WHERE artifact_id=?"
                " AND validator_id=? AND validator_version=?"
                " AND result='PASS' AND quarantined=0"
                " ORDER BY validated_at DESC LIMIT 1",
                (artifact_id, vid, ver)).fetchone()
            if rr is None or (rr["content_hash"] or "") != expected:
                problems.append(
                    f"missing PASS receipt {vid}/{ver} for {expected}")
        return problems

    def classify_staging_orphans(self, task_id: str | None = None) -> list[dict]:
        """Read-only: classify staged byte files with no artifact row.

        A crash between the fsync'd byte write and the artifact-row INSERT
        leaves inert orphan bytes. They are reported here — never adopted,
        never deleted, never treated as completion evidence. (Full orphan
        reconciliation on recovery events belongs to the later reconciler;
        this is the minimal read-only classifier R5 needs for I-4.)
        """
        out = []
        root = self.staging_root
        if not os.path.isdir(root):
            return out
        for dirpath, _dirs, filenames in os.walk(root):
            rel = os.path.relpath(dirpath, root)
            parts = rel.split(os.sep)
            # layout: staging/<task_id>/<job_id>/<attempt>/<hash>
            f_task = parts[0] if len(parts) > 0 and parts[0] != "." else None
            f_job = parts[1] if len(parts) > 1 else None
            if task_id is not None and f_task != task_id:
                continue
            for fn in filenames:
                row = self.store.conn.execute(
                    "SELECT artifact_id FROM artifacts WHERE artifact_id=?",
                    (fn,)).fetchone()
                if row is None:
                    out.append({
                        "path": os.path.join(dirpath, fn),
                        "artifact_id": fn,
                        "task_id": f_task, "job_id": f_job,
                        "classification": "orphan_bytes_no_row"})
        return out

    # ------------------------------------------------- R5 checkpoints
    def stage_checkpoint(self, task_id: str, *, actor: str, manifest: list,
                         trigger: str, stage_id: str | None = None,
                         ledger_tip_seq: int | None = None,
                         supersedes: str | None = None,
                         versions: dict | None = None) -> dict:
        """Stage a checkpoint candidate with deterministic identity (R5).

        checkpoint_id = sha256(canonical manifest). The manifest must
        identify the exact durable state: every entry names an artifact_id
        and its content_hash. Equivalent logical manifests serialize
        identically (entries sorted by artifact_id, canonical JSON), so
        equivalent manifests produce the same checkpoint ID and different
        manifests produce different IDs. No random IDs.

        The trigger must be one of CHECKPOINT_TRIGGERS (contract D8) —
        policy, pre_risky_operation, or operator_request. A bare timer
        without verification capacity is never a trigger.

        Staging creates an UNVERIFIED candidate: creation without
        verification is not a checkpoint and is never trusted for resume.
        Idempotent: re-staging the same canonical manifest returns the
        existing row (same identity), never a duplicate.
        """
        if _is_worker_actor(actor):
            raise TransitionRejected(
                f"checkpoint: actor {actor!r} may not advance checkpoint"
                f" state; stale workers cannot advance checkpoints")
        if trigger not in CHECKPOINT_TRIGGERS:
            raise TransitionRejected(
                f"checkpoint: unknown trigger {trigger!r}; expected one of"
                f" {list(CHECKPOINT_TRIGGERS)}")
        if not isinstance(manifest, list) or not manifest:
            raise TransitionRejected(
                "checkpoint: manifest must be a non-empty list of"
                " artifact/content identities")
        entries = []
        for e in manifest:
            if not isinstance(e, dict):
                raise TransitionRejected(
                    f"checkpoint: manifest entry {e!r} is not a dict")
            for k in ("artifact_id", "content_hash"):
                if not e.get(k):
                    raise TransitionRejected(
                        f"checkpoint: manifest entry missing {k!r}: {e!r}")
            entries.append({k: e[k] for k in
                            ("artifact_id", "content_hash", "size", "kind",
                             "job_id") if k in e})
        entries.sort(key=lambda d: d["artifact_id"])
        canonical = _canon(entries)
        checkpoint_id = hashlib.sha256(canonical.encode()).hexdigest()
        versions = versions or {}
        with self.store.write_txn() as (conn, now):
            self._get(conn, "tasks", "task_id", task_id)
            if ledger_tip_seq is None:
                ledger_tip_seq = conn.execute(
                    "SELECT COALESCE(MAX(seq), 0) FROM ledger").fetchone()[0]
            cur = conn.execute(
                "INSERT INTO checkpoints(checkpoint_id, task_id, stage_id,"
                " ledger_tip_seq, artifact_manifest, supersedes,"
                " input_version, schema_version, methodology_version,"
                " capability_versions, os_version, verification_status,"
                " trigger, canonical_manifest, created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?, 'UNVERIFIED', ?, ?, ?, ?)"
                " ON CONFLICT(checkpoint_id) DO NOTHING",
                (checkpoint_id, task_id, stage_id, ledger_tip_seq,
                 canonical, supersedes,
                 versions.get("input_version"), versions.get("schema_version"),
                 versions.get("methodology_version"),
                 _canon(versions.get("capability_versions", {})),
                 versions.get("os_version"), trigger, canonical, now, now),
            )
            if cur.rowcount == 1:
                self._append_event(
                    conn, now, "checkpoint.created",
                    {"checkpoint_id": checkpoint_id, "task_id": task_id,
                     "trigger": trigger,
                     "manifest_entries": len(entries)}, actor)
            return dict(self._get(conn, "checkpoints", "checkpoint_id",
                                  checkpoint_id))

    def verify_checkpoint(self, checkpoint_id: str, *, actor: str,
                          release: bool = False) -> dict:
        """Verify a staged checkpoint candidate (R5): UNVERIFIED -> VERIFIED
        or CORRUPT, atomically.

        Verification establishes, in one transaction:
        - the manifest is canonical and checkpoint_id == sha256(manifest);
        - every referenced artifact exists with the expected content hash;
        - every referenced artifact's bytes re-read from disk hash to the
          expected content hash (independently confirmed, not trusted);
        - required validator PASS receipts exist for each exact content
          hash;
        - a durable verification receipt records the exact checkpoint
          identity (stored on the checkpoint row as canonical JSON).

        Release checkpoints (release=True) always revalidate 100% of the
        manifest — no sampling, no trust in earlier partial verification.
        (Phase 1C always revalidates fully; release additionally records
        the release attestation in the receipt.)

        On success the checkpoint becomes VERIFIED and the task's
        latest-known-good pointer advances to the VERIFIED checkpoint with
        the greatest created_at (contract D8). On failure the candidate is
        CORRUPT (terminal, retained for forensics) and latest-known-good
        never moves: a failed verification can never replace it.
        """
        if _is_worker_actor(actor):
            raise TransitionRejected(
                f"checkpoint: actor {actor!r} may not verify checkpoints")
        with self.store.write_txn() as (conn, now):
            ck = self._get(conn, "checkpoints", "checkpoint_id",
                           checkpoint_id)
            if ck["verification_status"] != "UNVERIFIED":
                raise TransitionRejected(
                    f"checkpoint {checkpoint_id}: already"
                    f" {ck['verification_status']}")
            canonical = ck["canonical_manifest"]
            if not canonical:
                raise TransitionRejected(
                    f"checkpoint {checkpoint_id}: no canonical manifest;"
                    f" use stage_checkpoint() for R5 checkpoints")
            failures: list[str] = []
            if hashlib.sha256(canonical.encode()).hexdigest() \
                    != checkpoint_id:
                failures.append(
                    "checkpoint_id != sha256(canonical_manifest)")
            manifest = json.loads(canonical)
            art_results = []
            for e in manifest:
                ar = conn.execute(
                    "SELECT * FROM artifacts WHERE artifact_id=?",
                    (e["artifact_id"],)).fetchone()
                aprobs: list[str] = []
                if ar is None:
                    aprobs.append("artifact row missing")
                else:
                    a = dict(ar)
                    expected = e["content_hash"]
                    if a["status"] not in ("VALIDATED", "RELEASED"):
                        aprobs.append(
                            f"artifact status {a['status']}, not VERIFIED")
                    if (a["content_hash"] or a["artifact_id"]) != expected:
                        aprobs.append("artifact content hash != manifest")
                    data = self._read_staged_bytes(a["uri"])
                    if data is None:
                        aprobs.append("artifact bytes missing")
                    else:
                        h = hashlib.sha256(data).hexdigest()
                        if h != expected:
                            aprobs.append(
                                f"bytes re-hash to {h}, expected {expected}")
                        if a["size"] is not None and \
                                a["size"] != len(data):
                            aprobs.append("size mismatch")
                    for vid, ver in REQUIRED_VALIDATORS:
                        rr = conn.execute(
                            "SELECT content_hash FROM validations"
                            " WHERE artifact_id=? AND validator_id=?"
                            " AND validator_version=? AND result='PASS'"
                            " AND quarantined=0"
                            " ORDER BY validated_at DESC LIMIT 1",
                            (e["artifact_id"], vid, ver)).fetchone()
                        if rr is None or (rr["content_hash"] or "") \
                                != expected:
                            aprobs.append(
                                f"missing PASS receipt {vid}/{ver} for"
                                f" content hash")
                art_results.append({"artifact_id": e["artifact_id"],
                                    "ok": not aprobs, "problems": aprobs})
                failures.extend(
                    f"artifact {e['artifact_id']}: {p}" for p in aprobs)
            # Ledger-tip binding: seqs must be contiguous (no deletes in
            # normal operation); the tip is recorded in the receipt.
            tip = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) FROM ledger").fetchone()[0]
            cnt = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
            ledger_contiguous = (tip == cnt)
            if not ledger_contiguous:
                failures.append("ledger seq gap at verification tip")
            receipt = {
                "checkpoint_id": checkpoint_id,
                "manifest_hash": hashlib.sha256(
                    canonical.encode()).hexdigest(),
                "method": "full-revalidation",
                "release": bool(release),
                "manifest_full": True,
                "artifacts": art_results,
                "ledger_tip_seq": ck["ledger_tip_seq"],
                "ledger_tip_now": tip,
                "ledger_chain_contiguous": ledger_contiguous,
                "verified_at": now,
            }
            if failures:
                conn.execute(
                    "UPDATE checkpoints SET verification_status='CORRUPT',"
                    " verified_at=?, verification_receipt=?, updated_at=?"
                    " WHERE checkpoint_id=?",
                    (now, _canon({**receipt, "failures": failures}),
                     now, checkpoint_id))
                for ev_status in ("VERIFYING", "CORRUPT"):
                    self._append_event(
                        conn, now, "checkpoint.verification",
                        {"checkpoint_id": checkpoint_id,
                         "to": ev_status,
                         "receipt": {**receipt, "failures": failures}},
                        actor)
                return dict(self._get(conn, "checkpoints", "checkpoint_id",
                                      checkpoint_id))
            conn.execute(
                "UPDATE checkpoints SET verification_status='VERIFIED',"
                " verified_at=?, verify_method=?, manifest_full=1,"
                " verification_receipt=?, updated_at=?"
                " WHERE checkpoint_id=?",
                (now, "full-revalidation", _canon(receipt),
                 now, checkpoint_id))
            for ev_status in ("VERIFYING", "VERIFIED"):
                self._append_event(
                    conn, now, "checkpoint.verification",
                    {"checkpoint_id": checkpoint_id, "to": ev_status,
                     "receipt": receipt}, actor)
            # Latest-known-good: the VERIFIED checkpoint with the greatest
            # created_at for this task (contract D8). Only verified
            # checkpoints can advance it; failure above never reaches here.
            best = conn.execute(
                "SELECT checkpoint_id FROM checkpoints"
                " WHERE task_id=? AND verification_status='VERIFIED'"
                " ORDER BY created_at DESC LIMIT 1",
                (ck["task_id"],)).fetchone()
            conn.execute(
                "INSERT INTO checkpoint_pointers(name, checkpoint_id,"
                " task_id, updated_at) VALUES(?, ?, ?, ?)"
                " ON CONFLICT(name) DO UPDATE SET"
                " checkpoint_id=excluded.checkpoint_id,"
                " task_id=excluded.task_id, updated_at=excluded.updated_at",
                (f"lkg:{ck['task_id']}", best["checkpoint_id"],
                 ck["task_id"], now))
            return dict(self._get(conn, "checkpoints", "checkpoint_id",
                                  checkpoint_id))

    def invalidate_checkpoint(self, checkpoint_id: str, *, actor: str,
                              evidence: dict) -> dict:
        """Invalidate a VERIFIED checkpoint whose manifest/artifact bytes
        were corrupted AFTER successful verification.

        R14-B FI-05 finding: VERIFIED was a terminal state and
        latest_known_good() trusted the pointer row without re-validating,
        so a post-verification disk corruption was undetectable and the
        corrupted checkpoint stayed "known good" forever.

        VERIFIED -> CORRUPT, atomically, with a corruption receipt. The
        latest-known-good pointer is NOT moved here (invariant: the
        pointer advances only through verify_checkpoint() success);
        combined with the latest_known_good() hardening below, an
        invalidated checkpoint is never returned as trusted again —
        recovery must fall back to the previous intact checkpoint and
        re-derive coverage from the ledger.

        Authority: system / operator / test only. Workers and the
        lease-scoped recovery controller can never invalidate a VERIFIED
        checkpoint.
        """
        if actor not in ("system", "operator", "test"):
            raise TransitionRejected(
                f"invalidate_checkpoint: actor {actor!r} is not authorized;"
                " only system/operator/test may invalidate a VERIFIED"
                " checkpoint")
        if not isinstance(evidence, dict) or not evidence:
            raise TransitionRejected(
                "invalidate_checkpoint requires a non-empty corruption"
                " evidence dict")
        with self.store.write_txn() as (conn, now):
            ck = self._get(conn, "checkpoints", "checkpoint_id",
                           checkpoint_id)
            if ck["verification_status"] != "VERIFIED":
                raise TransitionRejected(
                    f"invalidate_checkpoint: checkpoint {checkpoint_id} is"
                    f" {ck['verification_status']}, not VERIFIED")
            receipt = {"checkpoint_id": checkpoint_id,
                       "from": "VERIFIED", "to": "CORRUPT",
                       "method": "post-verification-invalidation",
                       "corruption_evidence": evidence,
                       "invalidated_at": now, "invalidated_by": actor}
            conn.execute(
                "UPDATE checkpoints SET verification_status='CORRUPT',"
                " verification_receipt=?, updated_at=? WHERE checkpoint_id=?",
                (_canon(receipt), now, checkpoint_id))
            self._append_event(
                conn, now, "checkpoint.invalidated",
                {"checkpoint_id": checkpoint_id, "to": "CORRUPT",
                 "receipt": receipt}, actor)
            return dict(self._get(conn, "checkpoints", "checkpoint_id",
                                  checkpoint_id))

    def latest_known_good(self, task_id: str) -> dict | None:
        """Read-only: the latest verified checkpoint known to be safe for
        a task, or None when no verified checkpoint exists.

        Defined ONLY over VERIFIED checkpoints (never creation time alone,
        never the latest attempt, never an unverified candidate): the
        pointer advances exclusively through verify_checkpoint() success.

        Hardening (R14-B FI-05): a pointer that no longer resolves to a
        VERIFIED checkpoint is NOT "known good" — post-verification
        corruption (see invalidate_checkpoint()) invalidates trust even
        though the pointer row still names the checkpoint. None is
        returned in that case instead of the corrupted checkpoint.
        """
        row = self.store.conn.execute(
            "SELECT checkpoint_id FROM checkpoint_pointers WHERE name=?",
            (f"lkg:{task_id}",)).fetchone()
        if row is None:
            return None
        ck = dict(self._get(self.store.conn, "checkpoints",
                            "checkpoint_id", row["checkpoint_id"]))
        if ck["verification_status"] != "VERIFIED":
            return None
        return ck

    def commit_job_result(self, *args, **kwargs):
        """REMOVED in Phase 1C R5 (I-4 correction).

        The old direct-completion path — CLAIMED/RUNNING -> COMPLETE on a
        result string with no artifact, no content hash, no validation —
        violated invariant I-4 and no longer exists. The single
        authoritative completion contract is now:

            stage_artifact() -> begin_commit() -> verify_artifact()
                -> commit_artifact()

        (and fail_job_execution() for the FAILURE path). This stub exists
        only to fail loudly if any caller was missed in the migration.
        """
        raise TransitionRejected(
            "commit_job_result() was removed in Phase 1C R5 (I-4): a job"
            " may enter COMPLETE only through commit_artifact() with a"
            " verified artifact. See stage_artifact/begin_commit/"
            "verify_artifact/commit_artifact.")

    # --------------------------------------------------------------- workers
    def create_worker(self, worker_id: str | None, actor: str,
                      task_id: str | None = None,
                      capabilities: dict | None = None) -> dict:
        worker_id = worker_id or _new_id("worker-")
        with self.store.write_txn() as (conn, now):
            conn.execute(
                "INSERT INTO workers(worker_id, status, task_id, capabilities,"
                " created_at, updated_at)"
                " VALUES(?, 'PROVISIONING', ?, ?, ?, ?)",
                (worker_id, task_id, _canon(capabilities or {}), now, now),
            )
            self._append_event(conn, now, "worker.created",
                               {"worker_id": worker_id}, actor)
            return dict(self._get(conn, "workers", "worker_id", worker_id))

    def transition_worker(self, worker_id: str, to_state: str,
                          actor: str) -> dict:
        return self._transition(
            "workers", "worker_id", worker_id, to_state,
            T.WORKER_TRANSITIONS, actor,
            ledger_event="worker.transition",
            ledger_payload={"worker_id": worker_id},
        )

    def get_worker(self, worker_id: str) -> dict:
        """Read a worker row (read-only connection)."""
        return dict(self._get(self.store.conn, "workers", "worker_id",
                              worker_id))

    def mark_worker_seen(self, worker_id: str, actor: str) -> None:
        """last_seen_at is stamped with store time on ingest (04: Heartbeat
        says ts is store time on ingest) — never a worker clock."""
        with self.store.write_txn() as (conn, now):
            conn.execute(
                "UPDATE workers SET last_seen_at=?, updated_at=?"
                " WHERE worker_id=?", (now, now, worker_id))

    def ingest_heartbeat(self, worker_id: str, proc_id: str,
                         job_id: str | None, fencing_token: int | None,
                         hb_seq: int, worker_state: str | None,
                         current_operation: str | None, actor: str,
                         worker_reported_ts: float | None = None,
                         progress_done: float | None = None,
                         progress_total: float | None = None) -> dict:
        """Durable heartbeat ingestion (Phase 1B, extended R3).

        The heartbeat is a time-series evidence record, NOT authority:
        - ts is stamped with authoritative store time on ingest; the
          worker-supplied timestamp is stored as informational metadata only
          and never influences expiry, ordering, or fencing.
        - hb_seq must strictly increase per (worker_id, proc_id): a duplicate
          or out-of-order heartbeat is rejected WITHOUT mutating state, so
          duplicate delivery can never create contradictory state.
        - heartbeats from stale/fenced workers are rejected: if job_id names
          a job, the heartbeat's fencing token must equal the job's current
          fencing token and the worker must be the current lease owner. A
          fencing rejection journals a worker.heartbeat_fenced milestone
          (observed vs authoritative token/owner) and raises LeaseError.
          The rejection mutates nothing else.
        - R3 / D9 protocol: the heartbeat may carry informational progress
          (progress_done/progress_total). When present it is routed to the
          same _apply_progress path as update_job_progress — identical
          checks, one atomic transaction, progress_updated_at stamped. A
          heartbeat WITHOUT progress never touches progress_updated_at.
          A heartbeat that names no job cannot carry progress.
        - a heartbeat never extends a lease, never renews authority, never
          reclaims, never bumps the fencing token, and is never evidence of
          job success. No ledger event per accepted heartbeat
          (Phase 0 20: summaries and milestones only).
        """
        if isinstance(hb_seq, bool) or not isinstance(hb_seq, int) \
                or hb_seq < 0:
            raise TransitionRejected(
                f"heartbeat hb_seq must be a non-negative int, got {hb_seq!r}")
        if progress_done is not None and job_id is None:
            raise TransitionRejected(
                "heartbeat carries progress but names no job")
        try:
            with self.store.write_txn() as (conn, now):
                self._get(conn, "workers", "worker_id", worker_id)  # must exist
                if job_id is not None:
                    job = self._get(conn, "jobs", "job_id", job_id)
                    if fencing_token is None or \
                            job["fencing_token"] is None or \
                            int(fencing_token) != int(job["fencing_token"]):
                        raise _FencedHeartbeat(
                            worker_id, proc_id, job_id, fencing_token,
                            job["fencing_token"], job["owner_worker_id"],
                            f"stale heartbeat for job {job_id}: fencing token "
                            f"{fencing_token!r} != current "
                            f"{job['fencing_token']!r}")
                    if job["owner_worker_id"] != worker_id:
                        raise _FencedHeartbeat(
                            worker_id, proc_id, job_id, fencing_token,
                            job["fencing_token"], job["owner_worker_id"],
                            f"heartbeat for job {job_id} from non-owner "
                            f"{worker_id!r}")
                prev = conn.execute(
                    "SELECT MAX(hb_seq) FROM heartbeats"
                    " WHERE worker_id=? AND proc_id=?",
                    (worker_id, proc_id)).fetchone()[0]
                if prev is not None and hb_seq <= int(prev):
                    raise TransitionRejected(
                        f"duplicate/out-of-order heartbeat {worker_id}/{proc_id}:"
                        f" hb_seq {hb_seq} <= {prev}")
                cur = conn.execute(
                    "INSERT INTO heartbeats(worker_id, proc_id, job_id,"
                    " fencing_token, hb_seq, worker_state, current_operation,"
                    " worker_reported_ts, ts)"
                    " VALUES(?,?,?,?,?,?,?,?,?)",
                    (worker_id, proc_id, job_id, fencing_token, hb_seq,
                     worker_state, current_operation, worker_reported_ts, now),
                )
                if progress_done is not None:
                    # D9 informational channel: same checks, same
                    # transaction — all-or-nothing with the heartbeat row.
                    self._apply_progress(conn, now, job_id, worker_id,
                                         fencing_token, progress_done,
                                         progress_total)
                conn.execute(
                    "UPDATE workers SET last_seen_at=?, updated_at=?"
                    " WHERE worker_id=?", (now, now, worker_id))
                row = conn.execute(
                    "SELECT * FROM heartbeats WHERE seq=?",
                    (cur.lastrowid,)).fetchone()
                return dict(row)
        except _FencedHeartbeat as fh:
            # The fencing check failed inside the authoritative transaction;
            # the transaction rolled back with zero mutation. Journal the
            # rejection as a security-relevant milestone in a separate
            # transaction, then surface the fencing to the caller. Journaling
            # is best-effort: it must never mask the LeaseError.
            try:
                self.append_event(
                    "worker.heartbeat_fenced",
                    {"worker_id": fh.worker_id, "proc_id": fh.proc_id,
                     "job_id": fh.job_id, "observed_token": fh.observed_token,
                     "current_token": fh.current_token,
                     "current_owner": fh.current_owner,
                     "reason": fh.reason},
                    actor)
            except Exception:
                pass
            raise LeaseError(fh.reason)

    def heartbeats_for(self, worker_id: str, proc_id: str | None = None,
                       limit: int = 100) -> list[dict]:
        """Read heartbeat evidence (newest first). Read-only."""
        q = ("SELECT * FROM heartbeats WHERE worker_id=? "
             + ("AND proc_id=? " if proc_id else "")
             + "ORDER BY seq DESC LIMIT ?")
        params = (worker_id, proc_id, limit) if proc_id \
            else (worker_id, limit)
        return [dict(r) for r in
                self.store.conn.execute(q, params).fetchall()]

    # -------------------------------------------------------------- approvals
    def create_approval(self, approval_id: str | None, task_id: str,
                        reason: str, requested_action: str, actor: str,
                        job_id: str | None = None, stage_id: str | None = None,
                        evidence_refs: list | None = None, risk: str | None = None,
                        options: list | None = None, deadline: float | None = None,
                        checkpoint_id: str | None = None,
                        on_timeout: str | None = None) -> dict:
        approval_id = approval_id or _new_id("approval-")
        with self.store.write_txn() as (conn, now):
            self._get(conn, "tasks", "task_id", task_id)
            conn.execute(
                "INSERT INTO approvals(approval_id, task_id, job_id, stage_id,"
                " reason, requested_action, evidence_refs, risk, options,"
                " deadline, checkpoint_id, on_timeout, status,"
                " created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'PENDING',?,?)",
                (approval_id, task_id, job_id, stage_id, reason,
                 requested_action, _canon(evidence_refs or []), risk,
                 _canon(options or []), deadline, checkpoint_id, on_timeout,
                 now, now),
            )
            self._append_event(conn, now, "approval.requested",
                               {"approval_id": approval_id, "task_id": task_id},
                               actor)
            return dict(self._get(conn, "approvals", "approval_id", approval_id))

    def decide_approval(self, approval_id: str, decision: str, decided_by: str,
                        actor: str, decision_reason: str | None = None) -> dict:
        if decision not in ("APPROVED", "DENIED", "EXPIRED"):
            raise TransitionRejected(f"unknown approval decision {decision!r}")
        with self.store.write_txn() as (conn, now):
            row = self._get(conn, "approvals", "approval_id", approval_id)
            if row["status"] != "PENDING":
                raise TransitionRejected(
                    f"approval {approval_id} already decided: {row['status']}"
                )
            conn.execute(
                "UPDATE approvals SET status=?, decided_by=?, decided_at=?,"
                " decision_reason=?, updated_at=? WHERE approval_id=?",
                (decision, decided_by, now, decision_reason, now, approval_id),
            )
            self._append_event(
                conn, now, "approval.decided",
                {"approval_id": approval_id, "decision": decision,
                 "decided_by": decided_by}, actor)
            return dict(self._get(conn, "approvals", "approval_id", approval_id))

    # -------------------------------------------------------------- incidents
    def create_incident(self, incident_id: str | None, scope: str,
                        failure_class: str, stage_id: str | None,
                        capability_version: str | None, input_batch_id: str | None,
                        error_class: str, actor: str,
                        task_id: str | None = None,
                        detection: dict | None = None) -> dict:
        """Canonical incident signature per Phase 0 11.2/ADR-016:
        {failure_class, stage_id, capability_version, input_batch_id,
        error_class} — raw error messages excluded so variable parse errors
        from one poisoned batch collapse to one incident."""
        incident_id = incident_id or _new_id("incident-")
        signature = _canon({
            "failure_class": failure_class, "stage_id": stage_id,
            "capability_version": capability_version,
            "input_batch_id": input_batch_id, "error_class": error_class,
        })
        with self.store.write_txn() as (conn, now):
            conn.execute(
                "INSERT INTO incidents(incident_id, task_id, scope,"
                " failure_class, signature, detection, created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (incident_id, task_id, scope, failure_class, signature,
                 _canon(detection or {}), now, now),
            )
            self._append_event(conn, now, "incident.created",
                               {"incident_id": incident_id,
                                "signature": signature}, actor)
            return dict(self._get(conn, "incidents", "incident_id", incident_id))

    def set_incident_outcome(self, incident_id: str, outcome: str,
                             diagnosis: str | None, actor: str,
                             escalated_to: str | None = None) -> dict:
        with self.store.write_txn() as (conn, now):
            self._get(conn, "incidents", "incident_id", incident_id)
            conn.execute(
                "UPDATE incidents SET outcome=?, diagnosis=?, escalated_to=?,"
                " updated_at=? WHERE incident_id=?",
                (outcome, diagnosis, escalated_to, now, incident_id),
            )
            self._append_event(conn, now, "incident.outcome",
                               {"incident_id": incident_id, "outcome": outcome},
                               actor)
            return dict(self._get(conn, "incidents", "incident_id", incident_id))

    # ------------------------------------------------------- recovery records
    # The store records recovery evidence; the recovery controller (later
    # milestone) interprets it. Recovery is verified by progress (I-18), so
    # every attempt must carry observed effect + progress delta +
    # output-health delta, and 'success' requires positive progress delta
    # (enforced both here and by a CHECK constraint).
    def record_recovery_attempt(
        self, attempt_id: str | None, incident_id: str, rung: int,
        action: dict, observed_effect: str, progress_delta: float,
        output_health_delta: float, decision: str, actor: str,
        spend: dict | None = None,
    ) -> dict:
        if decision not in ("retry", "escalate", "success", "quarantine", "stop"):
            raise TransitionRejected(f"unknown recovery decision {decision!r}")
        if not observed_effect:
            raise TransitionRejected(
                "I-18: a recovery attempt must record its observed effect")
        if decision == "success" and progress_delta <= 0:
            raise TransitionRejected(
                "I-18: recovery recorded as 'success' requires a positive"
                " progress delta — action without progress is not recovery")
        attempt_id = attempt_id or _new_id("rec-")
        with self.store.write_txn() as (conn, now):
            self._get(conn, "incidents", "incident_id", incident_id)
            conn.execute(
                "INSERT INTO recovery_attempts(attempt_id, incident_id, rung,"
                " action, observed_effect, progress_delta, output_health_delta,"
                " decision, spend, actor, recorded_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (attempt_id, incident_id, rung, _canon(action), observed_effect,
                 progress_delta, output_health_delta, decision,
                 _canon(spend or {}), actor, now),
            )
            self._append_event(conn, now, "recovery.attempt",
                               {"attempt_id": attempt_id,
                                "incident_id": incident_id,
                                "decision": decision,
                                "progress_delta": progress_delta}, actor)
            row = conn.execute(
                "SELECT * FROM recovery_attempts WHERE attempt_id=?",
                (attempt_id,)).fetchone()
            return dict(row)

    def recovery_attempts_for(self, incident_id: str) -> list[dict]:
        rows = self.store.conn.execute(
            "SELECT * FROM recovery_attempts WHERE incident_id=?"
            " ORDER BY recorded_at", (incident_id,)).fetchall()
        return [dict(r) for r in rows]

    # --------------------------------------------- R8 recovery-controller
    # state. The recovery controller (Phase 1C R8) drives durable attempts
    # through CREATED -> RUNNING -> VERIFYING -> SUCCEEDED | FAILED (plus
    # CREATED -> BLOCKED and RUNNING -> UNCERTAIN) entirely in the store.
    # Every mutation below is a compare-and-swap inside ONE write_txn, so
    # two concurrent controllers produce exactly one authoritative attempt
    # and exactly one claim. These helpers never touch job ownership,
    # fencing tokens, leases, processes, or completion state — the
    # controller dispatches those effects only through R1/R2/R5 and the
    # existing execution substrate.
    _ATTEMPT_STATES = ("CREATED", "RUNNING", "VERIFYING", "SUCCEEDED",
                       "FAILED", "BLOCKED", "UNCERTAIN")
    _ATTEMPT_TERMINAL = ("SUCCEEDED", "FAILED", "BLOCKED")
    _ATTEMPT_OPEN = ("CREATED", "RUNNING", "VERIFYING", "UNCERTAIN")

    def open_recovery_incidents(self, scope: str = "recovery") -> list[dict]:
        """Read-only: every incident still open (no outcome recorded).

        Used by the recovery controller's crash-reconstruction pass to
        find attempts it may have orphaned. No mutation."""
        rows = self.store.conn.execute(
            "SELECT * FROM incidents WHERE outcome IS NULL AND scope=?"
            " ORDER BY created_at", (scope,)).fetchall()
        return [dict(r) for r in rows]

    def progress_evidence_for_job(self, job_id: str) -> dict:
        """Read-only authoritative progress evidence for recovery
        verification: the job row's own progress counters, the verified
        artifact count for the job's task, and the latest-known-good
        checkpoint pointer for the job's task. No mutation.

        This is the D6/contract progress bundle: heartbeats stay
        liveness-only; progress comes from this durable evidence."""
        row = self.store.conn.execute(
            "SELECT job_id, task_id, status, owner_worker_id,"
            " fencing_token, lease_acquired_at, lease_expires_at,"
            " progress_done, progress_total, progress_updated_at"
            " FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise TransitionRejected(f"unknown job {job_id}")
        job = dict(row)
        verified_artifacts = 0
        lkg = None
        if job.get("task_id"):
            verified_artifacts = self.store.conn.execute(
                "SELECT COUNT(*) AS n FROM artifacts"
                " WHERE task_id=? AND status='VERIFIED'",
                (job["task_id"],)).fetchone()["n"]
            ptr = self.latest_known_good(job["task_id"])
            lkg = ptr["checkpoint_id"] if ptr else None
        return {"job": job,
                "progress_done": job["progress_done"],
                "progress_total": job["progress_total"],
                "progress_updated_at": job["progress_updated_at"],
                "verified_artifacts": verified_artifacts,
                "latest_known_good": lkg}

    def find_or_create_recovery_incident(
            self, job_id: str, failure_class: str, detection: dict,
            actor: str) -> dict:
        """Stable incident identity for one underlying failure: one row
        per (job_id, failure_class), found or created inside ONE write_txn.
        Stable across controller/supervisor crashes, watchdog
        reevaluations, attempt retries, and process replacement — it is
        never derived from controller memory."""
        if failure_class not in ("STALLED", "DEAD", "LEASE_EXPIRED",
                                 "STALE_AUTHORITY", "UNCERTAIN"):
            raise TransitionRejected(
                f"unknown recovery failure class {failure_class!r}")
        signature = _canon({"scope": "recovery", "job_id": job_id,
                            "failure_class": failure_class})
        with self.store.write_txn() as (conn, now):
            row = conn.execute(
                "SELECT * FROM incidents WHERE scope='recovery'"
                " AND signature=?", (signature,)).fetchone()
            if row is not None:
                return dict(row)
            incident_id = _new_id("incident-")
            try:
                conn.execute(
                    "INSERT INTO incidents(incident_id, scope,"
                    " failure_class, signature, detection, created_at,"
                    " updated_at)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (incident_id, "recovery", failure_class, signature,
                     _canon(detection or {}), now, now),
                )
            except sqlite3.IntegrityError:
                # Lost the incident race: the winner's row is the
                # authoritative identity — observe it instead of
                # duplicating (UNIQUE incidents_recovery_signature).
                row = conn.execute(
                    "SELECT * FROM incidents WHERE scope='recovery'"
                    " AND signature=?", (signature,)).fetchone()
                if row is None:
                    raise
                return dict(row)
            self._append_event(conn, now, "incident.created",
                               {"incident_id": incident_id,
                                "job_id": job_id,
                                "failure_class": failure_class,
                                "signature": signature}, actor)
            return dict(self._get(conn, "incidents", "incident_id",
                                  incident_id))

    def get_recovery_incident(self, incident_id: str) -> dict | None:
        row = self.store.conn.execute(
            "SELECT * FROM incidents WHERE incident_id=?",
            (incident_id,)).fetchone()
        return dict(row) if row else None

    def create_recovery_attempt(
            self, *, incident_id: str, job_id: str, fencing_token: int,
            rung: int, rung_name: str, action: dict, failure_class: str,
            success_criterion: str, failure_criterion: str,
            evidence_before: dict, actor: str,
            idempotency_key: str | None = None) -> dict:
        """Durably create attempt N+1 for an incident (N = max existing
        attempt_number, in-transaction). Exactly one OPEN attempt per
        incident: the same transaction first returns an existing open
        attempt, so concurrent creators collapse to one authoritative
        attempt — the loser observes the winner. The UNIQUE(incident_id,
        attempt_number) constraint is the backstop for the residual
        same-number race. The attempt is born in CREATED with the full
        Recovery Contract fields; the external action must not run
        before this returns."""
        if not isinstance(fencing_token, int) or isinstance(
                fencing_token, bool):
            raise TransitionRejected(
                f"invalid fencing token {fencing_token!r}")
        if not action or not isinstance(action, dict) or \
                "name" not in action:
            raise TransitionRejected(
                "a recovery attempt requires a named action")
        idem = idempotency_key or _canon(
            {"incident_id": incident_id, "job_id": job_id,
             "fencing_token": fencing_token,
             "attempt_number": None})  # number filled below
        with self.store.write_txn() as (conn, now):
            self._get(conn, "incidents", "incident_id", incident_id)
            # Exactly one open attempt per incident: the check and the
            # insert are one atomic unit (BEGIN IMMEDIATE), so a loser
            # whose BEGIN blocked on the winner's commit observes the
            # winner's open row here instead of minting attempt N+1.
            # Terminal attempts do not suppress: the next attempt after a
            # completed one is a new number.
            open_row = conn.execute(
                "SELECT * FROM recovery_attempts WHERE incident_id=?"
                " AND attempt_state IN"
                " ('CREATED','RUNNING','VERIFYING','UNCERTAIN')"
                " ORDER BY attempt_number LIMIT 1",
                (incident_id,)).fetchone()
            if open_row is not None:
                return dict(open_row)
            mx = conn.execute(
                "SELECT MAX(attempt_number) FROM recovery_attempts"
                " WHERE incident_id=?", (incident_id,)).fetchone()[0]
            number = (mx or 0) + 1
            if idempotency_key is None:
                idem = _canon({"incident_id": incident_id,
                               "job_id": job_id,
                               "fencing_token": fencing_token,
                               "attempt_number": number})
            attempt_id = _new_id("rec-")
            try:
                conn.execute(
                    "INSERT INTO recovery_attempts(attempt_id, incident_id,"
                    " job_id, fencing_token, attempt_number, attempt_state,"
                    " rung, rung_name, action, observed_effect,"
                    " progress_delta,"
                    " output_health_delta, decision, spend, actor,"
                    " recorded_at, started_at, failure_class,"
                    " success_criterion, failure_criterion,"
                    " evidence_before, idempotency_key)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (attempt_id, incident_id, job_id, fencing_token,
                     number, "CREATED", rung, rung_name, _canon(action),
                     "", 0.0, 0.0, "retry", "{}", actor, now, now,
                     failure_class, success_criterion, failure_criterion,
                     _canon(evidence_before), idem),
                )
            except sqlite3.IntegrityError as exc:
                # Lost the creation race: the winner's attempt is the
                # authoritative one — observe it instead of duplicating.
                row = conn.execute(
                    "SELECT * FROM recovery_attempts WHERE incident_id=?"
                    " AND attempt_number=?",
                    (incident_id, number)).fetchone()
                if row is None:
                    raise
                return dict(row)
            self._append_event(conn, now, "recovery.attempt_created",
                               {"attempt_id": attempt_id,
                                "incident_id": incident_id,
                                "job_id": job_id,
                                "fencing_token": fencing_token,
                                "attempt_number": number,
                                "action": action,
                                "rung": rung}, actor)
            return dict(self._get(conn, "recovery_attempts", "attempt_id",
                                  attempt_id))

    def get_recovery_attempt(self, attempt_id: str) -> dict | None:
        row = self.store.conn.execute(
            "SELECT * FROM recovery_attempts WHERE attempt_id=?",
            (attempt_id,)).fetchone()
        return dict(row) if row else None

    def open_recovery_attempts(self) -> list[dict]:
        """Read-only: every attempt not yet in a terminal state."""
        rows = self.store.conn.execute(
            "SELECT * FROM recovery_attempts WHERE attempt_state IN"
            " ('CREATED','RUNNING','VERIFYING','UNCERTAIN')"
            " ORDER BY started_at").fetchall()
        return [dict(r) for r in rows]

    def claim_recovery_attempt(self, attempt_id: str,
                               controller_id: str) -> bool:
        """CAS: CREATED -> RUNNING for exactly one controller. Returns
        True when this controller won the claim."""
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                "UPDATE recovery_attempts SET attempt_state='RUNNING',"
                " controller_id=?, claimed_at=?"
                " WHERE attempt_id=? AND attempt_state='CREATED'",
                (controller_id, now, attempt_id))
            won = cur.rowcount == 1
            if won:
                self._append_event(conn, now, "recovery.attempt_claimed",
                                   {"attempt_id": attempt_id,
                                    "controller_id": controller_id},
                                   controller_id)
            return won

    def set_attempt_verify_after(self, attempt_id: str,
                                 verify_after: float) -> None:
        """Set the store-time verification horizon (called at dispatch)."""
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                "UPDATE recovery_attempts SET verify_after=?"
                " WHERE attempt_id=? AND attempt_state='RUNNING'",
                (verify_after, attempt_id))
            if cur.rowcount != 1:
                raise TransitionRejected(
                    f"attempt {attempt_id} is not RUNNING")

    def note_attempt_dispatch(self, attempt_id: str,
                              dispatch_evidence: dict,
                              verify_after: float, actor: str) -> dict:
        """Record that the external action was dispatched (merging the
        evidence into the attempt's action record) and arm the
        verification horizon — one transaction. Only the RUNNING claim
        holder may dispatch; the reconciler may also record a reconciled
        dispatch from UNCERTAIN (the claim is provably stale there).
        Anything else raises with zero mutation."""
        with self.store.write_txn() as (conn, now):
            row = self._get(conn, "recovery_attempts", "attempt_id",
                            attempt_id)
            if row["attempt_state"] not in ("RUNNING", "UNCERTAIN"):
                raise TransitionRejected(
                    f"attempt {attempt_id} is not RUNNING/UNCERTAIN"
                    f" (state={row['attempt_state']})")
            action = json.loads(row["action"])
            action["dispatch"] = dispatch_evidence
            conn.execute(
                "UPDATE recovery_attempts SET action=?, verify_after=?"
                " WHERE attempt_id=?",
                (_canon(action), verify_after, attempt_id))
            self._append_event(conn, now, "recovery.attempt_dispatched",
                               {"attempt_id": attempt_id,
                                "dispatch": dispatch_evidence}, actor)
            return dict(self._get(conn, "recovery_attempts", "attempt_id",
                                  attempt_id))

    def set_attempt_budget_context(self, attempt_id: str,
                                   budget_context: dict | None,
                                   actor: str) -> None:
        """Record the rung/budget context on the attempt. R8 writes the
        explicit deferred-to-R9 marker; R9 will supply real budgets
        through the RungProvider interface."""
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                "UPDATE recovery_attempts SET budget_context=?"
                " WHERE attempt_id=?",
                (_canon(budget_context) if budget_context is not None
                 else None, attempt_id))
            if cur.rowcount != 1:
                raise TransitionRejected(
                    f"unknown attempt {attempt_id}")

    def transition_recovery_attempt(self, attempt_id: str,
                                    from_states: tuple[str, ...],
                                    to_state: str,
                                    actor: str) -> dict:
        """Generic CAS state transition for the controller's drive loop."""
        if to_state not in self._ATTEMPT_STATES:
            raise TransitionRejected(
                f"unknown attempt state {to_state!r}")
        placeholders = ",".join("?" for _ in from_states)
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                f"UPDATE recovery_attempts SET attempt_state=?"
                f" WHERE attempt_id=? AND attempt_state IN ({placeholders})",
                (to_state, attempt_id, *from_states))
            if cur.rowcount != 1:
                raise TransitionRejected(
                    f"attempt {attempt_id} not in {from_states}")
            self._append_event(conn, now, "recovery.attempt_state",
                               {"attempt_id": attempt_id,
                                "to_state": to_state}, actor)
            return dict(self._get(conn, "recovery_attempts", "attempt_id",
                                  attempt_id))

    def complete_recovery_attempt(
            self, attempt_id: str, *, decision: str, observed_effect: str,
            progress_delta: float, resulting_state: str,
            evidence_after: dict, actor: str,
            escalation_target: str | None = None,
            output_health_delta: float = 0.0) -> dict:
        """Terminal CAS: (RUNNING|VERIFYING|UNCERTAIN) -> SUCCEEDED |
        FAILED | BLOCKED. Enforces I-18 (success requires positive
        progress delta) and the zero-progress rule: a zero-delta attempt
        can never complete as success."""
        if decision not in ("retry", "escalate", "success", "quarantine",
                            "stop"):
            raise TransitionRejected(
                f"unknown recovery decision {decision!r}")
        if not observed_effect:
            raise TransitionRejected(
                "I-18: a recovery attempt must record its observed effect")
        if decision == "success" and progress_delta <= 0:
            raise TransitionRejected(
                "I-18/zero-progress rule: success requires a positive"
                " progress delta — action without progress is not"
                " recovery")
        to_state = {"success": "SUCCEEDED", "retry": "FAILED",
                    "escalate": "FAILED", "quarantine": "BLOCKED",
                    "stop": "BLOCKED"}[decision]
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                "UPDATE recovery_attempts SET attempt_state=?, decision=?,"
                " observed_effect=?, progress_delta=?,"
                " output_health_delta=?, resulting_state=?,"
                " escalation_target=?, evidence_after=?"
                " WHERE attempt_id=? AND attempt_state IN"
                " ('RUNNING','VERIFYING','UNCERTAIN')",
                (to_state, decision, observed_effect, progress_delta,
                 output_health_delta, resulting_state, escalation_target,
                 _canon(evidence_after), attempt_id))
            if cur.rowcount != 1:
                raise TransitionRejected(
                    f"attempt {attempt_id} is not completable")
            self._append_event(conn, now, "recovery.attempt",
                               {"attempt_id": attempt_id,
                                "decision": decision,
                                "progress_delta": progress_delta,
                                "escalation_target": escalation_target},
                               actor)
            return dict(self._get(conn, "recovery_attempts", "attempt_id",
                                  attempt_id))

    def mark_recovery_attempt_uncertain(self, attempt_id: str, reason: str,
                                        actor: str) -> dict:
        """RUNNING -> UNCERTAIN when the controller cannot determine
        whether the dispatched action completed (crash/ambiguous). The
        next evaluation reconciles from durable evidence only."""
        return self.transition_recovery_attempt(
            attempt_id, ("RUNNING",), "UNCERTAIN", actor)

    def set_incident_escalated(self, incident_id: str, diagnosis: str,
                               actor: str,
                               escalated_to: str = "r9-policy") -> dict:
        """Durable escalation signal: two zero-progress attempts (or an
        unrecoverable block) escalate to the R9 recovery-policy layer.
        R8 stops creating attempts for an escalated incident."""
        return self.set_incident_outcome(incident_id, "escalated",
                                         diagnosis, actor,
                                         escalated_to=escalated_to)

    # --------------------------------------------- R9 recovery-policy state
    # R9 (recovery ladder, budgets, escalation policy) persists its durable
    # policy state in the recovery_policy table (migration v7): one row per
    # recovery incident. Every mutation below is a compare-and-swap on the
    # row's `version` inside ONE write_txn — two concurrent policy
    # controllers produce exactly one authoritative transition; the loser
    # gets PolicyConflict and must re-read. These helpers never touch job
    # ownership, fencing tokens, leases, processes, heartbeats, watchdog
    # verdicts, artifacts, checkpoints, or completion state: all recovery
    # action mechanics stay in R1–R8; R9 only decides and accounts.

    # Fields a CAS update may change. Deliberately EXCLUDES policy_version
    # (immutable identity), incident_budget and per_rung_budgets (budget
    # identity, set once at creation), and the budget-accounting columns
    # attempt_count / per_rung_attempts / remaining_budget /
    # consumed_attempt_number / last_attempt_id, which only
    # consume_policy_attempts may move (single writer of the money).
    _POLICY_UPDATABLE = frozenset({
        "current_rung", "rung_name",
        "result_consumed_attempt_number", "zero_progress_count",
        "last_attempt_result", "escalation_state", "escalation_target",
        "terminal_state", "superseded_by",
    })

    def get_recovery_policy(self, incident_id: str) -> dict | None:
        """Read-only fetch of an incident's R9 policy row (None if absent)."""
        return self._get_policy_row(self.store.conn, incident_id)

    def _get_policy_row(self, conn, incident_id: str) -> dict | None:
        row = conn.execute(
            "SELECT * FROM recovery_policy WHERE incident_id=?",
            (incident_id,)).fetchone()
        return dict(row) if row else None

    def ensure_recovery_policy(self, incident_id: str, *,
                               policy_version: str, current_rung: int,
                               rung_name: str, incident_budget: int,
                               per_rung_budgets: dict, actor: str) -> dict:
        """Find-or-create the policy row for an incident. Creation records
        the starting rung and the full budget identity from the caller's
        validated PolicyConfig. Concurrent creators serialize on the
        PRIMARY KEY: exactly one row wins, the loser re-reads it."""
        if (not isinstance(incident_budget, int)
                or isinstance(incident_budget, bool)
                or incident_budget <= 0):
            raise TransitionRejected(
                "incident_budget must be a positive int (no infinite budget)")
        with self.store.write_txn() as (conn, now):
            row = self._get_policy_row(conn, incident_id)
            if row is not None:
                return dict(row)
            try:
                conn.execute(
                    "INSERT INTO recovery_policy(incident_id, policy_version,"
                    " version, current_rung, rung_name, incident_budget,"
                    " per_rung_budgets, remaining_budget, updated_at)"
                    " VALUES(?, ?, 1, ?, ?, ?, ?, ?, ?)",
                    (incident_id, policy_version, current_rung, rung_name,
                     incident_budget, _canon(per_rung_budgets),
                     incident_budget, now),
                )
            except sqlite3.IntegrityError as exc:
                # Either lost the create race (PRIMARY KEY: the winner's
                # row is authoritative — observe it) or the incident row
                # does not exist (FOREIGN KEY: a real caller bug — never
                # mask it as a race).
                row = self._get_policy_row(conn, incident_id)
                if row is not None:
                    return dict(row)
                raise TransitionRejected(
                    f"cannot create policy row for {incident_id}:"
                    f" {exc}") from exc
            return dict(self._get_policy_row(conn, incident_id))

    def cas_update_recovery_policy(self, incident_id: str,
                                   expected_version: int,
                                   updates: dict, actor: str) -> dict:
        """Compare-and-swap policy update: applies `updates` (allowlisted
        keys only) iff the row's version still equals expected_version, in
        one write_txn. Raises PolicyConflict on a lost race — the caller
        must re-read the authoritative row and reconcile from durable
        state; it must never assume its stale view won."""
        bad = [k for k in updates if k not in self._POLICY_UPDATABLE]
        if bad:
            raise TransitionRejected(
                f"recovery_policy fields not updatable via CAS: {sorted(bad)}")
        if not updates:
            raise TransitionRejected("empty policy update")
        set_clauses, params = [], []
        for key in sorted(updates):
            value = updates[key]
            if (key in ("per_rung_budgets", "last_attempt_result")
                    and value is not None):
                value = _canon(value)
            set_clauses.append(f"{key}=?")
            params.append(value)
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                f"UPDATE recovery_policy SET {', '.join(set_clauses)},"
                " version=version+1, updated_at=? "
                "WHERE incident_id=? AND version=?",
                (*params, now, incident_id, expected_version),
            )
            if cur.rowcount != 1:
                raise PolicyConflict(
                    f"policy CAS lost for {incident_id}: expected version "
                    f"{expected_version}")
            return dict(self._get_policy_row(conn, incident_id))

    def consume_policy_attempts(self, incident_id: str, expected_version: int,
                                *, upto_attempt_number: int,
                                per_rung_attempts: dict,
                                last_attempt_id: str,
                                actor: str) -> dict:
        """Exactly-once budget accounting for newly observed R8 attempts.
        In ONE write_txn: advances the consumption watermark to
        upto_attempt_number, increments attempt_count and decrements
        remaining_budget by the payable number of new attempts — never
        below zero (remaining_budget>=payable is part of the UPDATE's
        WHERE clause). Returns {"row", "consumed", "exhausted"};
        exhausted=True when the incident budget could not cover every new
        attempt (the caller must terminal-escalate). Raises PolicyConflict
        on a lost race, an already-consumed watermark, or an empty budget.
        """
        with self.store.write_txn() as (conn, now):
            row = self._get_policy_row(conn, incident_id)
            if row is None:
                raise PolicyConflict(
                    f"no policy row for {incident_id}")
            if row["version"] != expected_version:
                raise PolicyConflict(
                    f"policy CAS lost for {incident_id}: expected version "
                    f"{expected_version}, found {row['version']}")
            already = row["consumed_attempt_number"]
            if upto_attempt_number <= already:
                raise PolicyConflict(
                    f"attempts through #{upto_attempt_number} for "
                    f"{incident_id} already consumed")
            new_count = upto_attempt_number - already
            payable = min(new_count, row["remaining_budget"])
            if payable <= 0:
                raise PolicyConflict(
                    f"incident budget exhausted for {incident_id}: "
                    f"{new_count} new attempt(s), remaining 0")
            cur = conn.execute(
                "UPDATE recovery_policy SET version=version+1,"
                " consumed_attempt_number=?,"
                " attempt_count=attempt_count+?,"
                " remaining_budget=remaining_budget-?,"
                " per_rung_attempts=?, last_attempt_id=?, updated_at=?"
                " WHERE incident_id=? AND version=? AND remaining_budget>=?",
                (already + payable, payable, payable,
                 _canon(per_rung_attempts), last_attempt_id, now,
                 incident_id, expected_version, payable),
            )
            if cur.rowcount != 1:
                raise PolicyConflict(
                    f"policy consume CAS lost for {incident_id}")
            return {
                "row": self._get_policy_row(conn, incident_id),
                "consumed": payable,
                "exhausted": payable < new_count,
            }

    def escalated_recovery_incidents(self,
                                     escalated_to: str = "r9-policy"
                                     ) -> list[dict]:
        """Read-only scan of incidents carrying a durable escalation
        signal (R8's intake guard escalates here; R9 reconciles them)."""
        rows = self.store.conn.execute(
            "SELECT * FROM incidents WHERE outcome='escalated'"
            " AND escalated_to=? ORDER BY created_at",
            (escalated_to,),
        ).fetchall()
        return [dict(r) for r in rows]

    def recovery_incidents_with_open_policy(self) -> list[dict]:
        """Read-only scan of incidents that already carry an outcome
        (closed by R8's authority) but whose R9 policy row is not terminal
        yet: R9 must converge its terminal policy state from the durable
        incident outcome."""
        rows = self.store.conn.execute(
            "SELECT i.* FROM incidents i JOIN recovery_policy p"
            " ON p.incident_id=i.incident_id"
            " WHERE p.terminal_state IS NULL AND i.outcome IS NOT NULL"
            " ORDER BY i.created_at",
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ checkpoints
    def create_checkpoint(self, checkpoint_id: str | None, task_id: str,
                          actor: str, stage_id: str | None = None,
                          ledger_tip_seq: int | None = None,
                          artifact_manifest: list | None = None,
                          supersedes: str | None = None,
                          versions: dict | None = None) -> dict:
        checkpoint_id = checkpoint_id or _new_id("ckpt-")
        versions = versions or {}
        with self.store.write_txn() as (conn, now):
            self._get(conn, "tasks", "task_id", task_id)
            conn.execute(
                "INSERT INTO checkpoints(checkpoint_id, task_id, stage_id,"
                " ledger_tip_seq, artifact_manifest, supersedes,"
                " input_version, schema_version, methodology_version,"
                " capability_versions, os_version, verification_status,"
                " created_at, updated_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?, 'UNVERIFIED', ?, ?)",
                (checkpoint_id, task_id, stage_id, ledger_tip_seq,
                 _canon(artifact_manifest or []), supersedes,
                 versions.get("input_version"), versions.get("schema_version"),
                 versions.get("methodology_version"),
                 _canon(versions.get("capability_versions", {})),
                 versions.get("os_version"), now, now),
            )
            self._append_event(conn, now, "checkpoint.created",
                               {"checkpoint_id": checkpoint_id,
                                "task_id": task_id}, actor)
            return dict(self._get(conn, "checkpoints", "checkpoint_id",
                                  checkpoint_id))

    def set_checkpoint_verification(self, checkpoint_id: str, status: str,
                                    actor: str, receipt: dict | None = None) -> dict:
        """Record the verification receipt (Phase 0 13, Q12).

        UNVERIFIED -> VERIFYING -> VERIFIED | CORRUPT. A VERIFIED checkpoint
        must carry a receipt proving: full manifest, ledger chain verified,
        and the revalidation outcome (sampled or 100%). No receipt, no trust.
        """
        receipt = receipt or {}
        if status == "VERIFIED" and not (
                receipt.get("manifest_full") and receipt.get("ledger_chain_verified")):
            raise TransitionRejected(
                "checkpoint VERIFIED requires a receipt with manifest_full"
                " and ledger_chain_verified")
        extra = {
            "verified_at": None, "verify_method": receipt.get("method"),
            "sample_rate": receipt.get("sample_rate"),
            "sample_seed": receipt.get("sample_seed"),
            "sample_count": receipt.get("sample_count"),
            "sample_passed": receipt.get("sample_passed"),
            "manifest_full": 1 if receipt.get("manifest_full") else 0,
            "ledger_chain_verified": 1 if receipt.get("ledger_chain_verified") else 0,
        }
        with self.store.write_txn() as (conn, now):
            row = self._get(conn, "checkpoints", "checkpoint_id", checkpoint_id)
            # Phase 1C R5: checkpoints created through the deterministic
            # stage_checkpoint() path carry a canonical manifest. Marking
            # one of those VERIFIED on caller assertion would bypass the
            # gate's revalidation pipeline — VERIFIED for them is reachable
            # only through verify_checkpoint(). Legacy record-API
            # checkpoints (no canonical manifest) keep their existing
            # record semantics.
            if status == "VERIFIED" and row["canonical_manifest"]:
                raise TransitionRejected(
                    f"checkpoint {checkpoint_id}: VERIFIED requires"
                    f" verify_checkpoint() revalidation; the record API"
                    f" cannot verify a canonical-manifest checkpoint")
            from_state = row["verification_status"]
            if status not in T.CHECKPOINT_TRANSITIONS.get(from_state, ()):
                raise TransitionRejected(
                    f"invalid checkpoint verification {checkpoint_id}:"
                    f" {from_state} -> {status}")
            if status in ("VERIFIED", "CORRUPT"):
                extra["verified_at"] = now
            set_clause = ", ".join(f"{k}=?" for k in
                                   ("verification_status", "updated_at", *extra))
            conn.execute(
                f"UPDATE checkpoints SET {set_clause} WHERE checkpoint_id=?",
                (status, now, *extra.values(), checkpoint_id),
            )
            self._append_event(conn, now, "checkpoint.verification",
                               {"checkpoint_id": checkpoint_id,
                                "from": from_state, "to": status,
                                "receipt": receipt}, actor)
            return dict(self._get(conn, "checkpoints", "checkpoint_id",
                                  checkpoint_id))

    # -------------------------------------------------------------- artifacts
    def create_artifact(self, artifact_id: str, task_id: str, actor: str,
                        kind: str | None = None, size: int | None = None,
                        uri: str | None = None,
                        producer: dict | None = None) -> dict:
        # artifact_id is the content hash (Phase 0 04/24): caller-supplied.
        with self.store.write_txn() as (conn, now):
            self._get(conn, "tasks", "task_id", task_id)
            conn.execute(
                "INSERT INTO artifacts(artifact_id, task_id, kind, size, uri,"
                " status, producer, created_at, updated_at)"
                " VALUES(?, ?, ?, ?, ?, 'STAGING', ?, ?, ?)",
                (artifact_id, task_id, kind, size, uri,
                 _canon(producer or {}), now, now),
            )
            self._append_event(conn, now, "artifact.created",
                               {"artifact_id": artifact_id,
                                "task_id": task_id}, actor)
            return dict(self._get(conn, "artifacts", "artifact_id", artifact_id))

    def transition_artifact(self, artifact_id: str, to_state: str,
                            actor: str) -> dict:
        # Phase 1C R5: workers never drive the artifact lifecycle directly.
        # A worker stages bytes through stage_artifact() (fenced) and the
        # gate performs verification through verify_artifact(); no
        # worker-owned actor may move artifact state itself.
        if _is_worker_actor(actor):
            raise TransitionRejected(
                f"artifact {artifact_id}: actor {actor!r} may not drive"
                f" artifact state directly; use stage_artifact() and the"
                f" gate's verify_artifact()")
        return self._transition(
            "artifacts", "artifact_id", artifact_id, to_state,
            T.ARTIFACT_TRANSITIONS, actor,
            ledger_event="artifact.transition",
            ledger_payload={"artifact_id": artifact_id},
        )

    # ----------------------------------------------------- validator records
    # Phase 0 identified validator error as a top risk (Q12, test 09). The
    # store preserves validator identity + version + result + receipt so a
    # quarantined validator's past verdicts can be re-examined later.
    def record_validation(self, validation_id: str | None, artifact_id: str,
                          validator_id: str, validator_version: str,
                          result: str, actor: str,
                          method: str | None = None, sampled: bool = False,
                          sample_desc: str | None = None,
                          receipt_ref: str | None = None,
                          notes: str | None = None) -> dict:
        if result not in ("PASS", "FAIL", "INCONCLUSIVE"):
            raise TransitionRejected(f"unknown validation result {result!r}")
        validation_id = validation_id or _new_id("val-")
        with self.store.write_txn() as (conn, now):
            art = self._get(conn, "artifacts", "artifact_id", artifact_id)
            # R5: idempotent on the caller-supplied validation identity —
            # repeating the same recording returns the same row instead of
            # duplicating the receipt.
            existing = conn.execute(
                "SELECT * FROM validations WHERE validation_id=?",
                (validation_id,)).fetchone()
            if existing is not None:
                return dict(existing)
            # R5: the receipt is bound to the EXACT content hash the
            # validator saw. A receipt recorded against content hash A
            # never validates content hash B — the verification predicate
            # joins receipts on content_hash, not just artifact identity.
            content_hash = art["content_hash"] or art["artifact_id"]
            conn.execute(
                "INSERT INTO validations(validation_id, artifact_id,"
                " validator_id, validator_version, result, method, sampled,"
                " sample_desc, receipt_ref, notes, validated_at,"
                " content_hash)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (validation_id, artifact_id, validator_id, validator_version,
                 result, method, 1 if sampled else 0, sample_desc, receipt_ref,
                 notes, now, content_hash),
            )
            self._append_event(conn, now, "validation.recorded",
                               {"validation_id": validation_id,
                                "artifact_id": artifact_id,
                                "content_hash": content_hash,
                                "validator_id": validator_id,
                                "validator_version": validator_version,
                                "result": result}, actor)
            row = conn.execute(
                "SELECT * FROM validations WHERE validation_id=?",
                (validation_id,)).fetchone()
            return dict(row)

    def quarantine_validator(self, validator_id: str, validator_version: str,
                             actor: str) -> int:
        """Mark every verdict by one validator version as quarantined, so the
        later integrity gate can re-examine them. Returns the count marked."""
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                "UPDATE validations SET quarantined=1"
                " WHERE validator_id=? AND validator_version=?"
                " AND quarantined=0",
                (validator_id, validator_version),
            )
            self._append_event(conn, now, "validator.quarantined",
                               {"validator_id": validator_id,
                                "validator_version": validator_version,
                                "count": cur.rowcount}, actor)
            return cur.rowcount

    def validations_for_validator(self, validator_id: str,
                                  validator_version: str) -> list[dict]:
        rows = self.store.conn.execute(
            "SELECT * FROM validations WHERE validator_id=?"
            " AND validator_version=? ORDER BY validated_at",
            (validator_id, validator_version)).fetchall()
        return [dict(r) for r in rows]

    # ----------------------------------------------------------------- ledger
    def append_event(self, event_type: str, payload: dict, actor: str) -> int:
        """Append a hash-chained ledger event (Phase 0 20). Raw heartbeats
        must never be appended here — summaries and milestones only."""
        with self.store.write_txn() as (conn, now):
            return self._append_event(conn, now, event_type, payload, actor)

    def verify_ledger_chain(self) -> tuple[bool, str]:
        """Recompute the hash chain. Returns (ok, detail)."""
        prev_hash = "0" * 64
        rows = self.store.conn.execute(
            "SELECT seq, event_type, payload, actor, ts, prev_hash, hash"
            " FROM ledger ORDER BY seq").fetchall()
        for r in rows:
            if r["prev_hash"] != prev_hash:
                return False, f"seq {r['seq']}: prev_hash mismatch"
            body = _canon({"seq": r["seq"], "type": r["event_type"],
                           "payload": json.loads(r["payload"]),
                           "actor": r["actor"], "ts": r["ts"]})
            h = hashlib.sha256(f"{prev_hash}:{body}".encode()).hexdigest()
            if h != r["hash"]:
                return False, f"seq {r['seq']}: hash mismatch (tampered?)"
            prev_hash = r["hash"]
        return True, f"ok, {len(rows)} events"

    # ------------------------------------------ R6 boot-recovery reads
    def owned_active_jobs(self) -> list[dict]:
        """Read-only: jobs in CLAIMED/RUNNING/COMMITTING with a non-null
        owner_worker_id, in job_id order. The records whose lease + owner
        authority the boot pass must reconcile. No mutation."""
        rows = self.store.conn.execute(
            """SELECT * FROM jobs
               WHERE status IN ('CLAIMED','RUNNING','COMMITTING')
                 AND owner_worker_id IS NOT NULL
               ORDER BY job_id"""
        ).fetchall()
        return [dict(r) for r in rows]

    def jobs_in_states(self, statuses: tuple[str, ...]) -> list[dict]:
        """Read-only: jobs in the given statuses, in job_id order. The R6
        boot pass uses this for the COMPLETE / COMMITTING / UNCERTAIN
        integrity inspection. No mutation."""
        if not statuses:
            return []
        q = ",".join("?" for _ in statuses)
        rows = self.store.conn.execute(
            f"SELECT * FROM jobs WHERE status IN ({q}) ORDER BY job_id",
            tuple(statuses)).fetchall()
        return [dict(r) for r in rows]

    def pending_jobs(self, limit: int) -> list[dict]:
        """Read-only (R10): PENDING jobs in deterministic admission order
        (created_at, then job_id), bounded by `limit`. The scheduler's
        candidate scan — the repo has no canonical job priority, so
        creation order + job_id tiebreak is the stable default. No
        mutation."""
        if limit < 1:
            return []
        rows = self.store.conn.execute(
            "SELECT * FROM jobs WHERE status='PENDING'"
            " ORDER BY created_at, job_id LIMIT ?",
            (limit,)).fetchall()
        return [dict(r) for r in rows]

    def sched_claimed_orphans(self, now: float) -> list[dict]:
        """Read-only (R10): CLAIMED jobs owned by a `sched-*` worker whose
        durable lease is still live (lease_expires_at > now), ordered by
        lease expiry then job_id. These are the scheduler's orphan
        re-dispatch candidates: claimed by a scheduler, but with no
        unreaped worker.proc_spawned evidence (the spawn never landed).
        Expired-lease rows are excluded — lease expiry is R4's domain and
        recycling is R1's; the scheduler never touches them. No mutation.
        """
        rows = self.store.conn.execute(
            "SELECT * FROM jobs WHERE status='CLAIMED'"
            " AND owner_worker_id LIKE 'sched-%'"
            " AND lease_expires_at IS NOT NULL"
            " AND lease_expires_at > ?"
            " ORDER BY lease_expires_at, job_id",
            (now,)).fetchall()
        return [dict(r) for r in rows]

    # --------------------------------- R10 scheduler claim-liveness beats
    # A scheduler-owned CLAIMED job with no spawn evidence is either
    # "its scheduler is alive and mid-dispatch" or "its scheduler died".
    # The beats table records that liveness durably (cross-process): the
    # beat is planted atomically with the claim, refreshed by the owning
    # scheduler on every evaluate_once, withdrawn when dispatch fails,
    # and cleared on graceful close. The orphan-redispatch path skips
    # live beats and re-dispatches only stale/absent ones — so a live
    # scheduler's fresh claim is never stolen (exactly one dispatch),
    # while a dead scheduler's orphan becomes re-dispatchable after a
    # bounded delay (crash between claim and dispatch self-heals).
    # Beats are liveness signals only: they confer no lease, no token,
    # and no execution authority.
    def refresh_scheduler_claim_beats(self, scheduler_id: str) -> int:
        """Refresh every beat owned by `scheduler_id`. Returns the number
        of rows touched. Called by a live scheduler on every pass."""
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                "UPDATE scheduler_claim_beats SET last_beat=?"
                " WHERE scheduler_id=?",
                (now, scheduler_id))
            return cur.rowcount

    def withdraw_scheduler_claim_beat(self, worker_id: str) -> None:
        """Remove the beat for `worker_id`: dispatch failed, so the claim
        stands undispatched and must be immediately re-dispatchable by
        the next scheduler (no staleness wait)."""
        with self.store.write_txn() as (conn, _now):
            conn.execute(
                "DELETE FROM scheduler_claim_beats WHERE worker_id=?",
                (worker_id,))

    def clear_scheduler_claim_beats(self, scheduler_id: str) -> int:
        """Remove every beat owned by `scheduler_id` (graceful shutdown).
        Returns the number of rows removed."""
        with self.store.write_txn() as (conn, _now):
            cur = conn.execute(
                "DELETE FROM scheduler_claim_beats WHERE scheduler_id=?",
                (scheduler_id,))
            return cur.rowcount

    def scheduler_claim_live(self, worker_id: str, stale_after_s: float,
                             now: float) -> bool:
        """Read-only: True iff `worker_id` carries a beat no older than
        `stale_after_s` — i.e. its owning scheduler is alive. Absent beat
        ⟹ not live (never claimed by a scheduler, or withdrawn)."""
        row = self.store.conn.execute(
            "SELECT last_beat FROM scheduler_claim_beats WHERE worker_id=?",
            (worker_id,)).fetchone()
        if row is None:
            return False
        return (now - row["last_beat"]) <= stale_after_s

    def all_latest_known_good(self) -> list[dict]:
        """Read-only: every latest-known-good checkpoint pointer, each
        with the checkpoint row it names (or None when it dangles).

        Used by the R6 boot pass to VERIFY the pointer still names a
        VERIFIED checkpoint. Boot never advances the pointer — only
        verify_checkpoint() writes it (R5, audited).
        """
        rows = self.store.conn.execute(
            "SELECT name, task_id, checkpoint_id FROM checkpoint_pointers"
            " WHERE name LIKE 'lkg:%' ORDER BY name").fetchall()
        out = []
        for r in rows:
            cp = self.store.conn.execute(
                "SELECT * FROM checkpoints WHERE checkpoint_id=?",
                (r["checkpoint_id"],)).fetchone()
            out.append({"task_id": r["task_id"],
                        "checkpoint_id": r["checkpoint_id"],
                        "checkpoint": dict(cp) if cp else None})
        return out

    # --------------------------------------- R7 watchdog verdict records
    # The watchdog (exec/watchdog.py) DETECTS and CLASSIFIES only. Verdicts
    # are recorded per execution identity (job_id, fencing_token): once
    # DEAD for an identity, that identity stays DEAD; a fencing-token bump
    # starts a new identity. Rows are written ONLY on verdict transitions —
    # re-evaluating to the same verdict is a zero-mutation no-op — so the
    # watchdog cannot produce an unbounded stream of duplicate milestone
    # events. Every transition also appends one watchdog.verdict ledger
    # event, atomically, in the same write_txn.
    #
    # This method performs no reclaim, no fencing-token bump, no owner
    # mutation, no process signalling, no scheduling, no recovery: it
    # records an observation. (Audited by A13.)

    def latest_watchdog_verdict(self, job_id: str,
                                fencing_token: int) -> dict | None:
        """Read-only: the latest recorded verdict for one execution
        identity (job_id, fencing_token), or None when never evaluated."""
        row = self.store.conn.execute(
            "SELECT * FROM watchdog_verdicts"
            " WHERE job_id=? AND fencing_token=?"
            " ORDER BY evaluated_at DESC, verdict_id DESC LIMIT 1",
            (job_id, fencing_token)).fetchone()
        if row is None:
            return None
        d = dict(row)
        d["evidence"] = json.loads(d["evidence"])
        return d

    def watchdog_verdicts_for(self, job_id: str) -> list[dict]:
        """Read-only: every recorded verdict row for a job, oldest first."""
        rows = self.store.conn.execute(
            "SELECT * FROM watchdog_verdicts WHERE job_id=?"
            " ORDER BY evaluated_at, verdict_id",
            (job_id,)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["evidence"] = json.loads(d["evidence"])
            out.append(d)
        return out

    def record_watchdog_verdict(self, job_id: str, fencing_token: int,
                                verdict: str, evidence: dict,
                                actor: str) -> dict:
        """Record a watchdog verdict observation for one execution
        identity, atomically and idempotently.

        Compare-and-swap inside ONE write_txn: the latest verdict for
        (job_id, fencing_token) is re-read inside the transaction, so two
        concurrent watchdog instances produce exactly one authoritative
        transition (BEGIN IMMEDIATE serializes the writers on one VM).

        - same verdict as the latest recorded -> zero mutation, returns
          {"transition": False, "verdict_id": None, ...}
        - latest recorded is DEAD -> DEAD is terminal for this identity:
          zero mutation, never overwritten
        - otherwise -> insert the verdict row + one watchdog.verdict
          ledger event, atomically

        Raises TransitionRejected for an unknown verdict. This is an
        observation record only: it never reclaims a lease, never bumps
        the fencing token, never clears ownership, never signals a
        process, never schedules or recovers anything.
        """
        if verdict not in ("HEALTHY", "STALLED", "DEAD"):
            raise TransitionRejected(f"unknown watchdog verdict {verdict!r}")
        # Fast path: same-verdict and DEAD-terminal re-evaluations are
        # true zero-mutation no-ops — no write_txn is opened at all, so
        # not even axos_meta.last_commit_ts is touched.
        fast = self._verdict_noop_result(
            job_id, fencing_token, verdict,
            self._latest_verdict_name(job_id, fencing_token))
        if fast is not None:
            return fast
        verdict_id = _new_id("wdv-")
        try:
            with self.store.write_txn() as (conn, now):
                latest = conn.execute(
                    "SELECT verdict FROM watchdog_verdicts"
                    " WHERE job_id=? AND fencing_token=?"
                    " ORDER BY evaluated_at DESC, verdict_id DESC LIMIT 1",
                    (job_id, fencing_token)).fetchone()
                inside = latest["verdict"] if latest else None
                if inside == verdict or inside == "DEAD":
                    # Lost the race with a concurrent writer: roll back
                    # with zero mutation and report the authoritative
                    # outcome.
                    raise _VerdictNoOp(self._verdict_noop_result(
                        job_id, fencing_token, verdict, inside, now=now))
                conn.execute(
                    "INSERT INTO watchdog_verdicts(verdict_id, job_id,"
                    " fencing_token, verdict, evidence, evaluated_at, actor)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (verdict_id, job_id, fencing_token, verdict,
                     _canon(evidence), now, actor),
                )
                self._append_event(
                    conn, now, "watchdog.verdict",
                    {"verdict_id": verdict_id, "job_id": job_id,
                     "fencing_token": fencing_token, "verdict": verdict,
                     "previous_verdict": inside, "evidence": evidence},
                    actor)
                return {"verdict_id": verdict_id, "transition": True,
                        "previous_verdict": inside, "verdict": verdict,
                        "evaluated_at": now}
        except _VerdictNoOp as exc:
            return exc.result

    def _latest_verdict_name(self, job_id: str,
                             fencing_token: int) -> str | None:
        """Read-only: the latest recorded verdict name for one execution
        identity, or None when never evaluated."""
        row = self.store.conn.execute(
            "SELECT verdict FROM watchdog_verdicts"
            " WHERE job_id=? AND fencing_token=?"
            " ORDER BY evaluated_at DESC, verdict_id DESC LIMIT 1",
            (job_id, fencing_token)).fetchone()
        return row["verdict"] if row else None

    def _verdict_noop_result(self, job_id: str, fencing_token: int,
                             verdict: str, previous: str | None,
                             now: float | None = None) -> dict | None:
        """The zero-mutation result for a same-verdict re-evaluation or a
        DEAD-terminal identity; None when a write is required."""
        at = now if now is not None else self.store.current_time()
        if previous == verdict:
            return {"verdict_id": None, "transition": False,
                    "previous_verdict": previous, "verdict": verdict,
                    "evaluated_at": at}
        if previous == "DEAD":
            # DEAD is terminal for this execution identity: a later
            # observation can never overwrite it. A fencing-token bump
            # (reclaim) starts a new identity with no verdict history.
            return {"verdict_id": None, "transition": False,
                    "previous_verdict": previous, "verdict": previous,
                    "evaluated_at": at,
                    "reason": "DEAD is terminal for (job_id,"
                              " fencing_token)"}
        return None

    # ------------------------------------- R11 desired-state reconciliation
    # The smallest gate-owned surface for desired-state convergence. The
    # reconciler (exec/reconciler.py) owns NO mutation authority: it
    # converges actual jobs toward the declared desired set by calling
    # exactly these methods. set/retire mutate the desired set itself;
    # ensure_job_for_desired_state is the single idempotent job-creation
    # op; the run-record methods are durable bookkeeping for each
    # converge pass.
    def _read_desired_head(self, conn) -> dict:
        row = conn.execute(
            "SELECT * FROM desired_state_head WHERE key='head'"
        ).fetchone()
        if row is None:
            raise TransitionRejected(
                "desired_state_head row is missing: schema v9 not applied")
        return dict(row)

    def _desired_items_parsed(self, conn) -> list[dict]:
        """All desired items with parsed specs, for snapshot hashing."""
        rows = conn.execute(
            "SELECT desired_work_id, spec, retired, version"
            " FROM desired_state").fetchall()
        return [{"id": r["desired_work_id"],
                 "spec": json.loads(r["spec"]),
                 "retired": r["retired"],
                 "version": r["version"]} for r in rows]

    def _bump_desired_head(self, conn, now: float, expected_version: int,
                           snapshot_hash: str) -> dict:
        """CAS-bump the head by exactly +1. BEGIN IMMEDIATE serializes
        writers; the WHERE clause pins the version read in this
        transaction, so a lost race raises PolicyConflict and the loser
        re-reads the authoritative head."""
        cur = conn.execute(
            "UPDATE desired_state_head SET version=version+1,"
            " snapshot_hash=?, updated_at=? WHERE key='head' AND version=?",
            (snapshot_hash, now, expected_version),
        )
        if cur.rowcount != 1:
            raise PolicyConflict(
                "desired-state head CAS lost: expected version"
                f" {expected_version}")
        return self._read_desired_head(conn)

    def set_desired_item(self, desired_work_id: str, spec: dict,
                         actor: str) -> dict:
        """Declare (or re-declare) a desired-work item. Upserts the item
        row (retired=0, version+1), recomputes the snapshot hash over ALL
        items (retired included), CAS-bumps the head by exactly +1, and
        journals desired_state.updated — all inside one write_txn.
        Returns the updated head row."""
        if not isinstance(desired_work_id, str) or not desired_work_id:
            raise TransitionRejected(
                "desired_work_id must be a non-empty string")
        if not isinstance(actor, str) or not actor:
            raise TransitionRejected("actor must be a non-empty string")
        norm = _normalize_desired_spec(spec)  # raises TransitionRejected
        canon_spec = _canon(norm)
        with self.store.write_txn() as (conn, now):
            head = self._read_desired_head(conn)
            existing = conn.execute(
                "SELECT desired_work_id FROM desired_state"
                " WHERE desired_work_id=?",
                (desired_work_id,)).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO desired_state(desired_work_id, spec,"
                    " retired, version, created_at, updated_at)"
                    " VALUES(?, ?, 0, 1, ?, ?)",
                    (desired_work_id, canon_spec, now, now),
                )
            else:
                conn.execute(
                    "UPDATE desired_state SET spec=?, retired=0,"
                    " version=version+1, updated_at=?"
                    " WHERE desired_work_id=?",
                    (canon_spec, now, desired_work_id),
                )
            snapshot = _desired_snapshot_hash(
                self._desired_items_parsed(conn))
            self._append_event(conn, now, "desired_state.updated",
                               {"desired_work_id": desired_work_id,
                                "spec_hash": _desired_spec_hash(norm)},
                               actor)
            return self._bump_desired_head(conn, now, head["version"],
                                           snapshot)

    def retire_desired_item(self, desired_work_id: str, actor: str) -> dict:
        """Tombstone a desired-work item: retired=1 (the row must exist —
        retiring an unknown identity is a caller bug), version+1, snapshot
        recomputed, head CAS-bumped, desired_state.retired journaled — one
        write_txn. The row is never deleted; the reconciler records
        OBSOLETE for it but never touches the job. Returns the updated
        head row."""
        if not isinstance(desired_work_id, str) or not desired_work_id:
            raise TransitionRejected(
                "desired_work_id must be a non-empty string")
        if not isinstance(actor, str) or not actor:
            raise TransitionRejected("actor must be a non-empty string")
        with self.store.write_txn() as (conn, now):
            head = self._read_desired_head(conn)
            self._get(conn, "desired_state", "desired_work_id",
                      desired_work_id)  # must exist
            conn.execute(
                "UPDATE desired_state SET retired=1, version=version+1,"
                " updated_at=? WHERE desired_work_id=?",
                (now, desired_work_id),
            )
            snapshot = _desired_snapshot_hash(
                self._desired_items_parsed(conn))
            self._append_event(conn, now, "desired_state.retired",
                               {"desired_work_id": desired_work_id}, actor)
            return self._bump_desired_head(conn, now, head["version"],
                                           snapshot)

    def get_desired_head(self) -> dict:
        """Read-only: the current desired-state head row."""
        return dict(self._get(self.store.conn, "desired_state_head",
                              "key", "head"))

    def get_desired_item(self, desired_work_id: str) -> dict:
        """Read-only: one desired item row (spec is canonical JSON)."""
        return dict(self._get(self.store.conn, "desired_state",
                              "desired_work_id", desired_work_id))

    def list_desired_items(self, include_retired: bool = False) -> list[dict]:
        """Read-only: desired items in desired_work_id order; retired rows
        are tombstones, excluded unless include_retired=True."""
        rows = self.store.conn.execute(
            "SELECT * FROM desired_state"
            + ("" if include_retired else " WHERE retired=0")
            + " ORDER BY desired_work_id").fetchall()
        return [dict(r) for r in rows]

    def get_desired_job_map(self, desired_work_id: str) -> dict | None:
        """Read-only: the canonical-identity map row for a desired-work
        item, or None when no job has been materialized for it yet."""
        row = self.store.conn.execute(
            "SELECT * FROM desired_job_map WHERE desired_work_id=?",
            (desired_work_id,)).fetchone()
        return dict(row) if row is not None else None

    def ensure_job_for_desired_state(self, *, desired_work_id: str,
                                     task_id: str, stage_id: str | None,
                                     max_attempts: int, policy: dict | None,
                                     desired_version: int,
                                     actor: str = "reconciler"
                                     ) -> tuple[dict, bool]:
        """The smallest gate-owned idempotent creation op: find-or-create
        the job for one desired-work item. The job_id is deterministic
        (canonical identity); concurrent creators serialize on the jobs
        PRIMARY KEY and the loser re-reads the winner.

        In ONE write_txn: verify the task exists (fail closed), INSERT the
        job row (PENDING) with the gate-owned `_desired` policy envelope,
        INSERT the desired_job_map row, and journal job.created — ONLY on
        actual creation.

        On sqlite3.IntegrityError the loser re-reads: if the job row
        exists AND the map row ties it to this desired_work_id with a
        matching spec_hash, the creation is idempotent — return
        (job, False). If the job exists but the map is absent/mismatched,
        the map exists without the job row, or the spec drifted under an
        existing mapping, raise DesiredStateConflict: a
        canonical-identity collision. Fail closed — never adopt a foreign
        job, never delete, never recreate."""
        self._check_work_creator(actor, "jobs")
        if not isinstance(desired_work_id, str) or not desired_work_id:
            raise TransitionRejected(
                "desired_work_id must be a non-empty string")
        norm = _normalize_desired_spec({"task_id": task_id,
                                        "stage_id": stage_id,
                                        "max_attempts": max_attempts,
                                        "policy": policy or {}})
        if (isinstance(desired_version, bool)
                or not isinstance(desired_version, int)
                or desired_version < 0):
            raise TransitionRejected(
                "desired_version must be an int >= 0,"
                f" got {desired_version!r}")
        job_id = _canonical_desired_job_id(desired_work_id)
        spec_hash = _desired_spec_hash(norm)
        desired_envelope = {"desired_work_id": desired_work_id,
                            "desired_version": desired_version,
                            "spec_hash": spec_hash}
        with self.store.write_txn() as (conn, now):
            # Fail closed: the task and the desired item must both exist.
            self._get(conn, "tasks", "task_id", task_id)
            self._get(conn, "desired_state", "desired_work_id",
                      desired_work_id)
            # Fail closed BEFORE mutating: contradictory actual state is
            # never silently repaired. A map row without its job row, a
            # foreign job squatting our canonical identity, or a drifted
            # spec under an existing mapping all raise
            # DesiredStateConflict here — deterministically, instead of
            # depending on which INSERT collides first. (The
            # IntegrityError handler below stays for the true
            # concurrent-creator race, where the winner's rows are
            # authoritative.)
            map_row = conn.execute(
                "SELECT * FROM desired_job_map WHERE desired_work_id=?",
                (desired_work_id,)).fetchone()
            job_row = conn.execute(
                "SELECT * FROM jobs WHERE job_id=?",
                (job_id,)).fetchone()
            if map_row is not None:
                if (job_row is not None
                        and map_row["job_id"] == job_id
                        and map_row["spec_hash"] == spec_hash):
                    return dict(job_row), False
                raise DesiredStateConflict(
                    f"canonical-identity collision for {desired_work_id!r}:"
                    f" job_id={job_id} (job row present="
                    f"{job_row is not None}) but its map row is"
                    f" absent/mismatched, the map exists without the job"
                    f" row, or the spec drifted under an existing mapping"
                ) from None
            if job_row is not None:
                raise DesiredStateConflict(
                    f"canonical-identity collision for {desired_work_id!r}:"
                    f" job_id={job_id} is owned by a foreign job with no"
                    f" map row") from None
            try:
                conn.execute(
                    "INSERT INTO jobs(job_id, task_id, stage_id, status,"
                    " max_attempts, policy, created_at, updated_at)"
                    " VALUES(?, ?, ?, 'PENDING', ?, ?, ?, ?)",
                    (job_id, task_id, norm["stage_id"],
                     norm["max_attempts"],
                     _canon({**(policy or {}),
                             "_desired": desired_envelope}),
                     now, now),
                )
                conn.execute(
                    "INSERT INTO desired_job_map(desired_work_id, job_id,"
                    " spec_hash, desired_version, created_at)"
                    " VALUES(?,?,?,?,?)",
                    (desired_work_id, job_id, spec_hash,
                     desired_version, now),
                )
            except sqlite3.IntegrityError:
                # Lost the create race (PRIMARY KEY: the winner's rows
                # are authoritative — observe them). Contradictory states
                # are rejected by the pre-check above; this handler is a
                # backstop for a race that slips between the pre-check
                # and the INSERTs.
                job_row = conn.execute(
                    "SELECT * FROM jobs WHERE job_id=?",
                    (job_id,)).fetchone()
                map_row = conn.execute(
                    "SELECT * FROM desired_job_map WHERE desired_work_id=?",
                    (desired_work_id,)).fetchone()
                if (job_row is not None and map_row is not None
                        and map_row["job_id"] == job_id
                        and map_row["spec_hash"] == spec_hash):
                    return dict(job_row), False
                raise DesiredStateConflict(
                    f"canonical-identity collision for {desired_work_id!r}:"
                    f" job_id={job_id} exists but its map row is"
                    f" absent/mismatched or the spec drifted under an"
                    f" existing mapping") from None
            self._append_event(conn, now, "job.created",
                               {"job_id": job_id, "task_id": task_id,
                                "desired_work_id": desired_work_id,
                                "spec_hash": spec_hash},
                               actor)
            return dict(self._get(conn, "jobs", "job_id", job_id)), True

    # ------------------------------------------ R11 reconciliation run log
    _RECONCILIATION_RESULTS = ("CONVERGED", "CHANGED", "BLOCKED",
                               "CONFLICT", "FAILED")

    def begin_reconciliation_run(self, desired_state_version: int,
                                 snapshot_hash: str, actor: str) -> dict:
        """Open a reconciliation run record: result='RUNNING'. The
        reconciler checkpoints per batch and finishes with one of the
        terminal results. Returns the run row."""
        if (isinstance(desired_state_version, bool)
                or not isinstance(desired_state_version, int)
                or desired_state_version < 0):
            raise TransitionRejected(
                "desired_state_version must be an int >= 0,"
                f" got {desired_state_version!r}")
        if not isinstance(snapshot_hash, str) or not snapshot_hash:
            raise TransitionRejected(
                "snapshot_hash must be a non-empty string")
        if not isinstance(actor, str) or not actor:
            raise TransitionRejected("actor must be a non-empty string")
        reconciliation_id = _new_id("rec-")
        with self.store.write_txn() as (conn, now):
            conn.execute(
                "INSERT INTO reconciliation_runs(reconciliation_id,"
                " desired_state_version, snapshot_hash, started_at,"
                " completed_at, result, items_examined, items_created,"
                " last_item_id, discrepancies, actor)"
                " VALUES(?, ?, ?, ?, NULL, 'RUNNING', 0, 0, NULL, '[]', ?)",
                (reconciliation_id, desired_state_version, snapshot_hash,
                 now, actor),
            )
            return dict(self._get(conn, "reconciliation_runs",
                                  "reconciliation_id", reconciliation_id))

    def checkpoint_reconciliation_run(self, reconciliation_id: str,
                                      last_item_id: str | None,
                                      items_examined: int,
                                      items_created: int) -> dict:
        """Durable per-batch cursor: the last processed item id and the
        running counters. A crashed pass is observable and resumable by
        inspection. The run must exist and still be RUNNING."""
        for name, value in (("items_examined", items_examined),
                            ("items_created", items_created)):
            if (isinstance(value, bool) or not isinstance(value, int)
                    or value < 0):
                raise TransitionRejected(
                    f"{name} must be an int >= 0, got {value!r}")
        with self.store.write_txn() as (conn, now):
            row = self._get(conn, "reconciliation_runs",
                            "reconciliation_id", reconciliation_id)
            if row["result"] != "RUNNING":
                raise TransitionRejected(
                    f"reconciliation run {reconciliation_id} is"
                    f" {row['result']}, not RUNNING: cannot checkpoint")
            conn.execute(
                "UPDATE reconciliation_runs SET last_item_id=?,"
                " items_examined=?, items_created=? WHERE reconciliation_id=?",
                (last_item_id, items_examined, items_created,
                 reconciliation_id),
            )
            return dict(self._get(conn, "reconciliation_runs",
                                  "reconciliation_id", reconciliation_id))

    def finish_reconciliation_run(self, reconciliation_id: str,
                                  result: str, items_examined: int,
                                  items_created: int,
                                  discrepancies: list) -> dict:
        """Close a run: stamp completed_at, set the terminal result, and
        store the discrepancies list as canonical JSON. The run must
        exist and still be RUNNING — a run finishes exactly once."""
        if result not in self._RECONCILIATION_RESULTS:
            raise TransitionRejected(
                f"unknown reconciliation result {result!r}: expected one of"
                f" {self._RECONCILIATION_RESULTS}")
        if not isinstance(discrepancies, list):
            raise TransitionRejected("discrepancies must be a list")
        for name, value in (("items_examined", items_examined),
                            ("items_created", items_created)):
            if (isinstance(value, bool) or not isinstance(value, int)
                    or value < 0):
                raise TransitionRejected(
                    f"{name} must be an int >= 0, got {value!r}")
        with self.store.write_txn() as (conn, now):
            row = self._get(conn, "reconciliation_runs",
                            "reconciliation_id", reconciliation_id)
            if row["result"] != "RUNNING":
                raise TransitionRejected(
                    f"reconciliation run {reconciliation_id} is already"
                    f" {row['result']}: a run finishes exactly once")
            conn.execute(
                "UPDATE reconciliation_runs SET completed_at=?, result=?,"
                " items_examined=?, items_created=?, discrepancies=?"
                " WHERE reconciliation_id=?",
                (now, result, items_examined, items_created,
                 _canon(discrepancies), reconciliation_id),
            )
            return dict(self._get(conn, "reconciliation_runs",
                                  "reconciliation_id", reconciliation_id))

    def get_reconciliation_run(self, reconciliation_id: str) -> dict:
        """Read-only: one reconciliation run row."""
        return dict(self._get(self.store.conn, "reconciliation_runs",
                              "reconciliation_id", reconciliation_id))

    def open_reconciliation_runs(self) -> list[dict]:
        """Read-only: runs that never finished (completed_at IS NULL), in
        start order. A non-empty result means a pass crashed or is still
        running — observable, resumable by inspection."""
        rows = self.store.conn.execute(
            "SELECT * FROM reconciliation_runs WHERE completed_at IS NULL"
            " ORDER BY started_at").fetchall()
        return [dict(r) for r in rows]

    # --------------------------------------- R12 circuit-breaker admission
    # Phase 1C R12 — the breaker is the admission-control layer in front of
    # claims. WHY a separate layer rather than folding failure counting
    # into jobs or the scheduler: admission must be decided atomically
    # with the claim (same write_txn) and must survive scheduler crashes,
    # so it belongs to durable gate-owned state, not to scheduler memory.
    # The controller (exec/) owns transitions: it records failure signals
    # from R7/R8/R9, moves CLOSED->OPEN when a scope's windowed failure
    # count warrants it, runs cooldown->HALF_OPEN probes, and closes or
    # re-opens on probe outcome. The admission path (claim_job_resilient)
    # only READS rows and allocates bounded half-open probes — it never
    # transitions a breaker, so a crashed controller fails closed (OPEN
    # keeps denying) rather than failing open.
    #
    # Authority: every op below is gate-owned; exec/ never touches this
    # SQL. All mutations run in one write_txn; every state change is a
    # version-pinned CAS UPDATE (the cas_update_recovery_policy discipline),
    # so concurrent controllers produce exactly one winner and the loser
    # gets BreakerConflict and re-reads.
    def _get_breaker_row(self, conn, scope_type: str,
                         scope_id: str) -> dict | None:
        row = conn.execute(
            "SELECT * FROM breaker_state WHERE scope_type=? AND scope_id=?",
            (scope_type, scope_id)).fetchone()
        return dict(row) if row is not None else None

    def _ensure_breaker_row_txn(self, conn, now: float, scope_type: str,
                                scope_id: str, cooldown_s: float) -> dict:
        """Get-or-create inside an open transaction: CLOSED, version 0,
        failure_count 0. Concurrent creators serialize on the composite
        PRIMARY KEY — exactly one row wins, the loser re-reads it. An
        existing row is returned UNCHANGED (ensure never retunes a live
        row's cooldown; that is the controller's transition authority)."""
        row = self._get_breaker_row(conn, scope_type, scope_id)
        if row is not None:
            return row
        try:
            conn.execute(
                "INSERT INTO breaker_state(scope_type, scope_id, state,"
                " version, failure_count, cooldown_s, updated_at)"
                " VALUES(?, ?, 'CLOSED', 0, 0, ?, ?)",
                (scope_type, scope_id, cooldown_s, now))
        except sqlite3.IntegrityError:
            # Lost the create race on the PRIMARY KEY: the winner's row
            # is authoritative — observe it. (No FOREIGN KEY on this
            # table, so IntegrityError can only be the lost race.)
            row = self._get_breaker_row(conn, scope_type, scope_id)
            if row is not None:
                return row
            raise
        return self._get_breaker_row(conn, scope_type, scope_id)

    def _claim_probe_txn(self, conn, now: float, scope_type: str,
                         scope_id: str, expected_version: int,
                         probe_id: str, probe_limit: int,
                         baseline_json: str | None) -> bool:
        """The single conditional half-open probe allocation (R12). One
        UPDATE that succeeds only when the row is HALF_OPEN at the
        expected version with probe budget remaining — so two racing
        claimants cannot double-allocate the same probe slot. True on
        allocation, False when the state/version/budget predicate failed
        (no mutation). Shared verbatim by claim_half_open_probe() and the
        resilient claim path: there is exactly one probe allocator."""
        cur = conn.execute(
            "UPDATE breaker_state SET half_open_probe_id=?,"
            " half_open_probes_used=half_open_probes_used+1,"
            " half_open_probe_at=?, half_open_probe_baseline=?,"
            " version=version+1, updated_at=?"
            " WHERE scope_type=? AND scope_id=? AND version=?"
            " AND state='HALF_OPEN' AND half_open_probes_used < ?",
            (probe_id, now, baseline_json, now,
             scope_type, scope_id, expected_version, probe_limit))
        return cur.rowcount == 1

    def get_breaker_state(self, scope_type: str,
                          scope_id: str) -> dict | None:
        """Read-only fetch of a breaker row, or None when the scope has
        never been seen. Missing is not an error: breaker_allows() reads
        a missing row as CLOSED, so the enforcement view stays total."""
        _check_breaker_scope(scope_type, scope_id)
        return self._get_breaker_row(self.store.conn, scope_type, scope_id)

    def list_breaker_states(self) -> list[dict]:
        """Read-only: every breaker row, ordered by (scope_type, scope_id)
        — deterministic for diagnostics and the controller's scan."""
        rows = self.store.conn.execute(
            "SELECT * FROM breaker_state ORDER BY scope_type, scope_id"
        ).fetchall()
        return [dict(r) for r in rows]

    def ensure_breaker_state(self, scope_type: str, scope_id: str, *,
                             cooldown_s: float, actor: str) -> dict:
        """Idempotent get-or-create: returns the existing row, or creates
        it CLOSED at version 0 with the given cooldown. WHY get-or-create
        rather than create-then-transition: the controller records signals
        for scopes it has never opened a breaker for (a first failure must
        be countable without a prior OPEN/CLOSED ceremony), and two racing
        controllers must converge on one row, not two."""
        _check_breaker_scope(scope_type, scope_id)
        _check_positive_duration(cooldown_s, "ensure_breaker_state",
                                 "cooldown_s")
        if not isinstance(actor, str) or not actor:
            raise TransitionRejected("actor must be a non-empty string")
        with self.store.write_txn() as (conn, now):
            return self._ensure_breaker_row_txn(conn, now, scope_type,
                                                scope_id, cooldown_s)

    def record_breaker_signal(self, scope_type: str, scope_id: str, *,
                              failure_kind: str, incident_id: str | None,
                              attempt_id: str | None,
                              failure_window_s: float, cooldown_s: float,
                              actor: str) -> dict:
        """Record one failure observation for a scope, exactly once, in
        one write_txn. Returns {"row", "deduped"}.

        WHY this shape: failure signals arrive from three independent
        producers (R7 watchdog verdicts, R8 attempt failures, R9
        escalations) plus RECOVERY_PRESSURE, and deliveries can retry. The
        canonical dedupe key (failure_kind, incident_id, attempt_id) in
        breaker_signals makes a retried delivery a no-op — the count,
        version, and ledger are untouched, so a failure is never
        double-counted. New signals count into a sliding window: a signal
        arriving after the window expired starts a fresh window (count 1)
        instead of accumulating stale history, so an ancient burst cannot
        keep a scope throttled forever. The ledger event carries the new
        count for the controller's trip decision.
        """
        _check_breaker_scope(scope_type, scope_id)
        if failure_kind not in _BREAKER_FAILURE_KINDS:
            raise TransitionRejected(
                "failure_kind must be one of"
                f" {sorted(_BREAKER_FAILURE_KINDS)}, got {failure_kind!r}")
        for label, value in (("incident_id", incident_id),
                             ("attempt_id", attempt_id)):
            if value is not None and not isinstance(value, str):
                raise TransitionRejected(
                    f"record_breaker_signal: {label} must be a string or"
                    f" null, got {value!r}")
        _check_positive_duration(failure_window_s, "record_breaker_signal",
                                 "failure_window_s")
        _check_positive_duration(cooldown_s, "record_breaker_signal",
                                 "cooldown_s")
        if not isinstance(actor, str) or not actor:
            raise TransitionRejected("actor must be a non-empty string")
        # Scope-qualified dedupe key (R12-03/R12-57): the same failure
        # delivered twice for the SAME scope counts once (INSERT OR IGNORE
        # under BEGIN IMMEDIATE serializes concurrent observers); the same
        # failure fanning out to DIFFERENT scopes counts once per scope,
        # so per-scope thresholds (JOB vs TASK vs GLOBAL) each observe
        # their own evidence. A scope-blind key would let only the first
        # scope's delivery count and starve the rest.
        signal_id = (f"{scope_type}:{scope_id}:{failure_kind}:"
                     f"{incident_id or '-'}:{attempt_id or '-'}")
        with self.store.write_txn() as (conn, now):
            self._ensure_breaker_row_txn(conn, now, scope_type, scope_id,
                                         cooldown_s)
            cur = conn.execute(
                "INSERT OR IGNORE INTO breaker_signals(signal_id,"
                " scope_type, scope_id, failure_kind, incident_id,"
                " attempt_id, observed_at, actor)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (signal_id, scope_type, scope_id, failure_kind,
                 incident_id, attempt_id, now, actor))
            if cur.rowcount == 0:
                # Duplicate delivery: the signal was already counted.
                # Return the authoritative row UNCHANGED — no count bump,
                # no version bump, no ledger event (no-op discipline §22).
                return {"row": self._get_breaker_row(conn, scope_type,
                                                     scope_id),
                        "deduped": True}
            row = self._get_breaker_row(conn, scope_type, scope_id)
            window_start = row["window_started_at"]
            if window_start is None or now - window_start > failure_window_s:
                failure_count, new_window = 1, now
            else:
                failure_count = row["failure_count"] + 1
                new_window = window_start
            conn.execute(
                "UPDATE breaker_state SET failure_count=?,"
                " window_started_at=?, last_failure_id=?,"
                " version=version+1, updated_at=?"
                " WHERE scope_type=? AND scope_id=?",
                (failure_count, new_window, signal_id, now,
                 scope_type, scope_id))
            self._append_event(
                conn, now, "breaker.signal_recorded",
                {"scope_type": scope_type, "scope_id": scope_id,
                 "failure_kind": failure_kind,
                 "incident_id": incident_id, "attempt_id": attempt_id,
                 "signal_id": signal_id, "failure_count": failure_count},
                actor)
            return {"row": self._get_breaker_row(conn, scope_type,
                                                 scope_id),
                    "deduped": False}

    def transition_breaker(self, scope_type: str, scope_id: str, *,
                           expected_version: int, to_state: str,
                           actor: str, reason: str,
                           evidence: dict | None = None) -> dict:
        """Controller-owned breaker state change, CAS on version, in one
        write_txn. Returns the new row; raises BreakerConflict on a lost
        race and TransitionRejected for an illegal transition (checked
        BEFORE any write) or a missing row (ensure_breaker_state first).

        WHY the controller — and only the controller — transitions: the
        admission path must never move a breaker on its own observations,
        or a burst of claims could flap CLOSED<->OPEN without the
        controller's trip/hold policy ever running. Legal edges only:
        CLOSED->OPEN (trip), OPEN->HALF_OPEN (cooldown elapsed, allow a
        bounded probe), HALF_OPEN->CLOSED (probe succeeded), HALF_OPEN->OPEN
        (probe failed). Side effects per edge: ->OPEN anchors opened_at
        and cooldown_until to the row's cooldown_s AT OPEN TIME, so a
        later cooldown retune cannot shorten an in-flight cooldown;
        ->HALF_OPEN clears any stale probe fields and restarts the probe
        budget; ->CLOSED resets the failure window and clears probe
        fields. Every edge bumps version and journals breaker.transition
        with old/new state, reason, evidence, and the new version.
        """
        _check_breaker_scope(scope_type, scope_id)
        if to_state not in _BREAKER_STATES:
            raise TransitionRejected(
                "breaker to_state must be one of"
                f" {sorted(_BREAKER_STATES)}, got {to_state!r}")
        if not isinstance(actor, str) or not actor:
            raise TransitionRejected("actor must be a non-empty string")
        if not isinstance(reason, str) or not reason:
            raise TransitionRejected("reason must be a non-empty string")
        if evidence is not None and not isinstance(evidence, dict):
            raise TransitionRejected(
                "evidence must be a dict or null,"
                f" got {type(evidence).__name__}")
        with self.store.write_txn() as (conn, now):
            row = self._get_breaker_row(conn, scope_type, scope_id)
            if row is None:
                raise TransitionRejected(
                    f"no breaker row for {scope_type}:{scope_id}:"
                    " call ensure_breaker_state first")
            old_state = row["state"]
            if to_state not in _BREAKER_TRANSITIONS.get(old_state, ()):
                raise TransitionRejected(
                    f"illegal breaker transition {scope_type}:{scope_id}:"
                    f" {old_state} -> {to_state}")
            sets: dict = {"state": to_state, "updated_at": now}
            if to_state == "OPEN":
                sets["opened_at"] = now
                sets["cooldown_until"] = now + row["cooldown_s"]
            elif to_state == "HALF_OPEN":
                sets.update({"half_open_probe_id": None,
                             "half_open_probe_at": None,
                             "half_open_probe_baseline": None,
                             "half_open_probes_used": 0})
            else:  # HALF_OPEN -> CLOSED: the probe trial succeeded.
                sets.update({"failure_count": 0,
                             "window_started_at": None,
                             "half_open_probe_id": None,
                             "half_open_probe_at": None,
                             "half_open_probe_baseline": None,
                             "half_open_probes_used": 0})
            set_clause = ", ".join(f"{k}=?" for k in sets)
            cur = conn.execute(
                f"UPDATE breaker_state SET {set_clause}, version=version+1"
                " WHERE scope_type=? AND scope_id=? AND version=?",
                (*sets.values(), scope_type, scope_id, expected_version))
            if cur.rowcount != 1:
                raise BreakerConflict(
                    f"breaker CAS lost for {scope_type}:{scope_id}:"
                    f" expected version {expected_version}")
            new = self._get_breaker_row(conn, scope_type, scope_id)
            self._append_event(
                conn, now, "breaker.transition",
                {"scope_type": scope_type, "scope_id": scope_id,
                 "old_state": old_state, "new_state": to_state,
                 "reason": reason, "evidence": evidence,
                 "version": new["version"]},
                actor)
            return new

    def claim_half_open_probe(self, scope_type: str, scope_id: str, *,
                              expected_version: int, probe_id: str,
                              probe_limit: int, baseline: dict | None,
                              actor: str) -> dict | None:
        """Allocate one half-open probe slot on a HALF_OPEN breaker, in
        one write_txn. Returns the updated row on allocation; returns None
        (no mutation) when the row is not HALF_OPEN or the probe budget is
        exhausted; raises BreakerConflict when the version moved under the
        caller (the caller must re-read the authoritative row).

        WHY probes are allocations, not just a flag: HALF_OPEN admits a
        BOUNDED number of trial executions (probe_limit per half-open
        episode), each bound to a probe_id so the controller can attribute
        the trial's outcome. The conditional UPDATE is the whole
        mechanism — state, version, and budget are checked and mutated in
        one atomic statement, so concurrent claimants cannot overshoot
        the budget. `baseline` (the job's progress/fencing/status at probe
        time, canonical JSON) is what the controller diffs against when
        the probe completes: I-18 discipline — the probe is verified by
        progress, not by having run. No ledger event: probe allocation is
        transient admission state; the durable audit trail is the row's
        probe fields and the job.claimed payload on the admitted claim.
        """
        _check_breaker_scope(scope_type, scope_id)
        if not isinstance(probe_id, str) or not probe_id:
            raise TransitionRejected(
                f"probe_id must be a non-empty string, got {probe_id!r}")
        if isinstance(probe_limit, bool) \
                or not isinstance(probe_limit, int) or probe_limit < 1:
            raise TransitionRejected(
                "probe_limit must be a positive int,"
                f" got {probe_limit!r}")
        if baseline is not None and not isinstance(baseline, dict):
            raise TransitionRejected(
                "baseline must be a dict or null,"
                f" got {type(baseline).__name__}")
        if not isinstance(actor, str) or not actor:
            raise TransitionRejected("actor must be a non-empty string")
        baseline_json = _canon(baseline) if baseline is not None else None
        with self.store.write_txn() as (conn, now):
            if self._claim_probe_txn(conn, now, scope_type, scope_id,
                                     expected_version, probe_id,
                                     probe_limit, baseline_json):
                return self._get_breaker_row(conn, scope_type, scope_id)
            row = self._get_breaker_row(conn, scope_type, scope_id)
            if row is None or row["version"] != expected_version:
                raise BreakerConflict(
                    f"half-open probe CAS lost for"
                    f" {scope_type}:{scope_id}:"
                    f" expected version {expected_version}")
            return None

    def breaker_allows(self, scope_type: str,
                       scope_id: str) -> tuple[bool, str]:
        """Read-only enforcement view over one breaker row. Returns
        (allowed, reason): missing row or CLOSED -> (True, "closed");
        OPEN -> (False, "open"); HALF_OPEN -> (True, "half_open"); a row
        that cannot be read as a breaker -> (False, "corrupt"). Fails
        closed and never raises on corrupt state: the admission decision
        must be total — a storage anomaly denies rather than crashing the
        scheduler. (Half-open probe ALLOCATION is not done here; only the
        claim path allocates probes, atomically with the claim.)"""
        _check_breaker_scope(scope_type, scope_id)
        row = self._get_breaker_row(self.store.conn, scope_type, scope_id)
        if row is None:
            return (True, "closed")
        verdict = _classify_breaker_row(row)
        if verdict == "closed":
            return (True, "closed")
        if verdict == "open":
            return (False, "open")
        if verdict == "half_open":
            return (True, "half_open")
        return (False, "corrupt")

    def get_desired_work_id_for_job(self, job_id: str) -> str | None:
        """Read-only reverse lookup over R11's desired_job_map: the
        desired_work_id whose canonical map row binds job_id, or None when
        the job was not materialized from desired state. Resolves the
        DESIRED breaker scope for a claim (jobs without a map row simply
        have no DESIRED scope)."""
        row = self.store.conn.execute(
            "SELECT desired_work_id FROM desired_job_map WHERE job_id=?",
            (job_id,)).fetchone()
        return row["desired_work_id"] if row is not None else None

    def claim_job_resilient(self, job_id: str, worker_id: str,
                            ttl_s: float, actor: str,
                            max_concurrent_jobs: int,
                            scheduler_id: str | None = None, *,
                            probe_limit: int = 1) -> bool:
        """PENDING -> CLAIMED with breaker admission control (R12), in ONE
        write_txn. Returns True on admission, False on denial.

        WHY this exists alongside claim_job_bounded: capacity alone cannot
        stop a scheduler from hammering a scope whose executions keep
        failing — the breaker layer denies claims into scopes the
        controller has tripped OPEN, and admits bounded half-open probes
        so the controller can verify recovery by progress (I-18). The
        capacity predicate, the claim core, and the scheduler beat are
        IDENTICAL to claim_job_bounded (same core, same SQL): there is
        still exactly one claim implementation, one capacity predicate,
        one beat-planting path.

        Steps, all in one transaction: (1) validate ttl /
        max_concurrent_jobs / probe_limit exactly like claim_job_bounded;
        (2) read the job row (task_id plus the probe baseline —
        progress_done/progress_total/fencing_token/status) and resolve
        the DESIRED scope via R11's desired_job_map (may be absent);
        (3) evaluate scopes in the fixed order GLOBAL, TASK (if any),
        DESIRED (if mapped), JOB: missing row allows, corrupt or OPEN
        denies (an OPEN row with an elapsed cooldown still denies — only
        the controller transitions; fail closed), HALF_OPEN allocates one
        probe via the shared conditional UPDATE (probe_id
        "<worker_id>:<job_id>", probe_limit, baseline); a failed
        allocation denies; (4) the claim_job_bounded capacity predicate;
        (5) the shared _claim_job_txn core with the half_open_probe
        allocation recorded in the job.claimed payload, then the
        scheduler beat exactly like claim_job_bounded.

        Denial discipline (no-op §22): a denial BEFORE any mutation is a
        plain False with no ledger event. A denial AFTER a probe was
        allocated raises the private _AdmissionDenied so the transaction
        rolls back — the probe slot is unwound atomically and can never
        leak without an admitted claim. The outer catch converts it to a
        plain False. No ledger event is ever written on denial.
        """
        _check_ttl(ttl_s, "claim_job_resilient")
        if isinstance(max_concurrent_jobs, bool) \
                or not isinstance(max_concurrent_jobs, int) \
                or max_concurrent_jobs < 1:
            raise TransitionRejected(
                "claim_job_resilient: max_concurrent_jobs must be an"
                " integer >= 1,"
                f" got {max_concurrent_jobs!r}")
        if isinstance(probe_limit, bool) \
                or not isinstance(probe_limit, int) or probe_limit < 1:
            raise TransitionRejected(
                "claim_job_resilient: probe_limit must be a positive int,"
                f" got {probe_limit!r}")
        try:
            with self.store.write_txn() as (conn, now):
                return self._claim_job_resilient_txn(
                    conn, now, job_id, worker_id, ttl_s, actor,
                    max_concurrent_jobs, scheduler_id, probe_limit)
        except _AdmissionDenied:
            return False

    def _claim_job_resilient_txn(self, conn, now: float, job_id: str,
                                 worker_id: str, ttl_s: float, actor: str,
                                 max_concurrent_jobs: int,
                                 scheduler_id: str | None,
                                 probe_limit: int) -> bool:
        job = conn.execute(
            "SELECT task_id, progress_done, progress_total, fencing_token,"
            " status FROM jobs WHERE job_id=?",
            (job_id,)).fetchone()
        if job is None:
            return False  # unknown job: not claimable, no writes made
        map_row = conn.execute(
            "SELECT desired_work_id FROM desired_job_map WHERE job_id=?",
            (job_id,)).fetchone()
        desired_work_id = (map_row["desired_work_id"]
                           if map_row is not None else None)
        # Scopes in FIXED order: concurrent claimants always probe the
        # same sequence, and BEGIN IMMEDIATE serializes the writers, so
        # probe allocation cannot interleave or deadlock.
        scopes = [("GLOBAL", "global")]
        if job["task_id"]:
            scopes.append(("TASK", job["task_id"]))
        if desired_work_id:
            scopes.append(("DESIRED", desired_work_id))
        scopes.append(("JOB", job_id))
        baseline_json = _canon({
            "progress_done": job["progress_done"],
            "progress_total": job["progress_total"],
            "fencing_token": job["fencing_token"],
            "status": job["status"],
        })
        probe_id = f"{worker_id}:{job_id}"
        allocations: list[dict] = []
        probe_allocated = False

        def deny(reason: str):
            # Denial before any mutation: plain False (no-op discipline
            # §22 — no ledger event, nothing to roll back). Denial after
            # a probe was allocated: unwind it via transaction rollback,
            # so no probe slot ever leaks without an admitted claim.
            if probe_allocated:
                raise _AdmissionDenied(reason)
            return False

        for scope_type, scope_id in scopes:
            row = self._get_breaker_row(conn, scope_type, scope_id)
            if row is None:
                continue  # no breaker row: the scope imposes no limit
            verdict = _classify_breaker_row(row)
            if verdict in ("open", "corrupt"):
                # OPEN with an elapsed cooldown still denies: only the
                # controller may move OPEN -> HALF_OPEN. A corrupt row
                # fails closed — never admitted on unreadable state.
                return deny(f"breaker {scope_type}:{scope_id} {verdict}")
            if verdict == "half_open":
                if not self._claim_probe_txn(
                        conn, now, scope_type, scope_id, row["version"],
                        probe_id, probe_limit, baseline_json):
                    # Lost the probe race, the row left HALF_OPEN, or the
                    # probe budget is exhausted: deny cleanly, with no
                    # half-allocated state.
                    return deny(
                        "half-open probe unavailable for"
                        f" {scope_type}:{scope_id}")
                probe_allocated = True
                allocations.append({"scope_type": scope_type,
                                    "scope_id": scope_id,
                                    "probe_id": probe_id})
        # Capacity predicate: identical to claim_job_bounded, evaluated in
        # the same transaction as the claim (read-then-claim across two
        # transactions would race and over-admit).
        active = conn.execute(
            "SELECT COUNT(*) FROM jobs"
            " WHERE status IN ('CLAIMED','RUNNING','COMMITTING')"
        ).fetchone()[0]
        if active >= max_concurrent_jobs:
            return deny("capacity full")
        extra = None
        if allocations:
            extra = {"half_open_probe":
                     allocations[0] if len(allocations) == 1
                     else allocations}
        ok = self._claim_job_txn(conn, now, job_id, worker_id, ttl_s,
                                 actor, extra_payload=extra)
        if not ok:
            # The claim lost its race (or the job was not PENDING): any
            # probe allocated above must be unwound, not leaked.
            return deny(f"job {job_id} not claimable")
        if scheduler_id is not None:
            conn.execute(
                "INSERT OR REPLACE INTO scheduler_claim_beats"
                "(worker_id, scheduler_id, last_beat)"
                " VALUES (?,?,?)",
                (worker_id, scheduler_id, now))
        return ok

    # --------------------------------- R13 finalization & release checkpoint
    # Phase 1C R13 — finalization is the terminal READ of a release
    # generation. It is a CONSUMER of R1–R12 evidence, never a second
    # authority: it creates no jobs, claims nothing, fences nothing,
    # reclaims nothing, selects no rungs, manages no budgets, mutates no
    # desired state, moves no breakers, clears no human gates, completes
    # no jobs, and performs no raw SQL outside the gate's write_txn
    # discipline. The only rows it ever mutates are its own
    # finalization_runs rows — plus the deterministic finalization task
    # row and the R5 release checkpoint documented below.
    #
    # Release-checkpoint reuse (mission: prefer extension over parallel
    # authority): the release checkpoint lives in R5's `checkpoints`
    # table and is staged/verified EXCLUSIVELY through R5's
    # stage_checkpoint()/verify_checkpoint() — there is no parallel
    # `release_checkpoints` table, no second checkpoint algorithm, and no
    # second identity scheme (checkpoint_id = sha256 of the canonical
    # artifact manifest, exactly as R5 computes it; the pure
    # _release_checkpoint_id() below replicates that computation so the
    # finalizer can name the checkpoint before staging it). R5
    # checkpoints require a task row; a release covers a whole
    # generation, not one task, so the finalizer ensures one
    # deterministic synthetic task row per generation (the "task-fin-"
    # row) as the checkpoint's container. That row never transitions and
    # no work is ever scheduled on it — it is a container, not a
    # work-creation act, so the I-16 work-creator gate does not apply.
    #
    # State machine: OPEN -> EVALUATING -> READY | BLOCKED | FAILED, with
    # FINALIZED terminal. EVALUATING is an in-transaction intermediate
    # (recorded in the ledger event's "via" field) and is never persisted.
    # Every state change is a version-pinned CAS UPDATE in ONE write_txn
    # with a ledger event; the loser gets FinalizationConflict and
    # re-reads. COMPLETE is terminal (transitions.py), so finalized
    # generations cannot be silently mutated; a desired-state change
    # CAS-bumps the head and therefore names a NEW generation.
    def _check_finalizer_actor(self, actor: str, op: str) -> None:
        if not isinstance(actor, str) or not actor:
            raise TransitionRejected(
                f"{op}: actor must be a non-empty string")
        if _is_worker_actor(actor):
            raise TransitionRejected(
                f"{op}: worker actors may not drive finalization")

    def begin_finalization_run(self, release_generation: str,
                               desired_state_version: int,
                               actor: str) -> dict:
        """Idempotent open of a finalization run: state OPEN, version 1.

        release_generation must be the canonical identity for
        desired_state_version ("ds-v" + version) — a mismatched pair is a
        caller bug and is rejected. Re-beginning an existing run returns
        the current row unchanged (no version bump, no duplicate ledger
        event); an existing row pinning a DIFFERENT version is a
        contradiction and fails closed with TransitionRejected."""
        self._check_finalizer_actor(actor, "begin_finalization_run")
        if (release_generation
                != canonical_release_generation(desired_state_version)):
            raise TransitionRejected(
                "begin_finalization_run: release_generation"
                f" {release_generation!r} is not the canonical identity"
                f" for desired_state_version {desired_state_version!r}"
                f" ({canonical_release_generation(desired_state_version)!r})")
        finalization_id = canonical_finalization_id(release_generation)
        with self.store.write_txn() as (conn, now):
            cur = conn.execute(
                "INSERT INTO finalization_runs(finalization_id,"
                " release_generation, desired_state_version, state,"
                " manifest_hash, checkpoint_id, version, started_at,"
                " completed_at, result, blockers, updated_at)"
                " VALUES(?, ?, ?, 'OPEN', NULL, NULL, 1, ?, NULL, NULL,"
                " '[]', ?)"
                " ON CONFLICT(finalization_id) DO NOTHING",
                (finalization_id, release_generation,
                 desired_state_version, now, now),
            )
            row = self._get(conn, "finalization_runs", "finalization_id",
                            finalization_id)
            if cur.rowcount == 1:
                self._append_event(
                    conn, now, "finalization.begun",
                    {"finalization_id": finalization_id,
                     "release_generation": release_generation,
                     "desired_state_version": desired_state_version},
                    actor)
            elif row["desired_state_version"] != desired_state_version:
                raise TransitionRejected(
                    "begin_finalization_run: contradictory re-begin of"
                    f" {release_generation}: the existing run pins version"
                    f" {row['desired_state_version']}, caller gave"
                    f" {desired_state_version} — failing closed")
            return dict(row)

    def get_finalization_run(self, release_generation: str) -> dict | None:
        """Read-only: the finalization run for a generation, or None."""
        row = self.store.conn.execute(
            "SELECT * FROM finalization_runs WHERE finalization_id=?",
            (canonical_finalization_id(release_generation),)).fetchone()
        return dict(row) if row is not None else None

    def list_finalization_runs(self) -> list[dict]:
        """Read-only: every finalization run, oldest first — deterministic
        for the finalizer's scan."""
        rows = self.store.conn.execute(
            "SELECT * FROM finalization_runs ORDER BY started_at"
        ).fetchall()
        return [dict(r) for r in rows]

    def _ensure_finalization_task(self, release_generation: str,
                                  actor: str) -> dict:
        """Idempotent ensure of the deterministic synthetic task row that
        contains this generation's release checkpoint (documented in the
        R13 banner above). Concurrent ensurers serialize on the tasks
        PRIMARY KEY; the loser observes the winner. A row squatting the
        deterministic id with foreign content is a contradiction and
        fails closed."""
        task_id = _canonical_finalization_task_id(release_generation)
        with self.store.write_txn() as (conn, now):
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id=?",
                (task_id,)).fetchone()
            if row is not None:
                try:
                    objective = json.loads(row["objective"] or "{}")
                except (ValueError, TypeError):
                    objective = {}
                if (not isinstance(objective, dict)
                        or objective.get("finalization")
                        != release_generation):
                    raise TransitionRejected(
                        "finalization task id"
                        f" {task_id!r} is squatted by foreign content:"
                        " failing closed")
                return dict(row)
            try:
                conn.execute(
                    "INSERT INTO tasks(task_id, status, objective, budgets,"
                    " created_at, updated_at)"
                    " VALUES(?, 'PROPOSED', ?, ?, ?, ?)",
                    (task_id,
                     _canon({"finalization": release_generation,
                             "note": "synthetic container task: owns the"
                                     " R13 release checkpoint for this"
                                     " generation; no work is ever scheduled"
                                     " on it and it never transitions"}),
                     _canon({}), now, now),
                )
            except sqlite3.IntegrityError:
                # Lost the ensure race: the winner's row is authoritative.
                row = conn.execute(
                    "SELECT * FROM tasks WHERE task_id=?",
                    (task_id,)).fetchone()
                if row is None:
                    raise
                return dict(row)
            self._append_event(
                conn, now, "finalization.task_ensured",
                {"task_id": task_id,
                 "release_generation": release_generation}, actor)
            return dict(self._get(conn, "tasks", "task_id", task_id))

    def _verify_finalized_record(self, conn, row: dict) -> dict:
        """Verify a FINALIZED record instead of trusting it: the release
        checkpoint must still exist and be VERIFIED. Zero writes. Raises
        TransitionRejected on contradiction — a finalized claim is never
        returned over degraded evidence."""
        ck = conn.execute(
            "SELECT verification_status FROM checkpoints"
            " WHERE checkpoint_id=?",
            (row["checkpoint_id"],)).fetchone()
        if (row["checkpoint_id"] is None or ck is None
                or ck["verification_status"] != "VERIFIED"):
            raise TransitionRejected(
                f"finalization {row['release_generation']} is FINALIZED but"
                f" its release checkpoint {row['checkpoint_id']!r} is not"
                " VERIFIED: contradictory finalized record")
        return dict(row)

    def _collect_finalization_evidence(self, conn, release_generation: str,
                                       desired_state_version: int) -> dict:
        """Read-only evidence snapshot for one release generation.

        All reads run on the caller's conn: the read-only connection for
        evaluation, the write transaction's connection for publish's
        authoritative re-read. Returns a JSON-safe dict consumed by
        _evaluate_finalization_blockers() and build_finalization_manifest().

        Raises _EvidenceFailure(CORRUPT_AUTHORITY, ...) when durable
        evidence is unreadable (storage errors, unparseable canonical
        JSON). Structural contradictions (map without job, job without
        task) are NOT raised here — they are evidence the blocker
        evaluator turns into CONTRADICTORY_STATE blockers."""
        finalization_task_id = _canonical_finalization_task_id(
            release_generation)
        try:
            head = self._read_desired_head(conn)
        except (TransitionRejected, sqlite3.Error) as exc:
            raise _EvidenceFailure(
                "CORRUPT_AUTHORITY", release_generation,
                "desired-state head unreadable:"
                f" {type(exc).__name__}: {exc}") from exc
        if (isinstance(head["version"], bool)
                or not isinstance(head["version"], int)):
            raise _EvidenceFailure(
                "CONTRADICTORY_STATE", release_generation,
                "desired-state head version is not an int:"
                f" {head['version']!r}")
        try:
            item_rows = conn.execute(
                "SELECT desired_work_id, spec, retired, version"
                " FROM desired_state ORDER BY desired_work_id").fetchall()
            map_rows = conn.execute(
                "SELECT desired_work_id, job_id FROM desired_job_map"
            ).fetchall()
            job_rows = conn.execute(
                "SELECT job_id, task_id, status, result_artifact_id,"
                " content_hash FROM jobs ORDER BY job_id").fetchall()
            open_incidents = [
                {"incident_id": r["incident_id"],
                 "failure_class": r["failure_class"]}
                for r in conn.execute(
                    "SELECT incident_id, failure_class FROM incidents"
                    " WHERE outcome IS NULL AND scope='recovery'"
                    " ORDER BY created_at").fetchall()]
            open_attempts: dict[str, dict] = {}
            for r in conn.execute(
                    "SELECT attempt_id, attempt_state, incident_id, job_id"
                    " FROM recovery_attempts WHERE attempt_state IN"
                    " ('CREATED','RUNNING','VERIFYING','UNCERTAIN')"
                    " ORDER BY attempt_id").fetchall():
                open_attempts[r["attempt_id"]] = {
                    "attempt_state": r["attempt_state"],
                    "incident_id": r["incident_id"],
                    "job_id": r["job_id"]}
            escalated = []
            for r in conn.execute(
                    "SELECT incident_id FROM incidents"
                    " WHERE outcome='escalated'"
                    " ORDER BY created_at").fetchall():
                pol = conn.execute(
                    "SELECT terminal_state FROM recovery_policy"
                    " WHERE incident_id=?",
                    (r["incident_id"],)).fetchone()
                escalated.append({
                    "incident_id": r["incident_id"],
                    "policy_terminal_state": (pol["terminal_state"]
                                              if pol is not None else None)})
            unconsumed = [r["incident_id"] for r in conn.execute(
                "SELECT i.incident_id FROM incidents i"
                " JOIN recovery_policy p ON p.incident_id=i.incident_id"
                " WHERE p.terminal_state IS NULL AND i.outcome IS NOT NULL"
                " ORDER BY i.created_at").fetchall()]
            breakers = [
                {"scope_type": r["scope_type"], "scope_id": r["scope_id"],
                 "state": r["state"], "version": r["version"],
                 "half_open_probes_used": r["half_open_probes_used"]}
                for r in conn.execute(
                    "SELECT scope_type, scope_id, state, version,"
                    " half_open_probes_used FROM breaker_state"
                    " ORDER BY scope_type, scope_id").fetchall()]
        except sqlite3.Error as exc:
            raise _EvidenceFailure(
                "CORRUPT_AUTHORITY", release_generation,
                f"evidence read failed: {exc}") from exc
        try:
            spawns = self._unreaped_proc_spawns_conn(conn)
        except (ValueError, TypeError) as exc:
            raise _EvidenceFailure(
                "CORRUPT_AUTHORITY", "ledger",
                f"spawn evidence payload unparseable: {exc}") from exc
        except sqlite3.Error as exc:
            raise _EvidenceFailure(
                "CORRUPT_AUTHORITY", "ledger",
                f"spawn evidence unreadable: {exc}") from exc

        desired_items = []
        for r in item_rows:
            try:
                spec = json.loads(r["spec"])
            except (ValueError, TypeError) as exc:
                raise _EvidenceFailure(
                    "CORRUPT_AUTHORITY", r["desired_work_id"],
                    f"desired spec JSON unparseable: {exc}") from exc
            if not isinstance(spec, dict):
                raise _EvidenceFailure(
                    "CORRUPT_AUTHORITY", r["desired_work_id"],
                    "desired spec JSON is not an object")
            if not r["retired"]:
                desired_items.append({"id": r["desired_work_id"],
                                      "spec": spec,
                                      "version": r["version"]})
        desired_job_map: dict[str, str | None] = {
            it["id"]: None for it in desired_items}
        for m in map_rows:
            if m["desired_work_id"] in desired_job_map:
                desired_job_map[m["desired_work_id"]] = m["job_id"]
        jobs: dict[str, dict] = {}
        for r in job_rows:
            jobs[r["job_id"]] = {"task_id": r["task_id"],
                                 "status": r["status"],
                                 "result_artifact_id":
                                     r["result_artifact_id"],
                                 "content_hash": r["content_hash"]}
        gen_job_ids = [jid for jid in
                       (desired_job_map[it["id"]] for it in desired_items)
                       if jid]
        task_ids = set()
        for jid in gen_job_ids:
            job = jobs.get(jid)
            if job is not None and job["task_id"]:
                task_ids.add(job["task_id"])
        task_ids.add(finalization_task_id)
        tasks: dict[str, dict | None] = {}
        try:
            for tid in sorted(task_ids):
                tr = conn.execute(
                    "SELECT task_id, status FROM tasks WHERE task_id=?",
                    (tid,)).fetchone()
                tasks[tid] = ({"status": tr["status"]}
                              if tr is not None else None)
        except sqlite3.Error as exc:
            raise _EvidenceFailure(
                "CORRUPT_AUTHORITY", release_generation,
                f"task evidence unreadable: {exc}") from exc

        artifacts: dict[str, dict] = {}
        validations: list[dict] = []
        try:
            for jid in gen_job_ids:
                job = jobs.get(jid)
                if job is None or job["status"] != "COMPLETE":
                    continue
                aid = job["result_artifact_id"]
                if not aid:
                    continue
                ar = conn.execute(
                    "SELECT artifact_id, task_id, job_id, kind, size, uri,"
                    " status, content_hash FROM artifacts"
                    " WHERE artifact_id=?", (aid,)).fetchone()
                if ar is None:
                    continue  # MISSING_ARTIFACT, judged by the evaluator
                data = self._read_staged_bytes(ar["uri"])
                artifacts[aid] = {
                    "content_hash": ar["content_hash"],
                    "status": ar["status"],
                    "job_id": ar["job_id"],
                    "size": ar["size"],
                    "kind": ar["kind"],
                    "bytes_present": data is not None,
                    "bytes_hash": (hashlib.sha256(data).hexdigest()
                                   if data is not None else None),
                }
                for vid, ver in REQUIRED_VALIDATORS:
                    vr = conn.execute(
                        "SELECT validator_id, validator_version, result,"
                        " content_hash FROM validations"
                        " WHERE artifact_id=? AND validator_id=?"
                        " AND validator_version=?"
                        " ORDER BY validated_at DESC LIMIT 1",
                        (aid, vid, ver)).fetchone()
                    validations.append({
                        "artifact_id": aid, "validator_id": vid,
                        "validator_version": ver,
                        "result": vr["result"] if vr is not None else None,
                        "content_hash": (vr["content_hash"]
                                         if vr is not None else None)})
        except sqlite3.Error as exc:
            raise _EvidenceFailure(
                "CORRUPT_AUTHORITY", release_generation,
                f"artifact evidence unreadable: {exc}") from exc
        validations.sort(key=lambda d: (d["artifact_id"], d["validator_id"],
                                        d["validator_version"]))

        entries = _release_checkpoint_entries(artifacts)
        release_checkpoint_id = _release_checkpoint_id(entries)
        try:
            ck_row = conn.execute(
                "SELECT * FROM checkpoints WHERE checkpoint_id=?",
                (release_checkpoint_id,)).fetchone()
        except sqlite3.Error as exc:
            raise _EvidenceFailure(
                "CORRUPT_AUTHORITY", release_checkpoint_id,
                f"checkpoint evidence unreadable: {exc}") from exc
        checkpoint = None
        if ck_row is not None:
            checkpoint = dict(ck_row)
            receipt_raw = checkpoint.get("verification_receipt")
            if receipt_raw:
                try:
                    checkpoint["receipt"] = json.loads(receipt_raw)
                except (ValueError, TypeError) as exc:
                    raise _EvidenceFailure(
                        "CORRUPT_AUTHORITY", release_checkpoint_id,
                        f"verification receipt unparseable: {exc}") from exc

        return {
            "release_generation": release_generation,
            "desired_state_version": desired_state_version,
            "finalization_task_id": finalization_task_id,
            "head": {"version": head["version"],
                     "snapshot_hash": head["snapshot_hash"]},
            "desired_state_hash": head["snapshot_hash"],
            "desired_items": desired_items,
            "desired_job_map": desired_job_map,
            "jobs": jobs,
            "tasks": tasks,
            "artifacts": artifacts,
            "validations": validations,
            "release_checkpoint_id": release_checkpoint_id,
            "release_checkpoint_entries": entries,
            "checkpoint": checkpoint,
            "open_incidents": open_incidents,
            "open_attempts": open_attempts,
            "escalated": escalated,
            "unconsumed_outcomes": unconsumed,
            "breakers": breakers,
            "unreaped_spawns": spawns,
        }

    def _record_evaluation_outcome(self, finalization_id: str,
                                   release_generation: str,
                                   expected_version: int, actor: str, *,
                                   blockers: list, final_state: str,
                                   manifest: dict,
                                   checkpoint_id: str | None) -> dict:
        """Phase C of an evaluation: the single authoritative write_txn.

        Re-pins the desired head inside the transaction — a head that
        moved under the evaluation refuses with STALE_GENERATION rather
        than recording a stale verdict. Then CAS-updates the run row on
        expected_version: the loser gets FinalizationConflict and
        re-reads. EVALUATING is the in-transaction intermediate (named in
        the ledger event's "via" field); it is never persisted. An
        unchanged verdict is a no-op: zero writes."""
        manifest_hash = finalization_manifest_hash(manifest)
        with self.store.write_txn() as (conn, now):
            cur_row = self._get(conn, "finalization_runs",
                                "finalization_id", finalization_id)
            if cur_row["state"] == "FINALIZED":
                # Won the race against a concurrent publish: the
                # finalized record is authoritative — verify and return
                # it, zero writes.
                return self._verify_finalized_record(conn, dict(cur_row))
            head = self._read_desired_head(conn)
            if head["version"] != cur_row["desired_state_version"]:
                raise TransitionRejected(
                    "evaluate_finalization: STALE_GENERATION for"
                    f" {release_generation}: the desired-state head moved"
                    " during evaluation"
                    f" (pinned v{cur_row['desired_state_version']}, now"
                    f" v{head['version']}) — re-evaluate")
            if cur_row["version"] != expected_version:
                raise FinalizationConflict(
                    "finalization CAS lost for"
                    f" {release_generation}: expected version"
                    f" {expected_version}, found {cur_row['version']}")
            try:
                stored_blockers = json.loads(cur_row["blockers"] or "[]")
            except (ValueError, TypeError):
                stored_blockers = None  # corrupt: never equal, rewrite
            if (cur_row["state"] == final_state
                    and stored_blockers == blockers
                    and cur_row["manifest_hash"] == manifest_hash
                    and cur_row["checkpoint_id"] == checkpoint_id):
                return dict(cur_row)
            cur = conn.execute(
                "UPDATE finalization_runs SET state=?, manifest_hash=?,"
                " checkpoint_id=?, blockers=?, version=version+1,"
                " updated_at=? WHERE finalization_id=? AND version=?",
                (final_state, manifest_hash, checkpoint_id,
                 _canon(blockers), now, finalization_id, expected_version))
            if cur.rowcount != 1:
                raise FinalizationConflict(
                    "finalization CAS lost for"
                    f" {release_generation}: expected version"
                    f" {expected_version}")
            self._append_event(
                conn, now, "finalization.evaluated",
                {"finalization_id": finalization_id,
                 "release_generation": release_generation,
                 "from": cur_row["state"], "via": "EVALUATING",
                 "to": final_state, "manifest_hash": manifest_hash,
                 "checkpoint_id": checkpoint_id,
                 "blockers": blockers}, actor)
            return dict(self._get(conn, "finalization_runs",
                                  "finalization_id", finalization_id))

    def evaluate_finalization(self, release_generation: str,
                              expected_version: int, actor: str,
                              **evidence_params) -> dict:
        """Evaluate every R13 prerequisite for a release generation and
        record the verdict, idempotently.

        Three phases: (A) read-only evidence collection on the read-only
        connection, canonical manifest construction
        (manifest_hash = sha256(_canon(manifest))), and the deterministic
        blocker evaluation; (B) when no blockers remain, the release
        checkpoint is staged and verified through R5's own
        stage_checkpoint()/verify_checkpoint() (each in its own
        write_txn — never nested); (C) one authoritative write_txn that
        re-pins the desired head and CAS-updates the run row to READY
        (checkpoint staged+verified), BLOCKED (deterministic blockers), or
        FAILED (unreadable evidence — CORRUPT_AUTHORITY).

        No-op discipline: evaluating a FINALIZED run verifies the record
        and returns it with zero writes; re-evaluating with unchanged
        evidence performs zero writes (idempotent stage/verify, unchanged
        verdict). A lost CAS raises FinalizationConflict. Any unreadable
        evidence fails closed to FAILED, never to FINALIZED."""
        self._check_finalizer_actor(actor, "evaluate_finalization")
        if evidence_params:
            raise TransitionRejected(
                "evaluate_finalization: unknown evidence params"
                f" {sorted(evidence_params)}: no evidence params are"
                " defined")
        if (isinstance(expected_version, bool)
                or not isinstance(expected_version, int)
                or expected_version < 1):
            raise TransitionRejected(
                "evaluate_finalization: expected_version must be an int"
                f" >= 1, got {expected_version!r}")
        finalization_id = canonical_finalization_id(release_generation)
        try:
            row = dict(self._get(self.store.conn, "finalization_runs",
                                 "finalization_id", finalization_id))
        except TransitionRejected:
            raise TransitionRejected(
                "evaluate_finalization: unknown release generation"
                f" {release_generation!r}: call begin_finalization_run"
                " first") from None
        if row["state"] == "FINALIZED":
            return self._verify_finalized_record(self.store.conn, row)

        # ---- Phase A: evidence, manifest, blockers (read-only).
        desired_state_version = row["desired_state_version"]
        try:
            evidence = self._collect_finalization_evidence(
                self.store.conn, release_generation, desired_state_version)
        except _EvidenceFailure as ef:
            failed_blockers = [{"category": ef.category,
                                "identity": ef.identity,
                                "detail": ef.detail}]
            return self._record_evaluation_outcome(
                finalization_id, release_generation, expected_version,
                actor, blockers=failed_blockers,
                final_state=("FAILED"
                             if ef.category == "CORRUPT_AUTHORITY"
                             else "BLOCKED"),
                manifest={"release_generation": release_generation,
                          "desired_state_version": desired_state_version,
                          "desired_state_hash": None,
                          "evidence_error": failed_blockers[0],
                          "prerequisites": {"ok": False,
                                            "blockers": failed_blockers}},
                checkpoint_id=None)
        blockers = _evaluate_finalization_blockers(evidence)

        # ---- Phase B: READY path — stage + verify the release checkpoint
        # through R5's own ops (their own write_txns, never nested).
        checkpoint_id = evidence["release_checkpoint_id"]
        if not blockers:
            self._ensure_finalization_task(release_generation, actor)
            staged = self.stage_checkpoint(
                evidence["finalization_task_id"], actor=actor,
                manifest=evidence["release_checkpoint_entries"],
                trigger=_RELEASE_CHECKPOINT_TRIGGER,
                versions={"methodology_version": "r13-release-v1"})
            if staged["checkpoint_id"] != checkpoint_id:
                raise TransitionRejected(
                    "evaluate_finalization: release checkpoint identity"
                    " shifted under a pinned desired head (staged"
                    f" {staged['checkpoint_id']}, expected"
                    f" {checkpoint_id}) — failing closed")
            status = staged["verification_status"]
            if status == "UNVERIFIED":
                try:
                    status = self.verify_checkpoint(
                        checkpoint_id, actor=actor,
                        release=True)["verification_status"]
                except TransitionRejected as exc:
                    # Lost the verification race: a concurrent
                    # evaluator verified the checkpoint between our
                    # stage read and this call. The end state is
                    # authoritative — the checkpoint IS verified —
                    # so converge on it instead of failing; the
                    # phase-C CAS remains the verdict arbiter.
                    if "already VERIFIED" not in str(exc):
                        raise
                    status = "VERIFIED"
            if status != "VERIFIED":
                blockers = [{"category": "INVALID_CHECKPOINT",
                             "identity": checkpoint_id,
                             "detail": "release checkpoint is"
                                       f" {status} after verification"}]

        # ---- Phase C: the authoritative verdict write.
        manifest = build_finalization_manifest(evidence, blockers)
        final_state = "READY" if not blockers else "BLOCKED"
        return self._record_evaluation_outcome(
            finalization_id, release_generation, expected_version, actor,
            blockers=blockers, final_state=final_state, manifest=manifest,
            checkpoint_id=checkpoint_id)

    def _revalidate_release_checkpoint_record(self, conn,
                                              release_generation: str,
                                              run_row: dict) -> dict:
        """Revalidate the release checkpoint RECORD at publish time.

        The bytes were revalidated authoritatively when the checkpoint
        became VERIFIED (VERIFIED is terminal); what publish re-checks is
        the record: identity, generation binding, and the release
        attestation in the verification receipt. Raises TransitionRejected
        on any defect — publish fails closed."""
        checkpoint_id = run_row["checkpoint_id"]
        if not checkpoint_id:
            raise TransitionRejected(
                "publish_finalization: run"
                f" {release_generation} is READY with no release checkpoint"
                " — contradiction")
        ck = conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?",
            (checkpoint_id,)).fetchone()
        if ck is None:
            raise TransitionRejected(
                "publish_finalization: release checkpoint"
                f" {checkpoint_id} is missing — contradiction")
        ck = dict(ck)
        if ck["verification_status"] != "VERIFIED":
            raise TransitionRejected(
                "publish_finalization: release checkpoint"
                f" {checkpoint_id} is {ck['verification_status']}, not"
                " VERIFIED")
        if ck["task_id"] != _canonical_finalization_task_id(
                release_generation):
            raise TransitionRejected(
                "publish_finalization: release checkpoint"
                f" {checkpoint_id} is bound to a foreign task")
        canon = ck["canonical_manifest"] or ""
        if hashlib.sha256(canon.encode()).hexdigest() != checkpoint_id:
            raise TransitionRejected(
                "publish_finalization: release checkpoint identity"
                " mismatch (checkpoint_id != sha256(canonical_manifest))")
        try:
            receipt = json.loads(ck["verification_receipt"] or "null")
        except (ValueError, TypeError) as exc:
            raise TransitionRejected(
                "publish_finalization: release checkpoint verification"
                f" receipt unparseable: {exc}") from exc
        if (not isinstance(receipt, dict)
                or receipt.get("release") is not True
                or receipt.get("checkpoint_id") != checkpoint_id):
            raise TransitionRejected(
                "publish_finalization: release checkpoint"
                f" {checkpoint_id} carries no release attestation")
        return ck

    def publish_finalization(self, release_generation: str,
                             expected_version: int,
                             expected_manifest_hash: str,
                             actor: str) -> dict:
        """Publish a READY finalization: the atomic FINALIZED transition.

        Two-phase, in ONE write_txn: re-read the authoritative state —
        the run is READY, its manifest hash matches the evaluated one,
        the desired head is still at the pinned version (else
        STALE_GENERATION), the release checkpoint record revalidates
        (identity, generation binding, release attestation receipt), and
        the full prerequisite evaluation over current evidence is empty
        (no unresolved uncertainty, no active required work, no
        OPEN/HALF_OPEN breaker on relevant scopes, no open recovery, no
        human gates). Then the CAS publication: state FINALIZED, version
        bump, completed_at, result, one ledger event — atomically.

        An already-FINALIZED run is verified and returned idempotently
        (no overwrite, no duplicate ledger event). A lost CAS raises
        FinalizationConflict. Anything else that fails the revalidation
        raises TransitionRejected with zero mutation."""
        self._check_finalizer_actor(actor, "publish_finalization")
        if (isinstance(expected_version, bool)
                or not isinstance(expected_version, int)
                or expected_version < 1):
            raise TransitionRejected(
                "publish_finalization: expected_version must be an int"
                f" >= 1, got {expected_version!r}")
        if (not isinstance(expected_manifest_hash, str)
                or not expected_manifest_hash):
            raise TransitionRejected(
                "publish_finalization: expected_manifest_hash must be a"
                " non-empty string")
        finalization_id = canonical_finalization_id(release_generation)
        with self.store.write_txn() as (conn, now):
            try:
                row = dict(self._get(conn, "finalization_runs",
                                     "finalization_id", finalization_id))
            except TransitionRejected:
                raise TransitionRejected(
                    "publish_finalization: unknown release generation"
                    f" {release_generation!r}: call begin_finalization_run"
                    " first") from None
            if row["state"] == "FINALIZED":
                if row["manifest_hash"] != expected_manifest_hash:
                    raise TransitionRejected(
                        "publish_finalization:"
                        f" {release_generation} is FINALIZED with manifest"
                        f" {row['manifest_hash']}, not"
                        f" {expected_manifest_hash}: refusing to contradict"
                        " the published record")
                return self._verify_finalized_record(conn, row)
            if row["state"] != "READY":
                raise TransitionRejected(
                    "publish_finalization: run"
                    f" {release_generation} is {row['state']}, not READY")
            if row["manifest_hash"] != expected_manifest_hash:
                raise TransitionRejected(
                    "publish_finalization: manifest mismatch for"
                    f" {release_generation}: evaluated"
                    f" {row['manifest_hash']}, caller presented"
                    f" {expected_manifest_hash}")
            head = self._read_desired_head(conn)
            if head["version"] != row["desired_state_version"]:
                raise TransitionRejected(
                    "publish_finalization: STALE_GENERATION for"
                    f" {release_generation}: pinned"
                    f" v{row['desired_state_version']}, head now"
                    f" v{head['version']}")
            self._revalidate_release_checkpoint_record(
                conn, release_generation, row)
            try:
                evidence = self._collect_finalization_evidence(
                    conn, release_generation, row["desired_state_version"])
            except _EvidenceFailure as ef:
                raise TransitionRejected(
                    "publish_finalization: evidence unreadable at publish"
                    f" ({ef.category} {ef.identity}: {ef.detail})") from ef
            blockers = _evaluate_finalization_blockers(evidence)
            if blockers:
                raise TransitionRejected(
                    "publish_finalization:"
                    f" {release_generation} is no longer releasable:"
                    f" {_canon(blockers)}")
            if evidence["release_checkpoint_id"] != row["checkpoint_id"]:
                raise TransitionRejected(
                    "publish_finalization: release checkpoint identity"
                    " shifted under a pinned desired head — failing closed")
            manifest_hash = finalization_manifest_hash(
                build_finalization_manifest(evidence, blockers))
            if manifest_hash != row["manifest_hash"]:
                raise TransitionRejected(
                    "publish_finalization: manifest drifted since"
                    f" evaluation ({row['manifest_hash']} ->"
                    f" {manifest_hash})")
            cur = conn.execute(
                "UPDATE finalization_runs SET state='FINALIZED',"
                " version=version+1, completed_at=?, result='FINALIZED',"
                " updated_at=? WHERE finalization_id=? AND version=?"
                " AND state='READY'",
                (now, now, finalization_id, expected_version))
            if cur.rowcount != 1:
                # Lost the race: distinguish "the winner finalized" (the
                # idempotent path) from a true version conflict.
                rerow = dict(self._get(conn, "finalization_runs",
                                       "finalization_id", finalization_id))
                if rerow["state"] == "FINALIZED":
                    if rerow["manifest_hash"] != expected_manifest_hash:
                        raise TransitionRejected(
                            "publish_finalization:"
                            f" {release_generation} was FINALIZED with a"
                            " different manifest: refusing to contradict"
                            " the published record")
                    return self._verify_finalized_record(conn, rerow)
                raise FinalizationConflict(
                    "finalization CAS lost for"
                    f" {release_generation}: expected version"
                    f" {expected_version}")
            self._append_event(
                conn, now, "finalization.published",
                {"finalization_id": finalization_id,
                 "release_generation": release_generation,
                 "manifest_hash": manifest_hash,
                 "checkpoint_id": row["checkpoint_id"],
                 "desired_state_version":
                     row["desired_state_version"]}, actor)
            return dict(self._get(conn, "finalization_runs",
                                  "finalization_id", finalization_id))


# ------------------------------------------------------- R13 pure helpers
# Canonical identities, the release manifest, and the prerequisite
# evaluator. Pure (no store access): the gate gathers evidence, these
# judge and hash it. exec/finalizer.py re-exports them — there is exactly
# one definition.

_FINALIZATION_STATES = ("OPEN", "EVALUATING", "READY", "BLOCKED", "FAILED",
                        "FINALIZED")

_FINALIZATION_BLOCKER_CATEGORIES = frozenset({
    "DESIRED_WORK_UNSATISFIED", "ACTIVE_EXECUTION", "UNCERTAIN_EXECUTION",
    "ACTIVE_RECOVERY", "OPEN_BREAKER", "HUMAN_GATE", "MISSING_ARTIFACT",
    "INVALID_ARTIFACT", "INVALID_CHECKPOINT", "STALE_GENERATION",
    "CONTRADICTORY_STATE", "CORRUPT_AUTHORITY",
})

# R5 checkpoint trigger for a release checkpoint (contract D8): the
# checkpoint precedes the risky operation — publication of the release.
_RELEASE_CHECKPOINT_TRIGGER = "pre_risky_operation"


class _EvidenceFailure(Exception):
    """Internal: finalization evidence collection hit unreadable
    (CORRUPT_AUTHORITY) or self-contradictory (CONTRADICTORY_STATE)
    durable state. Carries the blocker triple; never escapes the R13 gate
    ops (evaluate converts it to a FAILED/BLOCKED verdict, publish to a
    TransitionRejected)."""
    def __init__(self, category: str, identity: str, detail: str):
        super().__init__(f"{category} {identity}: {detail}")
        self.category = category
        self.identity = identity
        self.detail = detail


def canonical_release_generation(desired_state_version: int) -> str:
    """Canonical release-generation identity: "ds-v" + the desired-state
    version it releases. The generation IS the desired-state version — a
    desired-state change (head version bump) automatically names a NEW
    generation, so a finalized generation can never be silently mutated by
    later desired-state edits."""
    if (isinstance(desired_state_version, bool)
            or not isinstance(desired_state_version, int)
            or desired_state_version < 0):
        raise TransitionRejected(
            "desired_state_version must be an int >= 0,"
            f" got {desired_state_version!r}")
    return "ds-v" + str(desired_state_version)


def canonical_finalization_id(release_generation: str) -> str:
    """Deterministic finalization-run identity (the R11 canonical pattern:
    domain tag + sha256, 32 hex chars). The same generation always names
    the same run — re-begins are idempotent, never duplicates."""
    if not isinstance(release_generation, str) or not release_generation:
        raise TransitionRejected(
            "release_generation must be a non-empty string,"
            f" got {release_generation!r}")
    return ("fin-" + hashlib.sha256(
        ("axos-finalization:v1:" + release_generation).encode()
    ).hexdigest()[:32])


def _canonical_finalization_task_id(release_generation: str) -> str:
    """Deterministic synthetic container task for a generation's release
    checkpoint. The "task-fin-" infix keeps it disjoint from random
    "task-<uuid>" ids."""
    return ("task-fin-" + hashlib.sha256(
        ("axos-finalization:v1:" + release_generation).encode()
    ).hexdigest()[:32])


def _release_checkpoint_entries(artifacts: dict) -> list[dict]:
    """Pure: the R5 checkpoint manifest entries for a release — one
    {artifact_id, content_hash, job_id} per generation artifact, filtered
    and sorted EXACTLY as stage_checkpoint() filters them, so this pure
    identity always equals the checkpoint_id stage_checkpoint() mints for
    the same entries (no second identity scheme)."""
    entries = [{"artifact_id": aid,
                "content_hash": artifacts[aid]["content_hash"],
                "job_id": artifacts[aid]["job_id"]}
               for aid in sorted(artifacts)]
    entries = [{k: e[k] for k in ("artifact_id", "content_hash", "size",
                                  "kind", "job_id") if k in e}
               for e in entries]
    entries.sort(key=lambda d: d["artifact_id"])
    return entries


def _release_checkpoint_id(entries: list[dict]) -> str:
    """Pure: the R5 checkpoint identity — sha256 of the canonical
    manifest, exactly as stage_checkpoint() computes it."""
    return hashlib.sha256(_canon(entries).encode()).hexdigest()


def build_finalization_manifest(evidence: dict, blockers: list) -> dict:
    """Pure: the canonical release manifest over collected evidence.

    NO nondeterministic data inside the hashed content — no uuids, pids,
    or wall-clock timestamps (timestamps live on the finalization_runs
    row, OUTSIDE the hashed manifest). Every collection is sorted, so
    equivalent evidence serializes identically."""
    artifacts = evidence["artifacts"]
    return {
        "release_generation": evidence["release_generation"],
        "desired_state_version": evidence["desired_state_version"],
        "desired_state_hash": evidence["desired_state_hash"],
        "desired_items": sorted(
            ({"id": it["id"], "spec": it["spec"],
              "version": it["version"]}
             for it in evidence["desired_items"]),
            key=lambda d: d["id"]),
        "desired_job_map": {wid: evidence["desired_job_map"][wid]
                            for wid in sorted(evidence["desired_job_map"])},
        "actual_state": {jid: evidence["jobs"][jid]["status"]
                         for jid in sorted(evidence["jobs"])},
        "artifacts": {aid: artifacts[aid]["content_hash"]
                      for aid in sorted(artifacts)},
        "artifact_detail": {
            aid: {"job_id": artifacts[aid]["job_id"],
                  "status": artifacts[aid]["status"],
                  "size": artifacts[aid]["size"],
                  "kind": artifacts[aid]["kind"],
                  "bytes_present": artifacts[aid]["bytes_present"],
                  "bytes_hash": artifacts[aid]["bytes_hash"]}
            for aid in sorted(artifacts)},
        "validators": evidence["validations"],
        "checkpoints": {
            "release": {
                "checkpoint_id": evidence["release_checkpoint_id"],
                "task_id": evidence["finalization_task_id"],
                "trigger": _RELEASE_CHECKPOINT_TRIGGER,
            }},
        "recovery_summary": {
            "open_incidents": [i["incident_id"]
                               for i in evidence["open_incidents"]],
            "open_attempts": {
                aid: evidence["open_attempts"][aid]["attempt_state"]
                for aid in sorted(evidence["open_attempts"])},
            "escalated_unresolved": [
                e["incident_id"] for e in evidence["escalated"]
                if e["policy_terminal_state"] is None],
            "unconsumed_outcomes": list(evidence["unconsumed_outcomes"]),
        },
        "resilience_summary": {"breakers": evidence["breakers"]},
        "prerequisites": {"ok": not blockers, "blockers": list(blockers)},
    }


def finalization_manifest_hash(manifest: dict) -> str:
    """Pure: sha256 of the canonical manifest JSON (R5 hashing
    discipline)."""
    return hashlib.sha256(_canon(manifest).encode()).hexdigest()


def _evaluate_finalization_blockers(evidence: dict) -> list[dict]:
    """Pure: every R13 prerequisite over collected evidence → the
    deterministic blocker list, deduped and sorted by (category,
    identity). Empty means the generation is releasable."""
    blockers: list[dict] = []
    seen: set[tuple[str, str]] = set()

    def add(category: str, identity: str, detail: str) -> None:
        if category not in _FINALIZATION_BLOCKER_CATEGORIES:
            raise AssertionError(
                f"unknown finalization blocker category {category!r}")
        key = (category, str(identity))
        if key in seen:
            return
        seen.add(key)
        blockers.append({"category": category, "identity": key[1],
                         "detail": detail})

    generation = evidence["release_generation"]
    desired_state_version = evidence["desired_state_version"]
    head = evidence["head"]
    items = evidence["desired_items"]
    dmap = evidence["desired_job_map"]
    jobs = evidence["jobs"]
    tasks = evidence["tasks"]
    artifacts = evidence["artifacts"]
    validations = evidence["validations"]

    # -- generation currency
    if head["version"] != desired_state_version:
        add("STALE_GENERATION", generation,
            f"desired-state head is v{head['version']}, not the pinned"
            f" v{desired_state_version}: this generation is stale")

    if not items:
        add("DESIRED_WORK_UNSATISFIED", generation,
            "no non-retired desired-work items: nothing to release")

    gen_job_ids: list[str] = []
    for it in items:
        wid = it["id"]
        jid = dmap.get(wid)
        if not jid:
            add("DESIRED_WORK_UNSATISFIED", wid,
                "no job materialized for desired-work item")
            continue
        gen_job_ids.append(jid)
        job = jobs.get(jid)
        if job is None:
            add("CONTRADICTORY_STATE", wid,
                f"desired_job_map binds {wid} -> {jid} but the job row"
                " is missing")
            continue
        status = job["status"]
        if status != "COMPLETE":
            add("DESIRED_WORK_UNSATISFIED", jid,
                f"mapped job is {status}, not COMPLETE")
        if status == "BLOCKED":
            add("HUMAN_GATE", jid,
                "job is BLOCKED: a human decision is required; the"
                " finalizer never clears human gates")
        task = tasks.get(job["task_id"])
        if task is None:
            add("CONTRADICTORY_STATE", jid,
                f"job references missing task row {job['task_id']!r}")
        elif task["status"] in ("PAUSED_FOR_HUMAN", "MANUAL_REVIEW"):
            add("HUMAN_GATE", job["task_id"],
                f"task is {task['status']}: a human decision is required;"
                " the finalizer never clears human gates")
        if status == "COMPLETE":
            aid = job["result_artifact_id"]
            expected = job["content_hash"] or aid
            art = artifacts.get(aid) if aid else None
            if not aid or art is None:
                add("MISSING_ARTIFACT", jid,
                    "COMPLETE job has no recorded artifact"
                    f" (result_artifact_id={aid!r})")
            else:
                problems = []
                if art["job_id"] != jid:
                    problems.append(
                        f"artifact staged for job {art['job_id']!r},"
                        f" not {jid!r}")
                if art["content_hash"] != expected:
                    problems.append(
                        "recorded content hash does not match the job's")
                if art["status"] not in ("VALIDATED", "RELEASED"):
                    problems.append(
                        f"artifact status {art['status']}, not VERIFIED")
                if not art["bytes_present"]:
                    problems.append("artifact bytes missing")
                elif art["bytes_hash"] != expected:
                    problems.append(
                        f"bytes re-hash to {art['bytes_hash']}, expected"
                        f" {expected}")
                for vid, ver in REQUIRED_VALIDATORS:
                    rec = next(
                        (v for v in validations
                         if v["artifact_id"] == aid
                         and v["validator_id"] == vid
                         and v["validator_version"] == ver), None)
                    if (rec is None or rec["result"] != "PASS"
                            or (rec["content_hash"] or "") != expected):
                        problems.append(
                            f"missing PASS receipt {vid}/{ver} for the"
                            " content hash")
                if problems:
                    add("INVALID_ARTIFACT", aid, "; ".join(problems))

    # -- execution quiescence (global: a release certifies the store at
    # rest — any job still executing anywhere is active execution)
    for jid in sorted(jobs):
        status = jobs[jid]["status"]
        if status in ("CLAIMED", "RUNNING", "COMMITTING", "VERIFYING"):
            add("ACTIVE_EXECUTION", jid,
                f"job is {status}: execution still in flight")
        elif status == "UNCERTAIN":
            add("UNCERTAIN_EXECUTION", jid,
                "job is UNCERTAIN: completion state unknown")
    for sp in evidence["unreaped_spawns"]:
        add("ACTIVE_EXECUTION",
            f"spawn:{sp['worker_id']}:{sp['proc_id']}",
            "unreaped worker.proc_spawned with no proc_reaped"
            f" (job {sp['job_id']!r}): possibly-live execution not yet"
            " reconciled")

    # -- recovery quiescence
    for inc in evidence["open_incidents"]:
        add("ACTIVE_RECOVERY", inc["incident_id"],
            f"open {inc['failure_class']} incident")
    for aid in sorted(evidence["open_attempts"]):
        a = evidence["open_attempts"][aid]
        if a["attempt_state"] in ("RUNNING", "VERIFYING", "UNCERTAIN"):
            add("ACTIVE_RECOVERY", aid,
                f"recovery attempt is {a['attempt_state']}"
                f" (incident {a['incident_id']})")
        if a["attempt_state"] == "UNCERTAIN":
            add("UNCERTAIN_EXECUTION", aid,
                "recovery attempt is UNCERTAIN"
                f" (incident {a['incident_id']})")
    for e in evidence["escalated"]:
        if e["policy_terminal_state"] is None:
            add("ACTIVE_RECOVERY", e["incident_id"],
                "escalated incident with no terminal R9 policy state")
    for iid in evidence["unconsumed_outcomes"]:
        add("ACTIVE_RECOVERY", iid,
            "incident has an outcome but its R9 policy row is not terminal")

    # -- breakers: only GLOBAL and scopes covering this generation's
    # jobs/tasks block; a breaker on an unrelated scope does not
    relevant = {("GLOBAL", "global")}
    for it in items:
        relevant.add(("DESIRED", it["id"]))
    for jid in gen_job_ids:
        relevant.add(("JOB", jid))
        tid = jobs.get(jid, {}).get("task_id")
        if tid:
            relevant.add(("TASK", tid))
    relevant.add(("TASK", evidence["finalization_task_id"]))
    for b in evidence["breakers"]:
        if (b["scope_type"], b["scope_id"]) not in relevant:
            continue
        verdict = _classify_breaker_row(b)
        if verdict in ("open", "half_open"):
            add("OPEN_BREAKER", f"{b['scope_type']}:{b['scope_id']}",
                f"breaker is {b['state']}")
        elif verdict == "corrupt":
            add("CORRUPT_AUTHORITY",
                f"breaker:{b['scope_type']}:{b['scope_id']}",
                "breaker row is corrupt (unreadable state/version)")

    # -- the release checkpoint record, when one already exists
    ck = evidence["checkpoint"]
    if ck is not None:
        cid = ck["checkpoint_id"]
        if ck["task_id"] != evidence["finalization_task_id"]:
            add("CONTRADICTORY_STATE", cid,
                "release checkpoint is bound to a foreign task")
        canon = ck["canonical_manifest"] or ""
        if hashlib.sha256(canon.encode()).hexdigest() != cid:
            add("CONTRADICTORY_STATE", cid,
                "checkpoint_id != sha256(canonical_manifest)")
        if ck["verification_status"] == "CORRUPT":
            add("INVALID_CHECKPOINT", cid,
                "release checkpoint is CORRUPT (terminal)")
        elif ck["verification_status"] == "VERIFIED":
            receipt = ck.get("receipt") or {}
            if (not isinstance(receipt, dict)
                    or receipt.get("release") is not True
                    or receipt.get("checkpoint_id") != cid):
                add("INVALID_CHECKPOINT", cid,
                    "VERIFIED without a release attestation receipt")
        # UNVERIFIED: a previous evaluation staged but did not verify —
        # this evaluation verifies it; not a blocker.

    blockers.sort(key=lambda b: (b["category"], b["identity"]))
    return blockers
