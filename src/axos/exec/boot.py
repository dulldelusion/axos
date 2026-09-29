"""Phase 1C R6 — durable boot recovery & runtime reconstruction.

Answers: "the runtime disappeared — what does durable state say actually
happened, and what runtime state must be reconstructed?"

Runtime memory is disposable; durable state is authoritative. After a
supervisor/process restart this module reconstructs the authoritative
baseline from durable evidence ONLY:

  1. open authoritative store (already opened by the Supervisor)
  2. validate/recover database state (integrity_check + ledger chain)
  3. reconstruct durable execution records (jobs/workers/leases/spawns)
  4. inspect persisted process-spawn evidence (unreaped worker.proc_spawned)
  5. verify live PID/process identity (pid + start-time + session-leader)
  6. detect stale/orphaned runtime records -> explicit dispositions
  7. reconcile ownership/leases via the EXISTING R4/R1 primitives
     (R6 never reimplements reclaim)
  8. fence stale survivors through the EXISTING R2 supervisor sweep
     (R6 never kills a process itself)
  9. reconstruct supervisor runtime tracking (_adopted)
 10. expose the recovered state (the returned report)
 11. only then permit new execution scheduling (READY phase gate)

Dispositions (exact Phase 1C terminology where it exists):
  ADOPT        process identity matches AND durable authority is current
  FENCE        process alive but durable authority is stale -> R2 enforces
  ALREADY_DEAD process confirmed absent; durable recovery continues
  PID_REUSE    pid exists but process identity differs; never kill it
  ORPHAN       runtime evidence irreconcilable with durable ownership;
               surfaced, never guessed at, never killed
  UNCERTAIN    fail-closed: authority could not be read; do not touch

Authority boundaries (audited by A12):
  - R6 DETECTS; R4 evaluates expiry; R1 performs reclaim; R2 performs
    physical fencing. This module performs no transaction of its own, no
    SQL writes, no process signalling (os.kill appears only as the
    signal-0 existence probe, which sends no signal), no Popen handle
    management. Every durable mutation goes through TransitionGate
    methods (append_event, reclaim_lease, create_incident).
  - R5's artifact contract is read-only here: inspect_uncertain_completion
    and latest_known_good are observations; boot never completes a job,
    never advances latest-known-good, never manufactures artifacts.
"""
from __future__ import annotations

import os
import sqlite3
import uuid
from dataclasses import dataclass, field

from ..store import TransitionRejected, LeaseError, StoreError

# ---------------------------------------------------------------- constants

BOOT_PHASES = ("STARTING", "RECOVERING", "READY", "BLOCKED")

DISPOSITIONS = ("ADOPT", "FENCE", "ALREADY_DEAD", "PID_REUSE", "ORPHAN",
                "UNCERTAIN")

# Job states in which a live owner process may legitimately keep executing.
_ACTIVE_OWNER_STATES = ("CLAIMED", "RUNNING", "COMMITTING")

# Worker states that can still hold authority. DEAD/RETIRED are terminal.
_TERMINAL_WORKER_STATES = ("DEAD", "RETIRED")

# Legacy fallback slack (seconds) for spawn records that predate the
# durable start-identity field. Matches the R2 adoption rule.
_LEGACY_START_SLACK_S = 60.0


# ------------------------------------------------------- process identity
# Small module-level functions (monkeypatchable in tests) implementing the
# PID-reuse guard: a PID match alone NEVER establishes process identity.

def proc_start_jiffies(pid: int) -> int | None:
    """Kernel start-time identity of a process (jiffies since boot).

    Fixed at fork; identical for every read of the same process, different
    for any later process reusing the PID. None when unreadable.
    """
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            after_comm = f.read().rsplit(")", 1)[1].split()
        return int(after_comm[19])  # field 22 (starttime)
    except (OSError, IndexError, ValueError):
        return None


def proc_start_wall(pid: int) -> float | None:
    """Wall-clock start time of a process, via /proc (Linux)."""
    j = proc_start_jiffies(pid)
    if j is None:
        return None
    try:
        with open("/proc/stat", "r") as f:
            for line in f:
                if line.startswith("btime"):
                    btime = int(line.split()[1])
                    break
            else:
                return None
    except OSError:
        return None
    hz = os.sysconf("SC_CLK_TCK")
    return btime + j / hz


def proc_state(pid: int) -> str | None:
    """Single-letter process state from /proc, or None when unreadable."""
    try:
        with open(f"/proc/{pid}/stat", "r") as f:
            return f.read().rsplit(")", 1)[1].split()[0]
    except (OSError, IndexError):
        return None


@dataclass
class _Classification:
    """Result of verifying one persisted spawn record against the OS."""
    kind: str  # "dead" | "live_match" | "reuse" | "unverifiable"
    pid: int | None
    observed_start_jiffies: int | None = None
    observed_pgid: int | None = None
    detail: str = ""


def _classify_spawn(spawn: dict) -> _Classification:
    """Verify a persisted spawn record against current OS process state.

    Never invents liveness: a missing reap is not proof of life, and a PID
    match without matching start identity is PID reuse, not identity.
    """
    pid = spawn.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return _Classification("unverifiable", pid,
                               detail="no pid in spawn evidence")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return _Classification("dead", pid, detail="kill(pid,0) -> ESRCH")
    except PermissionError:
        return _Classification("unverifiable", pid,
                               detail="kill(pid,0) -> EPERM")
    state = proc_state(pid)
    if state in ("Z", "X", "x"):
        return _Classification("dead", pid,
                               detail=f"process state {state} (not live)")
    if state is None:
        return _Classification("unverifiable", pid,
                               detail="process state unreadable")
    try:
        pgid = os.getpgid(pid)
    except (ProcessLookupError, PermissionError):
        return _Classification("unverifiable", pid,
                               detail="pgid unreadable")
    # AXOS workers are spawned with start_new_session=True: they are
    # session leaders, so pgid == pid. Anything else cannot be our process.
    if pgid != pid:
        return _Classification("reuse", pid, observed_pgid=pgid,
                               detail=f"pgid {pgid} != pid (not a session"
                                      " leader: cannot be the spawned worker)")
    start = proc_start_jiffies(pid)
    durable = spawn.get("start_jiffies")
    if isinstance(durable, int) and isinstance(start, int):
        if start != durable:
            return _Classification(
                "reuse", pid, observed_start_jiffies=start,
                observed_pgid=pgid,
                detail=f"start identity differs: durable={durable},"
                       f" observed={start}")
        return _Classification("live_match", pid,
                               observed_start_jiffies=start,
                               observed_pgid=pgid)
    # Legacy spawn records predate the durable start-identity field:
    # fall back to the R2 wall-clock slack rule.
    wall = proc_start_wall(pid)
    if wall is None:
        return _Classification("unverifiable", pid,
                               detail="start time unreadable")
    if wall > spawn["spawn_ts"] + _LEGACY_START_SLACK_S:
        return _Classification("reuse", pid, observed_pgid=pgid,
                               detail="process started after the durable"
                                      " spawn milestone + slack")
    return _Classification("live_match", pid,
                           observed_start_jiffies=start,
                           observed_pgid=pgid)


# ------------------------------------------------------------- boot driver

def _new_boot_id() -> str:
    return "boot-" + uuid.uuid4().hex[:16]


def _lease_live(job: dict, now: float) -> bool:
    """Mirror of the R4 predicate's liveness half: expired is
    lease_expires_at <= now (boundary inclusive)."""
    exp = job.get("lease_expires_at")
    return exp is not None and exp > now


def boot_recover(supervisor, *, actor: str = "system",
                 boot_id: str | None = None) -> dict:
    """Run the deterministic boot recovery pass for a (re)started supervisor.

    `supervisor` supplies the store/gate handles, the durable-adoption
    registry, the R2 revocation reader, and the fence sweep. Returns the
    boot report; also stored on the supervisor as _boot_report. The
    supervisor sets its execution-readiness phase from report["phase"].

    The pass is idempotent: re-running it observes the durable results of
    the previous pass and converges to the same runtime state.
    """
    gate = supervisor.gate
    store = supervisor.store
    boot_id = boot_id or _new_boot_id()
    report: dict = {
        "boot_id": boot_id,
        "phase": "STARTING",
        "supervisor_actor": supervisor.actor,
        "dispositions": [],
        "leases_reclaimed": 0,
        "leases_reclaimed_jobs": [],
        "forced_reclaims": 0,
        "complete_jobs_checked": 0,
        "complete_jobs_consistent": 0,
        "checkpoints_checked": 0,
        "integrity_contradictions": [],
        "uncertain_inspected": [],
        "unresolved": [],
        "errors": [],
        "fence_sweep": {},
        "fully_recovered": False,
    }

    def _journal(event_type: str, payload: dict) -> None:
        payload = {"boot_id": boot_id, **payload}
        gate.append_event(event_type, payload, actor)

    now = store.current_time()
    _journal("boot.started", {"phase": "STARTING",
                              "supervisor_actor": supervisor.actor,
                              "at": now})
    report["phase"] = "RECOVERING"

    # -- step 2: validate/recover database state. Refuse to start on
    # corruption (alert via the journal, do not schedule).
    ok, detail = store.integrity_check()
    if not ok:
        return _blocked(report, _journal,
                        reason="store_integrity_failed", detail=detail)
    ok, detail = gate.verify_ledger_chain()
    if not ok:
        return _blocked(report, _journal,
                        reason="ledger_chain_broken", detail=detail)

    # -- step 3/4: durable execution records + persisted spawn evidence.
    # Identity classification performs NO mutation.
    try:
        spawns = gate.unreaped_proc_spawns()
    except (StoreError, sqlite3.Error) as e:
        return _blocked(report, _journal, reason="durable_read_failed",
                        detail=f"unreaped_proc_spawns: {e}")
    classified = []
    for sp in spawns:
        try:
            classified.append((sp, _classify_spawn(sp)))
        except Exception as e:  # one bad record never aborts boot
            report["errors"].append(
                {"spawn": {"worker_id": sp.get("worker_id"),
                           "proc_id": sp.get("proc_id")},
                 "error": f"{type(e).__name__}: {e}"})

    # -- step 7a: R4 evidence -> R1 routine reclaim for expired leases.
    # R4 is read-only; R1 is the ONLY mutation path for an expired lease.
    try:
        expired = gate.observe_expired_leases()
    except (StoreError, sqlite3.Error) as e:
        return _blocked(report, _journal, reason="durable_read_failed",
                        detail=f"observe_expired_leases: {e}")
    for ev in expired:
        try:
            gate.reclaim_lease(
                ev["job_id"], actor="system",
                reason=("boot: R4 observed expired lease"
                        f" (observed_at={ev['observed_at']})"),
                expected_owner=ev["owner_worker_id"],
                expected_token=ev["fencing_token"])
            report["leases_reclaimed"] += 1
            report["leases_reclaimed_jobs"].append(ev["job_id"])
        except (TransitionRejected, LeaseError):
            # Already resolved (ownerless / token moved / renewed): R1 is
            # idempotent-by-rejection, so there is nothing to do.
            pass
        except (StoreError, sqlite3.Error) as e:
            report["errors"].append(
                {"job_id": ev["job_id"],
                 "error": f"reclaim failed: {type(e).__name__}: {e}"})

    # -- step 7b: live lease + confirmed-dead owner -> forced R1 reclaim
    # with supervisor-observed-death evidence (the D1 forced tier allows
    # exactly this evidence shape). CLAIMED -> PENDING, RUNNING/COMMITTING
    # -> UNCERTAIN: the durable condition the later resolver needs.
    # A missing process is NOT completion, NOT a retry — it is UNCERTAIN.
    try:
        owned = gate.owned_active_jobs()
    except (StoreError, sqlite3.Error) as e:
        return _blocked(report, _journal, reason="durable_read_failed",
                        detail=f"owned_active_jobs: {e}")
    now = store.current_time()
    latest_spawn: dict[tuple, dict] = {}
    cls_by_key: dict[tuple, _Classification] = {}
    for sp, cls in classified:
        key = (sp.get("worker_id"), sp.get("proc_id"))
        cls_by_key[key] = cls
        prev = latest_spawn.get((sp.get("worker_id"), sp.get("job_id")))
        if prev is None or sp["spawn_ts"] > prev["spawn_ts"]:
            latest_spawn[(sp.get("worker_id"), sp.get("job_id"))] = sp
    for job in owned:
        if not _lease_live(job, now):
            # Expired: handled by the R4/R1 pass above. If one survived,
            # that is itself a contradiction worth surfacing.
            report["unresolved"].append(
                {"kind": "expired_lease_not_reclaimed",
                 "job_id": job["job_id"],
                 "detail": "R4 observed expiry but R1 did not reclaim"})
            continue
        owner = job["owner_worker_id"]
        sp = latest_spawn.get((owner, job["job_id"]))
        if sp is None:
            # No spawn evidence for the owner: the process state is UNKNOWN,
            # not observed-dead. Do not guess — the live lease stands until
            # it expires, then the routine path reclaims it.
            continue
        cls = cls_by_key.get((sp.get("worker_id"), sp.get("proc_id")))
        if cls is None or cls.kind not in ("dead", "reuse"):
            continue  # owner process live (or unverifiable): leave it
        incident = gate.create_incident(
            None, scope="boot", failure_class="owner_process_dead",
            stage_id=None, capability_version=None, input_batch_id=None,
            error_class="boot_observed_death", actor="system",
            task_id=job.get("task_id"),
            detection={"boot_id": boot_id, "job_id": job["job_id"],
                       "owner": owner,
                       "spawn_disposition": ("ALREADY_DEAD"
                                             if cls.kind == "dead"
                                             else "PID_REUSE"),
                       "evidence": "supervisor-observed process death at"
                                   " boot (D1 forced-tier evidence)"})
        try:
            gate.reclaim_lease(
                job["job_id"], actor="system", force=True, verdict="DEAD",
                incident_id=incident["incident_id"],
                reason=("boot: owner process confirmed dead with a live"
                        " lease; surrendering authority per D1"),
                expected_owner=owner,
                expected_token=job["fencing_token"])
            report["forced_reclaims"] += 1
            report["leases_reclaimed"] += 1
            report["leases_reclaimed_jobs"].append(job["job_id"])
        except (TransitionRejected, LeaseError, StoreError,
                sqlite3.Error) as e:
            report["unresolved"].append(
                {"kind": "forced_reclaim_failed",
                 "job_id": job["job_id"],
                 "error": f"{type(e).__name__}: {e}"})

    # -- step 6/8: explicit disposition for every persisted runtime record.
    for sp, cls in classified:
        try:
            disp = _decide_disposition(supervisor, gate, sp, cls, _journal,
                                       report, store.current_time())
        except Exception as e:  # never let one record abort boot
            disp = {"worker_id": sp.get("worker_id"),
                    "proc_id": sp.get("proc_id"),
                    "job_id": sp.get("job_id"), "pid": sp.get("pid"),
                    "disposition": "UNCERTAIN",
                    "detail": f"disposition error: {type(e).__name__}: {e}"}
            report["errors"].append(
                {"spawn": {"worker_id": sp.get("worker_id"),
                           "proc_id": sp.get("proc_id")},
                 "error": disp["detail"]})
            report["unresolved"].append(
                {"kind": "disposition_failed",
                 "worker_id": sp.get("worker_id"),
                 "proc_id": sp.get("proc_id"),
                 "detail": disp["detail"]})
        report["dispositions"].append(disp)
        _register_disposition(supervisor, sp, disp)

    _journal("boot.reconstructed", {
        "phase": "RECOVERING",
        "spawns_seen": len(classified),
        "disposition_counts": _count_dispositions(report["dispositions"]),
        "leases_reclaimed": report["leases_reclaimed"],
        "forced_reclaims": report["forced_reclaims"],
    })

    # -- step 10 (R5 composition): COMPLETE / COMMITTING / UNCERTAIN /
    # checkpoints are OBSERVED through the R5 read-only inspection — boot
    # never completes, never advances latest-known-good, never manufactures.
    _inspect_completion_state(gate, _journal, report)

    # -- step 8 (physical): fence stale survivors through the EXISTING R2
    # supervisor sweep. R6 detects; R2 terminates. No new kill path.
    try:
        sweep = supervisor.fence_sweep()
        report["fence_sweep"] = {
            "checked": sweep.get("checked", 0),
            "fenced_killed": len(sweep.get("fenced_killed", [])),
            "already_dead": len(sweep.get("already_dead", [])),
            "observation_failures": len(
                sweep.get("observation_failures", [])),
            "adopted_pruned": sweep.get("adopted_pruned", 0),
            "errors": sweep.get("errors", []),
        }
    except Exception as e:
        report["errors"].append(
            {"phase": "fence_sweep", "error": f"{type(e).__name__}: {e}"})

    # -- step 10/11: expose recovered state; READY means the authoritative
    # recovery pass completed — not that everything is fully recovered.
    report["fully_recovered"] = (not report["unresolved"]
                                 and not report["integrity_contradictions"]
                                 and not report["errors"])
    report["phase"] = "READY"
    _journal("boot.completed", {
        "phase": "READY",
        "disposition_counts": _count_dispositions(report["dispositions"]),
        "leases_reclaimed": report["leases_reclaimed"],
        "leases_reclaimed_jobs": report["leases_reclaimed_jobs"],
        "forced_reclaims": report["forced_reclaims"],
        "complete_jobs_checked": report["complete_jobs_checked"],
        "complete_jobs_consistent": report["complete_jobs_consistent"],
        "checkpoints_checked": report["checkpoints_checked"],
        "integrity_contradictions": len(report["integrity_contradictions"]),
        "unresolved": len(report["unresolved"]),
        "fully_recovered": report["fully_recovered"],
    })
    if not report["fully_recovered"]:
        # Factual: the pass completed but authoritative contradictions
        # remain for the later reconciler/recovery layers.
        _journal("boot.recovery_blocked", {
            "unresolved": report["unresolved"],
            "integrity_contradictions": report["integrity_contradictions"],
            "errors": report["errors"],
        })
    return report


def _blocked(report: dict, journal, *, reason: str, detail: str) -> dict:
    """Refuse to start: record the block, do NOT emit boot.completed, do
    NOT permit scheduling (phase stays BLOCKED)."""
    report["phase"] = "BLOCKED"
    report["blocked_reason"] = reason
    report["blocked_detail"] = detail
    report["fully_recovered"] = False
    journal("boot.recovery_blocked",
            {"reason": reason, "detail": detail, "phase": "BLOCKED"})
    return report


def _count_dispositions(dispositions: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for d in dispositions:
        k = d.get("disposition", "?")
        counts[k] = counts.get(k, 0) + 1
    return counts


def _decide_disposition(supervisor, gate, spawn: dict, cls: _Classification,
                        journal, report: dict, now: float) -> dict:
    """One persisted runtime record -> one explicit disposition.

    ADOPT only when process identity matches AND durable job authority is
    current (owner, token lineage, live lease, active state). Everything
    else is explicit: FENCE (stale but attributable -> R2 enforces),
    ALREADY_DEAD, PID_REUSE (never kill), ORPHAN (surface, never guess),
    UNCERTAIN (fail closed on unreadable authority).
    """
    wid = spawn.get("worker_id")
    proc_id = spawn.get("proc_id")
    job_id = spawn.get("job_id")
    pid = spawn.get("pid")
    base = {"worker_id": wid, "proc_id": proc_id, "job_id": job_id,
            "pid": pid}

    if cls.kind == "dead":
        journal("boot.process_already_dead", {**base, "detail": cls.detail})
        return {**base, "disposition": "ALREADY_DEAD", "detail": cls.detail}

    if cls.kind == "reuse":
        journal("boot.pid_reuse_detected", {
            **base,
            "durable_start_jiffies": spawn.get("start_jiffies"),
            "observed_start_jiffies": cls.observed_start_jiffies,
            "observed_pgid": cls.observed_pgid,
            "detail": cls.detail,
        })
        # The old runtime record is stale; the unrelated current process
        # is never signalled.
        return {**base, "disposition": "PID_REUSE", "detail": cls.detail}

    if cls.kind == "unverifiable":
        journal("boot.runtime_orphaned",
                {**base, "reason": "process_identity_unverifiable",
                 "detail": cls.detail})
        report["unresolved"].append(
            {**base, "kind": "process_identity_unverifiable",
             "detail": cls.detail})
        return {**base, "disposition": "ORPHAN", "detail": cls.detail}

    # Live process, identity matches: durable authority decides.
    try:
        worker = gate.get_worker(wid)
    except (TransitionRejected, StoreError, sqlite3.Error) as e:
        journal("boot.runtime_orphaned",
                {**base, "reason": "no_authoritative_worker_row",
                 "detail": f"{type(e).__name__}: {e}"})
        report["unresolved"].append(
            {**base, "kind": "no_authoritative_worker_row"})
        return {**base, "disposition": "ORPHAN",
                "detail": "no authoritative worker row; not guessed at"}
    if worker.get("status") in _TERMINAL_WORKER_STATES:
        journal("boot.runtime_orphaned",
                {**base, "reason": "worker_identity_terminal",
                 "worker_status": worker.get("status")})
        report["unresolved"].append(
            {**base, "kind": "worker_identity_terminal",
             "worker_status": worker.get("status")})
        return {**base, "disposition": "ORPHAN",
                "detail": f"worker row is {worker.get('status')}"}
    if not job_id:
        journal("boot.runtime_orphaned",
                {**base, "reason": "spawn_has_no_job_binding"})
        report["unresolved"].append({**base, "kind": "spawn_has_no_job"})
        return {**base, "disposition": "ORPHAN",
                "detail": "spawn evidence names no job"}
    try:
        job = gate.get_job(job_id)
    except (TransitionRejected, StoreError, sqlite3.Error) as e:
        journal("boot.runtime_orphaned",
                {**base, "reason": "job_row_unreadable",
                 "detail": f"{type(e).__name__}: {e}"})
        report["unresolved"].append({**base, "kind": "job_row_unreadable"})
        return {**base, "disposition": "ORPHAN",
                "detail": "job row unreadable; not guessed at"}

    # Authority rule (mirrors the R2 adopted-process detection, plus lease
    # liveness which R2's sweep does not judge): adopt only a process whose
    # durable authority is fully current.
    try:
        revoked = supervisor._revocation_after_spawn(
            wid, job_id, spawn["spawn_ts"], gate)
    except (StoreError, sqlite3.Error, TransitionRejected, AttributeError):
        # Authority unreadable: fail closed. Do not adopt (adoption asserts
        # authority), do not fence (fencing needs revocation evidence).
        journal("boot.runtime_orphaned",
                {**base, "reason": "authority_unreadable"})
        report["unresolved"].append(
            {**base, "kind": "authority_unreadable"})
        return {**base, "disposition": "UNCERTAIN",
                "detail": "revocation evidence unreadable; fail closed"}
    owner_ok = job.get("owner_worker_id") == wid
    token_ok = revoked is None
    lease_ok = _lease_live(job, now)
    status_ok = job.get("status") in _ACTIVE_OWNER_STATES
    if owner_ok and token_ok and lease_ok and status_ok:
        journal("boot.worker_adopted", {
            **base, "fencing_token": job.get("fencing_token"),
            "job_status": job.get("status"),
            "observed_pgid": cls.observed_pgid,
        })
        return {**base, "disposition": "ADOPT",
                "fencing_token": job.get("fencing_token"),
                "detail": "identity matches; durable authority current"}
    reasons = []
    if not owner_ok:
        reasons.append(f"owner_mismatch(current={job.get('owner_worker_id')!r})")
    if not token_ok:
        reasons.append("authority_revoked_after_spawn")
    if not lease_ok:
        reasons.append("lease_not_live")
    if not status_ok:
        reasons.append(f"job_status={job.get('status')}")
    journal("boot.worker_fenced", {**base, "reasons": reasons,
                                   "observed_pgid": cls.observed_pgid})
    return {**base, "disposition": "FENCE", "reasons": reasons,
            "detail": "process alive but durable authority stale;"
                      " handed to the R2 fence sweep"}


def _register_disposition(supervisor, spawn: dict, disp: dict) -> None:
    """Reconstruct supervisor runtime tracking from the disposition.

    ADOPT/FENCE processes stay registered for R2 containment/enforcement;
    every other disposition is REMOVED from the adoption registry so the
    sweep can never signal a dead, reused, orphaned, or uncertain PID.
    (The registry itself lives on the supervisor; see
    Supervisor._apply_boot_disposition.)
    """
    keep = disp["disposition"] in ("ADOPT", "FENCE")
    supervisor._apply_boot_disposition(spawn, keep)


def _inspect_completion_state(gate, journal, report: dict) -> None:
    """R5 composition at boot — observation only, never mutation.

    - COMPLETE + intact verified artifact -> preserved.
    - COMPLETE + missing/corrupt artifact -> integrity contradiction,
      surfaced; the COMPLETE row is NOT rewritten.
    - COMMITTING/UNCERTAIN -> R5 uncertain-completion inspection; adoptable
      artifacts are recorded as evidence for the later resolver. Completion
      is never inferred from filesystem presence.
    - Checkpoints: the latest-known-good pointer is verified still valid;
      it is never advanced, never replaced, never manufactured.
    """
    try:
        complete = gate.jobs_in_states(("COMPLETE",))
    except (StoreError, sqlite3.Error) as e:
        report["errors"].append({"phase": "inspect_complete",
                                 "error": f"{type(e).__name__}: {e}"})
        complete = []
    for job in complete:
        report["complete_jobs_checked"] += 1
        try:
            insp = gate.inspect_uncertain_completion(job["job_id"])
        except (StoreError, sqlite3.Error) as e:
            report["errors"].append(
                {"job_id": job["job_id"],
                 "error": f"inspect failed: {type(e).__name__}: {e}"})
            continue
        if insp["disposition"] == "COMMITTED":
            report["complete_jobs_consistent"] += 1
            continue
        problems = insp.get("problems", [])
        entry = {"kind": "complete_artifact_contradiction",
                 "job_id": job["job_id"],
                 "artifact_id": insp.get("artifact_id"),
                 "problems": problems}
        report["integrity_contradictions"].append(entry)
        report["unresolved"].append(entry)
        journal("boot.integrity_contradiction", entry)

    try:
        open_jobs = gate.jobs_in_states(("COMMITTING", "UNCERTAIN"))
    except (StoreError, sqlite3.Error) as e:
        report["errors"].append({"phase": "inspect_open",
                                 "error": f"{type(e).__name__}: {e}"})
        open_jobs = []
    for job in open_jobs:
        try:
            insp = gate.inspect_uncertain_completion(job["job_id"])
        except (StoreError, sqlite3.Error) as e:
            report["errors"].append(
                {"job_id": job["job_id"],
                 "error": f"inspect failed: {type(e).__name__}: {e}"})
            continue
        adoptable = insp.get("adoptable_artifacts") or []
        if insp["disposition"] == "UNCERTAIN" and adoptable:
            # Evidence for the later UNCERTAIN resolver — recorded, not
            # acted on. Adoption stays the resolver's decision.
            report["uncertain_inspected"].append(
                {"job_id": job["job_id"],
                 "adoptable_artifacts": adoptable})
            journal("boot.uncertain_completion_inspected",
                    {"job_id": job["job_id"],
                     "job_status": job["status"],
                     "adoptable_artifacts": adoptable})

    try:
        pointers = gate.all_latest_known_good()
    except (StoreError, sqlite3.Error) as e:
        report["errors"].append({"phase": "inspect_checkpoints",
                                 "error": f"{type(e).__name__}: {e}"})
        pointers = []
    for p in pointers:
        report["checkpoints_checked"] += 1
        cp = p.get("checkpoint")
        if cp is not None and cp.get("verification_status") == "VERIFIED":
            continue
        entry = {"kind": "checkpoint_pointer_contradiction",
                 "task_id": p["task_id"],
                 "checkpoint_id": p["checkpoint_id"],
                 "detail": ("pointer dangles (no checkpoint row)"
                            if cp is None else
                            f"pointed checkpoint is"
                            f" {cp.get('verification_status')},"
                            " not VERIFIED")}
        report["integrity_contradictions"].append(entry)
        report["unresolved"].append(entry)
        journal("boot.integrity_contradiction", entry)
    # NOTE: latest-known-good is never advanced here. verify_checkpoint()
    # remains the only writer of the pointer (R5, audited).
