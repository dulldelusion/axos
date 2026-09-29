"""Phase 1B — worker process entrypoint.

Run as a real OS subprocess by the supervisor::

    python -m axos.exec.worker --db PATH --worker-id W --proc-id P \
        --job-id J --ttl-s 60 --behavior '{...}' [--hb-interval-s 0.5]

Protocol (the worker NEVER manufactures authority):
1. Claim the job through TransitionGate.claim_job. If the claim fails,
   exit CLAIM_FAILED — another worker won; there is nothing to execute.
2. Read the authoritative lease triple back from the store (job_id,
   owner, fencing_token). These values are received, not invented.
3. Transition the job CLAIMED -> RUNNING (informational execution state).
4. Run the synthetic executor: heartbeat + progress through the gate.
   While it runs, a daemon thread renews the lease via the sanctioned
   gate.renew_lease path (unless --no-renew). If the gate reports
   LeaseError mid-execution, stop immediately (FencedError) — authority
   was lost; only a fresh claim can restore it.
5. Commit through the Phase 1C R5 authoritative artifact protocol —
   stage_artifact -> begin_commit -> verify_artifact -> commit_artifact —
   with REAL artifact bytes (the deterministic result record). The gate
   computes the content hash itself, verifies the bytes, and commits the
   job to COMPLETE in one atomic transaction only when every check holds.
   On FAILURE, fail_job_execution records FAILED. There is no
   artifact-free success path.

AUTHORITY BOUNDARY (F1): this process constructs TransitionGates over
Stores (one for the main thread, one for the lease-renewal thread — a
Store connection is not shared across threads), but it uses ONLY gate
methods. It never calls store.write_txn(), never touches store.conn for
writes, and never opens its own sqlite3 connection. The authority audit
greps for this.

Exit codes (process evidence only — NEVER job success):
  0   committed SUCCESS            10  committed FAILURE (execution failed)
  3   claim failed (lost the race)  4  fenced mid-execution
  5   commit rejected by the gate   6  fatal store error
  7   stale token (--expect-token mismatch: pre-claimed dispatch did not
      match the durable triple; no claim, no transition, no execution)
  143 SIGTERM (graceful stop, no commit attempted)

A heartbeat_loop / hang worker that is killed exits via signal (-9/-15).
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time

# Allow `python -m axos.exec.worker` from any cwd when the workspace root is
# on sys.path (the supervisor sets PYTHONPATH explicitly).
sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from axos.store import (open_store, TransitionGate, LeaseError,
                        TransitionRejected, StoreError)
from axos.exec.identity import LeaseRef, ProcessInstance
from axos.exec.synthetic import (SyntheticExecutor, BehaviorSpec, FencedError,
                                 InterruptedExecution)

EXIT_OK = 0
EXIT_COMMITTED_FAILURE = 10
EXIT_CLAIM_FAILED = 3
EXIT_FENCED = 4
EXIT_COMMIT_REJECTED = 5
EXIT_FATAL = 6
EXIT_SIGTERM = 143
EXIT_STALE_TOKEN = 7

_stop = threading.Event()
_finished = threading.Event()  # set when execution ends; halts renewal


def _on_sigterm(signum, frame):
    _stop.set()


def _parse_args(argv=None):
    p = argparse.ArgumentParser(description="AXOS Phase 1B worker process")
    p.add_argument("--db", required=True)
    p.add_argument("--worker-id", required=True)
    p.add_argument("--proc-id", required=True)
    p.add_argument("--job-id", required=True)
    p.add_argument("--ttl-s", type=float, required=True)
    p.add_argument("--behavior", required=True,
                   help="JSON BehaviorSpec dict")
    p.add_argument("--hb-interval-s", type=float, default=0.5)
    p.add_argument("--no-renew", action="store_true",
                   help="do NOT renew the lease while executing; the lease"
                   " expires at its TTL. Used to test expiry fencing"
                   " (Scenario B). Default: the worker renews via the"
                   " sanctioned gate.renew_lease path.")
    p.add_argument("--expect-token", type=int, default=None,
                   help="opt-in pre-claimed dispatch (Phase 1C R10): skip"
                   " gate.claim_job entirely. Instead, read the durable job"
                   " row and verify owner_worker_id == --worker-id and"
                   " fencing_token == N. Mismatch (or unreadable row) emits"
                   " a stale status and exits 7 with ZERO side effects: no"
                   " claim, no transition, no renewal, no execution.")
    return p.parse_args(argv)


def _emit(obj: dict) -> None:
    # One JSON line on stdout: debugging aid only, never authoritative.
    print(json.dumps(obj), flush=True)


def main(argv=None) -> int:
    args = _parse_args(argv)
    signal.signal(signal.SIGTERM, _on_sigterm)
    proc = ProcessInstance(proc_id=args.proc_id, worker_id=args.worker_id,
                           pid=os.getpid())
    actor = f"worker:{args.worker_id}"

    store = open_store(args.db)
    gate = TransitionGate(store)
    try:
        behavior = BehaviorSpec.from_dict(json.loads(args.behavior))
    except (ValueError, KeyError, TypeError) as e:
        _emit({"status": "bad_behavior", "error": str(e),
               "proc_id": proc.proc_id})
        return EXIT_FATAL

    # 1. Claim: the ONLY way to obtain authority — or verify a pre-existing
    # claim made by the R10 scheduler. With --expect-token the worker
    # NEVER claims: it reads the durable row and checks that the
    # (owner_worker_id, fencing_token) triple still matches. Any mismatch
    # (including an unreadable row) is a stale dispatch: emit and exit 7
    # with zero side effects — no claim, no transition, no renewal, no
    # execution. The default path (no flag) is byte-identical to before:
    # R8's worker-claims-itself path is untouched.
    if args.expect_token is not None:
        try:
            _pre = gate.get_job(args.job_id)
            _obs_token = _pre.get("fencing_token")
            _obs_token = None if _obs_token is None else int(_obs_token)
            _match = (_pre.get("owner_worker_id") == args.worker_id
                      and _obs_token == args.expect_token)
            _obs_owner = _pre.get("owner_worker_id")
        except (TransitionRejected, LeaseError, StoreError) as e:
            _match = False
            _obs_owner = None
            _obs_token = None
            _verify_error = str(e)
        if not _match:
            _emit({"status": "stale_token", "job_id": args.job_id,
                   "worker_id": args.worker_id, "proc_id": proc.proc_id,
                   "expected_token": args.expect_token,
                   "observed_owner": _obs_owner,
                   "observed_token": _obs_token,
                   "error": locals().get("_verify_error")})
            return EXIT_STALE_TOKEN
        _emit({"status": "claim_verified", "job_id": args.job_id,
               "worker_id": args.worker_id, "proc_id": proc.proc_id,
               "fencing_token": args.expect_token})
    else:
        try:
            claimed = gate.claim_job(args.job_id, args.worker_id, args.ttl_s,
                                     actor)
        except (TransitionRejected, LeaseError) as e:
            _emit({"status": "claim_error", "error": str(e),
                   "proc_id": proc.proc_id})
            return EXIT_FATAL
        if not claimed:
            _emit({"status": "claim_failed", "job_id": args.job_id,
                   "worker_id": args.worker_id, "proc_id": proc.proc_id})
            return EXIT_CLAIM_FAILED

    # 2. Receive the lease triple from the store — never manufactured.
    job = gate.get_job(args.job_id)
    lease = LeaseRef(job_id=args.job_id, worker_id=args.worker_id,
                     fencing_token=int(job["fencing_token"]))
    _emit({"status": "claimed", "lease_id": lease.lease_id,
           "proc_id": proc.proc_id})

    # 3. Mark execution start (informational; fencing enforced at commit).
    try:
        gate.transition_job(args.job_id, "RUNNING", actor)
    except (TransitionRejected, LeaseError) as e:
        _emit({"status": "run_transition_failed", "error": str(e),
               "lease_id": lease.lease_id})
        return EXIT_FATAL

    # 4. Execute. While it runs, a daemon thread keeps the lease alive
    # through the sanctioned renew path (brief section 8). Renewal is NOT
    # a heartbeat and confers no new authority: it only extends the
    # current lease triple while this process still owns it. If the gate
    # reports the lease gone (False), renewal stops — the executor's own
    # heartbeat/commit paths will surface the fencing.
    renew_thread = None
    if not args.no_renew:
        renew_thread = threading.Thread(
            target=_renew_loop,
            args=(args.db, args.job_id, args.worker_id, lease.fencing_token,
                  args.ttl_s, actor),
            name="lease-renewal", daemon=True)
        renew_thread.start()

    synth = SyntheticExecutor(
        gate=gate, worker_id=args.worker_id, proc_id=args.proc_id,
        lease=lease, behavior=behavior, hb_interval_s=args.hb_interval_s,
        actor=actor, stop_flag=_stop)
    t0 = time.monotonic()
    try:
        outcome, evidence = synth.run()
    except InterruptedExecution:
        # SIGTERM: stop promptly, commit NOTHING. Process death is not a
        # job outcome.
        _emit({"status": "interrupted_sigterm", "lease_id": lease.lease_id,
               "proc_id": proc.proc_id})
        return EXIT_SIGTERM
    except FencedError as e:
        _best_effort_milestone(
            gate, actor, "worker.fenced",
            {"job_id": args.job_id, "worker_id": args.worker_id,
             "proc_id": proc.proc_id, "lease_id": lease.lease_id,
             "reason": str(e)})
        _emit({"status": "fenced", "lease_id": lease.lease_id,
               "reason": str(e)})
        return EXIT_FENCED
    except StoreError as e:
        _emit({"status": "store_error", "error": str(e),
               "lease_id": lease.lease_id})
        return EXIT_FATAL
    finally:
        _finished.set()  # execution over: halt the renewal thread
    duration_s = time.monotonic() - t0

    if outcome == "NO_COMMIT":
        # heartbeat_loop / hang finished without being killed (only the
        # finite heartbeat_loop returns): authority untouched by design.
        _emit({"status": "no_commit", "lease_id": lease.lease_id,
               "evidence": evidence})
        return EXIT_OK

    # 5. The R5 authoritative completion contract: real artifact bytes ->
    # stage (fsync'd, gate-computed content hash) -> fenced begin_commit ->
    # gate verification -> one atomic commit. On SUCCESS exactly one path
    # can reach COMPLETE; on FAILURE the explicit failure operation is
    # used. A stale worker CANNOT win here: every step re-checks the
    # lease triple inside the gate's transactions.
    evidence = {**evidence, "duration_s": duration_s,
                "worker_actor": actor}
    if outcome == "FAILURE":
        reason = str(evidence.get("reason", "synthetic execution failed"))
        try:
            committed = gate.fail_job_execution(
                args.job_id, args.worker_id, lease.fencing_token,
                actor=actor, reason=reason, evidence=evidence)
        except (LeaseError, TransitionRejected) as e:
            _best_effort_milestone(
                gate, actor, "job.commit_rejected",
                {"job_id": args.job_id, "worker_id": args.worker_id,
                 "proc_id": proc.proc_id, "lease_id": lease.lease_id,
                 "outcome": outcome, "reason": str(e)})
            _emit({"status": "commit_rejected", "lease_id": lease.lease_id,
                   "reason": str(e)})
            return EXIT_COMMIT_REJECTED
        _emit({"status": "committed", "lease_id": lease.lease_id,
               "job_status": committed["status"]})
        return EXIT_COMMITTED_FAILURE

    # SUCCESS: the deterministic result record is the artifact. Same
    # inputs => same bytes => same content hash, by construction.
    artifact_bytes = _result_record_bytes(
        job_id=args.job_id, worker_id=args.worker_id,
        fencing_token=lease.fencing_token, outcome=outcome,
        evidence=evidence)
    try:
        job_row = gate.get_job(args.job_id)
        artifact = gate.stage_artifact(
            job_id=args.job_id, worker_id=args.worker_id,
            fencing_token=lease.fencing_token, task_id=job_row["task_id"],
            kind="result", data=artifact_bytes, actor=actor,
            producer={"executor": "synthetic", "behavior": behavior.kind
                       if hasattr(behavior, "kind") else "unknown"})
        gate.begin_commit(args.job_id, args.worker_id, lease.fencing_token,
                          artifact_id=artifact["artifact_id"], actor=actor)
        gate.verify_artifact(artifact["artifact_id"], actor=actor,
                             worker_id=args.worker_id,
                             fencing_token=lease.fencing_token,
                             job_id=args.job_id)
        committed = gate.commit_artifact(
            args.job_id, args.worker_id, lease.fencing_token,
            artifact_id=artifact["artifact_id"], actor=actor,
            evidence=evidence)
    except (LeaseError, TransitionRejected) as e:
        _best_effort_milestone(
            gate, actor, "job.commit_rejected",
            {"job_id": args.job_id, "worker_id": args.worker_id,
             "proc_id": proc.proc_id, "lease_id": lease.lease_id,
             "outcome": outcome, "reason": str(e)})
        _emit({"status": "commit_rejected", "lease_id": lease.lease_id,
               "reason": str(e)})
        return EXIT_COMMIT_REJECTED

    _emit({"status": "committed", "lease_id": lease.lease_id,
           "job_status": committed["status"],
           "artifact_id": committed["result_artifact_id"]})
    return EXIT_OK


def _result_record_bytes(*, job_id: str, worker_id: str, fencing_token: int,
                         outcome: str, evidence: dict) -> bytes:
    """The deterministic result record: real artifact bytes.

    Same (job_id, worker_id, fencing_token, outcome, deterministic
    evidence) => same bytes => same content hash, by construction.
    Wall-clock fields (duration_s) are excluded: they describe the run,
    not the result, and would destroy byte-level determinism. The record
    is canonical JSON (sorted keys, fixed separators), UTF-8 encoded.
    """
    ev = {k: v for k, v in evidence.items() if k != "duration_s"}
    record = {"axos_result_record": 1, "job_id": job_id,
              "worker_id": worker_id, "fencing_token": fencing_token,
              "outcome": outcome, "evidence": ev}
    return json.dumps(record, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _renew_loop(db_path: str, job_id: str, worker_id: str,
                fencing_token: int, ttl_s: float, actor: str) -> None:
    """Daemon: keep the current lease alive via the sanctioned renew path.

    Runs on its OWN store connection (one Store per thread — the worker's
    main connection is not thread-safe), renewing at TTL/3 intervals
    (bounded to [0.5s, 30s]) so a live worker is never spuriously fenced
    by its own TTL. Stops silently when:
    - execution finished (_finished set; nothing left to renew for),
    - SIGTERM arrived (_stop set; the process is going away),
    - the gate reports the lease gone (returns False: fenced, expired, or
      superseded — the executor's heartbeat/commit paths surface this),
    - the store errors (the executor will hit the same error on its next
      heartbeat and handle it per its own policy).

    Renewal extends ONLY the current (job, owner, fencing_token) triple;
    it can never resurrect a superseded token or steal another lease.
    """
    store = open_store(db_path)
    gate = TransitionGate(store)
    try:
        interval = max(0.5, min(ttl_s / 3.0, 30.0))
        while not _finished.is_set() and not _stop.is_set():
            _stop.wait(interval)
            if _finished.is_set() or _stop.is_set():
                return
            try:
                ok = gate.renew_lease(job_id, worker_id, fencing_token,
                                      ttl_s, actor)
            except (LeaseError, TransitionRejected, StoreError):
                return
            if not ok:
                return
    finally:
        store.close()


def _best_effort_milestone(gate, actor, event_type, payload) -> None:
    try:
        gate.append_event(event_type, payload, actor)
    except StoreError:
        pass  # evidence is best-effort; the rejection itself already stood


if __name__ == "__main__":
    sys.exit(main())
