"""Phase 1C R7 — watchdog: deterministic STALLED/DEAD detection.

Answers: "is this execution still making authoritative progress, or should
it be considered STALLED/DEAD?" The watchdog DETECTS and CLASSIFIES only —
it never recovers, never reclaims, never fences, never schedules.

Authority boundaries (audited by A13):
  - R1 remains the sole lease-reclaim authority. The watchdog never calls
    reclaim_lease, never clears owner_worker_id, never bumps fencing_token.
  - R2 remains the sole process-fencing authority. The watchdog never
    signals a process (no killpg, no SIGTERM/SIGKILL sends; os.kill appears
    nowhere here — process identity is read through exec.boot's
    _classify_spawn, which uses only the signal-0 existence probe).
  - R3: heartbeat is liveness evidence, never progress. A heartbeat loop
    with fresh heartbeats and stale progress is STALLED.
  - R4: lease-expiry detection is read-only and authoritative. The watchdog
    calls gate.observe_expired_leases() and never redefines the predicate.
  - R5: completion authority untouched (the watchdog never completes a
    job, never stages or verifies result data, never advances
    latest-known-good pointers).
  - R6: boot remains the reconstruction authority. The watchdog evaluates
    only when the runtime reports READY, and only from durable evidence.

Verdict semantics (per execution identity = (job_id, fencing_token)):
  HEALTHY  sufficient authoritative evidence of continued progress:
           progress fresh within the configured threshold, valid
           ownership, live lease.
  STALLED  execution is still associated with an active job/owner, but
           authoritative progress (jobs.progress_updated_at, or the
           claim-time baseline when no progress was ever recorded) is
           stale beyond the configured threshold. Fresh heartbeats do NOT
           prevent STALLED. An expired lease (R4 observation) is reported
           as STALLED with lease_expired evidence — never reclaimed here.
  DEAD     definitive death evidence only: the owner's worker row is in a
           terminal state, the owner's latest unreaped spawn record
           classifies as dead/reused under R6 process-identity rules
           (ESRCH/zombie, or the PID provably belongs to another process),
           or the supervisor's durable worker.proc_reaped milestone shows
           the owner's latest spawn generation was observed dead and
           reaped. A late heartbeat alone is never DEAD; stale progress
           alone is never DEAD; ambiguity resolves to the safer non-DEAD
           outcome.

Idempotency: verdict rows are written ONLY on transitions, via
gate.record_watchdog_verdict(), which compare-and-swaps on the latest
verdict for (job_id, fencing_token) inside one write_txn. Re-evaluating to
the same verdict is a zero-mutation no-op. Once DEAD for an identity, that
identity stays DEAD; a fencing-token bump starts a new identity.

Clocks: all staleness math uses store.current_time() (authoritative).
Timestamps claimed by the worker itself are never consulted. Stale is
(now - ts) > threshold, strictly — equal-to-threshold is NOT stale.

Fail-closed: if the store clock is unavailable or authoritative state
cannot be safely read, evaluate() raises WatchdogError and writes
nothing (a verdict is never invented from incomplete state).
"""
from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass

from ..store import (StoreError, TransitionGate, TransitionRejected,
                     open_store, migrate)
from . import boot as boot_mod

WATCHDOG_VERDICTS = ("HEALTHY", "STALLED", "DEAD")

# Worker rows in these states are terminal runtime evidence: the worker's
# execution identity is definitively over.
_TERMINAL_WORKER_STATES = ("DEAD", "RETIRED")

# Classifications from boot._classify_spawn that prove the spawned process
# identity no longer exists: "dead" (ESRCH/zombie) or "reuse" (the PID is
# provably a different process now — the original cannot still be alive
# under that PID). "unverifiable" is ambiguity, never death.
_DEFINITIVE_DEATH_CLASSIFICATIONS = ("dead", "reuse")


class WatchdogError(StoreError):
    """The watchdog could not evaluate safely (fail-closed)."""


class WatchdogNotReady(WatchdogError):
    """Evaluation was refused: the runtime has not reported READY."""


@dataclass(frozen=True)
class WatchdogConfig:
    """All watchdog timing thresholds. Constructor-injected; every value
    must be a positive finite number of seconds. No threshold may live as
    a literal inside the evaluation logic."""
    heartbeat_stale_s: float
    progress_stale_s: float
    evaluation_interval_s: float

    def __post_init__(self) -> None:
        for name in ("heartbeat_stale_s", "progress_stale_s",
                     "evaluation_interval_s"):
            value = getattr(self, name)
            if (not isinstance(value, (int, float))
                    or isinstance(value, bool)
                    or not math.isfinite(value)
                    or value <= 0):
                raise ValueError(
                    f"WatchdogConfig.{name} must be a positive finite"
                    f" number of seconds, got {value!r}")


def _is_stale(now: float, ts: float | None, threshold_s: float) -> bool:
    """Strict staleness: (now - ts) > threshold. A missing timestamp is
    never stale on its own — absence of evidence is not evidence."""
    if ts is None:
        return False
    return (now - ts) > threshold_s


class Watchdog:
    """Deterministic STALLED/DEAD detection over durable execution
    evidence. Detects and classifies only; see the module docstring for
    the authority boundaries."""

    def __init__(self, db_path: str, config: WatchdogConfig,
                 actor: str = "watchdog", readiness=None,
                 supervisor=None) -> None:
        """readiness: zero-arg callable returning True only when the
        runtime is fully reconstructed (boot READY). Pass supervisor= to
        derive readiness from a Supervisor's boot report instead. One of
        the two is required — the watchdog never evaluates against an
        incompletely reconstructed runtime by default."""
        if not isinstance(config, WatchdogConfig):
            raise TypeError(
                "config must be a WatchdogConfig with explicit thresholds")
        self.config = config
        self.actor = actor
        if supervisor is not None:
            self._readiness = (
                lambda: (supervisor._boot_report or {}).get("phase")
                == "READY")
        elif readiness is not None:
            self._readiness = readiness
        else:
            raise ValueError(
                "a readiness callable or a supervisor is required: the"
                " watchdog must not evaluate before boot is READY")
        self.db_path = db_path
        self.store = open_store(db_path)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        # sqlite3 connections are thread-bound: the background loop thread
        # gets its own Store/TransitionGate (same pattern as the
        # supervisor's fence-sweep thread). The constructing thread keeps
        # self.store/self.gate.
        self._owner_thread = threading.get_ident()
        self._thread_stores: dict[int, object] = {}
        self._thread_gates: dict[int, TransitionGate] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._errors: list[dict] = []
        self._last_skipped: list[dict] = []

    def _store_for_thread(self):  # type: ignore[no-untyped-def]
        """The Store bound to the calling thread (thread-bound sqlite3)."""
        tid = threading.get_ident()
        if tid == self._owner_thread:
            return self.store
        with self._lock:
            s = self._thread_stores.get(tid)
            if s is None:
                s = open_store(self.db_path)
                self._thread_stores[tid] = s
                self._thread_gates[tid] = TransitionGate(s)
            return s

    def _gate_for_thread(self) -> TransitionGate:
        """The TransitionGate bound to the calling thread."""
        tid = threading.get_ident()
        if tid == self._owner_thread:
            return self.gate
        with self._lock:
            g = self._thread_gates.get(tid)
            if g is None:
                self._store_for_thread()  # creates both
                g = self._thread_gates[tid]
            return g

    # ------------------------------------------------------------- evaluate
    def evaluate(self) -> list[dict]:
        """Evaluate every active execution and record verdict transitions.

        Returns one record per evaluated job:
        {job_id, fencing_token, verdict, previous_verdict, transition,
        verdict_id, evaluated_at, evidence}. Jobs whose worker row cannot
        be read are skipped (fail-closed per job) and listed in
        self._last_skipped.

        Raises WatchdogNotReady before READY, WatchdogError when the
        authoritative clock or state is unreadable. Two-phase scan: ALL
        jobs' verdicts are computed from read-only evidence first; only
        then are transitions recorded. If any job's authoritative evidence
        is unreadable, the scan fails closed before anything is written —
        a scan never writes job 1 and then discovers job 2 is unreadable.
        Never performs a partial write: verdict records go through the
        gate's single atomic compare-and-swap transaction.
        """
        with self._lock:
            if not self._readiness():
                raise WatchdogNotReady(
                    "watchdog refuses to evaluate before the runtime"
                    " reports READY")
            gate = self._gate_for_thread()
            store = self._store_for_thread()
            try:
                now = store.current_time()
            except Exception as exc:
                raise WatchdogError(
                    f"authoritative store clock unavailable: {exc}") from exc
            try:
                jobs = gate.owned_active_jobs()
                expired = {ev["job_id"]: ev
                           for ev in gate.observe_expired_leases()}
                spawns = gate.unreaped_proc_spawns()
            except Exception as exc:
                raise WatchdogError(
                    f"authoritative execution state unreadable: {exc}"
                ) from exc
            spawns_by_worker: dict[str, list[dict]] = {}
            for sp in spawns:
                spawns_by_worker.setdefault(
                    sp.get("worker_id"), []).append(sp)
            # Phase 1: compute every verdict from read-only evidence.
            computed: list[tuple] = []
            skipped: list[dict] = []
            for job in jobs:
                try:
                    c = self._evaluate_job(gate, job, now, expired,
                                           spawns_by_worker)
                except WatchdogError:
                    raise
                except Exception as exc:
                    raise WatchdogError(
                        "authoritative evidence unreadable for job"
                        f" {job.get('job_id')}: {exc}; scan fails closed"
                        " with nothing written") from exc
                if c is None:
                    skipped.append({"job_id": job.get("job_id"),
                                    "reason": "worker row missing"})
                    continue
                computed.append((job, c))
            self._last_skipped = skipped
            # Phase 2: record transitions (each its own atomic CAS).
            records: list[dict] = []
            for job, (verdict, evidence,
                      previous_verdict) in computed:
                records.append(self._record(
                    gate, job["job_id"], job["fencing_token"], verdict,
                    previous_verdict, now, evidence))
            return records

    def _evaluate_job(self, gate: TransitionGate, job: dict, now: float,
                      expired: dict, spawns_by_worker: dict
                      ) -> tuple | None:
        """Compute one job's (verdict, evidence, previous_verdict) from
        read-only evidence. Writes nothing. Returns None when the worker
        row is missing (fail-closed: no verdict from incomplete state).
        Raises on unreadable evidence. All staleness math uses the
        authoritative `now`."""
        cfg = self.config
        job_id = job["job_id"]
        owner = job["owner_worker_id"]
        token = job["fencing_token"]
        worker = gate.get_worker(owner)  # raises when unreadable
        if worker is None:
            return None

        previous = gate.latest_watchdog_verdict(job_id, token)
        previous_verdict = previous["verdict"] if previous else None

        # Once DEAD for this execution identity, stays DEAD: no
        # observation resurrects it, and no verdict ever restores revoked
        # leases, resets tokens, or changes ownership (this method has no
        # such capability at all).
        if previous_verdict == "DEAD":
            return ("DEAD",
                    {"reason": "DEAD is terminal for this execution"
                               " identity; retained from durable verdict"
                               " history",
                     "job_status": job["status"]},
                    previous_verdict)

        death_evidence = self._death_evidence(owner, worker,
                                              spawns_by_worker.get(owner),
                                              job_id,
                                              gate.latest_spawn_generation(
                                                  owner))
        heartbeats = gate.heartbeats_for(owner)
        last_hb_ts = heartbeats[0]["ts"] if heartbeats else None
        heartbeat_age = (now - last_hb_ts) if last_hb_ts is not None else None
        progress_baseline: float | None = None
        progress_age: float | None = None

        if death_evidence is not None:
            verdict = "DEAD"
            reason = ("definitive death evidence for owner"
                      f" {owner!r}: {death_evidence['kind']}")
        else:
            # Durable progress signal (R3). When no progress was ever
            # recorded, the claim-time baseline applies: a just-claimed
            # job is not instantly STALLED.
            baseline_candidates = [ts for ts in
                                   (job.get("lease_acquired_at"),
                                    job.get("created_at"))
                                   if ts is not None]
            progress_baseline = job.get("progress_updated_at")
            if progress_baseline is None and baseline_candidates:
                progress_baseline = max(baseline_candidates)
            progress_age = ((now - progress_baseline)
                            if progress_baseline is not None else None)
            expired_ev = expired.get(job_id)
            if expired_ev is not None:
                # R4 observation, read-only: the lease is expired. The
                # watchdog reports the condition; R1 alone may reclaim.
                verdict = "STALLED"
                reason = ("authoritative lease expired (R4 observation);"
                          " reclaim remains R1's sole authority")
            elif (progress_age is not None
                    and progress_age > cfg.progress_stale_s):
                # Fresh heartbeats do NOT prevent STALLED: heartbeat is
                # liveness, not progress.
                verdict = "STALLED"
                reason = ("authoritative progress stale: no durable"
                          " progress evidence within the configured"
                          " threshold")
            else:
                verdict = "HEALTHY"
                reason = ("sufficient authoritative evidence of continued"
                          " execution: progress fresh, ownership valid,"
                          " lease live")

        evidence = {
            "job_id": job_id,
            "owner_worker_id": owner,
            "fencing_token": token,
            "job_status": job["status"],
            "worker_status": worker["status"],
            "evaluated_at": now,
            "progress_updated_at": job.get("progress_updated_at"),
            "progress_baseline_ts": (progress_baseline
                                     if death_evidence is None else None),
            "progress_age_s": (progress_age
                               if death_evidence is None else None),
            "progress_stale": (progress_age is not None
                               and progress_age > cfg.progress_stale_s)
            if death_evidence is None else False,
            "last_heartbeat_ts": last_hb_ts,
            "heartbeat_age_s": heartbeat_age,
            # Heartbeat age is recorded as liveness evidence only: it is
            # never a verdict input by itself (a late heartbeat alone is
            # never DEAD, and fresh heartbeats never mask stale progress).
            "heartbeat_stale": _is_stale(now, last_hb_ts,
                                        cfg.heartbeat_stale_s),
            "lease_acquired_at": job.get("lease_acquired_at"),
            "lease_expires_at": job.get("lease_expires_at"),
            "lease_expired": job_id in expired,
            "death_evidence": death_evidence,
            "process_identity": self._process_identity_evidence(
                owner, spawns_by_worker.get(owner)),
            "thresholds": {
                "heartbeat_stale_s": cfg.heartbeat_stale_s,
                "progress_stale_s": cfg.progress_stale_s,
            },
            "previous_verdict": previous_verdict,
            "reason": reason,
        }
        return (verdict, evidence, previous_verdict)

    def _death_evidence(self, owner: str, worker: dict,
                        spawns: list[dict] | None,
                        job_id: str,
                        generation: dict | None) -> dict | None:
        """Definitive death evidence for the owner, or None.

        Terminal worker row, the owner's latest unreaped spawn record
        classifying as dead/reused under R6 process-identity rules, or
        the supervisor's durable worker.proc_reaped milestone for the
        owner's latest spawn generation. Late heartbeats, stale
        progress, and unverifiable identity are NOT death evidence —
        ambiguity resolves to the safer non-DEAD outcome."""
        if worker["status"] in _TERMINAL_WORKER_STATES:
            return {"kind": "worker_terminal",
                    "worker_status": worker["status"],
                    "detail": f"worker {owner!r} row is terminal"
                              f" ({worker['status']})"}
        latest = None
        if spawns:
            # Unreaped spawn evidence is ordered by spawn seq; the last
            # entry for this worker is its current process instance.
            latest = spawns[-1]
        if latest is None:
            # R14-A finding: the supervisor's R2 sweep reaps a dead
            # worker's process within H/2 and records a durable
            # worker.proc_reaped milestone. The unreaped-spawn path
            # above then has nothing to classify, so without this
            # branch the watchdog could never report DEAD in the
            # composed system (R7-05/R7-27 passed only by racing the
            # sweep thread). A reap for the owner's latest spawn
            # generation is definitive: every supervisor _reap caller
            # observes the process dead first, and the (worker_id,
            # proc_id) key pins the reap to the current generation — a
            # respawn supersedes it with a newer proc_id.
            if generation is not None and generation["reap"] is not None:
                reap = generation["reap"]
                return {"kind": "process_reaped",
                        "classification": "dead",
                        "pid": reap.get("pid"),
                        "proc_id": generation["proc_id"],
                        "returncode": reap.get("returncode"),
                        "signal": reap.get("signal"),
                        "how": reap.get("how"),
                        "reaped_at": reap.get("reaped_at"),
                        "detail": f"supervisor reaped {owner!r} generation"
                                  f" {generation['proc_id']!r}: process"
                                  " observed dead"
                                  f" (how={reap.get('how')},"
                                  f" signal={reap.get('signal')},"
                                  f" returncode={reap.get('returncode')})"}
            return None
        classification = boot_mod._classify_spawn(latest)
        if classification.kind in _DEFINITIVE_DEATH_CLASSIFICATIONS:
            return {"kind": "process_identity_dead",
                    "classification": classification.kind,
                    "pid": classification.pid,
                    "observed_start_jiffies":
                        classification.observed_start_jiffies,
                    "observed_pgid": classification.observed_pgid,
                    "detail": classification.detail}
        return None

    def _process_identity_evidence(self, owner: str,
                                   spawns: list[dict] | None) -> dict:
        """Non-authoritative identity observation, for verdict
        provenance only."""
        if not spawns:
            return {"spawn_evidence": "none"}
        latest = spawns[-1]
        classification = boot_mod._classify_spawn(latest)
        return {"spawn_evidence": "present",
                "proc_id": latest.get("proc_id"),
                "pid": classification.pid,
                "classification": classification.kind,
                "detail": classification.detail}

    def _record(self, gate: TransitionGate, job_id: str, token: int,
                verdict: str, previous_verdict: str | None, now: float,
                evidence: dict) -> dict:
        """Persist the verdict through the gate's atomic compare-and-swap.
        No partial writes: the insert and the ledger event commit in one
        write_txn, or the transaction rolls back and nothing is written."""
        try:
            stored = gate.record_watchdog_verdict(
                job_id, token, verdict, evidence, self.actor)
        except (TransitionRejected, StoreError) as exc:
            raise WatchdogError(
                f"verdict write failed (no partial state): {exc}") from exc
        return {"job_id": job_id, "fencing_token": token,
                "verdict": stored["verdict"],
                "previous_verdict": stored["previous_verdict"],
                "transition": stored["transition"],
                "verdict_id": stored["verdict_id"],
                "evaluated_at": stored["evaluated_at"],
                "evidence": evidence}

    # ------------------------------------------------------------ cadence
    def start(self) -> None:
        """Start the bounded background evaluation loop (one pass per
        evaluation_interval_s). Errors are collected in self._errors;
        the loop never silently dies."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop, name="axos-watchdog", daemon=True)
            self._thread.start()

    def _loop(self) -> None:
        while not self._stop.wait(self.config.evaluation_interval_s):
            try:
                self.evaluate()
            except Exception as exc:  # never silently die
                self._errors.append({"ts": time.time(),
                                     "error": repr(exc)})

    def stop(self) -> None:
        """Stop the background loop."""
        self._stop.set()
        t = self._thread
        if (t is not None and t.is_alive()
                and threading.get_ident() != t.ident):
            t.join(timeout=10)

    def close(self) -> None:
        """Stop the loop and release the store handle. Spawned OS
        processes are never touched here."""
        try:
            self.stop()
        finally:
            with self._lock:
                for s in self._thread_stores.values():
                    try:
                        s.close()
                    except Exception:
                        pass
                self._thread_stores.clear()
                self._thread_gates.clear()
            try:
                self.store.close()
            except Exception:
                pass
