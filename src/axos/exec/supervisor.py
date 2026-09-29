"""Phase 1B — deterministic supervisor.

Owns worker PROCESS lifecycle (spawn/observe/terminate/reap), never job
authority. The supervisor:

- spawns real OS subprocesses (python -m axos.exec.worker), one proc_id
  per spawn — a restart always mints a NEW proc_id;
- records worker identity (workers table) and process-instance evidence
  (worker.proc_spawned / worker.proc_reaped ledger milestones) through the
  TransitionGate;
- observes process state via poll() and drives WORKER lifecycle transitions
  (PROVISIONING->IDLE->ASSIGNED->RUNNING->IDLE, ->SUSPECT, ->DRAINING->
  RETIRED, ->DEAD) along legal graph edges only;
- NEVER infers job success from process state. Job truth comes from the
  store: the supervisor reads job rows; it never writes them.

Lifecycle semantics (Phase 0 worker graph, used as designed):
- unexpected proc death with an active job -> SUSPECT (held: the lease may
  still be live; the worker identity is restartable with a NEW proc_id,
  but it inherits NO authority — the new process must claim a fresh lease).
- unexpected death while idle -> DRAINING -> RETIRED.
- supervisor SIGKILL of a worker -> SUSPECT -> DEAD (confirmed kill).
- supervisor graceful stop (SIGTERM) -> DRAINING -> RETIRED.
- DEAD and RETIRED are terminal: a retired/killed identity is never
  resurrected — start a new worker_id. (Phase 0 graph has no outgoing edges.)

In-memory state (self._procs) holds ONLY process handles (Popen objects) —
never authority. After a supervisor crash, a new Supervisor reconstructs
everything from the store (reconstruct()); leases remain governed by store
time; nothing is fabricated from memory.

Idempotency: stop/kill/reap/restart are safe under duplicate delivery;
reap results are cached, repeated termination is a no-op, and a duplicate
restart simply replaces the process instance again.

Phase 1C R2 — enforced process-group self-fencing:
- The supervisor runs a continuous fence-sweep loop (every
  heartbeat_interval/2) over tracked AND adopted (post-restart) processes.
- A live tracked worker is fenced iff the DURABLE job row disagrees with
  the process's observed authority epoch: owner_worker_id != worker_id, or
  fencing_token != the token observed when this process held the job
  (known_token). Before any epoch was observed, only durable revocation
  evidence (job.lease_reclaimed / job.claimed ledger events after the
  spawn) may judge — absence of evidence is never evidence of fencing.
- Enforcement: SIGTERM the whole process group (killpg), grace
  heartbeat_interval/4, SIGKILL on survival, reap, and append a single
  worker.fence_enforced ledger event. The group must be dead within one
  heartbeat_interval of the fencing transaction.
- The supervisor NEVER decides ownership from local state, NEVER calls
  reclaim_lease (that stays a recovery-controller/reconciler/system/
  operator primitive), and NEVER kills when authoritative state cannot be
  read (fail closed). If the supervisor is unavailable, containment is
  delayed — never weakened: a restarted supervisor's first act is a
  fence-sweep over durable spawn evidence.

Phase 1C R6 — durable boot recovery & runtime reconstruction (exec/boot.py):
- __init__ runs a deterministic recovery pass before the fence-sweep
  thread starts and before any worker may be scheduled: validate the
  database and the ledger chain; reconcile unreaped spawn evidence
  against live process identity (PID + kernel start identity + process
  group); route expired leases through the R4 expiry observer and the R1
  reclaim primitive; force-reclaim only live leases whose owner process
  is confirmed dead; fence stale survivors through the existing R2 sweep;
  surface unresolved conditions instead of guessing.
- The pass is idempotent; boot.* ledger milestones journal it; READY
  means the authoritative recovery pass completed — not that every
  record is fully recovered (fully_recovered is reported separately).
  No new execution is scheduled before READY.
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

from ..store import (open_store, migrate, TransitionGate, TransitionRejected,
                     StoreError)
from ..store import transitions as T
from .boot import boot_recover, proc_start_jiffies
from .identity import new_proc_id, WorkerIdentity, ProcessInstance

WORKSPACE_ROOT = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))

TERMINAL_WORKER_STATES = ("DEAD", "RETIRED")
TERMINAL_JOB_STATES = ("COMPLETE", "FAILED")


# known_token / spawn_store_ts are EXECUTION EVIDENCE about this process
# instance (which authority epoch it was observed to hold, and when it
# spawned) — never ownership authority. Every fencing judgment re-reads
# the durable job row; these fields only let the sweep detect divergence.
@dataclass
class _ProcInfo:
    worker_id: str
    proc_id: str
    job_id: str | None
    behavior: dict
    popen: subprocess.Popen
    log_path: str
    log_file: object = field(repr=False)
    known_token: int | None = None
    spawn_store_ts: float = 0.0


class _FenceRefused(Exception):
    """Raised when a fence target fails process-group identity verification.

    Fail closed: the supervisor must never signal a group it cannot prove
    is the worker's own (in particular, never its own group).
    """


@dataclass
class _AdoptedProc:
    """A possibly-live worker process discovered from durable spawn
    evidence after a supervisor restart (no Popen handle exists)."""
    worker_id: str
    proc_id: str
    job_id: str | None
    pid: int
    spawn_store_ts: float


def _proc_start_wall_time(pid: int) -> float:
    """Wall-clock start time of a process, via /proc (Linux).

    Used as a PID-reuse guard when adopting processes after a restart:
    a pid whose process started after the durable spawn milestone cannot
    be the worker's process.
    """
    with open(f"/proc/{pid}/stat", "r") as f:
        after_comm = f.read().rsplit(")", 1)[1].split()
    # field 22 (starttime, jiffies since boot); index 19 after pid+comm.
    starttime_jiffies = int(after_comm[19])
    btime = None
    with open("/proc/stat", "r") as f:
        for line in f:
            if line.startswith("btime"):
                btime = int(line.split()[1])
                break
    if btime is None:
        raise OSError("btime not found in /proc/stat")
    hz = os.sysconf("SC_CLK_TCK")
    return btime + starttime_jiffies / hz


class Supervisor:
    """Process-lifecycle supervisor. See module docstring for semantics."""

    def __init__(self, db_path: str, actor: str = "supervisor",
                 heartbeat_interval_s: float = 0.5) -> None:
        """heartbeat_interval_s: the heartbeat interval H this supervisor
        enforces. The fence sweep runs every H/2, the SIGTERM grace is H/4,
        and a fenced process group must be dead within H of the fencing
        transaction. Workers supervised here should heartbeat on the same H.
        """
        if heartbeat_interval_s <= 0:
            raise ValueError("heartbeat_interval_s must be positive")
        self.db_path = db_path
        self.actor = actor
        self.store = open_store(db_path)
        migrate(self.store)  # ensure v2 schema for heartbeats/commit evidence
        self.gate = TransitionGate(self.store)
        self._heartbeat_interval_s = float(heartbeat_interval_s)
        self._sweep_interval_s = self._heartbeat_interval_s / 2.0
        self._fence_grace_s = self._heartbeat_interval_s / 4.0
        self._procs: dict[str, _ProcInfo] = {}
        self._reaped: dict[str, dict] = {}  # worker_id -> reap record
        # --- R2 concurrency scaffolding ---
        # sqlite3 connections are thread-bound: every thread (including the
        # fence-sweep thread) gets its own Store/TransitionGate. The
        # constructing thread keeps self.store/self.gate.
        self._owner_thread = threading.get_ident()
        self._thread_stores: dict[int, object] = {}
        self._thread_gates: dict[int, TransitionGate] = {}
        # RLock: _gate_for_thread() may call _store_for_thread() while
        # already holding this lock.
        self._thread_lock = threading.RLock()
        self._procs_lock = threading.RLock()  # guards _procs/_reaped/_adopted
        self._sweep_lock = threading.Lock()  # serializes fence_sweep()
        # Adopted processes: (worker_id, proc_id) -> _AdoptedProc, discovered
        # from durable spawn evidence (post-restart containment).
        self._adopted: dict[tuple[str, str], _AdoptedProc] = {}
        # Exactly-once enforcement evidence: (worker_id, proc_id) keys with
        # a worker.fence_enforced event already appended.
        self._fence_reported: set[tuple[str, str]] = set()
        # Workers currently in sweep-observation failure (one ledger event
        # per episode, not per tick).
        self._obs_failed: set[tuple[str, str]] = set()
        self._last_sweep: dict = {}
        self._sweep_errors: list[dict] = []
        self._sweep_stop = threading.Event()
        self._sweep_thread: threading.Thread | None = None
        # A restarted supervisor's first duty: discover possibly-live
        # worker processes from durable state (no in-memory fencing cache).
        self._adopt_orphaned_procs()
        # Phase 1C R6 — durable boot recovery & runtime reconstruction.
        # Runs before the fence-sweep thread starts and before any new
        # worker may be scheduled: no new execution until boot is READY.
        self._boot_report = boot_recover(self)
        if self._boot_report["phase"] != "BLOCKED":
            self._sweep_thread = threading.Thread(
                target=self._sweep_loop, name="axos-fence-sweep", daemon=True)
            self._sweep_thread.start()

    # --------------------------------------- thread-local store/gate access
    def _boot_ready(self) -> bool:
        """Phase 1C R6: no new execution before boot recovery reaches READY.

        The boot pass (exec/boot.py) reconciles durable state after a
        (re)start; scheduling before it completes could fork authority.
        """
        return getattr(self, "_boot_report", {}).get("phase") == "READY"

    def _require_boot_ready(self) -> None:
        if not self._boot_ready():
            raise TransitionRejected(
                "supervisor boot recovery has not reached READY: no new"
                " execution is permitted before the recovery pass completes")
    def _store_for_thread(self):  # type: ignore[no-untyped-def]
        """The Store bound to the calling thread (thread-bound sqlite3)."""
        tid = threading.get_ident()
        if tid == self._owner_thread:
            return self.store
        with self._thread_lock:
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
        with self._thread_lock:
            g = self._thread_gates.get(tid)
            if g is None:
                self._store_for_thread()  # creates both
                g = self._thread_gates[tid]
            return g

    # ------------------------------------------------------------- workers
    def _read_worker(self, worker_id: str) -> dict:
        # Read-only via the gate; the supervisor never touches store.conn
        # for worker state. Thread-local: safe from the sweep thread.
        return self._gate_for_thread().get_worker(worker_id)

    def _ensure_worker_row(self, worker_id: str) -> dict:
        try:
            self._gate_for_thread().create_worker(worker_id, self.actor)
        except sqlite3.IntegrityError:
            pass  # already exists — reuse the durable identity
        return self._read_worker(worker_id)

    def _drive_worker(self, worker_id: str, target: str) -> list[str]:
        """Walk the Phase 0 worker transition graph from the current state
        to target along legal edges, applying each step through the gate.
        Returns the applied path (empty if already there)."""
        current = self._read_worker(worker_id)["status"]
        if current == target:
            return []
        # BFS over the transition graph.
        prev: dict[str, str | None] = {current: None}
        queue = [current]
        while queue:
            node = queue.pop(0)
            for nxt in T.WORKER_TRANSITIONS.get(node, ()):
                if nxt not in prev:
                    prev[nxt] = node
                    queue.append(nxt)
        if target not in prev:
            raise TransitionRejected(
                f"no legal worker transition path {current} -> {target}")
        path: list[str] = []
        node = target
        while node != current:
            path.append(node)
            node = prev[node]  # type: ignore[assignment]
        path.reverse()
        gate = self._gate_for_thread()
        for step in path:
            gate.transition_worker(worker_id, step, self.actor)
        return path

    # ---------------------------------------------------------------- spawn
    def start_worker(self, worker_id: str, job_id: str,
                     behavior: dict | None = None,
                     ttl_s: float = 60.0,
                     hb_interval_s: float = 0.5,
                     renew: bool = True,
                     expect_token: int | None = None) -> str:
        """Spawn a worker process for a job. Returns the new proc_id.

        The worker process itself claims the lease through the gate; the
        supervisor never manufactures lease authority. If the worker row is
        in a terminal state (DEAD/RETIRED) this raises — start a new
        worker_id instead.

        renew=True (default): the worker renews its lease via the sanctioned
        gate.renew_lease path while it executes. renew=False: the worker
        never renews — used to test lease-expiry fencing (Scenario B).

        expect_token (Phase 1C R10): when set, the worker is spawned with
        --expect-token N and verifies the durable (owner_worker_id,
        fencing_token) triple instead of claiming — the pre-claimed
        scheduler-dispatch path. Default None: the worker claims the lease
        itself, unchanged behavior.
        """
        self._require_boot_ready()
        row = self._ensure_worker_row(worker_id)
        if row["status"] in TERMINAL_WORKER_STATES:
            raise TransitionRejected(
                f"worker {worker_id} is {row['status']}: terminal identities"
                " are never resurrected; use a new worker_id")
        # A tracked live proc for this worker is replaced (restart semantics).
        self._terminate_tracked(worker_id, timeout=2.0, how="unexpected")

        proc_id = new_proc_id()
        behavior = behavior or {"kind": "success_immediate"}
        log_path = os.path.join("/tmp", f"axos-{proc_id}.log")
        logf = open(log_path, "wb")  # non-authoritative debug log only
        env = os.environ.copy()
        env["PYTHONPATH"] = WORKSPACE_ROOT + os.pathsep + env.get(
            "PYTHONPATH", "")
        cmd = [sys.executable, "-m", "axos.exec.worker",
               "--db", self.db_path,
               "--worker-id", worker_id,
               "--proc-id", proc_id,
               "--job-id", job_id,
               "--ttl-s", str(ttl_s),
               "--behavior", json.dumps(behavior),
               "--hb-interval-s", str(hb_interval_s)]
        if not renew:
            cmd.append("--no-renew")
        if expect_token is not None:
            cmd.extend(["--expect-token", str(expect_token)])
        popen = subprocess.Popen(cmd, env=env, stdout=logf,
                                 stderr=subprocess.STDOUT,
                                 start_new_session=True)
        gate = self._gate_for_thread()
        store = self._store_for_thread()
        # Phase 1C R6: persist the exact process-start identity and the
        # process-group identity with the durable spawn evidence, so the
        # boot pass can prove a PID match is the SAME process (never by
        # PID alone). These are execution evidence, not authority.
        start_jiffies = proc_start_jiffies(popen.pid)
        try:
            spawn_pgid = os.getpgid(popen.pid)
        except (ProcessLookupError, PermissionError):
            spawn_pgid = None
        with self._procs_lock:
            self._procs[worker_id] = _ProcInfo(
                worker_id=worker_id, proc_id=proc_id, job_id=job_id,
                behavior=behavior, popen=popen, log_path=log_path,
                log_file=logf, known_token=None,
                spawn_store_ts=store.current_time())
            # A new process instance supersedes any cached reap record.
            self._reaped.pop(worker_id, None)
        gate.append_event(
            "worker.proc_spawned",
            {"worker_id": worker_id, "proc_id": proc_id, "pid": popen.pid,
             "start_jiffies": start_jiffies, "pgid": spawn_pgid,
             "job_id": job_id,
             "behavior_kind": behavior.get("kind")}, self.actor)
        # Record the assignment along legal edges; SUSPECT/RUNNING/ASSIGNED
        # rows are left for observe() to reconcile against real job state.
        if row["status"] in ("PROVISIONING", "IDLE"):
            self._drive_worker(worker_id, "ASSIGNED")
        return proc_id

    # ------------------------------------------------------------ terminate
    def _terminate_tracked(self, worker_id: str, timeout: float,
                           how: str) -> dict | None:
        """SIGTERM a tracked proc (escalating to SIGKILL), then reap."""
        with self._procs_lock:
            info = self._procs.get(worker_id)
            if info is None:
                return self._reaped.get(worker_id)
        proc = info.popen
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        return self._reap(worker_id, proc.poll(), how)

    def stop_worker(self, worker_id: str, timeout: float = 5.0) -> dict | None:
        """Graceful stop: SIGTERM, escalate to SIGKILL, reap. Idempotent.
        The worker identity is retired (DRAINING -> RETIRED)."""
        return self._terminate_tracked(worker_id, timeout, "stopped")

    def kill_worker(self, worker_id: str) -> dict | None:
        """Immediate SIGKILL, then reap. Idempotent. The worker identity is
        marked DEAD (confirmed kill by the supervisor)."""
        with self._procs_lock:
            info = self._procs.get(worker_id)
            if info is None:
                return self._reaped.get(worker_id)
        proc = info.popen
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        return self._reap(worker_id, proc.poll(), "killed")

    # ----------------------------------------------------------------- reap
    def reap(self, worker_id: str) -> dict | None:
        """Idempotent reap: returns the cached reap record when no process is
        tracked, None if the tracked process is still alive. A newly spawned
        process instance always supersedes any cached record."""
        with self._procs_lock:
            info = self._procs.get(worker_id)
            if info is None:
                return self._reaped.get(worker_id)
            rc = info.popen.poll()
            if rc is None:
                return None
        return self._reap(worker_id, rc, "unexpected")

    def _reap(self, worker_id: str, returncode: int | None,
              how: str) -> dict:
        """Record a process death durably and reconcile the WORKER lifecycle.
        Job state is only READ here — never written. A dead process is
        evidence about the process; the job row keeps whatever the gate
        says, which may be CLAIMED/RUNNING with a live lease."""
        gate = self._gate_for_thread()
        store = self._store_for_thread()
        with self._procs_lock:
            info = self._procs.pop(worker_id, None)
            if info is None:
                # Already reaped (duplicate delivery): return cached record.
                return self._reaped[worker_id]
        try:
            info.log_file.close()
        except Exception:
            pass
        now = store.current_time()
        sig = -returncode if returncode is not None and returncode < 0 else None
        record = {
            "worker_id": worker_id,
            "proc_id": info.proc_id,
            "pid": info.popen.pid,
            "job_id": info.job_id,
            "returncode": returncode,
            "signal": sig,
            "how": how,  # unexpected | killed | stopped
            "reaped_at": now,  # store time
            "log_path": info.log_path,  # debug only, NOT authoritative
        }
        gate.append_event("worker.proc_reaped", record, self.actor)

        # Reconcile the worker row from STORE state (job row), not memory.
        job_terminal = False
        if info.job_id:
            try:
                job = gate.get_job(info.job_id)
                job_terminal = job["status"] in TERMINAL_JOB_STATES
            except TransitionRejected:
                job_terminal = False
        worker_status = self._read_worker(worker_id)["status"]
        if worker_status not in TERMINAL_WORKER_STATES:
            try:
                if job_terminal:
                    # The job reached a terminal state through the gate: the
                    # worker's shift is over; it is ready again.
                    self._drive_worker(worker_id, "IDLE")
                elif how == "unexpected":
                    if worker_status == "IDLE":
                        self._drive_worker(worker_id, "RETIRED")
                    else:
                        # Proc gone, lease may still be live: hold at SUSPECT.
                        # The identity is restartable (new proc_id), but the
                        # new process inherits no authority.
                        self._drive_worker(worker_id, "SUSPECT")
                elif how == "killed":
                    if worker_status == "IDLE":
                        self._drive_worker(worker_id, "RETIRED")
                    else:
                        self._drive_worker(worker_id, "DEAD")
                elif how == "stopped":
                    self._drive_worker(worker_id, "RETIRED")
            except TransitionRejected:
                # No legal path (should not happen; BFS covers the graph) —
                # leave the row; the milestone above is the durable evidence.
                pass
        with self._procs_lock:
            self._reaped[worker_id] = record
        return record

    # -------------------------------------------------------------- observe
    def observe(self) -> dict:
        """Poll all tracked processes, reap the dead, and reconcile worker
        lifecycle rows against authoritative job state. Returns a snapshot
        (process evidence only — never a job verdict).

        This stays a process-observation primitive: fencing enforcement
        lives in fence_sweep(), not here.
        """
        with self._procs_lock:
            tracked = list(self._procs)
        for worker_id in tracked:
            self.reap(worker_id)  # unexpected-death path for found-dead procs
        gate = self._gate_for_thread()
        snapshot: dict[str, dict] = {}
        with self._procs_lock:
            items = list(self._procs.items())
        for worker_id, info in items:
            alive = info.popen.poll() is None
            wstatus = self._read_worker(worker_id)["status"]
            jstatus = None
            if info.job_id:
                try:
                    j = gate.get_job(info.job_id)
                    jstatus = j["status"]
                    owned = (j["owner_worker_id"] == worker_id
                             and jstatus in ("CLAIMED", "RUNNING"))
                    if owned and wstatus in ("ASSIGNED", "SUSPECT"):
                        # The worker holds a live lease and is executing.
                        self._drive_worker(worker_id, "RUNNING")
                        wstatus = "RUNNING"
                    elif jstatus in TERMINAL_JOB_STATES and \
                            wstatus == "RUNNING":
                        self._drive_worker(worker_id, "IDLE")
                        wstatus = "IDLE"
                except TransitionRejected:
                    pass
            snapshot[worker_id] = {
                "proc_id": info.proc_id,
                "pid": info.popen.pid,
                "alive": alive,
                "job_id": info.job_id,
                "worker_status": wstatus,
                "job_status": jstatus,
            }
        return snapshot

    # --------------------------------------------- R2 enforced self-fencing
    # Phase 1C D4: the supervisor owns PROCESS termination — never lease
    # authority. A continuous sweep (every H/2) detects durable
    # ownership/token divergence for live worker processes and kills the
    # whole process group (SIGTERM, grace H/4, SIGKILL), so a fenced worker
    # can never consume more than one heartbeat interval of compute
    # post-fencing. Detection re-reads durable state every pass; nothing
    # is decided from supervisor memory.

    def fence_sweep(self) -> dict:
        """One enforced-fencing pass over tracked and adopted processes.

        Safe to call from any thread; serialized against the background
        sweep loop. Read failures fail closed: no kill is attempted when
        authoritative state cannot be read. Every enforcement appends
        exactly one worker.fence_enforced ledger event per
        (worker_id, proc_id).
        """
        with self._sweep_lock:
            gate = self._gate_for_thread()
            store = self._store_for_thread()
            report: dict = {"swept_at": store.current_time(), "checked": 0,
                            "fenced_killed": [], "already_dead": [],
                            "observation_failures": [], "adopted_pruned": 0}
            with self._procs_lock:
                tracked = list(self._procs.items())
                adopted = list(self._adopted.items())
            for worker_id, info in tracked:
                try:
                    self._sweep_one_tracked(worker_id, info, gate, store,
                                            report)
                except Exception as e:  # one bad worker never breaks a sweep
                    report.setdefault("errors", []).append(
                        {"worker_id": worker_id,
                         "error": f"{type(e).__name__}: {e}"})
            for (worker_id, proc_id), ad in adopted:
                try:
                    self._sweep_one_adopted(worker_id, proc_id, ad, gate,
                                           store, report)
                except Exception as e:
                    report.setdefault("errors", []).append(
                        {"worker_id": worker_id, "proc_id": proc_id,
                         "error": f"{type(e).__name__}: {e}"})
            self._last_sweep = report
            return report

    def _sweep_loop(self) -> None:
        """Background enforcement loop: one sweep every
        heartbeat_interval/2 while the supervisor is active.

        The first sweep already ran synchronously inside the R6 boot pass
        (Supervisor.__init__), so the loop waits a full interval before
        its first pass — no back-to-back sweeps.
        """
        while not self._sweep_stop.is_set():
            self._sweep_stop.wait(self._sweep_interval_s)
            if self._sweep_stop.is_set():
                break
            try:
                self.fence_sweep()
            except Exception as e:  # the sweep thread must never die
                self._sweep_errors.append(
                    {"ts": time.time(), "error": f"{type(e).__name__}: {e}"})
                del self._sweep_errors[:-20]

    # ------------------------------------------------- sweep: one worker
    def _sweep_one_tracked(self, worker_id: str, info: _ProcInfo,
                           gate: TransitionGate, store, report: dict) -> None:
        key = (worker_id, info.proc_id)
        rc = info.popen.poll()
        if rc is not None:
            # Already dead: reap via the normal path. Fencing evidence is
            # recorded only when durable state shows it was fenced.
            with self._procs_lock:
                reported = key in self._fence_reported
            job = None
            fenced = False
            if not reported and info.job_id:
                try:
                    job = gate.get_job(info.job_id)
                    fenced = self._is_fenced_tracked(worker_id, info, job,
                                                     gate)
                except (TransitionRejected, StoreError, sqlite3.Error):
                    job = None  # cannot establish: reap without fence evidence
            self._reap(worker_id, rc, "unexpected")
            if fenced and job is not None:
                tx_ts, tx_source = self._fence_tx_evidence(
                    worker_id, info.job_id, info.spawn_store_ts, job, gate)
                self._record_fence_outcome(
                    worker_id=worker_id, proc_id=info.proc_id,
                    job_id=info.job_id, pid=info.popen.pid, pgid=None,
                    old_token=info.known_token,
                    new_token=int(job["fencing_token"]),
                    fence_tx_ts=tx_ts, fence_tx_source=tx_source,
                    signals=[], outcome="already_dead", adopted=False,
                    detect_mono=time.monotonic(),
                    gate=gate, store=store, report=report,
                    report_key="already_dead")
            report["checked"] += 1
            return
        # Live: read authoritative state; fail closed on read error.
        try:
            if not info.job_id:
                raise TransitionRejected("tracked proc has no job_id")
            job = gate.get_job(info.job_id)
        except (TransitionRejected, StoreError, sqlite3.Error) as e:
            self._note_observation_failure(key, worker_id, info.proc_id,
                                           gate, report,
                                           f"{type(e).__name__}: {e}")
            return
        self._clear_observation_failure(key)
        if self._is_fenced_tracked(worker_id, info, job, gate):
            self._enforce_fence_tracked(worker_id, info, job, gate, store,
                                        report)
        report["checked"] += 1

    def _sweep_one_adopted(self, worker_id: str, proc_id: str,
                           ad: _AdoptedProc, gate: TransitionGate, store,
                           report: dict) -> None:
        key = (worker_id, proc_id)
        try:
            os.kill(ad.pid, 0)
        except (ProcessLookupError, PermissionError):
            # Gone (or unsignalable): drop the adoption. A reap milestone
            # from the owning supervisor, if any, already pairs the spawn.
            with self._procs_lock:
                self._adopted.pop(key, None)
            report["adopted_pruned"] += 1
            return
        try:
            if not ad.job_id:
                raise TransitionRejected("adopted proc has no job_id")
            job = gate.get_job(ad.job_id)
        except (TransitionRejected, StoreError, sqlite3.Error) as e:
            self._note_observation_failure(key, worker_id, proc_id, gate,
                                           report,
                                           f"{type(e).__name__}: {e}")
            return
        self._clear_observation_failure(key)
        fenced = (
            job["owner_worker_id"] != worker_id
            or self._revocation_after_spawn(
                worker_id, ad.job_id, ad.spawn_store_ts, gate) is not None
        )
        if not fenced:
            report["checked"] += 1
            return
        self._enforce_fence_adopted(worker_id, proc_id, ad, job, gate, store,
                                    report)
        report["checked"] += 1

    # ------------------------------------------------- sweep: detection
    def _is_fenced_tracked(self, worker_id: str, info: _ProcInfo,
                           job: dict, gate: TransitionGate) -> bool:
        """D4 detection rule for a tracked process.

        Fenced iff the durable row disagrees with the process's observed
        authority epoch: a different owner, or a different token. The
        observed epoch (known_token) is learned on the first pass that sees
        this process as the owner; until any epoch is observed — or on the
        pass that learns it — only durable revocation evidence may judge.
        Absence of evidence is never evidence of fencing.
        """
        owner = job["owner_worker_id"]
        token = int(job["fencing_token"])
        learned_now = False
        if info.known_token is None and owner == worker_id:
            info.known_token = token
            learned_now = True
        if info.known_token is not None and not learned_now:
            return owner != worker_id or token != info.known_token
        return self._revocation_after_spawn(
            worker_id, info.job_id, info.spawn_store_ts, gate) is not None

    def _revocation_after_spawn(self, worker_id: str, job_id: str | None,
                                spawn_ts: float,
                                gate: TransitionGate) -> dict | None:
        """Earliest durable revocation of this worker's authority after the
        process spawned, or None.

        Revocation evidence (hash-chained ledger, store timestamps): a
        job.lease_reclaimed naming this worker as prev_owner, a
        job.claimed by a DIFFERENT worker, or a second job.claimed by this
        worker (a newer authority epoch exists — this process is stale).
        """
        if not job_id:
            return None
        claimed_self = 0
        for ev in gate.fencing_ledger_for_job(job_id):
            if ev["ts"] <= spawn_ts:
                continue
            p = ev["payload"] or {}
            if ev["event_type"] == "job.lease_reclaimed":
                if p.get("prev_owner") == worker_id:
                    return ev
            elif ev["event_type"] == "job.claimed":
                if p.get("worker_id") == worker_id:
                    claimed_self += 1
                    if claimed_self >= 2:
                        return ev
                else:
                    return ev
        return None

    def _fence_tx_evidence(self, worker_id: str, job_id: str | None,
                           spawn_ts: float, job: dict,
                           gate: TransitionGate) -> tuple[object, str]:
        """Best-effort store timestamp of the fencing transaction: the
        earliest post-spawn revocation event, else the job row's updated_at
        (any reclaim/claim bumps it)."""
        ev = self._revocation_after_spawn(worker_id, job_id, spawn_ts, gate)
        if ev is not None:
            return ev["ts"], "ledger:" + ev["event_type"]
        return job.get("updated_at"), "job.updated_at_fallback"

    def _prev_token_for(self, worker_id: str, job_id: str | None,
                        spawn_ts: float, gate: TransitionGate) -> object:
        """The token the fenced process most likely held: prev_token of the
        earliest post-spawn reclaim naming this worker. Informational."""
        ev = self._revocation_after_spawn(worker_id, job_id, spawn_ts, gate)
        if ev is not None and ev["event_type"] == "job.lease_reclaimed":
            return (ev["payload"] or {}).get("prev_token")
        return None

    # ------------------------------------------------- sweep: enforcement
    def _enforce_fence_tracked(self, worker_id: str, info: _ProcInfo,
                               job: dict, gate: TransitionGate, store,
                               report: dict) -> None:
        key = (worker_id, info.proc_id)
        with self._procs_lock:
            if key in self._fence_reported:
                return
        detect_mono = time.monotonic()
        pid = info.popen.pid
        try:
            pgid = self._resolve_target_pgid(pid)
        except ProcessLookupError:
            # Died between poll() and the signal: reap, record, done.
            self._reap(worker_id, info.popen.poll(), "unexpected")
            tx_ts, tx_source = self._fence_tx_evidence(
                worker_id, info.job_id, info.spawn_store_ts, job, gate)
            self._record_fence_outcome(
                worker_id=worker_id, proc_id=info.proc_id,
                job_id=info.job_id, pid=pid, pgid=None,
                old_token=info.known_token,
                new_token=int(job["fencing_token"]),
                fence_tx_ts=tx_ts, fence_tx_source=tx_source,
                signals=[], outcome="already_dead", adopted=False,
                detect_mono=detect_mono,
                gate=gate, store=store, report=report,
                report_key="already_dead")
            return
        except _FenceRefused as e:
            # Identity unverifiable: fail closed, record, do NOT signal.
            self._record_fence_outcome(
                worker_id=worker_id, proc_id=info.proc_id,
                job_id=info.job_id, pid=pid, pgid=None,
                old_token=info.known_token,
                new_token=int(job["fencing_token"]),
                fence_tx_ts=None, fence_tx_source="refused",
                signals=[], outcome="refused_unsafe_pgid", adopted=False,
                detect_mono=detect_mono, detail=str(e),
                gate=gate, store=store, report=report,
                report_key="already_dead")
            return
        new_token = int(job["fencing_token"])
        tx_ts, tx_source = self._fence_tx_evidence(
            worker_id, info.job_id, info.spawn_store_ts, job, gate)
        signals, group_dead = self._kill_process_group(pgid,
                                                       self._fence_grace_s,
                                                       info.popen)
        term_mono = time.monotonic()
        try:
            returncode = info.popen.wait(timeout=10)
        except subprocess.TimeoutExpired:
            returncode = info.popen.poll()
        reap_mono = time.monotonic()
        self._reap(worker_id, returncode, "killed")
        self._record_fence_outcome(
            worker_id=worker_id, proc_id=info.proc_id, job_id=info.job_id,
            pid=pid, pgid=pgid, old_token=info.known_token,
            new_token=new_token, fence_tx_ts=tx_ts,
            fence_tx_source=tx_source, signals=signals,
            outcome="killed" if group_dead else "signal_failed",
            adopted=False, detect_mono=detect_mono, term_mono=term_mono,
            reap_mono=reap_mono,
            gate=gate, store=store, report=report,
            report_key="fenced_killed")

    def _enforce_fence_adopted(self, worker_id: str, proc_id: str,
                              ad: _AdoptedProc, job: dict,
                              gate: TransitionGate, store,
                              report: dict) -> None:
        key = (worker_id, proc_id)
        with self._procs_lock:
            if key in self._fence_reported:
                return
            self._adopted.pop(key, None)
        detect_mono = time.monotonic()
        try:
            pgid = self._resolve_target_pgid(ad.pid)
        except ProcessLookupError:
            tx_ts, tx_source = self._fence_tx_evidence(
                worker_id, ad.job_id, ad.spawn_store_ts, job, gate)
            self._record_fence_outcome(
                worker_id=worker_id, proc_id=proc_id, job_id=ad.job_id,
                pid=ad.pid, pgid=None,
                old_token=self._prev_token_for(worker_id, ad.job_id,
                                               ad.spawn_store_ts, gate),
                new_token=int(job["fencing_token"]),
                fence_tx_ts=tx_ts, fence_tx_source=tx_source,
                signals=[], outcome="already_dead", adopted=True,
                detect_mono=detect_mono,
                gate=gate, store=store, report=report,
                report_key="already_dead")
            return
        except _FenceRefused as e:
            self._record_fence_outcome(
                worker_id=worker_id, proc_id=proc_id, job_id=ad.job_id,
                pid=ad.pid, pgid=None, old_token=None,
                new_token=int(job["fencing_token"]),
                fence_tx_ts=None, fence_tx_source="refused",
                signals=[], outcome="refused_unsafe_pgid", adopted=True,
                detect_mono=detect_mono, detail=str(e),
                gate=gate, store=store, report=report,
                report_key="already_dead")
            return
        new_token = int(job["fencing_token"])
        old_token = self._prev_token_for(worker_id, ad.job_id,
                                         ad.spawn_store_ts, gate)
        tx_ts, tx_source = self._fence_tx_evidence(
            worker_id, ad.job_id, ad.spawn_store_ts, job, gate)
        signals, group_dead = self._kill_process_group(pgid,
                                                       self._fence_grace_s)
        term_mono = time.monotonic()
        # No Popen handle (not our child): the durable reap milestone pairs
        # with the durable spawn milestone so future restarts do not
        # re-adopt, and the worker identity is retired from the fence.
        gate.append_event(
            "worker.proc_reaped",
            {"worker_id": worker_id, "proc_id": proc_id, "pid": ad.pid,
             "job_id": ad.job_id, "returncode": None, "signal": None,
             "how": "fenced", "adopted": True, "signals": signals,
             "reaped_at": store.current_time()}, self.actor)
        try:
            self._drive_worker(worker_id, "DEAD")
        except (TransitionRejected, StoreError, sqlite3.Error):
            pass
        self._record_fence_outcome(
            worker_id=worker_id, proc_id=proc_id, job_id=ad.job_id,
            pid=ad.pid, pgid=pgid, old_token=old_token,
            new_token=new_token, fence_tx_ts=tx_ts,
            fence_tx_source=tx_source, signals=signals,
            outcome="killed" if group_dead else "signal_failed",
            adopted=True, detect_mono=detect_mono, term_mono=term_mono,
            gate=gate, store=store, report=report,
            report_key="fenced_killed")

    def _record_fence_outcome(self, *, worker_id: str, proc_id: str,
                              job_id: str | None, pid: int | None,
                              pgid: int | None, old_token, new_token,
                              fence_tx_ts, fence_tx_source: str,
                              signals: list, outcome: str, adopted: bool,
                              detect_mono: float, term_mono=None,
                              reap_mono=None, detail=None,
                              gate: TransitionGate, store, report: dict,
                              report_key: str) -> None:
        """Append the worker.fence_enforced ledger event — exactly once per
        (worker_id, proc_id). D4 item 7 evidence fields plus the monotonic
        breakdown (detect/term/reap) the timing test reports."""
        key = (worker_id, proc_id)
        with self._procs_lock:
            if key in self._fence_reported:
                return
            self._fence_reported.add(key)
        death_ts = store.current_time()
        within_bound = None
        consumed_s = None
        if fence_tx_ts is not None:
            within_bound = ((death_ts - fence_tx_ts)
                            <= self._heartbeat_interval_s)
            consumed_s = max(0.0, death_ts - fence_tx_ts)
        payload = {
            "worker_id": worker_id, "proc_id": proc_id, "job_id": job_id,
            "pid": pid, "pgid": pgid,
            "old_token": old_token, "new_token": new_token,
            "fence_tx_ts": fence_tx_ts, "fence_tx_source": fence_tx_source,
            "signals": signals, "death_ts": death_ts,
            "within_bound": within_bound,
            "fenced_out_worker_seconds": consumed_s,
            "outcome": outcome,  # killed | already_dead |
                                 # refused_unsafe_pgid | signal_failed
            "adopted": adopted,
            "detect_mono": detect_mono, "term_mono": term_mono,
            "reap_mono": reap_mono,
            "actor": self.actor,
        }
        if detail:
            payload["detail"] = detail
        gate.append_event("worker.fence_enforced", payload, self.actor)
        report[report_key].append({"worker_id": worker_id, "proc_id": proc_id,
                                   "outcome": outcome, "signals": signals,
                                   "within_bound": within_bound})

    # ------------------------------------------------- sweep: signalling
    def _resolve_target_pgid(self, pid: int) -> int:
        """Return the pgid that is safe to signal for a worker pid.

        Guards: the child was spawned with start_new_session=True, so it is
        a session leader and pgid == pid. Refuse anything else — in
        particular, NEVER signal the supervisor's own process group.
        Raises _FenceRefused on unsafe identity, ProcessLookupError when
        the process is already gone.
        """
        pgid = os.getpgid(pid)  # raises ProcessLookupError when gone
        if pgid == os.getpgid(0):
            raise _FenceRefused("target pgid is the supervisor's own group")
        if pgid != pid:
            raise _FenceRefused(
                f"pid {pid} is not a session leader (pgid {pgid})")
        return pgid

    def _kill_process_group(self, pgid: int,
                            grace_s: float,
                            popen=None) -> tuple[list, bool]:
        """Dedicated process-group termination primitive.

        SIGTERM the whole group, wait grace_s for group death, escalate to
        SIGKILL on survival. Never raises for an already-gone group.
        Returns (signals_sent, group_dead).
        """
        signals: list = []
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            return signals, True
        except PermissionError:
            return signals, False
        signals.append("SIGTERM")
        if self._await_group_death(pgid, grace_s, popen):
            return signals, True
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            return signals, True
        except PermissionError:
            return signals, False
        signals.append("SIGKILL")
        # After SIGKILL death is near-immediate; bound the confirmation so
        # an unkillable group cannot stall the sweep.
        dead = self._await_group_death(pgid, min(grace_s, 5.0), popen)
        return signals, dead

    def _await_group_death(self, pgid: int, timeout_s: float,
                           popen=None) -> bool:
        """Poll until the process group is dead (or timeout).

        A group counts as dead when killpg(pgid, 0) fails OR when no live
        (non-zombie) member remains: a dead-but-unreaped child is still
        visible to killpg, so without the zombie-aware check the sweep
        would wrongly escalate to SIGKILL after a clean SIGTERM death.
        A supplied Popen handle is polled each iteration so the direct
        child's zombie is reaped promptly by its parent (this supervisor).
        Abort early when the supervisor is shutting down.
        """
        end = time.monotonic() + timeout_s
        while True:
            if popen is not None:
                try:
                    popen.poll()
                except Exception:
                    pass
            try:
                os.killpg(pgid, 0)
            except ProcessLookupError:
                return True
            except PermissionError:
                return False  # exists but unsignalable: treat as alive
            if not self._pgid_has_live_member(pgid):
                return True
            if self._sweep_stop.is_set():
                return False
            if time.monotonic() >= end:
                return False
            time.sleep(0.02)

    @staticmethod
    def _pgid_has_live_member(pgid: int) -> bool:
        """True if any non-zombie process remains in the process group.

        Fail closed: if /proc cannot be read, assume the group is alive
        so the sweep escalates rather than declaring victory blindly.
        """
        try:
            candidates = os.listdir("/proc")
        except OSError:
            return True
        for pid_s in candidates:
            if not pid_s.isdigit():
                continue
            try:
                with open(f"/proc/{pid_s}/stat", "rb") as f:
                    data = f.read()
            except OSError:
                continue
            try:
                # pid (comm) state ppid pgrp session ...
                parts = data.rsplit(b")", 1)[1].split()
                state = parts[0]
                pgrp = int(parts[2])
            except (IndexError, ValueError):
                continue
            if pgrp != pgid:
                continue
            if state in (b"Z", b"X", b"x"):
                continue
            return True
        return False

    # --------------------------------------- sweep: observation failures
    def _note_observation_failure(self, key: tuple[str, str], worker_id: str,
                                  proc_id: str, gate: TransitionGate,
                                  report: dict, error: str) -> None:
        """Fail closed on unreadable authoritative state: no kill is
        attempted; one ledger event per failure episode (not per tick)."""
        with self._procs_lock:
            first = key not in self._obs_failed
            if first:
                self._obs_failed.add(key)
        report["observation_failures"].append(
            {"worker_id": worker_id, "proc_id": proc_id, "error": error})
        if first:
            try:
                gate.append_event(
                    "worker.fence_sweep_observation_failed",
                    {"worker_id": worker_id, "proc_id": proc_id,
                     "error": error}, self.actor)
            except (StoreError, sqlite3.Error):
                pass  # the store is what's failing; don't recurse into it

    def _clear_observation_failure(self, key: tuple[str, str]) -> None:
        with self._procs_lock:
            self._obs_failed.discard(key)

    # ------------------------------------------------- sweep: adoption
    def _apply_boot_disposition(self, spawn: dict, keep: bool) -> None:
        """Phase 1C R6: reconcile the adoption registry with one boot
        disposition. ADOPT/FENCE records stay registered for R2
        containment/enforcement; every other disposition is removed so the
        sweep can never signal a dead, reused, orphaned, or uncertain PID.
        """
        key = (spawn.get("worker_id"), spawn.get("proc_id"))
        with self._procs_lock:
            if keep:
                self._adopted[key] = _AdoptedProc(
                    worker_id=spawn.get("worker_id"),
                    proc_id=spawn.get("proc_id"),
                    job_id=spawn.get("job_id"),
                    pid=spawn.get("pid"),
                    spawn_store_ts=spawn.get("spawn_ts") or 0.0)
            else:
                self._adopted.pop(key, None)

    def _adopt_orphaned_procs(self) -> None:
        """Adopt possibly-live worker processes from durable spawn evidence.

        After a supervisor restart the in-memory Popen handles are gone.
        The worker.proc_spawned ledger (paired with worker.proc_reaped) is
        the durable record of which processes may still be alive. Each
        candidate is identity-verified before adoption; anything
        unverifiable is left alone (fail closed).
        """
        try:
            spawns = self.gate.unreaped_proc_spawns()
        except (StoreError, sqlite3.Error):
            return  # fail closed: no adoption without durable evidence
        for s in spawns:
            pid = s.get("pid")
            if not isinstance(pid, int):
                continue
            if not self._verify_adopted_identity(pid, s["spawn_ts"]):
                continue
            key = (s["worker_id"], s["proc_id"])
            with self._procs_lock:
                self._adopted[key] = _AdoptedProc(
                    worker_id=s["worker_id"], proc_id=s["proc_id"],
                    job_id=s.get("job_id"), pid=pid,
                    spawn_store_ts=s["spawn_ts"])

    def _verify_adopted_identity(self, pid: int, spawn_ts: float) -> bool:
        """True iff pid is provably the spawned worker's process: alive, a
        session leader (pgid == pid, as start_new_session guarantees), not
        our own group, and not a PID reuse (process start must predate the
        durable spawn milestone)."""
        try:
            os.kill(pid, 0)
            pgid = os.getpgid(pid)
        except (ProcessLookupError, PermissionError):
            return False
        if pgid != pid or pgid == os.getpgid(0):
            return False
        try:
            started = _proc_start_wall_time(pid)
        except Exception:
            return False
        return started <= spawn_ts + 60.0

    # -------------------------------------------------------------- restart
    def restart_worker(self, worker_id: str, job_id: str,
                       behavior: dict | None = None,
                       ttl_s: float = 60.0,
                       hb_interval_s: float = 0.5,
                       renew: bool = True) -> str:
        """Explicitly commanded restart: replace the process instance with a
        NEW proc_id. Safe under duplicate delivery. The new process must
        claim a fresh lease — authority is never inherited from the old
        process instance, even with the same worker_id.

        Raises if the worker identity is terminal (DEAD/RETIRED)."""
        self._require_boot_ready()
        row = self._read_worker(worker_id)
        if row["status"] in TERMINAL_WORKER_STATES:
            raise TransitionRejected(
                f"worker {worker_id} is {row['status']}: cannot restart a"
                " terminal identity; use a new worker_id")
        # Reap the old process instance as an unexpected death (SUSPECT if it
        # held a job) — then spawn the replacement. No authority transfers.
        self._terminate_tracked(worker_id, timeout=3.0, how="unexpected")
        return self.start_worker(worker_id, job_id, behavior, ttl_s,
                                 hb_interval_s, renew)

    # ---------------------------------------------------------- reconstruct
    def reconstruct(self) -> dict:
        """Rebuild the worker/process/job/lease view from the STORE ONLY.

        Uses the read-only connection; never touches self._procs or any
        other supervisor memory. After a supervisor crash, a fresh
        Supervisor built on the same db file reproduces this view, proving
        supervisor memory was never the source of truth.

        pid liveness is checked with kill(pid, 0) and reported as a HINT
        only (pids can be reused) — never as authority.
        """
        conn = self._store_for_thread().conn  # read-only by construction
        workers = []
        for w in conn.execute(
                "SELECT * FROM workers ORDER BY worker_id").fetchall():
            w = dict(w)
            wid = w["worker_id"]
            hb = conn.execute(
                "SELECT * FROM heartbeats WHERE worker_id=?"
                " ORDER BY seq DESC LIMIT 1", (wid,)).fetchone()
            spawned = conn.execute(
                "SELECT payload, ts FROM ledger WHERE event_type=?"
                " AND json_extract(payload,'$.worker_id')=?"
                " ORDER BY seq DESC LIMIT 1",
                ("worker.proc_spawned", wid)).fetchone()
            reaped = conn.execute(
                "SELECT payload, ts FROM ledger WHERE event_type=?"
                " AND json_extract(payload,'$.worker_id')=?"
                " ORDER BY seq DESC LIMIT 1",
                ("worker.proc_reaped", wid)).fetchone()
            job = conn.execute(
                "SELECT job_id, status, owner_worker_id, fencing_token,"
                " lease_acquired_at, lease_expires_at, progress_done,"
                " commit_outcome FROM jobs WHERE owner_worker_id=?"
                " ORDER BY updated_at DESC LIMIT 1", (wid,)).fetchone()
            pid = None
            if spawned:
                try:
                    pid = json.loads(spawned["payload"]).get("pid")
                except (ValueError, TypeError):
                    pid = None
            os_reports_alive: str = "unknown"
            if pid:
                try:
                    os.kill(pid, 0)
                    os_reports_alive = "yes"
                except ProcessLookupError:
                    os_reports_alive = "no"
                except PermissionError:
                    os_reports_alive = "unknown"
            workers.append({
                "worker_id": wid,
                "worker_status": w["status"],
                "last_seen_at": w["last_seen_at"],
                "latest_heartbeat": dict(hb) if hb else None,
                "last_spawn": json.loads(spawned["payload"]) if spawned
                else None,
                "last_reap": json.loads(reaped["payload"]) if reaped
                else None,
                "pid": pid,
                "os_reports_alive": os_reports_alive,
                "job": dict(job) if job else None,
            })
        return {
            "reconstructed_from": "store only (read-only connection)",
            "supervisor_memory_used": False,
            "workers": workers,
        }

    # ------------------------------------------------------------------ end
    def close(self) -> None:
        """Stop the fence-sweep loop and close all store handles.

        OS processes are NOT touched — they are real processes, not
        supervisor-owned objects. A restarted supervisor re-discovers
        possibly-live workers from durable spawn evidence and enforces
        fencing itself.
        """
        self._sweep_stop.set()
        t = self._sweep_thread
        if (t is not None and t.is_alive()
                and threading.get_ident() != t.ident):
            t.join(timeout=10)
        with self._thread_lock:
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

    # context-manager convenience for tests
    def __enter__(self) -> "Supervisor":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
