"""Phase 1B — deterministic synthetic executor.

Runs INSIDE the worker process. It performs no real work: its purpose is
fault injection for the execution substrate. It never uses LLMs, external
APIs, network services, or real production side effects, and it uses no
randomness at all — every behavior is a deterministic function of its spec.

Behavior kinds:
- success_immediate: heartbeat once, report full progress, return SUCCESS.
- success_delayed:   N deterministic progress steps over duration_s, then SUCCESS.
- controlled_failure: deterministic partial progress, then FAILURE with a reason.
- heartbeat_loop:    heartbeat for duration_s, no progress, no commit
                     (worker exits without committing; authority untouched).
- hang:              heartbeat forever until killed (tests SIGKILL/timeout paths).
- crash:             progress, then os._exit(3) — no commit, non-zero exit.
- crash_after_stage: stage + begin_commit + verify_artifact (the gate
                     records the validator receipts itself), then os._exit(3)
                     BEFORE commit_artifact — the bytes are durable and
                     VALIDATED, and the job is left for the UNCERTAIN
                     resolver (adopt when the staged bytes verify, requeue
                     when they do not).
                     With corrupt_staged_bytes=True the staged file is
                     overwritten with garbage AFTER verification, so the
                     resolver must take the requeue path.
- sigkill_self:      progress, then SIGKILL itself mid-execution.
- expire_then_commit: sleep past the lease TTL (heartbeats do NOT renew),
                     then return SUCCESS so the worker attempts a commit
                     that the gate must reject. REQUIRES the worker to run
                     with lease renewal disabled (--no-renew): it tests
                     exactly the "worker did not renew" path.

Fencing response: if a heartbeat or progress update raises LeaseError, the
executor raises FencedError immediately — the worker has lost authority and
must stop executing. The AUTHORITY decision always lives in the gate's
atomic commit; stopping early is just the worker being polite.
"""
from __future__ import annotations

import os
import signal
import time
from dataclasses import dataclass, field

from ..store import TransitionGate, LeaseError
from .identity import LeaseRef

VALID_KINDS = (
    "success_immediate",
    "success_delayed",
    "controlled_failure",
    "heartbeat_loop",
    "hang",
    "wedged",
    "wedged_ignore_sigterm",
    "wedged_spawn_child",
    "crash",
    "crash_after_stage",
    "sigkill_self",
    "expire_then_commit",
)


class FencedError(Exception):
    """The worker lost lease authority mid-execution (stale token / expired /
    not owner). Execution must stop; only a fresh claim can re-authorize."""


class InterruptedExecution(Exception):
    """SIGTERM arrived: stop promptly WITHOUT committing. A terminated
    process is evidence about the process only — never job success/failure."""


@dataclass
class BehaviorSpec:
    kind: str
    duration_s: float = 1.0
    steps: int = 4
    progress_total: float = 100.0
    fail_reason: str = "synthetic controlled failure"
    sleep_s: float = 0.0  # expire_then_commit: how long to sleep past TTL
    # crash_after_stage only:
    staged_data: str | None = None  # bytes to stage (utf-8); default is
    # deterministic from the job id
    corrupt_staged_bytes: bool = False  # overwrite the staged file with
    # garbage after staging (the corruption injection)

    def __post_init__(self) -> None:
        if self.kind not in VALID_KINDS:
            raise ValueError(f"unknown behavior kind {self.kind!r}")
        if self.duration_s < 0 or self.sleep_s < 0:
            raise ValueError("durations must be non-negative")
        if self.steps < 1:
            raise ValueError("steps must be >= 1")

    @classmethod
    def from_dict(cls, d: dict) -> "BehaviorSpec":
        return cls(
            kind=d["kind"],
            duration_s=float(d.get("duration_s", 1.0)),
            steps=int(d.get("steps", 4)),
            progress_total=float(d.get("progress_total", 100.0)),
            fail_reason=str(d.get("fail_reason", "synthetic controlled failure")),
            sleep_s=float(d.get("sleep_s", 0.0)),
            staged_data=(None if d.get("staged_data") is None
                         else str(d.get("staged_data"))),
            corrupt_staged_bytes=bool(d.get("corrupt_staged_bytes", False)),
        )


@dataclass
class SyntheticExecutor:
    """Deterministic fake workload executed by a worker holding a lease."""
    gate: TransitionGate
    worker_id: str
    proc_id: str
    lease: LeaseRef
    behavior: BehaviorSpec
    hb_interval_s: float = 0.5
    actor: str = ""
    stop_flag: object = None  # threading.Event set by the SIGTERM handler

    _hb_seq: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self.actor = self.actor or f"worker:{self.worker_id}"

    # ------------------------------------------------------------ primitives
    def _check_stop(self) -> None:
        if self.stop_flag is not None and self.stop_flag.is_set():
            raise InterruptedExecution("SIGTERM received")

    def _sleep(self, seconds: float) -> None:
        """Interruptible sleep in small ticks (SIGTERM responsiveness)."""
        end = time.monotonic() + seconds
        while True:
            self._check_stop()
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.05, remaining))

    def _heartbeat(self, state: str, operation: str) -> None:
        try:
            self.gate.ingest_heartbeat(
                worker_id=self.worker_id,
                proc_id=self.proc_id,
                job_id=self.lease.job_id,
                fencing_token=self.lease.fencing_token,
                hb_seq=self._hb_seq,
                worker_state=state,
                current_operation=operation,
                actor=self.actor,
                worker_reported_ts=time.time(),  # informational only
            )
        except LeaseError as e:
            raise FencedError(str(e)) from e
        self._hb_seq += 1

    def _progress(self, done: float) -> None:
        try:
            self.gate.update_job_progress(
                self.lease.job_id, self.worker_id,
                self.lease.fencing_token, done,
                self.behavior.progress_total, self.actor)
        except LeaseError as e:
            raise FencedError(str(e)) from e

    def _evidence(self, **extra) -> dict:
        return {
            "behavior_kind": self.behavior.kind,
            "worker_id": self.worker_id,
            "proc_id": self.proc_id,
            "job_id": self.lease.job_id,
            "lease_id": self.lease.lease_id,
            "fencing_token": self.lease.fencing_token,
            **extra,
        }

    # ------------------------------------------------------------------ run
    def run(self) -> tuple[str, dict]:
        """Execute the behavior. Returns (outcome, evidence) where outcome is
        SUCCESS / FAILURE / NO_COMMIT. May raise FencedError,
        InterruptedExecution, or terminate the process (crash/sigkill)."""
        kind = self.behavior.kind
        if kind == "success_immediate":
            return self._do_success_immediate()
        if kind == "success_delayed":
            return self._do_delayed("SUCCESS")
        if kind == "controlled_failure":
            return self._do_delayed("FAILURE")
        if kind == "heartbeat_loop":
            return self._do_heartbeat_loop()
        if kind == "hang":
            return self._do_hang()
        if kind == "wedged":
            return self._do_wedged()
        if kind == "wedged_ignore_sigterm":
            return self._do_wedged_ignore_sigterm()
        if kind == "wedged_spawn_child":
            return self._do_wedged_spawn_child()
        if kind == "crash":
            return self._do_crash()
        if kind == "crash_after_stage":
            return self._do_crash_after_stage()
        if kind == "sigkill_self":
            return self._do_sigkill_self()
        if kind == "expire_then_commit":
            return self._do_expire_then_commit()
        raise AssertionError(f"unhandled kind {kind}")  # validated in spec

    # --------------------------------------------------------------- kinds
    def _do_success_immediate(self) -> tuple[str, dict]:
        self._heartbeat("RUNNING", "success_immediate")
        self._progress(self.behavior.progress_total)
        return "SUCCESS", self._evidence(steps_completed=1)

    def _do_delayed(self, outcome: str) -> tuple[str, dict]:
        per_step = self.behavior.duration_s / self.behavior.steps
        for i in range(1, self.behavior.steps + 1):
            self._check_stop()
            self._sleep(per_step)
            done = self.behavior.progress_total * i / self.behavior.steps
            self._heartbeat("RUNNING", f"{self.behavior.kind} step {i}/{self.behavior.steps}")
            # controlled_failure stops progress early: deterministic partial.
            if outcome == "FAILURE" and i == self.behavior.steps // 2 + 1:
                self._progress(done)
                return "FAILURE", self._evidence(
                    reason=self.behavior.fail_reason,
                    steps_completed=i,
                    progress_done=done)
            self._progress(done)
        ev = self._evidence(steps_completed=self.behavior.steps)
        if outcome == "FAILURE":
            ev["reason"] = self.behavior.fail_reason
        return outcome, ev

    def _do_heartbeat_loop(self) -> tuple[str, dict]:
        """Heartbeat for duration_s, then return WITHOUT committing."""
        end = time.monotonic() + self.behavior.duration_s
        n = 0
        while time.monotonic() < end:
            self._check_stop()
            self._heartbeat("RUNNING", "heartbeat_loop")
            n += 1
            self._sleep(self.hb_interval_s)
        return "NO_COMMIT", self._evidence(heartbeats_sent=n)

    def _do_hang(self) -> tuple[str, dict]:
        """Heartbeat forever; only an external kill ends this."""
        n = 0
        while True:
            self._check_stop()
            self._heartbeat("HANGING", "hang")
            n += 1
            self._sleep(self.hb_interval_s)
        # unreachable; present for type clarity
        return "NO_COMMIT", self._evidence(heartbeats_sent=n)  # pragma: no cover

    def _do_wedged(self) -> tuple[str, dict]:
        """Hold the process open with NO further gate interaction.

        Models a worker that stopped cooperating (wedged loop, lost thread,
        hostile code): it never heartbeats, so it never notices a fencing
        transaction itself and never exits on its own. Only the
        supervisor's enforced process-group fence can contain it.
        Responds to SIGTERM via _check_stop (graceful path).
        """
        while True:
            self._check_stop()
            time.sleep(0.05)
        return "NO_COMMIT", self._evidence()  # pragma: no cover

    def _do_wedged_ignore_sigterm(self) -> tuple[str, dict]:
        """Like wedged, but SIGTERM is ignored: only SIGKILL ends this."""
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        while True:
            time.sleep(0.05)
        return "NO_COMMIT", self._evidence()  # pragma: no cover

    def _do_wedged_spawn_child(self) -> tuple[str, dict]:
        """Wedged, but first spawns a child that spawns a grandchild.

        Exercises process-GROUP termination: the whole tree shares the
        worker's process group and all of it must die on enforcement.
        """
        import subprocess
        import sys
        grandchild = "import time; time.sleep(900)"
        child = ("import subprocess, sys, time; "
                 f"subprocess.Popen([sys.executable, '-c', {grandchild!r}]); "
                 "time.sleep(900)")
        subprocess.Popen([sys.executable, "-c", child])
        return self._do_wedged()

    def _do_crash(self) -> tuple[str, dict]:
        self._heartbeat("RUNNING", "crash: before")
        self._progress(self.behavior.progress_total / 2)
        os._exit(3)  # no commit, non-zero exit; finally blocks do NOT run

    def _do_crash_after_stage(self) -> tuple[str, dict]:
        """Stage artifact bytes (the gate fsyncs them), then die BEFORE
        commit. The staged bytes are durable; the job is left for the
        UNCERTAIN resolver — adopt when the staged bytes verify, requeue
        when they do not."""
        self._heartbeat("RUNNING", "crash_after_stage: before stage")
        self._progress(self.behavior.progress_total / 2)
        job = self.gate.get_job(self.lease.job_id)
        if self.behavior.staged_data is not None:
            data = self.behavior.staged_data.encode("utf-8")
        else:
            data = b"fi-artifact-" + self.lease.job_id.encode("utf-8")
        artifact = self.gate.stage_artifact(
            job_id=self.lease.job_id, worker_id=self.worker_id,
            fencing_token=self.lease.fencing_token,
            task_id=job["task_id"], kind="test", data=data,
            actor=self.actor)
        # Verify BEFORE the crash: the gate re-reads the staged bytes,
        # recomputes the hash, runs the structural checks, and records
        # the validator receipts itself. The artifact is VALIDATED (not
        # merely staged), so the UNCERTAIN resolver can adopt it via
        # commit_artifact() — or refuse it when the bytes do not verify.
        self.gate.begin_commit(
            self.lease.job_id, self.worker_id, self.lease.fencing_token,
            artifact_id=artifact["artifact_id"], actor=self.actor)
        self.gate.verify_artifact(
            artifact["artifact_id"], actor=self.actor,
            worker_id=self.worker_id,
            fencing_token=self.lease.fencing_token,
            job_id=self.lease.job_id)
        if self.behavior.corrupt_staged_bytes:
            # The corruption injection: the artifact row is VALIDATED for
            # the staged bytes, but the bytes on disk no longer match —
            # the resolver must take the requeue path.
            with open(artifact["uri"], "wb") as f:
                f.write(b"corrupted-bytes-fi2")
                f.flush()
                os.fsync(f.fileno())
        os._exit(3)  # staged+verified bytes are durable; no commit follows

    def _do_sigkill_self(self) -> tuple[str, dict]:
        self._heartbeat("RUNNING", "sigkill_self: before")
        self._progress(self.behavior.progress_total / 2)
        os.kill(os.getpid(), signal.SIGKILL)
        raise AssertionError("SIGKILL did not kill us")  # pragma: no cover

    def _do_expire_then_commit(self) -> tuple[str, dict]:
        """Sleep past the lease TTL — heartbeats must NOT renew authority —
        then return SUCCESS so the worker attempts the commit the gate must
        reject (Scenario B). No heartbeat after waking: the commit attempt
        itself is the fencing test."""
        self._heartbeat("RUNNING", "expire_then_commit: sleeping past TTL")
        self._sleep(self.behavior.sleep_s)
        return "SUCCESS", self._evidence(slept_s=self.behavior.sleep_s)
