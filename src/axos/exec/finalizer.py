"""Phase 1C R13 — release finalization controller.

The finalizer is the reader/evaluator half of R13's finalization. It owns
NO execution authority and never mutates jobs, tasks, leases, workers,
processes, artifacts, checkpoints, recovery budgets, recovery rungs,
desired state, or breakers. Its only writes go through the gate's
finalization API (begin/evaluate/publish on finalization_runs rows, plus
the deterministic finalization task row and the R5 release checkpoint
those ops stage), and it never touches a database connection or raw SQL
directly (F1: exec/ is forbidden from SQL; the authority audit greps for
this).

The canonical finalization identities and the release-manifest hashing
are re-exported from the gate (one definition — this module defines
nothing of its own).

One evaluate_once() pass does four things, in order:

1. Boot gate: no finalization before boot recovery reaches READY (R6 —
   same pattern as the scheduler).
2. Begin: the current desired-state head's generation gets its
   finalization run (idempotent — re-begins converge).
3. Evaluate: every non-finalized run is re-evaluated from durable
   evidence (CAS on version; the loser re-reads, never overwrites).
4. Publish: a run that evaluates READY is published — the gate
   re-validates everything authoritatively inside ONE write_txn and
   flips it to FINALIZED atomically, or refuses.

No-op discipline: a pass over an already-FINALIZED generation performs
zero writes. The finalizer never clears a human gate, never completes a
job, never moves a breaker.
"""
from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass

from axos.store import (
    open_store, migrate, TransitionGate, FinalizationConflict,
    canonical_release_generation, canonical_finalization_id,
    build_finalization_manifest, finalization_manifest_hash,
)
from axos.store.db import Store, StoreError, TransitionRejected

__all__ = ["FinalizationConfig", "Finalizer",
           "canonical_release_generation", "canonical_finalization_id",
           "build_finalization_manifest", "finalization_manifest_hash"]


@dataclass(frozen=True)
class FinalizationConfig:
    """Validated, frozen configuration for the finalizer.

    poll_interval_s: background loop cadence (seconds).
    batch_size: max runs re-evaluated/published per pass.
    actor: the actor string recorded on finalization ledger events
        (never a worker actor).
    """
    poll_interval_s: float = 30.0
    batch_size: int = 10
    actor: str = "finalizer"

    def __post_init__(self) -> None:
        v = self.poll_interval_s
        if (isinstance(v, bool) or not isinstance(v, (int, float))
                or not math.isfinite(v) or not v > 0):
            raise ValueError(
                "FinalizationConfig.poll_interval_s must be a positive"
                f" finite number of seconds, got {v!r}")
        v = self.batch_size
        if isinstance(v, bool) or not isinstance(v, int) or v < 1:
            raise ValueError(
                "FinalizationConfig.batch_size must be an integer >= 1,"
                f" got {v!r}")
        if not isinstance(self.actor, str) or not self.actor:
            raise ValueError(
                "FinalizationConfig.actor must be a non-empty string")
        if self.actor.startswith("worker:"):
            raise ValueError(
                "FinalizationConfig.actor must not start with 'worker:':"
                " worker actors cannot own controller authority")


class Finalizer:
    """R13 release finalization controller, runnable in a background
    thread or driven synchronously via evaluate_once(). Reads evidence
    through the gate's read-only methods; writes only through the gate's
    finalization API. Never creates/claims/completes/transitions jobs,
    never fences/reclaims workers, never selects rungs, never manages
    budgets, never mutates desired state, never moves breakers, never
    clears human gates."""

    def __init__(self, db_path: str, supervisor, config: FinalizationConfig,
                 *, boot_ready=None) -> None:
        if not isinstance(config, FinalizationConfig):
            raise TypeError(
                "config must be a FinalizationConfig with explicit values"
                " (poll_interval_s, batch_size, actor),"
                f" got {type(config).__name__}")
        self.db_path = db_path
        self.supervisor = supervisor
        self.config = config
        self.actor = config.actor
        # Same-package seam (documented), exactly like the scheduler's:
        # an explicit callable wins; otherwise the supervisor's
        # boot-recovery gate is consulted. No finalization before
        # boot_recover() reaches READY (R6): finalizing from pre-boot
        # evidence could fork authority. With neither (no supervisor),
        # there is no boot gate to consult, so evaluation proceeds —
        # embedding contexts that need a gate must pass boot_ready.
        if boot_ready is not None:
            self._boot_ready = boot_ready
        elif supervisor is not None:
            self._boot_ready = supervisor._boot_ready
        else:
            self._boot_ready = lambda: True  # noqa: E731
        self.store: Store = open_store(db_path)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        # --- R7/R8/R9/R12 thread-local pattern, exactly: sqlite3
        # connections are thread-bound; the background loop thread gets
        # its own Store/TransitionGate. The constructing thread keeps
        # self.store/self.gate.
        self._owner_thread = threading.get_ident()
        self._thread_lock = threading.RLock()
        self._thread_stores: dict[int, Store] = {}
        self._thread_gates: dict[int, TransitionGate] = {}
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_error: dict | None = None  # informational only

    # --------------------------------------- thread-local store/gate access
    def _store_for_thread(self) -> Store:
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
        self._store_for_thread()  # ensures the pair exists
        tid = threading.get_ident()
        if tid == self._owner_thread:
            return self.gate
        with self._thread_lock:
            return self._thread_gates[tid]

    # ------------------------------------------------------------ one pass
    def evaluate_once(self) -> dict:
        """One synchronous evaluation pass: the testable core.

        Returns a deterministic report dict::

            {"runs_seen": int, "runs_begun": [release_generation],
             "evaluated": [{"release_generation", "state", "blockers"}],
             "finalized": [{"release_generation", "manifest_hash",
                            "checkpoint_id"}],
             "errors": [{"scope", "phase", "error"}],
             "detail": str}

        `detail` is "boot-not-ready" when boot recovery has not reached
        READY (zero counts, no mutation), "ok" otherwise. A pass with
        nothing to do performs zero writes. Per-run failures are
        collected into `errors` and never abort the pass; a lost CAS is
        reported (not retried) — the next pass re-reads. Worker-actor
        configuration fails the pass loudly, not silently.
        """
        gate = self._gate_for_thread()
        report: dict = {"runs_seen": 0, "runs_begun": [],
                        "evaluated": [], "finalized": [], "errors": []}
        if not self._boot_ready():
            # No finalization before boot recovery reaches READY: the
            # head generation is not authoritative until then.
            return {**report, "detail": "boot-not-ready"}
        report["detail"] = "ok"
        errors: list[dict] = report["errors"]

        # ---- 1. Begin: the head's generation gets its run (idempotent).
        try:
            head = gate.get_desired_head()
            generation = canonical_release_generation(head["version"])
            gate.begin_finalization_run(generation, head["version"],
                                        self.actor)
            report["runs_begun"].append(generation)
        except Exception as exc:  # never abort the pass
            errors.append({"scope": "begin", "phase": "begin",
                           "error": f"{type(exc).__name__}: {exc}"})
            return report

        # ---- 2. Evaluate every non-finalized run; publish READY ones.
        try:
            runs = gate.list_finalization_runs()
        except Exception as exc:
            errors.append({"scope": "list", "phase": "list",
                           "error": f"{type(exc).__name__}: {exc}"})
            return report
        report["runs_seen"] = len(runs)
        for row in runs[:self.config.batch_size]:
            generation = row["release_generation"]
            try:
                if row["state"] == "FINALIZED":
                    # No-op discipline: verifying a finalized record
                    # performs zero writes.
                    gate.evaluate_finalization(generation, row["version"],
                                               self.actor)
                    report["evaluated"].append(
                        {"release_generation": generation, "state":
                         "FINALIZED", "blockers": []})
                    continue
                evaluated = gate.evaluate_finalization(
                    generation, row["version"], self.actor)
                report["evaluated"].append(
                    {"release_generation": generation,
                     "state": evaluated["state"],
                     "blockers": evaluated["blockers"]})
                if evaluated["state"] == "READY":
                    # The CAS loser re-reads on the next pass; it never
                    # overwrites, so publication must not retry here.
                    published = gate.publish_finalization(
                        generation, evaluated["version"],
                        evaluated["manifest_hash"], self.actor)
                    report["finalized"].append(
                        {"release_generation": generation,
                         "manifest_hash": published["manifest_hash"],
                         "checkpoint_id": published["checkpoint_id"]})
            except FinalizationConflict as exc:
                errors.append({"scope": generation, "phase": "cas",
                               "error": f"{type(exc).__name__}: {exc}"})
            except Exception as exc:
                errors.append({"scope": generation, "phase": "evaluate",
                               "error": f"{type(exc).__name__}: {exc}"})
        return report

    # ------------------------------------------------------- lifecycle
    def start(self) -> None:
        """Start the background evaluation loop. Idempotent."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="axos-finalizer",
            daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.evaluate_once()
            except Exception as exc:
                # The loop must never die silently; the last infra error
                # is informational evidence only (never authority).
                self._last_error = {
                    "error": f"{type(exc).__name__}: {exc}",
                    "at": time.time()}
            self._stop_event.wait(self.config.poll_interval_s)

    def stop(self) -> None:
        """Signal the loop to stop and join with a bounded timeout."""
        self._stop_event.set()
        t = self._thread
        if (t is not None and t.is_alive()
                and threading.get_ident() != t.ident):
            t.join(timeout=max(2.0 * self.config.poll_interval_s, 5.0))
        self._thread = None

    def close(self) -> None:
        """Bounded shutdown: stop the loop and release every store handle
        owned by this controller. OS processes are NOT touched."""
        self.stop()
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
