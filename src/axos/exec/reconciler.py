"""Phase 1C R11 — desired-state reconciliation (observe-and-converge).

The reconciler converges actual jobs toward a declared desired set. It is
deliberately the narrowest authority in the system — narrower even than the
scheduler:

- it never creates jobs except through the gate's single idempotent op
  `ensure_job_for_desired_state` (with the literal actor "reconciler");
- it never deletes anything, never moves a job between states, never
  completes, fails, blocks, or quarantines anything;
- it reads only through the gate's read-only methods;
- it owns no execution authority: no process management, no lease
  authority, no artifact or checkpoint authority, no recovery authority.

Every other component is out of reach by construction: this module never
opens a store transaction itself, never touches a database connection
directly, and never calls into the scheduler, policy, or recovery layers.
All writes happen inside `TransitionGate` methods; the reconciler only
decides *which* gate calls to make.

Reconciliation is deterministic: items are processed in sorted
desired_work_id order, in fixed-size batches. The desired-state version is
pinned at the start of a pass; if the head version moves mid-pass the
reconciler stops creating and closes the run as CONFLICT (fail closed).
A retired desired item is recorded as OBSOLETE and left strictly alone —
retirement is a tombstone, not a cancellation: PENDING has no legal cancel
edge in JOB_TRANSITIONS, and the reconciler owns no transition authority
at all.

Diff verdicts (see `diff_item`):

    desired retired                              -> OBSOLETE
    task row missing/unreadable                   -> CONTRADICTORY
    map row present but job row missing           -> CONTRADICTORY
    job present but map row missing               -> CONTRADICTORY
    map row's job_id != job's job_id              -> CONTRADICTORY
    desired spec hash != map row's spec_hash      -> CONTRADICTORY
    (spec drifted under an existing mapping)
    no job row                                   -> DESIRED_MISSING
    job PENDING                                  -> DESIRED_ALREADY_PRESENT
    job CLAIMED/RUNNING/COMMITTING/VERIFYING     -> DESIRED_ACTIVE
    job COMPLETE                                 -> DESIRED_COMPLETE
    job FAILED                                   -> DESIRED_FAILED
    job BLOCKED                                  -> DESIRED_BLOCKED
    job UNCERTAIN                                -> DESIRED_UNCERTAIN
    any other job status (e.g. QUARANTINED)      -> CONTRADICTORY

Only DESIRED_MISSING causes a gate call (and only when the task exists and
is not PAUSED_FOR_HUMAN — I-17: a human paused that scope, and the
reconciler never creates work under it). Every other verdict is recorded
as a discrepancy and nothing else happens.
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass

from axos.store import open_store, migrate, TransitionGate
from axos.store.db import (Store, StoreError, TransitionRejected,
                           DesiredStateConflict, StorageEngineError)
from axos.store.gate import (_canonical_desired_job_id,
                              _desired_snapshot_hash, _desired_spec_hash,
                              _normalize_desired_spec)

__all__ = ["ReconcilerConfig", "Reconciler", "canonical_job_id", "diff_item"]


def canonical_job_id(desired_work_id: str) -> str:
    """Deterministic job identity for a desired-work item — pure.

    Delegates to the gate's canonical construction so the reconciler and
    the gate can never disagree on what the job_id for a desired_work_id
    is: the same construction, by construction."""
    return _canonical_desired_job_id(desired_work_id)


def diff_item(desired: dict, job: dict | None, map_row: dict | None,
              task_missing: bool) -> str:
    """Pure, deterministic diff of one desired item against actual state.

    `desired` is the desired-state row (spec may be the raw JSON string or
    an already-parsed dict); `job` is the jobs row or None; `map_row` is
    the desired_job_map row or None; `task_missing` reports whether the
    referenced task row could not be read. Returns exactly one of the
    verdicts documented in the module docstring. Never raises for
    well-formed inputs; an unparseable or ill-shaped desired spec is
    contradictory actual state (CONTRADICTORY), never silently skipped.
    """
    if not isinstance(desired, dict):
        return "CONTRADICTORY"
    if desired.get("retired"):
        # A retired item is a tombstone: OBSOLETE no matter what the job
        # rows say. The caller records it and moves on — never deletes,
        # never transitions.
        return "OBSOLETE"
    if task_missing:
        return "CONTRADICTORY"
    if map_row is not None and job is None:
        # The map row promises a job that does not exist: contradictory.
        return "CONTRADICTORY"
    if map_row is None and job is not None:
        # A job sits under our canonical identity with no map row tying
        # it to this desired item: never adopt a foreign job.
        return "CONTRADICTORY"
    if map_row is not None:
        if map_row.get("job_id") != job.get("job_id"):
            return "CONTRADICTORY"
        spec = desired.get("spec")
        if isinstance(spec, str):
            try:
                spec = json.loads(spec)
            except ValueError:
                return "CONTRADICTORY"
        try:
            want_hash = _desired_spec_hash(spec)
        except (TransitionRejected, TypeError, AttributeError):
            return "CONTRADICTORY"
        if map_row.get("spec_hash") != want_hash:
            # The desired spec changed under an existing mapping. The
            # deterministic job_id cannot represent two specs; the gate
            # refuses to recreate it (DesiredStateConflict). Recorded,
            # never acted on.
            return "CONTRADICTORY"
    if job is None:
        return "DESIRED_MISSING"
    status = job.get("status")
    if status == "PENDING":
        return "DESIRED_ALREADY_PRESENT"
    if status in ("CLAIMED", "RUNNING", "COMMITTING", "VERIFYING"):
        return "DESIRED_ACTIVE"
    if status == "COMPLETE":
        return "DESIRED_COMPLETE"
    if status == "FAILED":
        return "DESIRED_FAILED"
    if status == "BLOCKED":
        return "DESIRED_BLOCKED"
    if status == "UNCERTAIN":
        return "DESIRED_UNCERTAIN"
    # QUARANTINED and anything outside the lifecycle graph: the
    # reconciler has no verdict for it, so it is contradictory by
    # default — recorded, never acted on.
    return "CONTRADICTORY"


@dataclass(frozen=True)
class ReconcilerConfig:
    """Validated reconciler configuration. `poll_interval_s` is the
    background-loop cadence; `batch_size` bounds how many items are
    processed between durable checkpoints (and between desired-state
    version re-pins). `actor` is provenance for the reconciliation run
    records only — it must be "reconciler" or start with "reconciler:".
    Job creation always passes the literal "reconciler" to the gate,
    because the work-creator check requires exact membership."""
    poll_interval_s: float
    batch_size: int
    actor: str = "reconciler"

    def __post_init__(self) -> None:
        v = self.poll_interval_s
        if isinstance(v, bool) or not isinstance(v, (int, float)) \
                or not v > 0:
            raise ValueError(
                "ReconcilerConfig.poll_interval_s must be a positive number"
                f" of seconds, got {v!r}")
        b = self.batch_size
        if isinstance(b, bool) or not isinstance(b, int) or b < 1:
            raise ValueError(
                "ReconcilerConfig.batch_size must be an integer >= 1,"
                f" got {b!r}")
        a = self.actor
        if (not isinstance(a, str) or not a
                or (a != "reconciler"
                    and not a.startswith("reconciler:"))):
            raise ValueError(
                "ReconcilerConfig.actor must be 'reconciler' or start with"
                f" 'reconciler:', got {a!r}")


class Reconciler:
    """Desired-state convergence, driven synchronously via reconcile() /
    evaluate_once() or in a background thread via start()/stop().

    The reconciler holds a writable Store only to construct its
    TransitionGate — the same shape as the scheduler — and every mutation
    it causes flows through gate methods. It never opens a store
    transaction itself and never touches a connection directly."""

    def __init__(self, db_path: str, config: ReconcilerConfig,
                 supervisor=None) -> None:
        if not isinstance(config, ReconcilerConfig):
            raise TypeError(
                "config must be a ReconcilerConfig with explicit values"
                f" (poll_interval_s, batch_size), got"
                f" {type(config).__name__}")
        self.db_path = db_path
        self.config = config
        self.supervisor = supervisor
        self.store: Store = open_store(db_path)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        # --- thread-local pattern, exactly as the scheduler's: database
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
        """The Store bound to the calling thread (thread-bound database
        connections)."""
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

    # ------------------------------------------------------- one converge pass
    def reconcile(self, desired_version: int | None = None) -> dict:
        """One synchronous converge pass: the testable core.

        Pins the desired-state version (explicit, or the current head),
        verifies the head's snapshot hash over ALL items (retired
        included), then walks the items in sorted desired_work_id order in
        batches, creating PENDING jobs only for DESIRED_MISSING items
        whose task exists and is not PAUSED_FOR_HUMAN. A durable cursor
        is checkpointed after every batch.

        Returns a report dict::

            {"run": finished run row or None, "result": one of
             CONVERGED/CHANGED/BLOCKED/CONFLICT/FAILED,
             "items_examined": int, "items_created": int,
             "discrepancies": [...], "summary": {...}}

        `run` is None when the pass refused before opening a run record
        (stale pin, or an internally contradictory desired state): those
        refusals perform no mutation at all — not even bookkeeping.

        Result precedence: FAILED (a fail-closed error stopped the pass)
        > CONFLICT (the desired version moved mid-pass) > BLOCKED (a task
        under a human gate needed a job) > CHANGED (jobs were created) >
        CONVERGED (nothing to do).
        """
        gate = self._gate_for_thread()
        if desired_version is not None and (
                isinstance(desired_version, bool)
                or not isinstance(desired_version, int)
                or desired_version < 0):
            raise ValueError(
                "desired_version must be an int >= 0 or None,"
                f" got {desired_version!r}")
        head = gate.get_desired_head()
        pinned = head["version"] if desired_version is None \
            else desired_version
        try:
            if head["version"] != pinned:
                # Stale pin: the desired set moved under us. Refuse before
                # opening any run record — zero mutation, not even a run row.
                return self._refusal(
                    "CONFLICT",
                    [{"type": "stale desired-state pin", "pinned": pinned,
                      "head_version": head["version"],
                      "note": "no mutation performed"}])
            rows = gate.list_desired_items(include_retired=True)
            try:
                parsed = [{"id": r["desired_work_id"],
                           "spec": json.loads(r["spec"]),
                           "retired": r["retired"],
                           "version": r["version"]} for r in rows]
            except (ValueError, TypeError, KeyError) as exc:
                return self._refusal(
                    "FAILED",
                    [{"type": "unparseable desired-state row",
                      "error": f"{type(exc).__name__}: {exc}",
                      "note": "no mutation performed"}])
            if _desired_snapshot_hash(parsed) != head["snapshot_hash"]:
                # The head's hash does not match the items it claims to
                # cover: the desired state is internally contradictory.
                # Fail closed — zero mutation.
                return self._refusal(
                    "FAILED",
                    [{"type": "desired-state snapshot mismatch",
                      "head_version": head["version"],
                      "note": "no mutation performed"}])
        except (StoreError, StorageEngineError) as exc:
            # The desired state itself is unreadable (missing tables,
            # storage I/O failure — the engine's own errors are not
            # StoreError subclasses, so StorageEngineError (the store's
            # named engine-error type) is caught here explicitly): fail
            # closed with zero mutation — not even a run record.
            return self._refusal(
                "FAILED",
                [{"type": "unreadable desired state",
                  "error": f"{type(exc).__name__}: {exc}",
                  "note": "no mutation performed"}])
        actives = sorted((r for r in rows if not r["retired"]),
                         key=lambda r: r["desired_work_id"])
        retired = sorted((r for r in rows if r["retired"]),
                         key=lambda r: r["desired_work_id"])
        run = gate.begin_reconciliation_run(pinned, head["snapshot_hash"],
                                            self.config.actor)
        rec_id = run["reconciliation_id"]
        discrepancies: list[dict] = []
        items_examined = 0
        items_created = 0
        batches = 0
        last_item_id: str | None = None
        conflict = False
        blocked = False
        failed = False
        work = ([("active", r) for r in actives]
                + [("retired", r) for r in retired])
        bs = self.config.batch_size
        for start in range(0, len(work), bs):
            batch = work[start:start + bs]
            # Re-pin before every batch: a concurrent set/retire moves
            # the head version, and creating jobs against a stale desired
            # set is forbidden. When the pin is stale we stop creating
            # and close the run as CONFLICT. An unreadable head fails the
            # run closed as FAILED instead.
            try:
                cur_version = gate.get_desired_head()["version"]
            except (StoreError, StorageEngineError) as exc:
                failed = True
                discrepancies.append(
                    {"type": "unreadable desired state",
                     "error": f"{type(exc).__name__}: {exc}",
                     "note": "stopped creating; run closed as FAILED"})
                break
            if cur_version != pinned:
                conflict = True
                discrepancies.append(
                    {"type": "stale desired-state pin", "pinned": pinned,
                     "head_version": cur_version,
                     "note": "stopped creating; run closed as CONFLICT"})
                break
            for kind, row in batch:
                dwid = row["desired_work_id"]
                try:
                    outcome = self._process_item(gate, kind, row, pinned)
                except (StoreError, StorageEngineError) as exc:
                    # Unreadable actual state mid-pass (e.g. the jobs
                    # table vanished under us — the engine's own errors are
                    # not StoreError subclasses, so StorageEngineError is
                    # caught here explicitly): fail closed — stop creating,
                    # close the run as FAILED. Nothing created so far is
                    # rolled back; creation is idempotent, so a later pass
                    # resumes cleanly.
                    failed = True
                    discrepancies.append(
                        {"desired_work_id": dwid, "diff": "unknown",
                         "action": "failed",
                         "error": "unreadable actual state:"
                                  f" {type(exc).__name__}: {exc}"})
                    items_examined += 1
                    last_item_id = dwid
                    break  # fail closed: stop creating
                if outcome["failed"]:
                    failed = True
                    discrepancies.append(outcome["discrepancy"])
                    items_examined += 1
                    last_item_id = dwid
                    break  # fail closed: stop creating
                if outcome["created"]:
                    items_created += 1
                if outcome["blocked"]:
                    blocked = True
                discrepancies.append(outcome["discrepancy"])
                items_examined += 1
                last_item_id = dwid
            batches += 1
            gate.checkpoint_reconciliation_run(
                rec_id, last_item_id, items_examined, items_created)
            if failed:
                break
        if failed:
            result = "FAILED"
        elif conflict:
            result = "CONFLICT"
        elif blocked:
            result = "BLOCKED"
        elif items_created > 0:
            result = "CHANGED"
        else:
            result = "CONVERGED"
        finished = gate.finish_reconciliation_run(
            rec_id, result, items_examined, items_created, discrepancies)
        return {"run": finished, "result": result,
                "items_examined": items_examined,
                "items_created": items_created,
                "discrepancies": discrepancies,
                "summary": {"result": result,
                            "desired_state_version": pinned,
                            "batches": batches,
                            "items_examined": items_examined,
                            "items_created": items_created}}

    @staticmethod
    def _refusal(result: str, discrepancies: list[dict]) -> dict:
        """The zero-mutation report for a pass that refused before opening
        a run record: no run row, no jobs, no checkpoints."""
        return {"run": None, "result": result, "items_examined": 0,
                "items_created": 0, "discrepancies": discrepancies,
                "summary": {"result": result, "run": None,
                            "note": "refused before opening a run record"}}

    # ------------------------------------------------------------- diff help
    @staticmethod
    def _read_job(gate: TransitionGate, job_id: str) -> dict | None:
        """The jobs row for job_id, or None when there is none. A missing
        row is data for the diff, never an error."""
        try:
            return gate.get_job(job_id)
        except (TransitionRejected, StoreError):
            return None

    def _diff_active(self, gate: TransitionGate, row: dict) -> str:
        """Diff one non-retired item. Looks the job up through the
        canonical-identity map (falling back to the deterministic id so a
        job without a map row still surfaces as contradictory)."""
        dwid = row["desired_work_id"]
        map_row = gate.get_desired_job_map(dwid)
        job = (self._read_job(gate, map_row["job_id"]) if map_row
               else self._read_job(gate, canonical_job_id(dwid)))
        task_missing = False
        try:
            spec = row["spec"]
            task_id = (json.loads(spec) if isinstance(spec, str)
                       else spec)["task_id"]
            gate.get_task(task_id)
        except (TransitionRejected, StoreError, ValueError, TypeError,
                KeyError):
            task_missing = True
        return diff_item(row, job, map_row, task_missing)

    def _diff_retired(self, gate: TransitionGate, row: dict) -> str:
        """Diff one retired item: always OBSOLETE. The job (if any) is
        looked up only so the discrepancy record is informative; it is
        never acted on."""
        dwid = row["desired_work_id"]
        map_row = gate.get_desired_job_map(dwid)
        job = (self._read_job(gate, map_row["job_id"]) if map_row
               else self._read_job(gate, canonical_job_id(dwid)))
        return diff_item(row, job, map_row, False)

    def _create_for_missing(self, gate: TransitionGate, row: dict,
                            pinned: int) -> dict:
        """Handle one DESIRED_MISSING item. Returns a small outcome dict;
        the only gate call that can create a job lives here, and it always
        passes the literal "reconciler" actor (the work-creator check
        requires exact membership — config.actor is provenance for run
        records only)."""
        dwid = row["desired_work_id"]
        base = {"desired_work_id": dwid, "diff": "DESIRED_MISSING",
                "created": False, "blocked": False, "failed": False,
                "discrepancy": None}
        try:
            spec = _normalize_desired_spec(
                json.loads(row["spec"]) if isinstance(row["spec"], str)
                else row["spec"])
        except (TransitionRejected, ValueError, TypeError) as exc:
            base["failed"] = True
            base["discrepancy"] = {
                "desired_work_id": dwid, "diff": "DESIRED_MISSING",
                "action": "failed",
                "error": f"unusable desired spec: {type(exc).__name__}:"
                         f" {exc}"}
            return base
        try:
            task_status = gate.get_task(spec["task_id"]).get("status")
        except (TransitionRejected, StoreError):
            # The task vanished between the diff and the creation
            # attempt: the world moved; fail closed.
            base["failed"] = True
            base["discrepancy"] = {
                "desired_work_id": dwid, "diff": "DESIRED_MISSING",
                "action": "failed",
                "error": "task row unreadable at creation time"}
            return base
        if task_status == "PAUSED_FOR_HUMAN":
            # I-17: a human paused that scope; the reconciler never
            # creates work under it. Recorded, never created.
            base["blocked"] = True
            base["discrepancy"] = {
                "desired_work_id": dwid, "diff": "DESIRED_MISSING",
                "action": "blocked",
                "reason": "task PAUSED_FOR_HUMAN"}
            return base
        try:
            _, created = gate.ensure_job_for_desired_state(
                desired_work_id=dwid, task_id=spec["task_id"],
                stage_id=spec["stage_id"],
                max_attempts=spec["max_attempts"],
                policy=spec["policy"], desired_version=pinned,
                actor="reconciler")
        except StoreError as exc:
            # DesiredStateConflict (canonical-identity collision) or any
            # other store refusal: fail closed — stop creating, close the
            # run as FAILED.
            base["failed"] = True
            base["discrepancy"] = {
                "desired_work_id": dwid, "diff": "DESIRED_MISSING",
                "action": "failed",
                "error": f"{type(exc).__name__}: {exc}"}
            return base
        base["created"] = created
        base["discrepancy"] = {
            "desired_work_id": dwid, "diff": "DESIRED_MISSING",
            "action": "created" if created else "already-present"}
        return base

    def _process_item(self, gate: TransitionGate, kind: str, row: dict,
                      pinned: int) -> dict:
        """Diff and (maybe) create for one item. Returns an outcome dict
        with keys created/blocked/failed/discrepancy — the same shape
        _create_for_missing returns. Store-level read failures
        (StoreError, StorageEngineError: unreadable actual state)
        propagate;
        the caller fails the pass closed on them."""
        dwid = row["desired_work_id"]
        if kind == "retired":
            return {"created": False, "blocked": False, "failed": False,
                    "discrepancy": {
                        "desired_work_id": dwid,
                        "diff": self._diff_retired(gate, row),
                        "action": "none",
                        "note": "retired item: tombstone; job left alone"}}
        verdict = self._diff_active(gate, row)
        if verdict != "DESIRED_MISSING":
            return {"created": False, "blocked": False, "failed": False,
                    "discrepancy": {"desired_work_id": dwid,
                                    "diff": verdict, "action": "none"}}
        return self._create_for_missing(gate, row, pinned)

    # ------------------------------------------------------------ entry points
    def evaluate_once(self) -> dict:
        """One synchronous converge pass against the current head version."""
        return self.reconcile()

    # ------------------------------------------------------------ background
    def start(self) -> None:
        """Start the background converge loop. Idempotent."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="axos-reconciler", daemon=True)
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
        owned by this reconciler. OS processes are NOT touched (the
        reconciler never owns any)."""
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
