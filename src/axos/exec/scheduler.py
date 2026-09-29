"""Phase 1C R10 — scheduler & admission control gate.

The scheduler admits PENDING jobs and dispatches worker processes for
them. It is deliberately narrow — admission + atomic claim + dispatch —
and owns NO other authority:

- it never creates or deletes jobs (R11's domain — job creation/deletion
  policy lives outside this layer);
- it never reconciles desired state, never trips circuit breakers, never
  load-sheds, never finalizes (hard stop before R11);
- it never reclaims or releases leases (R1/R4), never restarts
  STALLED/DEAD/UNCERTAIN jobs (R7 -> R9 -> R8 only), never touches
  artifact/checkpoint authority, and never COMPLETEs a job (R5).

Admission is deterministic: the PENDING candidates are read
`ORDER BY created_at, job_id` (the repo has no canonical job priority —
see the note on evaluate_once — so creation order + job_id tiebreak is
the stable deterministic default), the capacity predicate and the claim
UPDATE share one gate transaction (BEGIN IMMEDIATE serializes writers),
and randomness is confined to identity minting (worker_ids), exactly
like `new_proc_id` in exec/identity.py.

Eligibility: PENDING only. The scheduler never admits a job in
CLAIMED, RUNNING, COMMITTING, UNCERTAIN, QUARANTINED, BLOCKED, FAILED,
COMPLETE, REJECTED, CANCELLED, or FINALIZED. Human-gated states
(BLOCKED, and task-level PAUSED_FOR_HUMAN where it surfaces) are never
scheduled, and stay sticky across restarts: the scheduler never writes
or clears them — it only claims PENDING -> CLAIMED.

Dispatch idempotency: execution identity is (job_id, fencing_token).
Before every spawn the scheduler consults
`gate.unreaped_proc_spawns()` and never dispatches twice under the same
claim identity. Claim liveness: every scheduler claim plants a
liveness beat atomically inside the claim transaction (claim ⟹ beat),
refreshed on every evaluate_once; the orphan-redispatch path skips
orphans whose owner still beats (a live scheduler mid-dispatch is never
stolen — racing schedulers produce exactly one dispatch), and
re-dispatches only orphans with absent/stale beats (a dead scheduler's
orphan self-heals after a bounded delay). Documented residual: two
schedulers racing one orphaned CLAIMED dispatch of a DEAD scheduler may
briefly spawn two processes; exactly one wins the
atomic CLAIMED -> RUNNING transition (serialized by BEGIN IMMEDIATE) and
executes — the loser gets TransitionRejected before executing (R2's
fence sweep then contains its process group). No duplicate execution.

Orphan recovery: a CLAIMED job owned by a `sched-*` worker with a live
lease and no unreaped `worker.proc_spawned` evidence for (owner, job_id)
is re-dispatched under the SAME claim identity (the re-read
durable token travels as expect_token) - never re-claimed.
Expired-lease orphans are skipped: lease expiry is R4's domain and
recycling is R1's; the scheduler does not touch them.

Backpressure: when `claim_job_resilient` returns False the job lost the
claim race, capacity is full, or a breaker denied admission. The
scheduler moves on — PENDING stays PENDING, no fake claims, no failure
marking, no recovery escalation.

AUTHORITY BOUNDARY (F1): the scheduler touches the store only through
TransitionGate methods and SELECT-only reads on its thread-local
read-only Store connection. It never calls store.write_txn(), never
opens sqlite3 connections, never signals or kills processes, and never
manufactures lease authority. The authority audit (audit 09, A16)
greps for this.
"""
from __future__ import annotations

import json
import threading
import time
import uuid
from dataclasses import dataclass

from axos.store import open_store, migrate, TransitionGate
from axos.store.db import Store, StoreError, TransitionRejected, LeaseError

__all__ = ["SchedulerConfig", "Scheduler"]


@dataclass(frozen=True)
class SchedulerConfig:
    """Validated scheduler configuration. No magic values: every field is
    explicit and must be positive; `max_concurrent_jobs` and `batch_size`
    are additionally integers >= 1. `scheduler_stale_after_s` bounds how
    long a claim-liveness beat stays fresh: an orphaned CLAIMED job whose
    owner's beat is older than this is treated as a dead scheduler's
    orphan and becomes re-dispatchable."""
    poll_interval_s: float
    lease_ttl_s: float
    max_concurrent_jobs: int
    batch_size: int
    scheduler_stale_after_s: float = 2.0
    resilience_probe_limit: int = 1

    def __post_init__(self) -> None:
        for name in ("poll_interval_s", "lease_ttl_s",
                     "scheduler_stale_after_s"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)) \
                    or not v > 0:
                raise ValueError(
                    f"SchedulerConfig.{name} must be a positive number of"
                    f" seconds, got {v!r}")
        for name in ("max_concurrent_jobs", "batch_size",
                     "resilience_probe_limit"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, int) or v < 1:
                raise ValueError(
                    f"SchedulerConfig.{name} must be an integer >= 1,"
                    f" got {v!r}")


class Scheduler:
    """Admission + atomic claim + dispatch, running in a background thread
    or driven synchronously via evaluate_once()."""

    def __init__(self, db_path: str, supervisor, config: SchedulerConfig,
                 scheduler_id: str = "s0", boot_ready=None,
                 actor: str | None = None) -> None:
        if not isinstance(config, SchedulerConfig):
            raise TypeError(
                "config must be a SchedulerConfig with explicit values"
                f" (poll_interval_s, lease_ttl_s, max_concurrent_jobs,"
                f" batch_size, scheduler_stale_after_s),"
                f" got {type(config).__name__}")
        self.db_path = db_path
        self.supervisor = supervisor
        self.config = config
        self.scheduler_id = scheduler_id
        self.actor = actor or f"scheduler:{scheduler_id}"
        # Same-package seam (documented): by default the scheduler
        # consults the supervisor's boot-recovery gate, which blocks all
        # new execution until boot_recover() reaches READY. An explicit
        # callable may be injected instead (tests, embedding).
        self._boot_ready = (boot_ready if boot_ready is not None
                            else supervisor._boot_ready)
        # Same-package seam (documented): the supervisor's configured
        # heartbeat interval keeps worker renewal cadence coherent with
        # the fence sweep (sweep every H/2, grace H/4).
        self._hb_interval_s = float(
            getattr(supervisor, "_heartbeat_interval_s", 0.5))
        self.store: Store = open_store(db_path)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        # --- R7/R8/R9 thread-local pattern, exactly: sqlite3 connections
        # are thread-bound; the background loop thread gets its own
        # Store/TransitionGate. The constructing thread keeps
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
        tid = threading.get_ident()
        if tid == self._owner_thread:
            return self.gate
        with self._thread_lock:
            g = self._thread_gates.get(tid)
            if g is None:
                self._store_for_thread()  # creates both
                g = self._thread_gates[tid]
            return g

    # ------------------------------------------------------------- admission
    def evaluate_once(self) -> dict:
        """One synchronous admission pass: the testable core.

        Returns a deterministic report dict::

            {"admitted": int, "redispatched": int, "skipped": int,
             "resilience_denied": int,
             "errors": [{"job_id", "phase", "error"}], "detail": str}

        `resilience_denied` counts PENDING candidates the breaker
        preflight denied (an OPEN or unreadable breaker row on one of the
        claim's scopes): the job stays PENDING and no claim is attempted.
        It is advisory-routed, never authoritative — the claim
        transaction revalidates the breaker state authoritatively and a
        stale "allow" is corrected fail-closed inside the claim txn (a
        stale "deny" only defers the candidate to a later pass).

        `detail` is "boot-not-ready" when boot recovery has not reached
        READY (no claims, no dispatch), "ok" otherwise. `errors` carries
        per-job dispatch failures where the claim was LEFT STANDING: the
        live lease expires, R4 observes, R1 recycles — the scheduler
        never rolls back and never invents repair.

        Priority: the repo has no canonical job priority, so PENDING jobs
        are admitted `ORDER BY created_at, job_id` — the stable
        deterministic default (mission §14). Same durable state yields the
        same admission order; only minted worker_ids vary.
        """
        gate = self._gate_for_thread()
        store = self._store_for_thread()
        if not self._boot_ready():
            # No claims, no dispatch: scheduling before boot recovery
            # reaches READY could fork authority.
            return {"admitted": 0, "redispatched": 0, "skipped": 0,
                    "resilience_denied": 0,
                    "errors": [], "detail": "boot-not-ready"}
        now = store.current_time()
        # Claim-liveness: a live scheduler refreshes its beats every
        # pass, so a racing scheduler never mistakes its fresh claims
        # for dead orphans.
        gate.refresh_scheduler_claim_beats(self.scheduler_id)
        spawned = self._unreaped_spawn_keys(gate)
        errors: list[dict] = []
        redispatched = 0
        skipped = 0
        resilience_denied = 0

        # (b) Orphan recovery: CLAIMED + sched-owned + live lease + no
        # unreaped spawn evidence + owning scheduler NOT live ->
        # re-dispatch under the SAME claim identity. Never re-claim;
        # never touch expired-lease orphans (R4 observes, R1 recycles);
        # never steal a live scheduler's fresh claim mid-dispatch
        # (the beat is planted atomically with the claim, so a racing
        # scheduler always observes claim ⟹ fresh beat ⟹ skip).
        for orphan in self._claimed_orphans(gate, now):
            key = (orphan["owner_worker_id"], orphan["job_id"])
            if key in spawned:
                skipped += 1
                continue
            if gate.scheduler_claim_live(
                    orphan["owner_worker_id"],
                    self.config.scheduler_stale_after_s, now):
                skipped += 1
                continue
            try:
                self.supervisor.start_worker(
                    orphan["owner_worker_id"], orphan["job_id"],
                    behavior=None, ttl_s=self.config.lease_ttl_s,
                    hb_interval_s=self._hb_interval_s,
                    expect_token=int(orphan["fencing_token"]))
            except Exception as exc:
                # Defense in depth (e.g. boot not READY inside
                # start_worker): leave the claim standing; lease expiry
                # -> R4 -> R1 recycles it. Never roll back, never invent
                # repair.
                errors.append({"job_id": orphan["job_id"],
                               "phase": "redispatch",
                               "error": f"{type(exc).__name__}: {exc}"})
                continue
            redispatched += 1

        # (c) PENDING candidates, deterministic order. (d) Skip jobs with
        # an open recovery incident — R8 owns those; the scheduler must
        # not race R8's replacement dispatch. Also skip PENDING jobs
        # whose task is PAUSED_FOR_HUMAN — I-17: a human paused that
        # scope, and the scheduler never admits new work under it.
        recovery_ids = self._open_recovery_job_ids(gate)
        pending = gate.pending_jobs(self.config.batch_size)
        paused_tasks = self._paused_task_ids(gate, pending)
        admitted = 0
        for row in pending:
            job_id = row["job_id"]
            if job_id in recovery_ids:
                skipped += 1
                continue
            if row.get("task_id") in paused_tasks:
                skipped += 1
                continue
            # (e) Mint a fresh identity; breaker preflight, then admit
            # atomically with capacity. uuid is identity minting only
            # (like new_proc_id) — it never feeds a decision. The
            # preflight is ADVISORY: an OPEN or unreadable breaker row on
            # one of the claim's scopes routes the candidate to
            # resilience_denied without attempting the claim; the claim
            # transaction revalidates authoritatively, so a stale "allow"
            # is corrected fail-closed inside the txn and a stale "deny"
            # merely defers the candidate to a later pass. False = lost
            # race or capacity full: backpressure, PENDING stays PENDING.
            # The claim plants the scheduler's liveness beat atomically
            # (claim ⟹ beat).
            worker_id = (f"sched-{self.scheduler_id}-"
                         f"{uuid.uuid4().hex[:8]}")
            if self._breaker_denies(gate, job_id, row.get("task_id")):
                resilience_denied += 1
                continue
            won = gate.claim_job_resilient(
                job_id, worker_id, self.config.lease_ttl_s, self.actor,
                self.config.max_concurrent_jobs,
                scheduler_id=self.scheduler_id,
                probe_limit=self.config.resilience_probe_limit)
            if not won:
                skipped += 1
                continue
            # (f) Re-read the durable row; fail closed if the claim did
            # not land on our identity, then consult spawn evidence
            # before dispatching.
            job = gate.get_job(job_id)
            token = job.get("fencing_token")
            if job.get("owner_worker_id") != worker_id or token is None:
                skipped += 1
                continue
            if (worker_id, job_id) in spawned:
                skipped += 1
                continue
            try:
                self.supervisor.start_worker(
                    worker_id, job_id, behavior=None,
                    ttl_s=self.config.lease_ttl_s,
                    hb_interval_s=self._hb_interval_s,
                    expect_token=int(token))
            except Exception as exc:
                # e.g. boot not READY raised inside start_worker.
                # LEAVE the claim standing; the lease expires -> R4
                # observes -> R1 recycles it. Never roll back the claim,
                # never mark the job failed, never invent repair. The
                # liveness beat is withdrawn so the next scheduler can
                # re-dispatch immediately (no staleness wait).
                gate.withdraw_scheduler_claim_beat(worker_id)
                errors.append({"job_id": job_id, "worker_id": worker_id,
                               "phase": "dispatch",
                               "error": f"{type(exc).__name__}: {exc}"})
                continue
            admitted += 1
        return {"admitted": admitted, "redispatched": redispatched,
                "skipped": skipped, "resilience_denied": resilience_denied,
                "errors": errors, "detail": "ok"}

    def _breaker_scopes(self, gate: TransitionGate, job_id: str,
                        task_id: str | None) -> list[tuple[str, str]]:
        """Claim-time breaker scopes in the gate's fixed evaluation
        order: GLOBAL, TASK (when the job has one), DESIRED (when R11
        mapped the job from desired state), JOB. Mirrors the scope
        resolution inside gate.claim_job_resilient; an unreadable DESIRED
        mapping fails closed to the remaining scopes (the claim txn
        resolves authoritatively anyway)."""
        scopes = [("GLOBAL", "global")]
        if task_id:
            scopes.append(("TASK", task_id))
        try:
            desired = gate.get_desired_work_id_for_job(job_id)
        except (TransitionRejected, StoreError):
            desired = None
        if desired:
            scopes.append(("DESIRED", desired))
        scopes.append(("JOB", job_id))
        return scopes

    def _breaker_denies(self, gate: TransitionGate, job_id: str,
                        task_id: str | None) -> bool:
        """Advisory breaker preflight for one PENDING candidate: True
        when any claim scope currently denies admission (an OPEN row, or
        a row that cannot be read as a breaker — both deny in
        breaker_allows). Read-only; the scheduler never moves a breaker
        on its observations — only the resilience controller
        transitions rows. The claim transaction revalidates
        authoritatively, so this answer can go stale immediately: a stale
        "deny" only defers the candidate, a stale "allow" is corrected
        fail-closed inside the claim."""
        for scope_type, scope_id in self._breaker_scopes(
                gate, job_id, task_id):
            try:
                allowed, _reason = gate.breaker_allows(scope_type, scope_id)
            except (TransitionRejected, StoreError):
                return True  # unreadable scope: fail closed, deny
            if not allowed:
                return True
        return False

    def _claimed_orphans(self, gate: TransitionGate,
                       now: float) -> list[dict]:
        """Scheduler-owned CLAIMED jobs with a live lease, via the gate's
        read-only primitive (the scheduler never touches store.conn:
        authority audit A4)."""
        return gate.sched_claimed_orphans(now)

    @staticmethod
    def _unreaped_spawn_keys(gate: TransitionGate) -> set[tuple[str, str]]:
        """(worker_id, job_id) pairs with unreaped proc_spawned evidence —
        the dispatch idempotency guard. Execution identity is
        (job_id, fencing_token); a pair present here is already
        dispatched under its claim identity."""
        return {(s.get("worker_id"), s.get("job_id"))
                for s in gate.unreaped_proc_spawns()}

    @staticmethod
    def _open_recovery_job_ids(gate: TransitionGate) -> set[str]:
        """job_ids carrying an open scope='recovery' incident. R8's
        incident signature is {"scope":"recovery","job_id":...,
        "failure_class":...}; unparsable signatures are ignored, never
        treated as evidence."""
        out: set[str] = set()
        for inc in gate.open_recovery_incidents(scope="recovery"):
            try:
                sig = json.loads(inc.get("signature") or "{}")
            except (ValueError, TypeError):
                continue
            jid = sig.get("job_id")
            if jid:
                out.add(jid)
        return out

    @staticmethod
    def _paused_task_ids(gate: TransitionGate,
                         pending: list[dict]) -> set[str]:
        """task_ids among the candidates whose task is PAUSED_FOR_HUMAN.

        I-17: human-gated scopes are sticky across restarts; the
        scheduler never admits new work under a paused task. Read-only
        gate reads (one per distinct task, cached for the pass). A task
        row that cannot be read fails closed — the candidate is skipped,
        never admitted on an unproven scope."""
        out: set[str] = set()
        seen: dict[str, str | None] = {}
        for row in pending:
            tid = row.get("task_id")
            if not tid or tid in seen:
                continue
            try:
                status = gate.get_task(tid).get("status")
            except (TransitionRejected, StoreError):
                status = None  # unreadable: fail closed below
            seen[tid] = status
            if status != "PAUSED_FOR_HUMAN" and status is not None:
                continue
            out.add(tid)
        return out

    # ------------------------------------------------------------ background
    def start(self) -> None:
        """Start the background evaluation loop. Idempotent."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name=f"axos-scheduler-{self.scheduler_id}",
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
        """Bounded shutdown: stop the loop, withdraw this scheduler's
        claim-liveness beats (graceful shutdown: its claims are all
        dispatched or withdrawn, so no other scheduler should wait out
        the staleness bound), and release every store handle owned by
        this scheduler. OS processes are NOT touched."""
        self.stop()
        try:
            gate = self._gate_for_thread()
            gate.clear_scheduler_claim_beats(self.scheduler_id)
        except Exception:
            pass
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
