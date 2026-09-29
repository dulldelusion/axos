"""Phase 1C R12 — circuit-breaker resilience controller.

The resilience controller is the reader/evaluator half of R12's circuit
breaker. It owns NO execution authority and never mutates jobs, tasks,
leases, workers, processes, artifacts, checkpoints, recovery budgets, or
recovery rungs. Its only writes go through the gate's breaker API, and it
never touches a database connection or raw SQL directly (F1: exec/ is
forbidden from SQL; the authority audit greps for this).

One evaluate_once() pass does five things, in order:

1. Boot gate: no mutation before boot recovery reaches READY.
2. Evidence scan: watchdog DEAD verdicts, terminal-FAILED recovery
   attempts, and escalated recovery incidents are recorded as
   breaker signals (per JOB / TASK / GLOBAL scope). Recording is
   idempotent: the gate's signal dedupe key makes rescans zero-effect,
   so a crashed pass can simply be re-run.
3. Threshold: a CLOSED breaker whose windowed failure_count reaches its
   threshold opens (CAS; the loser re-reads and never overwrites).
4. Cooldown: an OPEN breaker whose cooldown_until has passed moves to
   HALF_OPEN (CAS, same loser discipline). Probe allocation itself is
   the gate's job (claim_half_open_probe runs inside the claim txn).
5. Probe results: a HALF_OPEN breaker carrying a probe allocation is
   evaluated from durable evidence only — job COMPLETE (R5 commit) or
   positive progress delta over the probe baseline closes the breaker;
   a counted failure after the probe allocation reopens it. Heartbeats
   and process existence are NEVER success evidence.

No-op discipline: a pass with nothing to do performs zero writes.
Corrupt probe state fails closed (reopen), never silently resets.
OPEN never transitions directly to CLOSED — only HALF_OPEN probe
success closes a breaker.
"""
from __future__ import annotations

import json
import math
import threading
import time
from dataclasses import dataclass

from axos.store import open_store, migrate, TransitionGate, BreakerConflict
from axos.store import transitions as T
from axos.store.db import Store, StoreError, TransitionRejected

__all__ = ["ResilienceConfig", "ResilienceController",
           "GLOBAL_SCOPE_TYPE", "GLOBAL_SCOPE_ID"]

GLOBAL_SCOPE_TYPE = "GLOBAL"
GLOBAL_SCOPE_ID = "global"

# Failure kinds recorded by this controller (must match the gate's
# breaker_signals failure_kind domain).
_R7_DEAD = "R7_DEAD"
_R8_ATTEMPT_FAILED = "R8_ATTEMPT_FAILED"
_R9_ESCALATED = "R9_ESCALATED"
_RECOVERY_PRESSURE = "RECOVERY_PRESSURE"

# Recovery attempt states that count as "in flight" for the pressure
# gauge (mirrors gate._ATTEMPT_OPEN; kept as a literal here so a gate
# refactor cannot silently change what this controller measures —
# the names are asserted in _validate_attempt_states on first use).
_ATTEMPT_OPEN_STATES = ("CREATED", "RUNNING", "VERIFYING", "UNCERTAIN")


def _threshold_for_scope(config: "ResilienceConfig",
                         scope_type: str) -> int:
    """Pure: per-scope failure threshold (GLOBAL uses the global
    threshold, every other scope the per-scope one)."""
    if scope_type == GLOBAL_SCOPE_TYPE:
        return config.global_failure_threshold
    return config.failure_threshold


def _parse_probe_id(probe_id) -> tuple[str, str] | None:
    """Pure: split a probe allocation id into (worker_id, job_id).

    Probe ids are minted as "<worker_id>:<job_id>" and worker ids never
    contain ':' — anything else is corrupt (None), never guessed at.
    """
    if not isinstance(probe_id, str) or ":" not in probe_id:
        return None
    worker_id, job_id = probe_id.rsplit(":", 1)
    if not worker_id or not job_id or ":" in worker_id:
        return None
    return (worker_id, job_id)


def _probe_outcome(job, baseline_progress,
                   failures_after_probe: bool) -> str:
    """Pure: 'success' | 'failure' | 'inflight' | 'corrupt'.

    - success: the job COMPLETEs (only reachable via R5 commit_artifact)
      or its durable progress_done strictly exceeds the probe baseline.
    - failure: counted failure evidence exists after the probe allocation.
    - inflight: neither — the probe is still running; no mutation.
    - corrupt: the inputs cannot be interpreted; the caller fails closed
      (reopen), never treats corruption as success or as "no evidence".
    """
    if not isinstance(job, dict):
        return "corrupt"
    if (isinstance(baseline_progress, bool)
            or not isinstance(baseline_progress, (int, float))):
        return "corrupt"
    if job.get("status") == "COMPLETE":
        return "success"
    progress = job.get("progress_done")
    if (isinstance(progress, (int, float))
            and not isinstance(progress, bool)
            and progress > baseline_progress):
        return "success"
    if failures_after_probe:
        return "failure"
    return "inflight"


@dataclass(frozen=True)
class ResilienceConfig:
    """Validated resilience-controller configuration. No magic values:
    every field is explicit. Durations must be positive finite numbers;
    counts must be integers >= 1. `actor` identifies this controller in
    the ledger and must not be a worker actor."""
    failure_window_s: float = 300.0
    failure_threshold: int = 3
    global_failure_threshold: int = 10
    cooldown_s: float = 60.0
    half_open_probe_limit: int = 1
    recovery_pressure_threshold: int = 5
    poll_interval_s: float = 5.0
    batch_size: int = 100
    actor: str = "resilience-controller"

    def __post_init__(self) -> None:
        for name in ("failure_window_s", "cooldown_s", "poll_interval_s"):
            v = getattr(self, name)
            if (isinstance(v, bool) or not isinstance(v, (int, float))
                    or not math.isfinite(v) or not v > 0):
                raise ValueError(
                    f"ResilienceConfig.{name} must be a positive finite"
                    f" number of seconds, got {v!r}")
        for name in ("failure_threshold", "global_failure_threshold",
                     "half_open_probe_limit", "recovery_pressure_threshold",
                     "batch_size"):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, int) or v < 1:
                raise ValueError(
                    f"ResilienceConfig.{name} must be an integer >= 1,"
                    f" got {v!r}")
        if not isinstance(self.actor, str) or not self.actor:
            raise ValueError("ResilienceConfig.actor must be a non-empty"
                             " string")
        if self.actor.startswith("worker:"):
            raise ValueError(
                "ResilienceConfig.actor must not start with 'worker:':"
                " worker actors cannot own controller authority")


class ResilienceController:
    """R12 circuit-breaker evaluator, runnable in a background thread or
    driven synchronously via evaluate_once(). Reads evidence through the
    gate's read-only methods; writes only through the gate's breaker
    API. Never starts/kills workers, never reclaims/fences, never
    claims/completes/transitions jobs, never touches R9 budgets or rung
    selection."""

    def __init__(self, db_path: str, config: ResilienceConfig, *,
                 supervisor=None, boot_ready=None) -> None:
        if not isinstance(config, ResilienceConfig):
            raise TypeError(
                "config must be a ResilienceConfig with explicit values"
                f" (failure_window_s, failure_threshold,"
                f" global_failure_threshold, cooldown_s,"
                f" half_open_probe_limit, recovery_pressure_threshold,"
                f" poll_interval_s, batch_size, actor),"
                f" got {type(config).__name__}")
        self.db_path = db_path
        self.supervisor = supervisor
        self.config = config
        self.actor = config.actor
        # Same-package seam (documented), exactly like the scheduler's:
        # an explicit callable wins; otherwise the supervisor's
        # boot-recovery gate is consulted. With neither (no supervisor),
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
        self._attempt_states_validated = False

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

    # ------------------------------------------------------------------ read
    def resilience_allows(self, scope_type: str, scope_id: str) -> bool:
        """Advisory read-only preflight: does the breaker for this scope
        currently admit work? The claim transaction revalidates
        authoritatively — this answer can go stale immediately."""
        allowed, _reason = self._gate_for_thread().breaker_allows(
            scope_type, scope_id)
        return bool(allowed)

    # ------------------------------------------------------------- evaluation
    def evaluate_once(self) -> dict:
        """One synchronous evaluation pass: the testable core.

        Returns a deterministic report dict::

            {"signals_recorded": int, "signals_deduped": int,
             "breakers_opened": [{"scope_type", "scope_id"}],
             "breakers_half_opened": [{"scope_type", "scope_id"}],
             "breakers_closed": [{"scope_type", "scope_id"}],
             "probes_evaluated": int,
             "errors": [{"scope", "phase", "error"}],
             "detail": str}

        `detail` is "boot-not-ready" when boot recovery has not reached
        READY (zero counts, no mutation), "ok" otherwise. A pass with
        nothing to do performs zero writes. Per-scope failures are
        collected into `errors` and never abort the pass.
        """
        gate = self._gate_for_thread()
        store = self._store_for_thread()
        zero = {"signals_recorded": 0, "signals_deduped": 0,
                "breakers_opened": [], "breakers_half_opened": [],
                "breakers_closed": [], "probes_evaluated": 0,
                "errors": []}
        if not self._boot_ready():
            # No evaluation before boot recovery reaches READY: opening
            # breakers from pre-boot evidence could fork authority.
            return {**zero, "detail": "boot-not-ready"}
        now = store.current_time()
        errors: list[dict] = zero["errors"]
        report = {**zero, "errors": errors, "detail": "ok"}
        self._validate_attempt_states(gate)

        recorded, deduped, touched = self._record_failure_signals(
            gate, now, errors)
        report["signals_recorded"] = recorded
        report["signals_deduped"] = deduped

        report["breakers_opened"].extend(
            self._apply_thresholds(gate, now, touched, errors))
        report["breakers_half_opened"].extend(
            self._apply_cooldowns(gate, now, errors))
        opened, closed, evaluated = self._evaluate_probes(gate, errors)
        report["breakers_opened"].extend(opened)
        report["breakers_closed"].extend(closed)
        report["probes_evaluated"] = evaluated
        return report

    # ------------------------------------------------------- evidence -> keys
    @staticmethod
    def _incident_job_id(incident: dict) -> str | None:
        """job_id from a recovery incident's canonical signature. R8's
        incident signature is {"scope": "recovery", "job_id": ...,
        "failure_class": ...}; unparsable signatures are ignored, never
        treated as evidence (same discipline as the scheduler)."""
        try:
            sig = json.loads(incident.get("signature") or "{}")
        except (ValueError, TypeError):
            return None
        jid = sig.get("job_id") if isinstance(sig, dict) else None
        return jid or None

    def _validate_attempt_states(self, gate: TransitionGate) -> None:
        """Assert once per controller that the gate's open-attempt states
        are exactly the set this controller's pressure gauge measures.
        A gate refactor that renames an attempt state must fail loudly
        here, not silently change what "recovery pressure" means."""
        if self._attempt_states_validated:
            return
        actual = tuple(gate._ATTEMPT_OPEN)
        if actual != _ATTEMPT_OPEN_STATES:
            raise AssertionError(
                "gate._ATTEMPT_OPEN changed to"
                f" {actual!r}; ResilienceController measures"
                f" {_ATTEMPT_OPEN_STATES!r} — reconcile before running")
        self._attempt_states_validated = True

    def _scopes_for_job(self, gate: TransitionGate,
                        job_id: str) -> list[tuple[str, str]]:
        """Scopes a job-level failure counts against: the JOB scope, its
        TASK scope (skipped when the job has no task), and GLOBAL. An
        unreadable job row fails closed to JOB+GLOBAL only — the failure
        is still counted where it can be attributed."""
        scopes = [("JOB", job_id)]
        try:
            task_id = gate.get_job(job_id).get("task_id")
        except (TransitionRejected, StoreError):
            task_id = None
        if task_id:
            scopes.append(("TASK", task_id))
        scopes.append((GLOBAL_SCOPE_TYPE, GLOBAL_SCOPE_ID))
        return scopes

    def _record_failure_signals(self, gate: TransitionGate, now: float,
                                errors: list[dict]
                                ) -> tuple[int, int, dict]:
        """Scan authoritative evidence and record breaker signals.

        Returns (recorded, deduped, touched) where touched maps
        (scope_type, scope_id) -> set of failure kinds seen this pass.
        Every scan is windowed so rescans stay idempotent via the gate's
        signal dedupe key; a pass with no evidence performs zero writes.
        """
        cfg = self.config
        scan_cutoff = now - cfg.failure_window_s - 1.0  # small epsilon
        touched: dict[tuple[str, str], set[str]] = {}
        recorded = 0
        deduped = 0

        def record(kind: str, incident_id: str | None,
                   attempt_id: str | None, job_id: str | None) -> None:
            nonlocal recorded, deduped
            if not job_id:
                return
            for scope_type, scope_id in self._scopes_for_job(
                    gate, job_id):
                try:
                    res = gate.record_breaker_signal(
                        scope_type, scope_id, failure_kind=kind,
                        incident_id=incident_id, attempt_id=attempt_id,
                        failure_window_s=cfg.failure_window_s,
                        cooldown_s=cfg.cooldown_s, actor=self.actor)
                except Exception as exc:  # per-scope: log, never abort
                    errors.append(
                        {"scope": {"scope_type": scope_type,
                                   "scope_id": scope_id},
                         "phase": "record-signal",
                         "error": f"{type(exc).__name__}: {exc}"})
                    continue
                if res.get("deduped"):
                    deduped += 1
                else:
                    recorded += 1
                touched.setdefault(
                    (scope_type, scope_id), set()).add(kind)

        # (a) Watchdog DEAD verdicts: terminal per execution identity.
        # The verdict_id is the canonical signal identity (passed as
        # incident_id: a verdict is a failure observation record, not a
        # recovery attempt) so each verdict counts exactly once.
        for job_id in self._verdict_candidate_jobs(gate):
            try:
                verdicts = gate.watchdog_verdicts_for(job_id)
            except (TransitionRejected, StoreError) as exc:
                errors.append({"scope": {"scope_type": "JOB",
                                         "scope_id": job_id},
                               "phase": "scan-verdicts",
                               "error": f"{type(exc).__name__}: {exc}"})
                continue
            for v in verdicts:
                if v.get("verdict") != "DEAD":
                    continue
                if (v.get("evaluated_at") or 0) < scan_cutoff:
                    continue
                record(_R7_DEAD, v.get("verdict_id"), None, job_id)

        # (b) Terminal-FAILED recovery attempts, via their incidents.
        # (c) Escalated recovery incidents (durable R9 escalation signal).
        open_incs = gate.open_recovery_incidents("recovery")
        esc_incs = gate.escalated_recovery_incidents()
        failed_attempt_ts: list[float] = []
        for inc in open_incs + esc_incs:
            incident_id = inc.get("incident_id")
            job_id = self._incident_job_id(inc)
            try:
                attempts = gate.recovery_attempts_for(incident_id)
            except (TransitionRejected, StoreError) as exc:
                errors.append({"scope": {"scope_type": "JOB",
                                         "scope_id": job_id or incident_id},
                               "phase": "scan-attempts",
                               "error": f"{type(exc).__name__}: {exc}"})
                continue
            for a in attempts:
                if a.get("attempt_state") != "FAILED":
                    continue
                observed = (a.get("started_at")
                            or a.get("recorded_at") or 0)
                failed_attempt_ts.append(observed)
                if observed < scan_cutoff:
                    continue
                record(_R8_ATTEMPT_FAILED, incident_id,
                       a.get("attempt_id"),
                       a.get("job_id") or job_id)
        for inc in esc_incs:
            incident_id = inc.get("incident_id")
            record(_R9_ESCALATED,
                   f"{incident_id}:escalated" if incident_id else None,
                   None, self._incident_job_id(inc))
            # Escalated incidents are re-read on every pass; the dedupe
            # key (incident_id + ":escalated") makes the rescan a no-op.

        # (d) Recovery pressure: one GLOBAL signal per window while the
        # recovery subsystem is under load — open attempts created in the
        # window, FAILED attempts in the window, and recently-escalated
        # incidents. The attempt_id embeds the window index, so the
        # signal dedupes within a window and re-fires next window only
        # if pressure persists.
        pressure_cutoff = now - cfg.failure_window_s
        try:
            open_attempts = gate.open_recovery_attempts()
        except (TransitionRejected, StoreError) as exc:
            errors.append({"scope": {"scope_type": GLOBAL_SCOPE_TYPE,
                                     "scope_id": GLOBAL_SCOPE_ID},
                           "phase": "scan-pressure",
                           "error": f"{type(exc).__name__}: {exc}"})
            open_attempts = []
        pressure = sum(
            1 for a in open_attempts
            if (a.get("started_at") or a.get("recorded_at") or 0)
            >= pressure_cutoff)
        pressure += sum(1 for ts in failed_attempt_ts
                        if ts >= pressure_cutoff)
        pressure += sum(
            1 for inc in esc_incs
            if (inc.get("updated_at") or inc.get("created_at") or 0)
            >= pressure_cutoff)
        if pressure >= cfg.recovery_pressure_threshold:
            try:
                res = gate.record_breaker_signal(
                    GLOBAL_SCOPE_TYPE, GLOBAL_SCOPE_ID,
                    failure_kind=_RECOVERY_PRESSURE,
                    incident_id=None,
                    attempt_id=f"pressure:{int(now // cfg.failure_window_s)}",
                    failure_window_s=cfg.failure_window_s,
                    cooldown_s=cfg.cooldown_s, actor=self.actor)
            except Exception as exc:
                errors.append(
                    {"scope": {"scope_type": GLOBAL_SCOPE_TYPE,
                               "scope_id": GLOBAL_SCOPE_ID},
                     "phase": "record-pressure",
                     "error": f"{type(exc).__name__}: {exc}"})
            else:
                if res.get("deduped"):
                    deduped += 1
                else:
                    recorded += 1
                touched.setdefault(
                    (GLOBAL_SCOPE_TYPE, GLOBAL_SCOPE_ID),
                    set()).add(_RECOVERY_PRESSURE)
        return recorded, deduped, touched

    def _verdict_candidate_jobs(self, gate: TransitionGate) -> list[str]:
        """job_ids that could carry a recent watchdog verdict: every job
        that ever held an execution identity (all job states except
        PENDING, which is never executed). Deterministic job_id order,
        bounded by batch_size — the window scan is idempotent, so a
        truncated pass is completed by later passes."""
        statuses = tuple(s for s in T.JOB_TRANSITIONS if s != "PENDING")
        jobs = gate.jobs_in_states(statuses)
        return [j["job_id"] for j in jobs[:self.config.batch_size]]

    # -------------------------------------------------------------- threshold
    def _apply_thresholds(self, gate: TransitionGate, now: float,
                          touched: dict[tuple[str, str], set[str]],
                          errors: list[dict]) -> list[dict]:
        """Open CLOSED breakers whose windowed failure_count reached the
        per-scope threshold. CAS: on BreakerConflict the row is re-read
        and the pass continues — the loser never overwrites."""
        cfg = self.config
        opened: list[dict] = []
        for scope_type, scope_id in sorted(touched):
            try:
                row = gate.get_breaker_state(scope_type, scope_id)
            except (TransitionRejected, StoreError) as exc:
                errors.append({"scope": {"scope_type": scope_type,
                                         "scope_id": scope_id},
                               "phase": "threshold-read",
                               "error": f"{type(exc).__name__}: {exc}"})
                continue
            if row is None or row.get("state") != "CLOSED":
                continue
            threshold = _threshold_for_scope(cfg, scope_type)
            if (row.get("failure_count") or 0) < threshold:
                continue
            window_started = row.get("window_started_at")
            if (window_started is None
                    or now - window_started > cfg.failure_window_s):
                # Stale window: the gate owns window rollover on the
                # next counted signal; this controller never invents a
                # window.
                continue
            try:
                gate.transition_breaker(
                    scope_type, scope_id,
                    expected_version=row["version"], to_state="OPEN",
                    actor=self.actor, reason="failure-threshold",
                    evidence={
                        "failure_count": row.get("failure_count"),
                        "threshold": threshold,
                        "window_started_at": window_started,
                        "failure_kinds": sorted(touched[(scope_type,
                                                         scope_id)])})
            except BreakerConflict:
                # Lost the CAS race: re-read so later phases see the
                # authoritative row; never overwrite the winner.
                try:
                    gate.get_breaker_state(scope_type, scope_id)
                except (TransitionRejected, StoreError):
                    pass
                continue
            except (TransitionRejected, StoreError) as exc:
                errors.append({"scope": {"scope_type": scope_type,
                                         "scope_id": scope_id},
                               "phase": "threshold-open",
                               "error": f"{type(exc).__name__}: {exc}"})
                continue
            opened.append({"scope_type": scope_type,
                           "scope_id": scope_id})
        return opened

    # --------------------------------------------------------------- cooldown
    def _apply_cooldowns(self, gate: TransitionGate, now: float,
                         errors: list[dict]) -> list[dict]:
        """Move OPEN breakers whose cooldown has elapsed to HALF_OPEN.
        Probe allocation/reset is the gate's job (claim_half_open_probe
        runs inside the claim txn); this controller only advances the
        state machine. Same CAS loser discipline as thresholds."""
        half_opened: list[dict] = []
        try:
            states = gate.list_breaker_states()
        except (TransitionRejected, StoreError) as exc:
            errors.append({"scope": None, "phase": "cooldown-list",
                           "error": f"{type(exc).__name__}: {exc}"})
            return half_opened
        for row in states:
            if row.get("state") != "OPEN":
                continue
            cooldown_until = row.get("cooldown_until")
            if cooldown_until is None or cooldown_until > now:
                continue
            scope_type, scope_id = (row["scope_type"], row["scope_id"])
            try:
                gate.transition_breaker(
                    scope_type, scope_id,
                    expected_version=row["version"], to_state="HALF_OPEN",
                    actor=self.actor, reason="cooldown-elapsed")
            except BreakerConflict:
                try:
                    gate.get_breaker_state(scope_type, scope_id)
                except (TransitionRejected, StoreError):
                    pass
                continue
            except (TransitionRejected, StoreError) as exc:
                errors.append({"scope": {"scope_type": scope_type,
                                         "scope_id": scope_id},
                               "phase": "cooldown-half-open",
                               "error": f"{type(exc).__name__}: {exc}"})
                continue
            half_opened.append({"scope_type": scope_type,
                                "scope_id": scope_id})
        return half_opened

    # ----------------------------------------------------------------- probes
    def _evaluate_probes(self, gate: TransitionGate,
                         errors: list[dict]
                         ) -> tuple[list[dict], list[dict], int]:
        """Evaluate HALF_OPEN probe allocations from durable evidence.

        Returns (reopened, closed, probes_evaluated). A probe with no
        verdict either way is in flight: no mutation. Success closes
        (the only path OPEN -> CLOSED is via HALF_OPEN probe success);
        failure or corrupt probe state reopens. Heartbeats and process
        existence are never consulted.
        """
        reopened: list[dict] = []
        closed: list[dict] = []
        evaluated = 0
        try:
            states = gate.list_breaker_states()
        except (TransitionRejected, StoreError) as exc:
            errors.append({"scope": None, "phase": "probe-list",
                           "error": f"{type(exc).__name__}: {exc}"})
            return reopened, closed, evaluated
        for row in states:
            if row.get("state") != "HALF_OPEN":
                continue
            probe_id = row.get("half_open_probe_id")
            if not probe_id:
                continue
            evaluated += 1
            scope_type, scope_id = (row["scope_type"], row["scope_id"])
            scope = {"scope_type": scope_type, "scope_id": scope_id}
            outcome, detail = self._evaluate_probe(gate, row)
            if outcome == "inflight":
                continue
            to_state = "CLOSED" if outcome == "success" else "OPEN"
            reason = ("probe-succeeded" if outcome == "success"
                      else "probe-failed")
            try:
                gate.transition_breaker(
                    scope_type, scope_id,
                    expected_version=row["version"], to_state=to_state,
                    actor=self.actor, reason=reason,
                    evidence={"probe_id": probe_id, "outcome": outcome,
                              **detail})
            except BreakerConflict:
                try:
                    gate.get_breaker_state(scope_type, scope_id)
                except (TransitionRejected, StoreError):
                    pass
                continue
            except (TransitionRejected, StoreError) as exc:
                errors.append({"scope": scope, "phase": "probe-transition",
                               "error": f"{type(exc).__name__}: {exc}"})
                continue
            (closed if outcome == "success" else reopened).append(scope)
        return reopened, closed, evaluated

    def _evaluate_probe(self, gate: TransitionGate,
                        row: dict) -> tuple[str, dict]:
        """Evaluate one probe allocation. Returns (outcome, detail) with
        outcome in 'success' | 'failure' | 'inflight' | 'corrupt'."""
        probe_id = row.get("half_open_probe_id")
        probe_at = row.get("half_open_probe_at")
        parsed = _parse_probe_id(probe_id)
        if (parsed is None or isinstance(probe_at, bool)
                or not isinstance(probe_at, (int, float))):
            return ("corrupt", {"reason": "unparseable probe_id or"
                                          " probe_at",
                                "probe_id": probe_id,
                                "probe_at": probe_at})
        _worker_id, job_id = parsed
        try:
            job = gate.get_job(job_id)
        except (TransitionRejected, StoreError):
            # The probe's job is unreadable: fail closed (reopen), never
            # close a breaker on an unproven scope.
            return ("corrupt", {"reason": "probe job unreadable",
                                "job_id": job_id})
        baseline = self._parse_probe_baseline(
            row.get("half_open_probe_baseline"))
        if baseline is None:
            return ("corrupt", {"reason": "corrupt probe baseline",
                                "job_id": job_id})
        baseline_progress = baseline.get("progress_done")
        failures_after = self._failures_after_probe(gate, job_id, probe_at)
        outcome = _probe_outcome(job, baseline_progress, failures_after)
        return (outcome, {"job_id": job_id,
                          "job_status": job.get("status"),
                          "baseline_progress": baseline_progress,
                          "observed_progress": job.get("progress_done"),
                          "failures_after_probe": failures_after})

    @staticmethod
    def _parse_probe_baseline(raw) -> dict | None:
        """Parse the gate's canonical probe-baseline JSON. None or
        unparsable is corrupt — never defaulted, never silently reset."""
        if raw is None:
            return None
        if isinstance(raw, dict):
            data = raw
        elif isinstance(raw, str):
            try:
                data = json.loads(raw)
            except (ValueError, TypeError):
                return None
        else:
            return None
        return data if isinstance(data, dict) else None

    def _failures_after_probe(self, gate: TransitionGate, job_id: str,
                              probe_at: float) -> bool:
        """Durable counted-failure evidence for this job after the probe
        allocation: a DEAD verdict, a terminal-FAILED recovery attempt,
        or an escalation. This mirrors exactly what _record_failure_signals
        would count as signals for the job — heartbeats excluded."""
        try:
            verdicts = gate.watchdog_verdicts_for(job_id)
        except (TransitionRejected, StoreError):
            verdicts = []
        for v in verdicts:
            if (v.get("verdict") == "DEAD"
                    and (v.get("evaluated_at") or 0) > probe_at):
                return True
        try:
            incidents = (gate.open_recovery_incidents("recovery")
                         + gate.escalated_recovery_incidents())
        except (TransitionRejected, StoreError):
            incidents = []
        for inc in incidents:
            if self._incident_job_id(inc) != job_id:
                continue
            if ((inc.get("outcome") == "escalated")
                    and (inc.get("updated_at") or 0) > probe_at):
                return True
            try:
                attempts = gate.recovery_attempts_for(
                    inc.get("incident_id"))
            except (TransitionRejected, StoreError):
                continue
            for a in attempts:
                if (a.get("attempt_state") == "FAILED"
                        and (a.get("started_at")
                             or a.get("recorded_at") or 0) > probe_at):
                    return True
        return False

    # ------------------------------------------------------------ background
    def start(self) -> None:
        """Start the background evaluation loop. Idempotent."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="axos-resilience-controller",
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
