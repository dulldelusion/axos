"""Phase 1C R8 — Recovery Controller & Recovery Contract Gate.

Answers: "a failure was detected — what is the smallest authorized recovery
action, did it actually work, and when do we escalate?" The controller
CONSUMES R7 verdicts (never redefines health), creates durable recovery
incidents and attempts carrying the full Recovery Contract fields, dispatches
the smallest authorized action ONLY through existing authorities, verifies
the result from authoritative durable evidence (never from action return
codes, heartbeats, or controller memory), and emits a durable escalation
signal after two consecutive zero-progress attempts.

Authority boundaries (audited by A14):
  - R1 remains the sole lease-reclaim authority. The controller calls
    gate.reclaim_lease(...) with actor="recovery-controller" and never
    UPDATEs job ownership, never bumps fencing_token, never clears a lease
    itself, never implements another reclaim path.
  - R2 remains the sole physical-fencing authority. The controller invokes
    supervisor.fence_sweep() (the existing R2 mechanism) and never calls
    killpg, never sends a termination signal, never ends a process group.
    No process-signalling or process-spawning capability is imported here.
  - R3: heartbeat/progress evidence is read through the gate only; no
    second progress mechanism is created.
  - R4: lease-expiry observation is consumed via
    gate.observe_expired_leases(); the predicate is never redefined.
  - R5: completion authority untouched — the controller never completes a
    job, never stages/verifies/commits artifacts, never advances
    latest-known-good pointers.
  - R6: boot remains the reconstruction authority. The controller evaluates
    only when the runtime reports READY.
  - R7: verdict semantics are consumed, never redefined. No staleness math,
    no threshold comparisons on heartbeat/progress live here.

Recovery Contract (every attempt durably records): incident identity,
failure classification, recovery rung (+ budget context interface for R9),
attempt number, success condition, failure condition, observed progress
delta, resulting state, escalation target.

R8/R9 boundary (deliberate, documented):
  - R8 implements the contract's normative two-attempt rule: two consecutive
    attempts with zero authoritative progress -> failed recovery ->
    durable escalation signal (escalation_target "r9-policy"). R8 then
    stops: it creates no further attempts for an escalated incident.
  - R8 records the canonical rung (contract doc E.1) for each action but
    implements NO retry budgets, NO ladder traversal policy, NO loop
    breaker. budget_context is recorded as an explicit "deferred to R9"
    marker through the RungProvider interface R9 will implement.
  - The controller never auto-sequences reclaim -> fence -> restart as a
    ladder. Each attempt's action is selected from CURRENT durable
    evidence (the smallest applicable authorized action). Multi-attempt
    sequences emerge from evidence, not from policy.

Progress semantics (D6, per-rung canonical expectations from contract E.1):
  - reclaim (rung 2 "re-claim/requeue"): success = the lease was actually
    revoked in durable state — fencing_token increased, owner cleared,
    job transitioned to PENDING/UNCERTAIN, ledger evidence present.
  - fence (rung 3 "replace/reassign"): success = the stale process that
    was R6-live before the sweep is dead/reaped after it.
  - restart (rung 3 "replace/reassign"): success = a replacement worker
    claimed the job under the current token AND authoritative progress
    evidence advanced within the observation window (progress_updated_at,
    new artifacts, or new verified checkpoints).
  progress_delta is 1.0 when the rung's success criterion holds on the
  before/after evidence diff, else 0.0. I-18 / the zero-progress rule: an
  attempt whose action "succeeded" but moved no authoritative evidence can
  never complete as success. Heartbeats never count as progress.

Fencing-token safety: every attempt binds (job_id, fencing_token). If the
durable token changed between attempt creation and action dispatch, the
attempt is aborted to BLOCKED — a stale controller never acts against new
ownership. Incident identity is (job_id, failure_class): stable across
controller/supervisor crashes, watchdog reevaluations, attempt retries,
and process replacement; it is never derived from controller memory.

Concurrency: attempt creation is UNIQUE(incident_id, attempt_number) inside
one write_txn (the loser observes the winner); claiming is a CAS
CREATED -> RUNNING (the loser observes). Two controllers produce exactly
one authoritative attempt and one claim. Reconciliation after a crash is
read-only: a controller never re-dispatches another controller's external
action — it determines the outcome from durable evidence, and any retry is
an explicit new attempt bounded by the two-attempt rule.

Fail-closed: unreadable clock/state -> RecoveryError, nothing written;
contradictory state, changed token, ambiguous action effect, or missing
authority -> BLOCKED / UNCERTAIN / ESCALATE per the state model. The
controller never guesses.
"""
from __future__ import annotations

import math
import threading
import time
import json
import uuid
from dataclasses import dataclass
from typing import Protocol

from ..store import (StoreError, TransitionGate, TransitionRejected,
                     LeaseError, open_store, migrate)
from . import boot as boot_mod

RECOVERY_FAILURE_CLASSES = ("STALLED", "DEAD", "LEASE_EXPIRED",
                            "STALE_AUTHORITY", "UNCERTAIN")
RECOVERY_ACTIONS = ("reclaim", "fence", "restart")
_TERMINAL_JOB_STATES = ("COMPLETE", "FAILED")
_ACTIVE_JOB_STATES = ("CLAIMED", "RUNNING", "COMMITTING")

# Contract-normative bound (contract doc E, section 4; mission R8 section
# 11): zero authoritative progress across TWO consecutive recovery attempts
# is failed recovery -> escalate. This is the contract's fixed rule, not a
# configurable retry budget: R8 creates at most this trailing run of
# non-success attempts, then emits the durable escalation signal and stops.
# R9 owns everything beyond (ladder, budgets, loop breaker).
_TWO_ATTEMPT_ESCALATION_RUN = 2

# Where R8 escalates to. R8 produces the signal; R9 owns the decision.
_ESCALATION_TARGET = "r9-policy"

# Rung names verbatim from the canonical five-rung ladder (contract E.1).
# R8 records the rung context; it does not implement ladder policy.
_CANONICAL_RUNGS = {
    "reclaim": (2, "re-claim/requeue"),
    "fence": (3, "replace/reassign"),
    "restart": (3, "replace/reassign"),
}


class RecoveryError(StoreError):
    """The controller could not evaluate safely (fail-closed)."""


class RecoveryNotReady(RecoveryError):
    """Evaluation was refused: the runtime has not reported READY."""


@dataclass(frozen=True)
class RecoveryConfig:
    """Constructor-injected controller timing. Every value must be a
    positive finite number of seconds. No timing literal may live inside
    the control logic.

    The restart_* fields parameterize the replacement worker spawned
    through the existing execution substrate (supervisor.start_worker):
    how long it runs, its lease TTL, and its heartbeat cadence."""
    observation_window_s: float
    evaluation_interval_s: float
    claim_timeout_s: float
    restart_worker_duration_s: float = 3600.0
    restart_worker_ttl_s: float = 60.0
    restart_worker_hb_interval_s: float = 0.3

    def __post_init__(self) -> None:
        for name in ("observation_window_s", "evaluation_interval_s",
                     "claim_timeout_s", "restart_worker_duration_s",
                     "restart_worker_ttl_s",
                     "restart_worker_hb_interval_s"):
            value = getattr(self, name)
            if (not isinstance(value, (int, float))
                    or isinstance(value, bool)
                    or not math.isfinite(value)
                    or value <= 0):
                raise ValueError(
                    f"RecoveryConfig.{name} must be a positive finite"
                    f" number of seconds, got {value!r}")


@dataclass(frozen=True)
class RungContext:
    """The rung/budget context recorded on every recovery attempt.

    rung/name come from the canonical five-rung ladder (contract E.1).
    budget_context is the typed R8/R9 interface: R8 records an explicit
    deferred-to-R9 marker and performs no budget arithmetic; R9 supplies
    real budgets through RungProvider."""
    rung: int
    name: str
    budget_context: dict | None


class RungProvider(Protocol):
    """Typed interface R9 implements to supply ladder/budget policy.

    R8's default (CanonicalRungProvider) maps each action to its canonical
    rung with budget_context deferred — no ladder traversal, no budgets."""

    def select_rung(self, *, incident: dict, failure_class: str,
                    action_name: str,
                    history: list[dict]) -> RungContext:
        ...


class CanonicalRungProvider:
    """R8's rung provider: canonical rung per action, budgets deferred.

    This is NOT ladder policy — it only records which canonical rung each
    R8 action belongs to (contract E.1), so the Recovery Contract's rung
    field is always populated from the existing canonical definitions."""

    def select_rung(self, *, incident: dict, failure_class: str,
                    action_name: str,
                    history: list[dict]) -> RungContext:
        rung, name = _CANONICAL_RUNGS[action_name]
        return RungContext(
            rung=rung, name=name,
            budget_context={"r8_budget_policy": "none",
                            "deferred_to": "r9-policy",
                            "note": "R8 implements the contract's fixed"
                                    " two-attempt rule only; retry budgets"
                                    " and ladder traversal belong to R9"})


class RecoveryController:
    """Durable recovery controller over the verified R1-R7 kernel.

    Consumes R7 verdicts and R4 expiry observations; dispatches the
    smallest authorized recovery action through R1/R2/the existing
    execution substrate; verifies from authoritative durable evidence;
    escalates after two consecutive zero-progress attempts. See the module
    docstring for the authority boundaries and the R8/R9 split."""

    def __init__(self, db_path: str, config: RecoveryConfig,
                 actor: str = "recovery-controller", readiness=None,
                 supervisor=None,
                 rung_provider: RungProvider | None = None) -> None:
        """readiness: zero-arg callable, True only when the runtime is
        fully reconstructed (boot READY). Pass supervisor= to derive
        readiness from a Supervisor's boot report instead; the same
        supervisor handle is used to dispatch fence/restart actions
        through the existing substrate. One of the two is required."""
        if not isinstance(config, RecoveryConfig):
            raise TypeError(
                "config must be a RecoveryConfig with explicit timings")
        self.config = config
        self.actor = actor
        self.controller_id = f"controller-{uuid.uuid4().hex[:12]}"
        if supervisor is not None:
            self._supervisor = supervisor
            self._readiness = (
                lambda: (supervisor._boot_report or {}).get("phase")
                == "READY")
        elif readiness is not None:
            self._supervisor = None
            self._readiness = readiness
        else:
            raise ValueError(
                "a readiness callable or a supervisor is required: the"
                " controller must not evaluate before boot is READY")
        self._rung_provider = rung_provider or CanonicalRungProvider()
        self.db_path = db_path
        self.store = open_store(db_path)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        # sqlite3 connections are thread-bound: the background loop thread
        # gets its own Store/TransitionGate (same pattern as the watchdog
        # and the supervisor's fence-sweep thread).
        self._owner_thread = threading.get_ident()
        self._thread_stores: dict[int, object] = {}
        self._thread_gates: dict[int, TransitionGate] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._errors: list[dict] = []

    def _store_for_thread(self):  # type: ignore[no-untyped-def]
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
        tid = threading.get_ident()
        if tid == self._owner_thread:
            return self.gate
        with self._lock:
            g = self._thread_gates.get(tid)
            if g is None:
                self._store_for_thread()  # creates both
                g = self._thread_gates[tid]
            return g

    # ------------------------------------------------------------ evaluate
    def evaluate(self) -> list[dict]:
        """Drive recovery: advance open attempts, then take in new
        failures. Returns one outcome record per incident/attempt acted
        on. Idempotent: repeated evaluation with no state change writes
        nothing. Raises RecoveryNotReady before READY, RecoveryError when
        authoritative state is unreadable."""
        gate = self._gate_for_thread()
        if not self._readiness():
            raise RecoveryNotReady(
                "recovery controller refuses before boot READY (R6)")
        try:
            now = gate.store.current_time()
        except Exception as exc:
            raise RecoveryError(
                f"authoritative clock unreadable: {exc}") from exc
        outcomes: list[dict] = []
        # Phase 1: drive every open attempt from durable state (never
        # create a duplicate while one is open).
        try:
            open_attempts = gate.open_recovery_attempts()
        except Exception as exc:
            raise RecoveryError(
                f"open attempts unreadable: {exc}") from exc
        for att in open_attempts:
            outcomes.append(self._drive_attempt(gate, att, now))
        # Phase 2: intake — new failures become incidents/attempts.
        for candidate in self._candidates(gate, now):
            outcomes.append(self._intake(gate, candidate, now))
        return outcomes

    # --------------------------------------------------------------- drive
    def _drive_attempt(self, gate: TransitionGate, att: dict,
                       now: float) -> dict:
        """Advance one open attempt according to its durable state."""
        state = att["attempt_state"]
        claimed_by_me = att.get("controller_id") == self.controller_id
        try:
            if state == "CREATED":
                return self._drive_created(gate, att, now, claimed_by_me)
            if state == "RUNNING":
                return self._drive_running(gate, att, now, claimed_by_me)
            if state == "VERIFYING":
                return self._verify(gate, att, now,
                                    via="verifying-reentry")
            if state == "UNCERTAIN":
                return self._reconcile(gate, att, now)
        except (TransitionRejected, StoreError) as exc:
            raise RecoveryError(
                f"attempt {att['attempt_id']} drive failed: {exc}") from exc
        return {"attempt_id": att["attempt_id"], "outcome": "observed",
                "detail": f"state {state}: no drive action this pass"}

    def _drive_created(self, gate: TransitionGate, att: dict, now: float,
                       claimed_by_me: bool) -> dict:
        claimed_by_other = (att.get("controller_id") is not None
                            and not claimed_by_me)
        if claimed_by_other and not self._claim_stale(att, now):
            return {"attempt_id": att["attempt_id"],
                    "outcome": "observed",
                    "detail": "claimed by a live peer controller"}
        if not claimed_by_me:
            if not gate.claim_recovery_attempt(att["attempt_id"],
                                               self.controller_id):
                return {"attempt_id": att["attempt_id"],
                        "outcome": "observed",
                        "detail": "lost the claim race; peer drives it"}
            att = gate.get_recovery_attempt(att["attempt_id"])
        return self._dispatch(gate, att, now)

    def _claim_stale(self, att: dict, now: float) -> bool:
        claimed_at = att.get("claimed_at")
        if claimed_at is None:
            return True
        return (now - claimed_at) > self.config.claim_timeout_s

    def _drive_running(self, gate: TransitionGate, att: dict, now: float,
                       claimed_by_me: bool) -> dict:
        verify_after = att.get("verify_after")
        if verify_after is None:
            # Claimed but the action was never dispatched (crash between
            # claim and dispatch), or a peer is dispatching right now.
            # Never re-dispatch blindly: reconcile from evidence first.
            if claimed_by_me:
                return self._dispatch(gate, att, now)
            if self._claim_stale(att, now):
                return self._reconcile(gate, att, now)
            return {"attempt_id": att["attempt_id"],
                    "outcome": "observed",
                    "detail": "peer dispatch in flight"}
        if verify_after > now:
            return {"attempt_id": att["attempt_id"],
                    "outcome": "observed",
                    "detail": "observation window not elapsed"}
        return self._verify(gate, att, now, via="window-elapsed")

    # ------------------------------------------------------------ dispatch
    def _dispatch(self, gate: TransitionGate, att: dict,
                  now: float) -> dict:
        """Claimed (by this controller) -> perform the external action
        through the existing authority, then arm verification. The attempt
        row is durable (CREATED) before anything external runs."""
        action = att["action"]
        if isinstance(action, str):
            action = json.loads(action)
        name = action["name"]
        job_id = att["job_id"]
        token = att["fencing_token"]
        # Fencing-token safety (mission section 15): re-read the durable
        # row inside the dispatch path; a changed token aborts the stale
        # attempt BEFORE any external action.
        try:
            job = gate.get_job(job_id)
        except Exception as exc:
            return self._block(gate, att,
                               f"job row unreadable at dispatch: {exc}",
                               escalate=True)
        if job["fencing_token"] != token:
            if not self._token_change_explained_by_dispatch(action, job):
                return self._block(
                    gate, att,
                    f"fencing token changed ({token} ->"
                    f" {job['fencing_token']}): aborting stale attempt",
                    escalate=False)
            # The token moved because of this attempt's own dispatch
            # (e.g. the replacement worker claimed, the reclaim bumped
            # the token): not stale. The action's idempotency guard
            # below prevents duplicate external effects.
        if job["status"] in _TERMINAL_JOB_STATES:
            return self._block(
                gate, att,
                f"job is terminal ({job['status']}): recovery cannot"
                " resurrect terminal execution",
                escalate=False)
        try:
            if name == "reclaim":
                dispatch_ev = self._dispatch_reclaim(gate, att, job, now)
            elif name == "fence":
                dispatch_ev = self._dispatch_fence(gate, att, job, now)
            elif name == "restart":
                dispatch_ev = self._dispatch_restart(gate, att, job, now)
            else:
                raise RecoveryError(f"unknown action {name!r}")
        except (TransitionRejected, LeaseError, StoreError) as exc:
            # The authority refused: record the failure, zero progress.
            return self._complete(
                gate, att, decision="retry",
                observed_effect=f"action {name} refused by authority:"
                                f" {type(exc).__name__}: {exc}",
                progress_delta=0.0,
                resulting_state=self._resulting_state(gate, job_id),
                evidence_after=self._snapshot(gate, job_id, now),
                note="authority-refused")
        gate.note_attempt_dispatch(
            att["attempt_id"], dispatch_ev,
            now + self.config.observation_window_s, self.actor)
        return {"attempt_id": att["attempt_id"], "outcome": "dispatched",
                "action": name, "dispatch": dispatch_ev,
                "verify_after": now + self.config.observation_window_s}

    def _token_change_explained_by_dispatch(self, action: dict,
                                              job: dict) -> bool:
        """True only when the current token/owner is exactly what this
        attempt's own durable dispatch evidence produced. Any other
        token change is external and the attempt is stale."""
        dispatch = action.get("dispatch") or {}
        name = action.get("name")
        if name == "restart":
            prior = dispatch.get("replacement_worker_id")
            return bool(prior) and job["owner_worker_id"] == prior
        if name == "reclaim":
            return dispatch.get("token_after") == job["fencing_token"]
        # Fence never changes the token: any change is external.
        return False

    def _dispatch_reclaim(self, gate: TransitionGate, att: dict, job: dict,
                          now: float) -> dict:
        """Through R1 only. Routine when the R4 observation shows an
        expired lease; forced (verdict + incident) when the lease is live."""
        action = att["action"]
        if isinstance(action, str):
            action = json.loads(action)
        prior = (action.get("dispatch") or {})
        if prior.get("token_after") == job["fencing_token"]:
            # This attempt already reclaimed: the token is exactly what
            # its dispatch produced. Do not reclaim again.
            return {**prior,
                    "note": "already reclaimed by this attempt;"
                            " not duplicated"}
        expired = {e["job_id"] for e in gate.observe_expired_leases()}
        forced = att["job_id"] not in expired
        if forced and att["failure_class"] not in ("STALLED", "DEAD"):
            raise RecoveryError(
                "forced reclaim requires a DEAD/STALLED verdict")
        result = gate.reclaim_lease(
            att["job_id"], actor="recovery-controller",
            reason=(f"r8 recovery attempt {att['attempt_number']} for"
                    f" incident {att['incident_id']}:"
                    f" {att['failure_class']}"),
            expected_owner=job["owner_worker_id"],
            expected_token=job["fencing_token"],
            force=forced,
            verdict=(att["failure_class"] if forced else None),
            incident_id=att["incident_id"])
        return {"authority": "r1.reclaim_lease", "forced": forced,
                "token_before": job["fencing_token"],
                "token_after": result["fencing_token"],
                "job_status_after": result["status"]}

    def _dispatch_fence(self, gate: TransitionGate, att: dict, job: dict,
                        now: float) -> dict:
        """Through the existing R2 mechanism only: one fence-sweep pass.
        The controller never signals a process itself."""
        sup = self._supervisor
        if sup is None:
            raise RecoveryError(
                "fence action requires a supervisor handle (R2 mechanism"
                " unavailable)")
        report = sup.fence_sweep()
        action = att["action"]
        if isinstance(action, str):
            action = json.loads(action)
        return {"authority": "r2.fence_sweep",
                "swept_at": report.get("swept_at"),
                "fenced_killed": report.get("fenced_killed", []),
                "already_dead": report.get("already_dead", []),
                "target": action.get("target")}

    def _dispatch_restart(self, gate: TransitionGate, att: dict,
                          job: dict, now: float) -> dict:
        """Through the existing execution substrate only: the supervisor
        spawns a replacement worker that claims its own lease through the
        gate. Never inherits authority: fresh worker_id, current token."""
        sup = self._supervisor
        if sup is None:
            raise RecoveryError(
                "restart action requires a supervisor handle")
        # Idempotency first: the idempotency key binds this attempt; if
        # a prior dispatch already spawned the replacement and it owns
        # the job, do not spawn again. (The owner guard below is for the
        # first dispatch — on re-drive the job is owned by our own
        # replacement, which is the expected state, not a refusal.)
        action = att["action"]
        if isinstance(action, str):
            action = json.loads(action)
        prior = (action.get("dispatch") or {}).get("replacement_worker_id")
        if prior:
            current = gate.get_job(att["job_id"])
            if current["owner_worker_id"] == prior:
                return {"authority": "supervisor.start_worker",
                        "replacement_worker_id": prior,
                        "note": "already spawned by this attempt;"
                                " not duplicated"}
        if job["owner_worker_id"] is not None:
            raise RecoveryError(
                "restart refused: job has a live owner; reclaim/fence"
                " first")
        worker_id = (f"rc-{att['incident_id'][-8:]}"
                     f"-a{att['attempt_number']}")
        proc_id = sup.start_worker(
            worker_id, att["job_id"],
            {"kind": "heartbeat_loop",
             "duration_s": self.config.restart_worker_duration_s},
            ttl_s=self.config.restart_worker_ttl_s,
            hb_interval_s=self.config.restart_worker_hb_interval_s,
            renew=True)
        return {"authority": "supervisor.start_worker",
                "replacement_worker_id": worker_id, "proc_id": proc_id,
                "idempotency_key": att["idempotency_key"]}

    # ----------------------------------------------------------- verify
    def _snapshot(self, gate: TransitionGate, job_id: str,
                  now: float) -> dict:
        """Authoritative progress-evidence snapshot: the gate's read-only
        progress-evidence bundle (job row progress counters, verified
        artifact count, latest-known-good checkpoint pointer) plus the
        owner process identity. Worker claims are never consulted."""
        ev = gate.progress_evidence_for_job(job_id)
        owner_identity = None
        owner = ev["job"]["owner_worker_id"]
        if owner:
            owner_identity = self._process_identity(gate, owner)
        return {
            "at": now,
            "job_id": job_id,
            "job_status": ev["job"]["status"],
            "owner_worker_id": owner,
            "fencing_token": ev["job"]["fencing_token"],
            "lease_acquired_at": ev["job"].get("lease_acquired_at"),
            "lease_expires_at": ev["job"].get("lease_expires_at"),
            "progress_updated_at": ev["progress_updated_at"],
            "progress_done": ev["progress_done"],
            "verified_artifacts": ev["verified_artifacts"],
            "latest_known_good": ev["latest_known_good"],
            "owner_process_identity": owner_identity,
        }

    def _process_identity(self, gate: TransitionGate,
                          worker_id: str) -> dict:
        """R6 identity classification of the worker's latest unreaped
        spawn (read-only evidence, never authority)."""
        spawns = [s for s in gate.unreaped_proc_spawns()
                  if s.get("worker_id") == worker_id]
        if not spawns:
            return {"spawn_evidence": "none"}
        latest = spawns[-1]
        classification = boot_mod._classify_spawn(latest)
        return {"spawn_evidence": "present",
                "proc_id": latest.get("proc_id"),
                "pid": classification.pid,
                "classification": classification.kind,
                "detail": classification.detail}

    def _progress_delta(self, before: dict, after: dict,
                        action_name: str,
                        action: dict | None = None) -> tuple[float, str]:
        """Deterministic progress comparison per the rung's canonical
        success criterion (contract E.1). Returns (delta, explanation).
        delta is 1.0 iff the criterion holds on the before/after diff."""
        if action_name == "reclaim":
            # Rung 2: "job re-claimed and new durable evidence within the
            # window" — the lease revocation took durable effect.
            ok = (
                after["fencing_token"] > before["fencing_token"]
                and after["owner_worker_id"] is None
                and after["job_status"] in ("PENDING", "UNCERTAIN")
            )
            return (1.0 if ok else 0.0,
                    "rung-2 reclaim criterion: token increased, owner"
                    f" cleared, job PENDING/UNCERTAIN -> {'met' if ok else 'not met'}")
        if action_name == "fence":
            # Rung 3 (fence part): the stale process R6-live before is
            # dead/reaped after.
            before_live = ((before.get("target_identity") or {})
                           .get("classification") == "live_match")
            after_id = after.get("target_identity") or {}
            after_dead = after_id.get("classification") in ("dead", "reuse") \
                or after_id.get("spawn_evidence") == "none"
            if not before_live:
                # Desired end state already held before our action: the
                # attempt verified it (idempotent no-op success).
                ok = after_dead
                return (1.0 if ok else 0.0,
                        "fence: stale process already not-live before"
                        f" action; end state holds -> {'met' if ok else 'not met'}")
            ok = after_dead
            return (1.0 if ok else 0.0,
                    "rung-3 fence criterion: stale process dead/reaped"
                    f" after sweep -> {'met' if ok else 'not met'}")
        if action_name == "restart":
            # Rung 3: "replacement healthy for the observation window AND
            # progressing" — the job is owned by THIS attempt's
            # replacement worker (from the durable dispatch evidence)
            # and authoritative progress evidence advanced. The
            # replacement's claim bumps the fencing token; that is its
            # own fresh authority, not staleness.
            expected = ((action or {}).get("dispatch") or {}) \
                .get("replacement_worker_id")
            claimed = (expected is not None
                       and after["owner_worker_id"] == expected)
            progressed = (
                (after.get("progress_updated_at") or 0)
                > (before.get("progress_updated_at") or 0)
                or after.get("verified_artifacts", 0)
                > before.get("verified_artifacts", 0)
                or (after.get("latest_known_good") is not None
                    and after.get("latest_known_good")
                    != before.get("latest_known_good"))
            )
            ok = claimed and progressed
            return (1.0 if ok else 0.0,
                    "rung-3 restart criterion: replacement claimed"
                    f" ({claimed}) and progressing ({progressed})"
                    f" -> {'met' if ok else 'not met'}")
        raise RecoveryError(f"unknown action {action_name!r}")

    def _verify(self, gate: TransitionGate, att: dict, now: float,
                via: str) -> dict:
        """RUNNING/VERIFYING -> terminal, from authoritative evidence."""
        action = att["action"]
        if isinstance(action, str):
            action = json.loads(action)
        name = action["name"]
        before = json.loads(att["evidence_before"]) \
            if att.get("evidence_before") else {}
        after = self._snapshot(gate, att["job_id"], now)
        if name == "fence":
            after["target_identity"] = self._process_identity(
                gate, (action.get("target") or {}).get("worker_id")
                or "__none__")
        try:
            gate.transition_recovery_attempt(
                att["attempt_id"], ("RUNNING", "UNCERTAIN"), "VERIFYING",
                self.actor)
        except TransitionRejected:
            pass  # already VERIFYING (crash re-entry): re-verify anyway
        delta, explanation = self._progress_delta(before, after, name,
                                                      action)
        if delta > 0:
            completed = self._complete(
                gate, att, decision="success",
                observed_effect=f"{name} action verified: {explanation}",
                progress_delta=delta,
                resulting_state=self._resulting_state(gate, att["job_id"]),
                evidence_after=after, note=f"verified-{via}")
            self._maybe_close_incident(gate, att, completed, now)
            return completed
        # Zero progress: not successful recovery (I-18 / zero-progress
        # rule). Record the failure; the contract's two-attempt rule is
        # decided BEFORE the single terminal completion: this attempt
        # completes once, as "retry" or as "escalate". The trailing run
        # counts only attempts before this one (it is not terminal yet).
        history = gate.recovery_attempts_for(att["incident_id"])
        trailing = self._consecutive_failures(
            history, below=att["attempt_number"])
        escalate = (trailing + 1) >= _TWO_ATTEMPT_ESCALATION_RUN
        completed = self._complete(
            gate, att,
            decision=("escalate" if escalate else "retry"),
            observed_effect=f"{name} action produced zero authoritative"
                            f" progress: {explanation}",
            progress_delta=0.0,
            resulting_state=self._resulting_state(gate, att["job_id"]),
            evidence_after=after, note=f"verified-{via}",
            escalation_target=(
                _ESCALATION_TARGET if escalate else None))
        if escalate:
            gate.set_incident_escalated(
                att["incident_id"],
                f"two consecutive zero-progress attempts"
                f" (latest: attempt {att['attempt_number']})",
                self.actor)
            completed["outcome"] = "escalated"
            completed["decision"] = "escalate"
        return completed

    def _complete(self, gate: TransitionGate, att: dict, *,
                  decision: str, observed_effect: str,
                  progress_delta: float, resulting_state: str,
                  evidence_after: dict, note: str,
                  escalation_target: str | None = None) -> dict:
        row = gate.complete_recovery_attempt(
            att["attempt_id"], decision=decision,
            observed_effect=observed_effect, progress_delta=progress_delta,
            resulting_state=resulting_state, evidence_after=evidence_after,
            escalation_target=escalation_target, actor=self.actor)
        return {"attempt_id": att["attempt_id"], "outcome": "completed",
                "decision": decision, "progress_delta": progress_delta,
                "note": note, "attempt": row}

    def _block(self, gate: TransitionGate, att: dict, reason: str,
               escalate: bool) -> dict:
        """Safe stop for one attempt: BLOCKED, never guessing."""
        decision = "escalate" if escalate else "stop"
        row = gate.complete_recovery_attempt(
            att["attempt_id"], decision=decision,
            observed_effect=f"blocked: {reason}", progress_delta=0.0,
            resulting_state=self._resulting_state(gate, att["job_id"]),
            evidence_after={}, actor=self.actor,
            escalation_target=_ESCALATION_TARGET if escalate else None)
        if escalate:
            gate.set_incident_escalated(
                att["incident_id"], f"blocked: {reason}", self.actor)
        else:
            gate.set_incident_outcome(att["incident_id"], "stopped",
                                      f"blocked: {reason}", self.actor)
        return {"attempt_id": att["attempt_id"], "outcome": "blocked",
                "decision": decision, "reason": reason, "attempt": row}

    def _resulting_state(self, gate: TransitionGate, job_id: str) -> str:
        try:
            job = gate.get_job(job_id)
        except Exception:
            return "job-unreadable"
        return (f"status={job['status']},owner={job['owner_worker_id']},"
                f"token={job['fencing_token']}")

    def _maybe_close_incident(self, gate: TransitionGate, att: dict,
                              completed: dict, now: float) -> None:
        """A successful attempt closes the incident — unless the stale
        process lingers, in which case the incident stays open so a
        fence attempt can finish the job."""
        incident_id = att["incident_id"]
        lingering = self._lingering_process(gate, att)
        if lingering is None:
            action_name = att["action"]
            if not isinstance(action_name, str):
                action_name = json.loads(action_name).get("name")
            gate.set_incident_outcome(incident_id, "success",
                                      f"attempt {att['attempt_number']}"
                                      f" ({action_name}) verified with"
                                      " durable progress",
                                      self.actor)
        # else: incident stays open; the next pass creates the fence
        # attempt against the lingering (worker_id, proc_id).

    def _lingering_process(self, gate: TransitionGate,
                           att: dict) -> dict | None:
        """The pre-attempt owner process, if still R6-live after the
        attempt (logically fenced but physically present)."""
        before = att.get("evidence_before")
        if isinstance(before, str):
            before = json.loads(before)
        owner = (before or {}).get("owner_worker_id")
        if not owner:
            return None
        identity = self._process_identity(gate, owner)
        if identity.get("classification") == "live_match":
            return {"worker_id": owner,
                    "proc_id": identity.get("proc_id")}
        return None

    # ---------------------------------------------------------- reconcile
    def _reconcile(self, gate: TransitionGate, att: dict,
                   now: float) -> dict:
        """UNCERTAIN (or stale-claimed RUNNING with no dispatch record):
        determine from DURABLE evidence whether the external action took
        effect. Never re-dispatches another controller's action — any
        retry is an explicit new attempt bounded by the two-attempt rule."""
        gate.transition_recovery_attempt(
            att["attempt_id"], ("RUNNING", "UNCERTAIN"), "UNCERTAIN",
            self.actor)
        effect = self._action_effect_present(gate, att)
        if effect is None:
            # Evidence unreadable: safe stop with escalation.
            return self._block(
                gate, att,
                "action effect indeterminate: authoritative evidence"
                " unreadable", escalate=True)
        if effect:
            gate.note_attempt_dispatch(
                att["attempt_id"], {"reconciled": True,
                                    "note": "action effect found in"
                                            " durable evidence"},
                now + self.config.observation_window_s, self.actor)
            return self._verify(gate, gate.get_recovery_attempt(
                att["attempt_id"]), now, via="reconciled")
        # No effect and no dispatch record: the action never ran. Release
        # the stale claim back to CREATED so it can be claimed cleanly.
        gate.transition_recovery_attempt(
            att["attempt_id"], ("UNCERTAIN",), "CREATED", self.actor)
        return {"attempt_id": att["attempt_id"], "outcome": "reconciled",
                "detail": "no action effect in durable evidence;"
                          " claim released to CREATED"}

    def _action_effect_present(self, gate: TransitionGate,
                               att: dict) -> bool | None:
        """Did the dispatched action take durable effect? True/False, or
        None when the evidence cannot be read (fail-closed)."""
        try:
            job = gate.get_job(att["job_id"])
        except Exception:
            return None
        action = att["action"]
        if isinstance(action, str):
            action = json.loads(action)
        name = action["name"]
        before = json.loads(att["evidence_before"]) \
            if att.get("evidence_before") else {}
        token_before = before.get("fencing_token")
        if name == "reclaim":
            # A reclaim took effect iff the durable token moved past the
            # attempt's bound token (only this attempt's reclaim could
            # have done that after its creation — anything else aborts
            # the attempt via the token check).
            if token_before is None:
                return None
            return job["fencing_token"] > token_before
        if name == "fence":
            target = (action.get("target") or {})
            identity = self._process_identity(
                gate, target.get("worker_id") or "__none__")
            before_live = ((before.get("target_identity") or {})
                           .get("classification") == "live_match")
            # Effect present iff the target is no longer live.
            return (identity.get("classification") in ("dead", "reuse")
                    or identity.get("spawn_evidence") == "none"
                    or not before_live)
        if name == "restart":
            wid = ((action.get("dispatch") or {})
                   .get("replacement_worker_id"))
            return (wid is not None
                    and job["owner_worker_id"] == wid)
        return None

    # -------------------------------------------------------------- intake
    def _candidates(self, gate: TransitionGate,
                    now: float) -> list[dict]:
        """Failures needing recovery, from R7 verdicts + R4 expiry
        observations. Never invents health semantics: HEALTHY (or no
        verdict) means no candidate."""
        cands: list[dict] = []
        try:
            owned = gate.owned_active_jobs()
        except Exception as exc:
            raise RecoveryError(
                f"owned active jobs unreadable: {exc}") from exc
        try:
            expired = {e["job_id"] for e in gate.observe_expired_leases()}
        except Exception as exc:
            raise RecoveryError(
                f"R4 expiry observation unreadable: {exc}") from exc
        for job in owned:
            job_id = job["job_id"]
            token = job["fencing_token"]
            try:
                latest = gate.latest_watchdog_verdict(job_id, token)
            except Exception as exc:
                raise RecoveryError(
                    f"watchdog verdict unreadable for {job_id}:"
                    f" {exc}") from exc
            verdict = latest["verdict"] if latest else None
            if job_id in expired:
                failure_class: str | None = "LEASE_EXPIRED"
            elif verdict in ("STALLED", "DEAD"):
                failure_class = verdict
            else:
                continue  # HEALTHY or no verdict: no recovery incident
            cands.append({"job_id": job_id, "fencing_token": token,
                          "failure_class": failure_class,
                          "verdict": verdict,
                          "detection": {
                              "verdict": verdict,
                              "job_id": job_id,
                              "lease_expired": job_id in expired,
                              "job_status": job["status"],
                              "owner_worker_id": job["owner_worker_id"],
                              "fencing_token": token,
                              "observed_at": now}})
        # Ownerless jobs with a lingering stale process continue an
        # existing incident (fence); the controller never invents an
        # incident for an ownerless job (no scheduler behavior).
        for inc in self._open_recovery_incidents(gate):
            job_id = self._incident_job_id(gate, inc)
            if job_id is None:
                continue
            try:
                job = gate.get_job(job_id)
            except Exception:
                continue
            if job["owner_worker_id"] is not None:
                continue
            if job["status"] in _TERMINAL_JOB_STATES:
                gate.set_incident_outcome(inc["incident_id"], "stopped",
                                          "job reached terminal state",
                                          self.actor)
                continue
            lingering = self._lingering_for_incident(gate, inc)
            if lingering is not None:
                cands.append({"job_id": job_id,
                              "fencing_token": job["fencing_token"],
                              "failure_class": "STALE_AUTHORITY",
                              "verdict": None,
                              "lingering": lingering,
                              "incident": inc,
                              "detection": {
                                  "lingering_worker_id":
                                      lingering["worker_id"],
                                  "lingering_proc_id":
                                      lingering["proc_id"],
                                  "job_status": job["status"],
                                  "observed_at": now}})
        return cands

    def _open_recovery_incidents(self, gate: TransitionGate) -> list[dict]:
        return gate.open_recovery_incidents(scope="recovery")

    def _incident_job_id(self, gate: TransitionGate,
                         inc: dict) -> str | None:
        try:
            detection = json.loads(inc.get("detection") or "{}")
        except ValueError:
            return None
        sig = json.loads(inc["signature"])
        return sig.get("job_id") or detection.get("job_id")

    def _lingering_for_incident(self, gate: TransitionGate,
                                inc: dict) -> dict | None:
        """A still-R6-live process for the incident's last-known owner."""
        try:
            detection = json.loads(inc.get("detection") or "{}")
        except ValueError:
            return None
        owner = detection.get("owner_worker_id")
        if not owner:
            return None
        identity = self._process_identity(gate, owner)
        if identity.get("classification") == "live_match":
            return {"worker_id": owner,
                    "proc_id": identity.get("proc_id")}
        return None

    def _intake(self, gate: TransitionGate, cand: dict,
                now: float) -> dict:
        """One candidate failure -> find-or-create incident, then maybe
        create the next attempt. Never duplicates: an open attempt, a
        closed incident, or the two-attempt bound all suppress creation."""
        job_id = cand["job_id"]
        failure_class = cand["failure_class"]
        # Terminal execution is never recovered (R8-26).
        try:
            job = gate.get_job(job_id)
        except Exception as exc:
            raise RecoveryError(
                f"job row unreadable at intake: {exc}") from exc
        if job["status"] in _TERMINAL_JOB_STATES:
            return {"job_id": job_id, "outcome": "no-incident",
                    "detail": f"job terminal ({job['status']})"}
        incident = cand.get("incident") or \
            gate.find_or_create_recovery_incident(
                job_id, failure_class, cand["detection"], self.actor)
        if incident.get("outcome") is not None:
            return {"incident_id": incident["incident_id"],
                    "outcome": "no-attempt",
                    "detail": f"incident closed ({incident['outcome']})"}
        history = gate.recovery_attempts_for(incident["incident_id"])
        if any(h["attempt_state"] in ("CREATED", "RUNNING", "VERIFYING",
                                      "UNCERTAIN") for h in history):
            return {"incident_id": incident["incident_id"],
                    "outcome": "no-attempt",
                    "detail": "an attempt is already open"}
        if any(h["attempt_state"] == "SUCCEEDED" for h in history):
            # A succeeded attempt with no incident outcome yet — unless
            # the stale process lingers, in which case the incident
            # stays open and we fall through to the fence attempt below.
            if self._lingering_for_incident(gate, incident) is None:
                # (the drive phase normally closes this first).
                gate.set_incident_outcome(
                    incident["incident_id"], "success",
                    "attempt verified with durable progress", self.actor)
                return {"incident_id": incident["incident_id"],
                        "outcome": "no-attempt",
                        "detail": "incident already recovered"}
        if self._consecutive_failures(history) >= \
                _TWO_ATTEMPT_ESCALATION_RUN:
            gate.set_incident_escalated(
                incident["incident_id"],
                "two consecutive zero-progress attempts (intake guard)",
                self.actor)
            return {"incident_id": incident["incident_id"],
                    "outcome": "escalated",
                    "detail": "two-attempt bound reached"}
        action_name, rung_action = self._select_action(
            gate, cand, job, history)
        if action_name is None:
            return {"incident_id": incident["incident_id"],
                    "outcome": "no-attempt",
                    "detail": rung_action}
        return self._create_attempt(gate, incident, cand, job,
                                    action_name, now)

    def _consecutive_failures(self, history: list[dict],
                              below: int | None = None) -> int:
        """Trailing run of consecutive FAILED/BLOCKED terminal attempts.
        `below` excludes the in-flight attempt (it is not terminal yet)
        — used at verify time; intake passes None (all history is past)."""
        by_number = {h["attempt_number"]: h for h in history
                     if h["attempt_number"] is not None
                     and (below is None
                          or h["attempt_number"] < below)}
        if not by_number:
            return 0
        n = max(by_number)
        run = 0
        while by_number.get(n, {}).get("attempt_state") in ("FAILED",
                                                             "BLOCKED"):
            run += 1
            n -= 1
        return run

    def _select_action(self, gate: TransitionGate, cand: dict, job: dict,
                       history: list[dict]) -> tuple[str | None, str]:
        """The smallest applicable authorized action for the current
        evidence. Returns (action_name, note)."""
        failure_class = cand["failure_class"]
        if job["owner_worker_id"] is not None:
            # Owned: the lease must be revoked through R1.
            return ("reclaim", "owned job -> R1 reclaim")
        # Ownerless: only continue an existing incident (never schedule).
        lingering = cand.get("lingering")
        if lingering is not None:
            return ("fence",
                    f"lingering stale process {lingering['worker_id']}/"
                    f"{lingering['proc_id']} -> R2 sweep")
        return (None, "ownerless with no lingering process: nothing for"
                      " R8 to do (no scheduler behavior)")

    def _create_attempt(self, gate: TransitionGate, incident: dict,
                        cand: dict, job: dict, action_name: str,
                        now: float) -> dict:
        incident_id = incident["incident_id"]
        history = gate.recovery_attempts_for(incident_id)
        rung_ctx = self._rung_provider.select_rung(
            incident=incident, failure_class=cand["failure_class"],
            action_name=action_name, history=history)
        success_criterion, failure_criterion = self._criteria(action_name)
        action: dict = {"name": action_name,
                        "rung": rung_ctx.rung,
                        "rung_name": rung_ctx.name}
        target = None
        if action_name == "fence":
            target = cand.get("lingering")
            action["target"] = target
        evidence_before = self._snapshot(gate, cand["job_id"], now)
        if action_name == "fence" and target:
            evidence_before["target_identity"] = \
                self._process_identity(gate, target["worker_id"])
        att = gate.create_recovery_attempt(
            incident_id=incident_id, job_id=cand["job_id"],
            fencing_token=job["fencing_token"], rung=rung_ctx.rung,
            rung_name=rung_ctx.name, action=action,
            failure_class=cand["failure_class"],
            success_criterion=success_criterion,
            failure_criterion=failure_criterion,
            evidence_before=evidence_before, actor=self.actor)
        # Budget context: recorded on the attempt row for the contract;
        # R8 defers all budget policy to R9 (see RungContext).
        gate.set_attempt_budget_context(att["attempt_id"],
                                        rung_ctx.budget_context,
                                        self.actor)
        att = gate.get_recovery_attempt(att["attempt_id"])
        return {"incident_id": incident_id,
                "attempt_id": att["attempt_id"],
                "outcome": "attempt-created",
                "action": action_name,
                "attempt_number": att["attempt_number"],
                "rung": rung_ctx.rung, "rung_name": rung_ctx.name}

    def _criteria(self, action_name: str) -> tuple[str, str]:
        if action_name == "reclaim":
            return (
                "rung-2: fencing_token increased, owner cleared, job"
                " PENDING/UNCERTAIN — the lease revocation took durable"
                " effect within the observation window",
                "token unchanged, owner still set, or authority refused"
                " the reclaim")
        if action_name == "fence":
            return (
                "rung-3: the stale process R6-live before the sweep is"
                " dead/reaped after it",
                "stale process still R6-live after the sweep, or the R2"
                " mechanism unavailable")
        if action_name == "restart":
            return (
                "rung-3: replacement worker claimed the job under the"
                " current token AND authoritative progress evidence"
                " advanced within the observation window",
                "no claim, or heartbeats without durable progress")
        raise RecoveryError(f"unknown action {action_name!r}")

    # -------------------------------------------------------------- public
    def dispatch_restart(self, job_id: str) -> dict:
        """Typed restart entry point for the R9 layer (and tests): create
        a restart attempt for an ownerless job and dispatch it through
        the existing execution substrate. The autonomous evaluate() loop
        does NOT call this on its own — restarting execution without a
        scheduling decision would be scheduler behavior, which is out of
        scope for R8."""
        gate = self._gate_for_thread()
        if not self._readiness():
            raise RecoveryNotReady(
                "recovery controller refuses before boot READY (R6)")
        now = gate.store.current_time()
        job = gate.get_job(job_id)
        if job["status"] in _TERMINAL_JOB_STATES:
            raise RecoveryError("cannot restart terminal execution")
        if job["owner_worker_id"] is not None:
            raise RecoveryError("restart refused: job has a live owner")
        incident = gate.find_or_create_recovery_incident(
            job_id, "STALE_AUTHORITY",
            {"job_id": job_id, "job_status": job["status"],
             "explicit": "dispatch_restart", "observed_at": now},
            self.actor)
        history = gate.recovery_attempts_for(incident["incident_id"])
        if any(h["attempt_state"] in ("CREATED", "RUNNING", "VERIFYING",
                                      "UNCERTAIN") for h in history):
            raise RecoveryError("a restart attempt is already open")
        cand = {"job_id": job_id, "fencing_token": job["fencing_token"],
                "failure_class": "STALE_AUTHORITY"}
        created = self._create_attempt(gate, incident, cand, job,
                                       "restart", now)
        att = gate.get_recovery_attempt(created["attempt_id"])
        if not gate.claim_recovery_attempt(att["attempt_id"],
                                           self.controller_id):
            raise RecoveryError("lost the claim race")
        att = gate.get_recovery_attempt(att["attempt_id"])
        return self._dispatch(gate, att, now)

    # -------------------------------------------------------------- cadence
    def start(self) -> None:
        """Start the bounded background evaluation loop (one pass per
        evaluation_interval_s). Errors are collected in self._errors;
        the loop never silently dies."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop, name="axos-recovery-controller",
                daemon=True)
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
        """Stop the loop and release the store handles. Spawned OS
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
