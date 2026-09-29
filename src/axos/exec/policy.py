"""Phase 1C R9 — Recovery Ladder, Budgets & Escalation Policy.

R9 is the policy layer above R8's execution/verification substrate:

* R9 CHOOSES the recovery rung (the canonical 5-rung ladder, contract
  §E.1), BOUNDS recovery (durable per-rung and per-incident attempt
  budgets), and DECIDES terminal escalation. It never executes a recovery
  action and never verifies one.
* R8 EXECUTES exactly one attempt per decision and VERIFIES it against
  authoritative evidence. R9 invokes R8 only through R8's typed seams:
  RecoveryController.evaluate() (drive + intake) and
  RecoveryController.dispatch_restart() (ownerless restart). R9 never
  calls reclaim_lease / fence_sweep / start_worker, never touches fencing
  tokens, leases, processes, heartbeats, watchdog verdicts, artifacts,
  checkpoints, or completion state, and never redefines R5 completion or
  R7 verdict semantics.

Canonical ladder (contract §E.1), verbatim:
  1. "Retry with backoff"      — R8 reclaim (the routine requeue/retry
                                 primitive)
  2. "Restart worker"          — R8 restart via dispatch_restart. Per D1 the
                                 full rung-2 sequence is
                                 reclaim → terminate → provision fresh →
                                 re-claim; R8's restart action is the
                                 provision step, reached after reclaim.
  3. "Replace / reassign"      — R8 fence and/or restart via existing paths
  4. "Widen scope"             — NO R9 executor (stage-widening belongs to
                                 the R10+ scheduler/reconciler). The rung is
                                 still traversed durably and monotonically
                                 before terminal escalation.
  5. "Replan / pause"          — terminal: persist escalation, enter the
                                 human-gated PAUSED_FOR_HUMAN task state
                                 through TransitionGate (sticky under I-17;
                                 never auto-cleared).

  Naming correspondence (one ladder, two views): R8's default provider
  records the reclaim-action view (2, "re-claim/requeue") while realizing
  canonical rung 2 "Restart worker" (D1's reclaim step), and the
  replacement-action view (3, "replace/reassign") while realizing canonical
  rung 3 "Replace / reassign". R9's provider returns the verbatim §E.1
  names; the rung NUMBER is the join key. R8's constants are untouched.

Budgets (contract §E.2): per-rung caps (r1 ≤ 2, r2 ≤ 2, r3 ≤ 3, r4 = 0,
r5 = 0 — rung 4 has zero executable attempts in R9, rung 5 is terminal)
and one per-incident cap (default 7). No time budget exists in the
contract, so none is invented. Budget consumption is exactly-once:
attempts are counted at reconcile time via a durable watermark
(consumed_attempt_number) in a single CAS UPDATE guarded by
remaining_budget bounds; two concurrent controllers produce exactly one
authoritative consumption and the loser re-reads.

Progress (D6): worker-reported progress is informational only.
zero_progress_count counts CONSECUTIVE terminal attempts whose
authoritative progress_delta is 0 (heartbeat-only is 0). Two consecutive
zero-progress attempts escalate one rung. Genuine progress (delta > 0)
resets the count; it never rewinds the ladder (the contract defines no
rewind). Healthy work is never recovered: R9 only ever sees
open/escalated recovery incidents.

Concurrency/crash: every policy transition is one write_txn with
version=version+1 WHERE version=?. A crash between R8 attempt creation
and policy accounting is healed by the watermark (the next pass consumes
exactly once). Unknown or ambiguous durable state fails closed (terminal
block / manual escalation); R9 never guess-retries.
"""

from __future__ import annotations

import json
import math
import threading
from dataclasses import dataclass, field

from ..store import PolicyConflict, StoreError, TransitionRejected
from ..store import transitions as T
from . import boot as boot_mod
from .recovery import (RecoveryConfig, RecoveryController, RecoveryError,
                       RungContext, RungProvider)


class PolicyError(StoreError):
    """An R9 policy operation was refused (misconfiguration, unreadable
    state, provider not bound)."""


class PolicyNotReady(PolicyError):
    """Policy evaluation refused: boot phase is not READY."""


class PolicyCorrupt(PolicyError):
    """Durable policy state is missing, unreadable, or self-contradictory.
    The incident must fail closed (block / manual escalation); R9 never
    guess-retries from a corrupt row."""


# --------------------------------------------------------------------------
# Canonical ladder (contract §E.1) — the ONE ladder. R8's
# CanonicalRungProvider keeps its own action-view mapping byte-identical;
# the rung number is the join key between the two views.

CANONICAL_LADDER: dict[int, str] = {
    1: "Retry with backoff",
    2: "Restart worker",
    3: "Replace / reassign",
    4: "Widen scope",
    5: "Replan / pause",
}

# Terminal states an R9 policy row can reach. Terminal is sticky: once set,
# no further rung selection, budget consumption, or execution happens for
# the incident.
T_RECOVERY_COMPLETE = "RECOVERY_COMPLETE"
T_BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
T_R5_TERMINAL = "R5_TERMINAL"
T_BLOCKED_CONTRADICTORY = "BLOCKED_CONTRADICTORY"
T_BLOCKED_STATE_UNAVAILABLE = "BLOCKED_STATE_UNAVAILABLE"
T_SUPERSEDED = "SUPERSEDED"

_TERMINAL_STATES = frozenset({
    T_RECOVERY_COMPLETE, T_BUDGET_EXHAUSTED, T_R5_TERMINAL,
    T_BLOCKED_CONTRADICTORY, T_BLOCKED_STATE_UNAVAILABLE, T_SUPERSEDED,
})

# R8 attempt states that still own the incident (R8 drives them; R9 waits).
_R8_OPEN_STATES = frozenset({"CREATED", "RUNNING", "VERIFYING", "UNCERTAIN"})
# R8 attempt states whose authoritative result R9 may consume.
_R8_RESULT_STATES = frozenset({"SUCCEEDED", "FAILED", "BLOCKED"})


@dataclass(frozen=True)
class PolicyConfig:
    """Validated, immutable R9 configuration.

    per_rung_max_attempts maps each canonical rung to its attempt budget.
    Rung 4 ("Widen scope") must be 0 — R9 has no stage-widening executor
    (R10+ reconciler work) — and rung 5 ("Replan / pause") must be 0 — it
    is terminal. incident_max_attempts is the per-incident cap (no infinite
    budget; the contract defines no time budget, so none is configured).
    policy_version is persisted on every policy row; a row written under a
    different version is never silently reinterpreted.
    """
    per_rung_max_attempts: dict = field(
        default_factory=lambda: {1: 2, 2: 2, 3: 3, 4: 0, 5: 0})
    incident_max_attempts: int = 7
    policy_version: str = "r9-policy/v1"
    evaluation_interval_s: float = 1.0

    def __post_init__(self) -> None:
        budgets = self.per_rung_max_attempts
        if not isinstance(budgets, dict):
            raise ValueError("per_rung_max_attempts must be a dict")
        for rung, cap in budgets.items():
            if rung not in (1, 2, 3, 4, 5):
                raise ValueError(f"unknown rung {rung!r}")
            if (not isinstance(cap, int) or isinstance(cap, bool)
                    or cap < 0):
                raise ValueError(
                    f"rung {rung}: budget must be a non-negative int")
        if budgets.get(4, 0) != 0:
            raise ValueError(
                "rung 4 'Widen scope' has no R9 executor: budget must be 0")
        if budgets.get(5, 0) != 0:
            raise ValueError(
                "rung 5 'Replan / pause' is terminal: budget must be 0")
        if (not isinstance(self.incident_max_attempts, int)
                or isinstance(self.incident_max_attempts, bool)
                or self.incident_max_attempts <= 0):
            raise ValueError(
                "incident_max_attempts must be a positive int "
                "(no infinite budget)")
        if (not isinstance(self.policy_version, str)
                or not self.policy_version):
            raise ValueError("policy_version must be a non-empty string")
        if (not isinstance(self.evaluation_interval_s, (int, float))
                or isinstance(self.evaluation_interval_s, bool)
                or not math.isfinite(self.evaluation_interval_s)
                or self.evaluation_interval_s <= 0):
            raise ValueError(
                "evaluation_interval_s must be a positive finite number")
        # Defensive copy: the config is frozen but the dict is mutable.
        # Missing rungs materialize to 0 (no budget: the rung is
        # skipped), so every downstream consumer sees the full 1..5 map.
        object.__setattr__(self, "per_rung_max_attempts",
                           {r: budgets.get(r, 0) for r in (1, 2, 3, 4, 5)})


@dataclass(frozen=True)
class RungDecision:
    """Pure selection outcome: rung (1..5), terminal (None unless this
    selection ends the incident), human-readable reason, whether the rung
    advanced, and whether the escalation is immediate (any rung → 5)."""
    rung: int
    terminal: str | None
    reason: str
    advanced: bool
    immediate: bool


def _advance(current_rung: int, reason: str) -> RungDecision:
    nxt = min(current_rung + 1, 5)
    if nxt == 5:
        return RungDecision(5, T_R5_TERMINAL,
                            f"{reason}; reached terminal rung 5",
                            advanced=True, immediate=False)
    return RungDecision(nxt, None, f"{reason}; advanced to rung {nxt}",
                        advanced=True, immediate=False)


def select_rung(*, current_rung: int, attempts_at_rung: int,
                per_rung_budgets: dict[int, int],
                incident_attempts: int, incident_budget: int,
                zero_progress_count: int,
                state_available: bool = True,
                contradictory: bool = False,
                intake_blocked: bool = False,
                rung_executable: bool = True) -> RungDecision:
    """Deterministic rung selection from durable state only.

    Rules, in order:
    1. Authoritative state unavailable → rung 5, BLOCKED_STATE_UNAVAILABLE
       (fail closed).
    2. Contradictory evidence → rung 5, BLOCKED_CONTRADICTORY (fail closed).
    3. Incident attempt budget consumed → rung 5, BUDGET_EXHAUSTED.
    4. Two consecutive zero-progress attempts → escalate one rung (D6).
    5. Per-rung attempt budget consumed → escalate one rung.
    6. Rung 4 has no R9 executor → traverse to rung 5 (durable, monotonic).
    7. R8's intake guard holds (two zero-progress attempts) and the rung is
       not otherwise executable → skip the ineffective rung (advance; the
       ladder never rewinds and never re-attempts a rung).
    8. Otherwise → continue at the current rung.

    Selection never rewinds: the returned rung is always >= current_rung,
    and is 5 only for terminal escalations.
    """
    if (not isinstance(current_rung, int)
            or isinstance(current_rung, bool)
            or current_rung not in (1, 2, 3, 4, 5)):
        raise ValueError(f"current_rung must be 1..5, got {current_rung!r}")
    if current_rung == 5:
        return RungDecision(5, T_R5_TERMINAL, "already at terminal rung",
                            advanced=False, immediate=True)
    if not state_available:
        return RungDecision(5, T_BLOCKED_STATE_UNAVAILABLE,
                            "authoritative state unavailable; fail closed",
                            advanced=True, immediate=True)
    if contradictory:
        return RungDecision(5, T_BLOCKED_CONTRADICTORY,
                            "contradictory evidence; fail closed",
                            advanced=True, immediate=True)
    if incident_attempts >= incident_budget:
        return RungDecision(5, T_BUDGET_EXHAUSTED,
                            "incident budget exhausted "
                            f"({incident_attempts}/{incident_budget})",
                            advanced=True, immediate=True)
    if zero_progress_count >= 2:
        return _advance(current_rung,
                        "two consecutive zero-progress attempts (D6)")
    budget = per_rung_budgets.get(current_rung, 0)
    if attempts_at_rung >= budget:
        return _advance(current_rung,
                        f"rung {current_rung} budget exhausted "
                        f"({attempts_at_rung}/{budget})")
    if current_rung == 4:
        return _advance(4, "rung 4 'Widen scope' has no R9 executor; "
                           "traversed durably before terminal escalation")
    if intake_blocked and not rung_executable:
        return _advance(current_rung,
                        "R8 intake guard holds (two zero-progress attempts) "
                        "and the rung is not executable via R8; skipping "
                        "the ineffective rung")
    return RungDecision(current_rung, None,
                        f"continue rung {current_rung} "
                        f"({attempts_at_rung}/{budget} used)",
                        advanced=False, immediate=False)


def _incident_job_id(inc: dict) -> str | None:
    try:
        sig = json.loads(inc.get("signature") or "{}")
    except ValueError:
        sig = {}
    job_id = sig.get("job_id")
    if job_id:
        return job_id
    try:
        det = json.loads(inc.get("detection") or "{}")
    except ValueError:
        det = {}
    return det.get("job_id")


def _validate_policy_row(pol: dict) -> None:
    """Fail-closed validation of a durable policy row. Anything missing,
    unreadable, or self-contradictory raises PolicyCorrupt."""
    try:
        assert pol["current_rung"] in (1, 2, 3, 4, 5), "current_rung not 1..5"
        assert isinstance(pol["version"], int) and pol["version"] >= 1, \
            "version not positive"
        assert isinstance(pol["incident_budget"], int) \
            and pol["incident_budget"] > 0, "incident_budget not positive"
        assert 0 <= pol["remaining_budget"] <= pol["incident_budget"], \
            "remaining_budget out of range"
        assert isinstance(pol["attempt_count"], int) \
            and pol["attempt_count"] >= 0, "attempt_count negative"
        assert (pol["attempt_count"] + pol["remaining_budget"]
                == pol["incident_budget"]), \
            "attempt_count + remaining_budget != incident_budget"
        assert pol["consumed_attempt_number"] >= 0, \
            "consumed_attempt_number negative"
        assert pol["result_consumed_attempt_number"] >= 0, \
            "result_consumed_attempt_number negative"
        assert pol["zero_progress_count"] >= 0, \
            "zero_progress_count negative"
        per_rung = json.loads(pol["per_rung_attempts"] or "{}")
        assert isinstance(per_rung, dict), "per_rung_attempts not a dict"
        budgets = json.loads(pol["per_rung_budgets"] or "{}")
        assert isinstance(budgets, dict), "per_rung_budgets not a dict"
        for rung, cap in budgets.items():
            assert int(rung) in (1, 2, 3, 4, 5), \
                f"per_rung_budgets unknown rung {rung!r}"
            assert isinstance(cap, int) and not isinstance(cap, bool) \
                and cap >= 0, \
                f"per_rung_budgets rung {rung}: not a non-negative int"
        assert pol["policy_version"], "policy_version empty"
        if pol["terminal_state"] is not None:
            assert pol["terminal_state"] in _TERMINAL_STATES, \
                "unknown terminal_state"
    except (AssertionError, ValueError, TypeError, KeyError) as exc:
        raise PolicyCorrupt(
            f"policy row for {pol.get('incident_id')}: {exc}") from exc


class PolicyRungProvider(RungProvider):
    """R9's RungProvider for R8: returns the durable policy-selected rung
    with the verbatim §E.1 name and the policy row's real budget context.

    R8 consults this provider when it creates an attempt, so every R8
    attempt row records the R9-selected rung and the durable budget state
    that authorized it."""

    def __init__(self) -> None:
        self._policy: PolicyController | None = None

    def _bind(self, policy: PolicyController) -> None:
        self._policy = policy

    def select_rung(self, *, incident: dict, failure_class: str,
                    action_name: str, history: list[dict]) -> RungContext:
        policy = self._policy
        if policy is None:
            raise PolicyError("provider not bound to a PolicyController")
        gate = policy._gate_for_thread()
        try:
            pol = policy._ensure_policy(gate, incident)
        except PolicyCorrupt as exc:
            # Fail closed: never guess a rung for an unreadable policy row.
            # The incident is contained separately (escalated to human).
            raise PolicyError(
                f"refusing rung selection for corrupt policy: {exc}") from exc
        budgets = json.loads(pol["per_rung_budgets"] or "{}")
        return RungContext(
            rung=pol["current_rung"],
            name=CANONICAL_LADDER[pol["current_rung"]],
            budget_context={
                "rung": pol["current_rung"],
                "rung_name": pol["rung_name"],
                "rung_budget": budgets.get(str(pol["current_rung"]), 0),
                "remaining_budget": pol["remaining_budget"],
                "incident_budget": pol["incident_budget"],
                "attempts_used": pol["attempt_count"],
                "policy_version": pol["policy_version"],
            },
        )


class PolicyController:
    """R9 recovery-policy controller.

    Owns the canonical ladder position, the durable attempt budgets, and
    the escalation decisions for every recovery incident. Composes with —
    never reimplements — R8: one evaluate() pass reconciles policy state
    from durable R8 evidence, drives R8 (evaluate / dispatch_restart),
    then reconciles the results. All policy mutations are CAS; all inputs
    are durable rows.
    """

    def __init__(self, db_path: str, policy_config: PolicyConfig,
                 recovery_config: RecoveryConfig,
                 actor: str = "recovery-controller",
                 supervisor=None, readiness=None) -> None:
        if not isinstance(policy_config, PolicyConfig):
            raise PolicyError("policy_config must be a PolicyConfig")
        if not isinstance(recovery_config, RecoveryConfig):
            raise PolicyError("recovery_config must be a RecoveryConfig")
        self.policy_config = policy_config
        self.actor = actor
        self.db_path = db_path
        self._provider = PolicyRungProvider()
        self._provider._bind(self)
        if supervisor is not None:
            rc_kwargs = {"supervisor": supervisor}
            # Same readiness derivation as RecoveryController.
            self._readiness = (
                lambda: (supervisor._boot_report or {}).get("phase")
                == "READY")
        elif readiness is not None:
            rc_kwargs = {"readiness": readiness}
            self._readiness = readiness
        else:
            raise PolicyError(
                "a readiness callable or a supervisor is required")
        self.rc = RecoveryController(db_path, recovery_config, actor=actor,
                                     rung_provider=self._provider,
                                     **rc_kwargs)
        self.store = self.rc.store
        self.gate = self.rc.gate
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # -- thread-local gate -------------------------------------------
    def _gate_for_thread(self):
        # R9 shares R8's thread-local gate machinery: every thread that
        # touches the store gets its own handle; the main thread reuses
        # the controller's.
        return self.rc._gate_for_thread()

    # -- policy row lifecycle -----------------------------------------
    def _ensure_policy(self, gate, incident: dict) -> dict:
        """Fetch-or-create the durable policy row; validate it. Raises
        PolicyCorrupt on version mismatch or unreadable state (fail
        closed: the caller must block/escalate, never reinterpret)."""
        incident_id = incident["incident_id"]
        pol = gate.get_recovery_policy(incident_id)
        if pol is None:
            pol = gate.ensure_recovery_policy(
                incident_id,
                policy_version=self.policy_config.policy_version,
                current_rung=1,
                rung_name=CANONICAL_LADDER[1],
                incident_budget=self.policy_config.incident_max_attempts,
                per_rung_budgets=self.policy_config.per_rung_max_attempts,
                actor=self.actor,
            )
            gate.append_event(
                "recovery.policy_created",
                {"incident_id": incident_id,
                 "policy_version": self.policy_config.policy_version,
                 "rung": 1,
                 "incident_budget":
                     self.policy_config.incident_max_attempts},
                self.actor)
        if pol["policy_version"] != self.policy_config.policy_version:
            raise PolicyCorrupt(
                f"incident {incident_id}: policy row version "
                f"{pol['policy_version']!r} != controller "
                f"{self.policy_config.policy_version!r}; refusing to "
                f"reinterpret old policy state under a new definition")
        _validate_policy_row(pol)
        return pol

    def _cas(self, gate, incident_id: str, pol: dict,
             updates: dict) -> dict | None:
        """CAS policy update; None on a lost race (caller defers and the
        next pass re-reads the authoritative row)."""
        try:
            return gate.cas_update_recovery_policy(
                incident_id, pol["version"], updates, self.actor)
        except PolicyConflict:
            return None

    # -- main pass ------------------------------------------------------
    def evaluate(self) -> list[dict]:
        """One R9 policy pass: reconcile policy state from durable R8
        evidence, drive R8 (exactly one attempt per decision at most),
        then reconcile the results. Deterministic: identical durable state
        in → identical pass out."""
        if not self._readiness():
            raise PolicyNotReady("boot not READY: policy evaluation refused")
        gate = self._gate_for_thread()
        outcomes: list[dict] = []
        outcomes.extend(self._reconcile_all(gate))
        outcomes.extend(self.rc.evaluate())
        outcomes.extend(self._reconcile_all(gate))
        return outcomes

    def start(self) -> None:
        """Start the bounded background evaluation loop (one pass per
        evaluation_interval_s). The loop never silently dies; it is the
        cadence loop, not a retry loop — every attempt is durably recorded
        between ticks."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop, name="axos-r9-policy", daemon=True)
            self._thread.start()

    def _loop(self) -> None:
        while not self._stop.wait(
                self.policy_config.evaluation_interval_s):
            try:
                self.evaluate()
            except Exception:
                # Never silently die. (Errors are loud to the caller of
                # evaluate(); the loop keeps the cadence.)
                continue

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        if (t is not None and t.is_alive()
                and threading.get_ident() != t.ident):
            t.join(timeout=10)

    def close(self) -> None:
        try:
            self.stop()
        finally:
            self.rc.close()

    # -- reconciliation ---------------------------------------------------
    def _reconcile_all(self, gate) -> list[dict]:
        outcomes = []
        seen = set()
        for inc in gate.open_recovery_incidents(scope="recovery"):
            seen.add(inc["incident_id"])
            outcomes.append(self._reconcile_incident(gate, inc["incident_id"]))
        for inc in gate.escalated_recovery_incidents(escalated_to="r9-policy"):
            if inc["incident_id"] in seen:
                continue
            seen.add(inc["incident_id"])
            outcomes.append(self._reconcile_incident(gate, inc["incident_id"]))
        # Incidents R8 already closed but whose policy row is not terminal
        # yet: converge the terminal policy state from the durable
        # incident outcome.
        for inc in gate.recovery_incidents_with_open_policy():
            if inc["incident_id"] in seen:
                continue
            seen.add(inc["incident_id"])
            outcomes.append(self._reconcile_incident(gate, inc["incident_id"]))
        return outcomes

    def _reconcile_incident(self, gate, incident_id: str) -> dict:
        try:
            inc = gate.get_recovery_incident(incident_id)
        except Exception as exc:
            raise PolicyError(
                f"incident {incident_id} unreadable: {exc}") from exc
        if inc is None:
            return {"incident_id": incident_id, "outcome": "blocked",
                    "detail": "unknown incident: fail closed, no action"}
        try:
            pol = self._ensure_policy(gate, inc)
        except PolicyCorrupt as exc:
            return self._block_corrupt(gate, inc, str(exc))
        if pol["terminal_state"] is not None:
            return {"incident_id": incident_id, "outcome": "terminal",
                    "terminal_state": pol["terminal_state"]}
        # Converge incident-level outcomes set by R8's authority.
        outcome = inc.get("outcome")
        if outcome == "success":
            return self._apply_terminal(
                gate, incident_id, pol, None, T_RECOVERY_COMPLETE,
                f"R8 closed incident as success ({inc.get('diagnosis')})")
        if outcome == "stopped":
            return self._apply_terminal(
                gate, incident_id, pol, None, T_RECOVERY_COMPLETE,
                f"R8 stopped incident ({inc.get('diagnosis')})")
        if outcome == "superseded":
            return self._apply_terminal(
                gate, incident_id, pol, None, T_SUPERSEDED,
                "incident superseded: rung continues under another incident")
        if (outcome == "escalated"
                and inc.get("escalated_to") == "human"):
            return self._apply_terminal(
                gate, incident_id, pol, None, T_R5_TERMINAL,
                "incident already human-escalated: converging policy")
        # Resolve the job (read-only; unreadable → fail closed, never
        # guess).
        job_id = _incident_job_id(inc)
        try:
            job = gate.get_job(job_id) if job_id else None
        except TransitionRejected:
            job = None
        except Exception as exc:
            raise PolicyError(
                f"job {job_id} unreadable: {exc}") from exc
        if job is None:
            return self._apply_terminal(
                gate, incident_id, pol, None, T_BLOCKED_STATE_UNAVAILABLE,
                "job row unavailable: fail closed")
        if job["status"] in ("COMPLETE", "FAILED"):
            return self._apply_terminal(
                gate, incident_id, pol, job, T_RECOVERY_COMPLETE,
                f"job terminal ({job['status']}): nothing to recover")
        try:
            task = gate.get_task(job["task_id"])
        except Exception as exc:
            raise PolicyError(
                f"task {job['task_id']} unreadable: {exc}") from exc
        if task["status"] == "PAUSED_FOR_HUMAN":
            # I-17: the human gate is sticky and never auto-cleared; R9
            # stands down rather than working around it.
            return {"incident_id": incident_id, "outcome": "stood-down",
                    "detail": "task PAUSED_FOR_HUMAN: auto-recovery stood "
                              "down (I-17)"}
        history = gate.recovery_attempts_for(incident_id)
        if any(a["attempt_number"] is None for a in history):
            # Unnumbered attempt rows predate R8's numbered attempts; the
            # watermark scheme cannot account them. Contradictory
            # evidence: fail closed, never guess.
            return self._block_corrupt(
                gate, inc,
                "recovery attempt row(s) without attempt_number: "
                "unaccountable evidence")
        pol = self._consume_created(gate, incident_id, pol, job, history)
        if pol is None:
            return {"incident_id": incident_id, "outcome": "deferred",
                    "detail": "policy CAS lost; re-read next pass"}
        if pol["terminal_state"] is not None:
            return {"incident_id": incident_id, "outcome": "terminal",
                    "terminal_state": pol["terminal_state"]}
        pol = self._consume_results(gate, incident_id, pol, history)
        if pol is None:
            return {"incident_id": incident_id, "outcome": "deferred",
                    "detail": "policy CAS lost; re-read next pass"}
        contradictory = any(
            a["attempt_state"] in ("BLOCKED", "FAILED")
            and a.get("decision") == "escalate"
            for a in history)
        intake_blocked = self.rc._consecutive_failures(history) >= 2
        open_attempts = [a for a in history
                         if a["attempt_state"] in _R8_OPEN_STATES]
        restart_executable = (
            pol["current_rung"] in (2, 3)
            and job["owner_worker_id"] is None
            and not open_attempts
            and not self._lingering(gate, inc, job)
        )
        rung_executable = (not intake_blocked) or restart_executable
        per_rung_budgets = {int(k): int(v) for k, v in
                            json.loads(pol["per_rung_budgets"] or "{}").items()}
        attempts_here = {
            str(k): int(v) for k, v in
            json.loads(pol["per_rung_attempts"] or "{}").items()
        }.get(str(pol["current_rung"]), 0)
        decision = select_rung(
            current_rung=pol["current_rung"],
            attempts_at_rung=attempts_here,
            per_rung_budgets=per_rung_budgets,
            incident_attempts=pol["attempt_count"],
            incident_budget=pol["incident_budget"],
            zero_progress_count=pol["zero_progress_count"],
            contradictory=contradictory,
            intake_blocked=intake_blocked,
            rung_executable=rung_executable,
        )
        if decision.terminal is not None:
            return self._apply_terminal(gate, incident_id, pol, job,
                                        decision.terminal, decision.reason)
        if decision.advanced:
            pol = self._advance_rung(gate, incident_id, pol, decision)
            if pol is None:
                return {"incident_id": incident_id, "outcome": "deferred",
                        "detail": "rung-advance CAS lost; re-read next pass"}
        return self._maybe_execute(gate, incident_id, pol, job, history,
                                   restart_executable)

    # -- budget + result consumption ---------------------------------------
    def _consume_created(self, gate, incident_id: str, pol: dict, job: dict,
                         history: list[dict]) -> dict | None:
        """Exactly-once accounting for R8 attempts observed since the last
        pass. The durable watermark (consumed_attempt_number) plus the
        CAS `remaining_budget >= payable` guard means two concurrent
        controllers produce exactly one authoritative consumption: the
        loser gets PolicyConflict and defers. A crash between R8 attempt
        creation and this commit heals on the next pass (the watermark
        still trails the attempt rows). When the incident budget is
        already zero but R8 created further attempts under its own
        authority, no CAS can ever succeed — converge to BUDGET_EXHAUSTED
        instead of deferring forever."""
        upto = max((a["attempt_number"] for a in history), default=0)
        if upto <= pol["consumed_attempt_number"]:
            return pol
        if pol["remaining_budget"] <= 0:
            # The incident budget is gone but R8 created further attempts
            # under its own authority (its intake's two-attempt rule is
            # independent of R9's budget): no further autonomous attempts
            # are possible, so converge to terminal escalation instead of
            # deferring on a CAS that can never succeed.
            self._apply_terminal(
                gate, incident_id, pol, job, T_BUDGET_EXHAUSTED,
                f"incident budget exhausted "
                f"({pol['attempt_count']}/{pol['incident_budget']} "
                f"consumed); R8 attempt(s) beyond the budget observed: "
                f"no further autonomous attempts")
            return gate.get_recovery_policy(incident_id)
        new = {a["attempt_number"]: a for a in history
               if a["attempt_number"] > pol["consumed_attempt_number"]}
        per_rung = {str(k): int(v) for k, v in
                    json.loads(pol["per_rung_attempts"] or "{}").items()}
        for n in sorted(new):
            r = str(new[n]["rung"])
            per_rung[r] = per_rung.get(r, 0) + 1
        last = new[max(new)]
        try:
            res = gate.consume_policy_attempts(
                incident_id, pol["version"],
                upto_attempt_number=upto,
                per_rung_attempts=per_rung,
                last_attempt_id=last["attempt_id"],
                actor=self.actor)
        except PolicyConflict:
            return None
        new_pol = res["row"]
        gate.append_event(
            "recovery.policy_attempts_consumed",
            {"incident_id": incident_id,
             "attempt_numbers": sorted(new),
             "consumed": res["consumed"],
             "attempt_count": new_pol["attempt_count"],
             "remaining_budget": new_pol["remaining_budget"],
             "budget_exhausted": res["exhausted"]},
            self.actor)
        if res["exhausted"]:
            # The incident budget could not cover every observed attempt:
            # no further autonomous attempts; terminal escalation.
            self._apply_terminal(
                gate, incident_id, new_pol, job, T_BUDGET_EXHAUSTED,
                f"incident budget exhausted "
                f"({new_pol['attempt_count']}/{new_pol['incident_budget']} "
                f"consumed): no further autonomous attempts")
            new_pol = gate.get_recovery_policy(incident_id)
        return new_pol

    def _consume_results(self, gate, incident_id: str, pol: dict,
                         history: list[dict]) -> dict | None:
        """Fold terminal R8 attempt results into zero_progress_count (D6:
        consecutive authoritative zero-progress attempts; genuine progress
        resets the count). Consumed via a second watermark so each result
        is counted exactly once."""
        wm = pol["result_consumed_attempt_number"]
        terminal = [a for a in history
                    if a["attempt_number"] > wm
                    and a["attempt_state"] in _R8_RESULT_STATES]
        if not terminal:
            return pol
        terminal.sort(key=lambda a: a["attempt_number"])
        zpc = pol["zero_progress_count"]
        for a in terminal:
            delta = a.get("progress_delta") or 0.0
            if delta > 0:
                # Genuine authoritative progress: the consecutive run
                # breaks. The ladder never rewinds (no rung reset).
                zpc = 0
            else:
                zpc += 1
        last = terminal[-1]
        new_pol = self._cas(gate, incident_id, pol, {
            "result_consumed_attempt_number":
                max(a["attempt_number"] for a in terminal),
            "zero_progress_count": zpc,
            "last_attempt_result": {
                "attempt_id": last["attempt_id"],
                "attempt_number": last["attempt_number"],
                "attempt_state": last["attempt_state"],
                "decision": last.get("decision"),
                "progress_delta": last.get("progress_delta"),
                "rung": last.get("rung"),
            },
        })
        if new_pol is None:
            return None
        gate.append_event(
            "recovery.policy_results_consumed",
            {"incident_id": incident_id,
             "attempt_numbers": [a["attempt_number"] for a in terminal],
             "zero_progress_count": zpc},
            self.actor)
        return new_pol

    # -- rung transitions ----------------------------------------------------
    def _advance_rung(self, gate, incident_id: str, pol: dict,
                      decision: RungDecision) -> dict | None:
        assert decision.rung > pol["current_rung"], \
            "the ladder never rewinds"
        assert decision.rung <= 5, "no rung above 5"
        new_pol = self._cas(gate, incident_id, pol, {
            "current_rung": decision.rung,
            "rung_name": CANONICAL_LADDER[decision.rung],
            "zero_progress_count": 0,
        })
        if new_pol is None:
            return None
        gate.append_event(
            "recovery.policy_rung_advanced",
            {"incident_id": incident_id,
             "from_rung": pol["current_rung"],
             "to_rung": decision.rung,
             "rung_name": CANONICAL_LADDER[decision.rung],
             "reason": decision.reason,
             "policy_version": self.policy_config.policy_version},
            self.actor)
        return new_pol

    def _apply_terminal(self, gate, incident_id: str, pol: dict,
                        job: dict | None, terminal: str,
                        reason: str) -> dict:
        """Sticky terminal: persist the terminal state (CAS), escalate to
        a human for the manual-review terminals, and — for rung 5 — pause
        the task through TransitionGate. Terminal never auto-clears."""
        assert terminal in _TERMINAL_STATES, f"unknown terminal {terminal}"
        if pol["terminal_state"] is not None:
            return {"incident_id": incident_id, "outcome": "terminal",
                    "terminal_state": pol["terminal_state"]}
        note = None
        if terminal == T_R5_TERMINAL and job is not None:
            note = self._pause_task_for_human(gate, job, incident_id, reason)
        escalate = terminal in (T_R5_TERMINAL, T_BUDGET_EXHAUSTED,
                                T_BLOCKED_CONTRADICTORY,
                                T_BLOCKED_STATE_UNAVAILABLE)
        if escalate:
            gate.set_incident_escalated(incident_id, reason, self.actor,
                                        escalated_to="human")
        new_pol = self._cas(gate, incident_id, pol, {
            "terminal_state": terminal,
            "escalation_state": "terminal",
            **({"escalation_target": "human"} if escalate else {}),
        })
        if new_pol is None:
            return {"incident_id": incident_id, "outcome": "deferred",
                    "detail": "terminal CAS lost; re-read next pass"}
        gate.append_event(
            "recovery.policy_terminal",
            {"incident_id": incident_id, "terminal_state": terminal,
             "reason": reason, "task_pause_note": note,
             "policy_version": self.policy_config.policy_version},
            self.actor)
        return {"incident_id": incident_id, "outcome": "terminal",
                "terminal_state": terminal, "reason": reason}

    def _converge_terminal(self, gate, incident_id: str, pol: dict,
                           terminal: str, reason: str) -> dict:
        return self._apply_terminal(gate, incident_id, pol, None,
                                    terminal, reason)

    def _converge_from_stale(self, gate, incident_id: str, pol: dict,
                               stale: dict) -> dict:
        """The STALE_AUTHORITY restart incident was already closed by R8's
        authority before R9 dispatched: the original incident has nothing
        left to decide. Converge its policy from the stale incident's
        durable outcome (never defer forever, never re-dispatch)."""
        sid = stale["incident_id"]
        outcome = stale.get("outcome")
        if outcome in ("success", "stopped"):
            return self._apply_terminal(
                gate, incident_id, pol, None, T_RECOVERY_COMPLETE,
                f"restart incident {sid} already resolved by R8 "
                f"({outcome}): nothing left to recover")
        if outcome == "superseded":
            return self._apply_terminal(
                gate, incident_id, pol, None, T_SUPERSEDED,
                f"restart incident {sid} superseded")
        if outcome == "escalated" and stale.get("escalated_to") == "human":
            return self._apply_terminal(
                gate, incident_id, pol, None, T_R5_TERMINAL,
                f"restart incident {sid} human-escalated: converging "
                f"policy")
        # R8 escalated the restart incident to r9-policy: the stale
        # incident's own R9 flow owns the ladder now; link and stand
        # down here.
        new_pol = self._cas(gate, incident_id, pol, {
            "superseded_by": sid,
            "terminal_state": T_SUPERSEDED,
            "escalation_state": "terminal",
        })
        if new_pol is None:
            return {"incident_id": incident_id, "outcome": "deferred",
                    "detail": "stale-converge CAS lost; re-read next pass"}
        gate.set_incident_outcome(
            incident_id, "superseded",
            f"restart continues under incident {sid} "
            f"(escalated to r9-policy)", self.actor)
        gate.append_event(
            "recovery.policy_superseded",
            {"from_incident": incident_id, "to_incident": sid,
             "reason": "stale incident already escalated to r9-policy"},
            self.actor)
        return {"incident_id": incident_id, "outcome": "terminal",
                "terminal_state": T_SUPERSEDED,
                "reason": f"restart incident {sid} owns the ladder"}

    def _block_corrupt(self, gate, inc: dict, detail: str) -> dict:
        incident_id = inc["incident_id"]
        try:
            gate.set_incident_escalated(
                incident_id,
                f"R9 policy state corrupt/unreadable: {detail}",
                self.actor, escalated_to="human")
        except (TransitionRejected, StoreError):
            pass  # incident already resolved; containment holds regardless
        return {"incident_id": incident_id, "outcome": "blocked",
                "detail": f"policy state corrupt ({detail}); escalated to "
                          f"human; no retry attempted"}

    def _pause_task_for_human(self, gate, job: dict, incident_id: str,
                              reason: str) -> str:
        """Rung 5's action: enter the human-gated task state. Best-effort
        and fully journaled; never invents a transition the task graph
        forbids, never auto-clears (I-17)."""
        try:
            task = gate.get_task(job["task_id"])
        except Exception as exc:
            return f"task unreadable: {exc}"
        if task["status"] == "PAUSED_FOR_HUMAN":
            return "already PAUSED_FOR_HUMAN (I-17 sticky; never auto-cleared)"
        if "PAUSED_FOR_HUMAN" not in T.TASK_TRANSITIONS.get(task["status"],
                                                            ()):
            return (f"cannot pause from {task['status']}: no transition "
                    f"attempted; left for human")
        try:
            gate.transition_task(
                job["task_id"], "PAUSED_FOR_HUMAN", self.actor,
                pause_reason=f"r9-policy rung 5 terminal: {reason}",
                pause_diagnostic={
                    "incident_id": incident_id,
                    "rung": 5,
                    "rung_name": CANONICAL_LADDER[5],
                    "job_id": job["job_id"],
                    "policy_version": self.policy_config.policy_version,
                })
        except (TransitionRejected, StoreError) as exc:
            return f"pause refused: {exc}"
        return "task paused for human (R9 rung 5)"

    # -- execution (R8 seams only) ----------------------------------------------
    def _lingering(self, gate, inc: dict, job: dict) -> bool:
        """Read-only lingering-process evidence (R6 classifier over durable
        spawn rows), mirroring R8's check. Never touches processes."""
        try:
            det = json.loads(inc.get("detection") or "{}")
        except ValueError:
            return False
        prev_owner = det.get("owner_worker_id")
        if not prev_owner:
            return False
        spawns = gate.unreaped_proc_spawns(prev_owner)
        latest = max(spawns, key=lambda s: s["spawned_at"]) if spawns else None
        if latest is None:
            return False
        verdict, _, _ = boot_mod._classify_spawn(latest)
        return verdict == "live_match"

    def _maybe_execute(self, gate, incident_id: str, pol: dict, job: dict,
                       history: list[dict],
                       restart_executable: bool) -> dict:
        rung = pol["current_rung"]
        if rung == 4:
            return {"incident_id": incident_id, "outcome": "no-executor",
                    "detail": "rung 4 'Widen scope' has no R9 executor "
                              "(stage-widening is R10+ reconciler work); "
                              "rung traversed durably, terminal escalation "
                              "next pass"}
        if rung not in (2, 3):
            return {"incident_id": incident_id, "outcome": "no-execution",
                    "detail": f"rung {rung} executes via the R8 intake path; "
                              f"R9 does not act directly"}
        if not restart_executable:
            open_n = sum(1 for a in history
                         if a["attempt_state"] in _R8_OPEN_STATES)
            return {"incident_id": incident_id,
                    "outcome": "deferred-to-intake",
                    "detail": "rung 2/3 restart not directly executable "
                              f"(owned={job['owner_worker_id'] is not None}, "
                              f"open_attempts={open_n}): the R8 intake owns "
                              f"the next move"}
        return self._execute_restart(gate, incident_id, pol, job, rung)

    def _execute_restart(self, gate, incident_id: str, pol: dict,
                         job: dict, rung: int) -> dict:
        """Rung 2/3 restart via R8's typed seam (dispatch_restart) only.
        The restart continues under a STALE_AUTHORITY incident that
        inherits the ladder position, so rung and budget identity stay
        coherent; the originating incident is superseded (durable link)."""
        try:
            job = gate.get_job(job["job_id"])
        except Exception as exc:
            raise PolicyError(
                f"job {job['job_id']} unreadable: {exc}") from exc
        if job is None or job["status"] in ("COMPLETE", "FAILED"):
            return {"incident_id": incident_id, "outcome": "deferred",
                    "detail": "job terminal or vanished before restart"}
        if job["owner_worker_id"] is not None:
            return {"incident_id": incident_id,
                    "outcome": "deferred-to-intake",
                    "detail": "job owned: restart refused without reclaim; "
                              "R8 intake owns the reclaim"}
        inc = gate.get_recovery_incident(incident_id)
        failure_class = None
        try:
            failure_class = json.loads(
                inc.get("signature") or "{}").get("failure_class")
        except ValueError:
            pass
        if failure_class == "STALE_AUTHORITY":
            target_id, target_pol = incident_id, pol
        else:
            now = gate.store.current_time()
            stale = gate.find_or_create_recovery_incident(
                job["job_id"], "STALE_AUTHORITY",
                {"job_id": job["job_id"],
                 "explicit": f"r9 rung-{rung} restart",
                 "from_incident": incident_id,
                 "observed_at": now},
                self.actor)
            if stale.get("outcome") is not None:
                # R8 already resolved the restart incident under its own
                # authority: converge the original from the stale
                # incident's durable outcome instead of deferring
                # forever on a restart that will never be dispatched.
                return self._converge_from_stale(
                    gate, incident_id, pol, stale)
            try:
                target_pol = self._ensure_policy(gate, stale)
            except PolicyCorrupt as exc:
                return self._block_corrupt(gate, stale, str(exc))
            if target_pol["terminal_state"] is not None:
                return {"incident_id": incident_id, "outcome": "deferred",
                        "detail": "restart incident policy terminal"}
            if target_pol["current_rung"] < rung:
                target_pol = self._cas(gate, stale["incident_id"],
                                       target_pol, {
                                           "current_rung": rung,
                                           "rung_name": CANONICAL_LADDER[rung],
                                       })
                if target_pol is None:
                    return {"incident_id": incident_id,
                            "outcome": "deferred",
                            "detail": "restart-incident rung CAS lost"}
            pol = self._cas(gate, incident_id, pol, {
                "superseded_by": stale["incident_id"],
                "terminal_state": T_SUPERSEDED,
                "escalation_state": "terminal",
            })
            if pol is None:
                return {"incident_id": incident_id, "outcome": "deferred",
                        "detail": "supersede CAS lost; re-read next pass"}
            gate.set_incident_outcome(
                incident_id, "superseded",
                f"rung-{rung} restart continues as incident "
                f"{stale['incident_id']}", self.actor)
            gate.append_event(
                "recovery.policy_superseded",
                {"from_incident": incident_id,
                 "to_incident": stale["incident_id"], "rung": rung},
                self.actor)
            target_id = stale["incident_id"]
        try:
            result = self.rc.dispatch_restart(job["job_id"])
        except (RecoveryError, TransitionRejected, StoreError) as exc:
            return {"incident_id": target_id, "outcome": "deferred",
                    "detail": f"R8 dispatch_restart refused: {exc}"}
        gate.append_event(
            "recovery.policy_restart_dispatched",
            {"incident_id": target_id, "rung": rung,
             "attempt_id": result["attempt_id"]}, self.actor)
        return {"incident_id": target_id, "outcome": "restart-dispatched",
                "rung": rung, "attempt_id": result["attempt_id"]}
