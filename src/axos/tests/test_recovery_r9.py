"""Phase 1C R9 — Recovery Ladder, Budgets & Escalation Policy tests.

STANDARD: real SQLite/WAL; real components (store, gate, R8 controller,
supervisor where the action needs one); no mocks of the store/gate/policy.
Deterministic seeding through the real R8 authority APIs; deterministic
polling instead of blind sleeps; every spawned process is killed and
reaped in tearDown.

R9 is the policy layer above R8's execution/verification substrate: it
CHOOSES the rung (the canonical 5-rung ladder), BOUNDS recovery (durable
per-rung and per-incident attempt budgets), and DECIDES terminal
escalation. It never executes a recovery action and never verifies one.

NOTE on drivers: most ladder tests drive exactly one
``pc._reconcile_incident(pc.gate, incident_id)`` pass per rung step (the
deterministic unit under test). A full ``pc.evaluate()`` runs reconcile,
then R8, then reconcile again; its trailing pass legitimately applies the
rule-7 rung skip (R8's intake guard holds and the rung is not executable),
so per-rung assertions are written against single passes.

Test IDs:
  Ladder / selection
  R9-01  new incident -> rung 1 "Retry with backoff" (policy row via evaluate)
  R9-02  CANONICAL_LADDER verbatim; R8's _CANONICAL_RUNGS byte-identical
  R9-03  determinism: same durable state -> identical select_rung decisions;
         provider returns identical RungContext twice
  R9-04  rung-1 repeat within budget (pure select_rung)
  R9-05  rung-1 budget exhausted -> advance to 2 (pure select_rung)
  R9-06  R1->R2 integration: two FAILED rung-1 attempts -> rung 2
  R9-07  R2->R3: two FAILED rung-2 attempts -> rung 3
  R9-08  R3->R4: two FAILED rung-3 attempts -> rung 4 "Widen scope";
         _maybe_execute reports "no-executor"
  R9-09  R4->R5: terminal R5_TERMINAL, human escalation, task PAUSED_FOR_HUMAN
  R9-10  per-rung budget race: one new attempt, two reconcilers ->
         attempt_count incremented exactly once
  R9-11  incident budget race: remaining_budget never negative, exactly-once
  R9-12  two PolicyControllers racing evaluate() -> single consumption
  R9-13  rung-transition race: zpc=2 durable -> exactly one advance
  Progress
  R9-14  zero-progress attempt -> zero_progress_count increments to 1
  R9-15  two consecutive zero-progress -> escalate (advance rung)
  R9-16  genuine progress resets zpc and never rewinds the ladder
  Composition / authority
  R9-17  R9 invokes R8 only via the typed seams (AST + behavioral):
         rung-2 restart dispatches through pc.rc.dispatch_restart
  R9-18  attempt rows retain the selected rung + real budget context
         (no "deferred_to" marker)
  R9-19  successful recovery never marks the job COMPLETE
  Durability / restart
  R9-20  rung-5 terminal sticky across controller restart
  R9-21  restart preserves rung
  R9-22  restart preserves budget
  R9-23  restart preserves escalation state
  Crash boundaries
  R9-24  crash after R8 attempt creation, before policy commit (C3/C4):
         exactly-once consumption
  R9-25  crash after dispatch, before reconcile (C4/C5): counted once
  R9-26  crash after R8 success (C6): converges to RECOVERY_COMPLETE
  R9-27  crash before policy result commit (C8): re-reconcile is a no-op
  R9-28  corrupt policy row fails closed (blocked, human, no retry)
  Policy version
  R9-29  policy_version persisted and reconstructed by a new controller
  R9-30  version mismatch -> blocked, human-escalated, no rung selection
  Authority / static
  R9-31  no hidden retry loop (AST + behavioral: one evaluate() creates at
         most one attempt per incident)
  R9-32  cannot directly reclaim leases (AST + hasattr)
  R9-33  cannot directly fence (AST)
  R9-34  cannot bypass R5 completion (AST; transition_task is allowed)
  R9-35  consumes R7/R8 evidence, redefines nothing (AST + behavioral zpc)
  R9-36  full R1-R8 composition: STALLED -> forced reclaim -> success
  Migration
  R9-M1  v6 -> v7: recovery_policy table exists; policy row round-trips
  Race extras
  R9-RACE-1  budget double-consume -> exactly one consumption
  R9-RACE-2  rung double-advance -> advances exactly once (2, not 3)
  R9-RACE-3  two controllers racing evaluate() where R8 would create an
             attempt -> exactly one attempt row
  Crash extras
  R9-C1  crash before policy evaluation: only the policy row is created
  R9-C2  crash after policy decision: select_rung recomputes identically
  R9-C7  crash after failure result: counted exactly once
  R9-C9  crash after escalation commit: still terminal, single terminal event
  Fix follow-ups (implementation defects found in coordinator review; each
  is fixed in the product and pinned here as a regression test)
  R9-ZB1  partial per_rung_max_attempts materializes missing rungs to 0;
         unknown/bool/negative/non-zero-r4/r5/non-positive-incident budgets
         still rejected
  R9-ZB2  zero per-rung budget -> the rung is skipped without executing
  R9-BE1  zero remaining budget + new R8 attempts -> terminal
         BUDGET_EXHAUSTED, never an infinite defer
  R9-UN1  unnumbered attempt rows -> fail closed (blocked, human escalation)
  R9-SC1  closed STALE_AUTHORITY restart incident -> the original converges
         from its durable outcome instead of deferring forever
  R9-FK1  ensure_recovery_policy on a missing incident -> TransitionRejected
         (FK violations are never masked as create races)
  R9-FK2  per_rung_budgets is not CAS-updatable (budget identity is set once
         at creation)
  R9-PB1  corrupt per_rung_budgets blob -> blocked, human escalation
"""
import ast
import json
import os
import signal
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace

from axos.exec.policy import (CANONICAL_LADDER, PolicyConfig,  # noqa: E402
                              PolicyController, PolicyCorrupt, RungDecision,
                              select_rung, T_RECOVERY_COMPLETE,
                              T_BUDGET_EXHAUSTED, T_R5_TERMINAL,
                              T_BLOCKED_CONTRADICTORY,
                              T_BLOCKED_STATE_UNAVAILABLE, T_SUPERSEDED)
from axos.exec.recovery import (RecoveryConfig, _CANONICAL_RUNGS)  # noqa: E402
from axos.exec.supervisor import Supervisor  # noqa: E402
from axos.store import (PolicyConflict, StoreError, TransitionGate,  # noqa: E402
                        TransitionRejected, open_store, migrate)
from axos.store.migrations import MIGRATIONS  # noqa: E402


def _policy_tree():
    path = os.path.join(HERE, "..", "exec", "policy.py")
    with open(path) as f:
        return ast.parse(f.read())


def _called_names(tree):
    """Every called name/attribute in the module (Name.id + Attribute.attr)."""
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            if isinstance(fn, ast.Name):
                names.add(fn.id)
            elif isinstance(fn, ast.Attribute):
                names.add(fn.attr)
    return names


def _imported_modules(tree):
    mods = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                mods.add(a.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                mods.add(node.module.split(".")[0])
    return mods


def _while_called_names(tree):
    """For each `while` loop, the names called anywhere inside it."""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.While):
            names = set()
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call):
                    fn = sub.func
                    if isinstance(fn, ast.Name):
                        names.add(fn.id)
                    elif isinstance(fn, ast.Attribute):
                        names.add(fn.attr)
            out.append(names)
    return out


class RecoveryBase(unittest.TestCase):
    H = 0.4

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r9-")
        self.db = os.path.join(self.tmp, "t.db")
        self._sups: list[Supervisor] = []
        self._pcs: list[PolicyController] = []
        self._temp_stores: list = []
        self.sup = self._new_sup(H=self.H, actor="test-r9")
        self.gate = self.sup.gate
        self.gate.create_task("t", {"objective": "x"}, {"usd": 1},
                              "scheduler")

    def tearDown(self):
        for pc in self._pcs:
            try:
                pc.close()
            except Exception:
                pass
        for s in self._sups:
            try:
                for wid, info in list(s._procs.items()):
                    p = info.popen
                    try:
                        if p.poll() is None:
                            os.killpg(p.pid, signal.SIGKILL)
                    except Exception:
                        pass
                for wid, info in list(s._procs.items()):
                    try:
                        info.popen.wait(timeout=10)
                    except Exception:
                        pass
            except Exception:
                pass
        for s in self._sups:
            try:
                s.close()
            except Exception:
                pass
        for st in self._temp_stores:
            try:
                st.close()
            except Exception:
                pass
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- helpers
    def _new_sup(self, H=None, actor=None) -> Supervisor:
        s = Supervisor(self.db, actor=actor or f"test-r9-{len(self._sups)}",
                       heartbeat_interval_s=H or self.H)
        self._sups.append(s)
        return s

    def _new_pc(self, supervisor=None, readiness=None,
                **kw) -> PolicyController:
        pcfg = PolicyConfig(**kw)
        rcfg = RecoveryConfig(
            observation_window_s=0.2, evaluation_interval_s=0.1,
            claim_timeout_s=60.0, restart_worker_duration_s=30.0,
            restart_worker_ttl_s=10.0, restart_worker_hb_interval_s=0.2)
        pckw: dict = {}
        if supervisor is not None:
            pckw["supervisor"] = supervisor
        elif readiness is not None:
            pckw["readiness"] = readiness
        else:
            pckw["readiness"] = lambda: True
        pc = PolicyController(self.db, pcfg, rcfg,
                              actor=f"test-pc-{len(self._pcs)}", **pckw)
        self._pcs.append(pc)
        return pc

    def _seed_owned(self, jid, wid, ttl=60.0):
        """Owned RUNNING job + worker row; live lease, no verdict."""
        gate = self.gate
        gate.create_job(jid, "t", "s", "scheduler")
        self.sup._ensure_worker_row(wid)
        self.assertTrue(gate.claim_job(jid, wid, ttl, "scheduler"))
        gate.transition_job(jid, "RUNNING", f"worker:{wid}")
        return gate.get_job(jid)

    def _seed_incident(self, jid, failure_class="LEASE_EXPIRED",
                       detection=None):
        return self.gate.find_or_create_recovery_incident(
            jid, failure_class, detection or {"job_id": jid}, "test")

    def _seed_failed(self, iid, jid, rung, n=2, delta=0.0,
                     decision="retry", action_name="reclaim",
                     failure_class="LEASE_EXPIRED"):
        """Seed n terminal FAILED attempts through the real R8 authority
        APIs (create -> claim -> complete)."""
        gate = self.gate
        rung_name = CANONICAL_LADDER[rung]
        atts = []
        for _ in range(n):
            tok = gate.get_job(jid)["fencing_token"]
            att = gate.create_recovery_attempt(
                incident_id=iid, job_id=jid, fencing_token=tok,
                rung=rung, rung_name=rung_name,
                action={"name": action_name, "rung": rung,
                        "rung_name": rung_name},
                failure_class=failure_class, success_criterion="s",
                failure_criterion="f", evidence_before={}, actor="test")
            self.assertTrue(
                gate.claim_recovery_attempt(att["attempt_id"],
                                            "seed-controller"))
            gate.complete_recovery_attempt(
                att["attempt_id"], decision=decision,
                observed_effect="none", progress_delta=delta,
                resulting_state="FAILED", evidence_after={}, actor="test")
            atts.append(att)
        return atts

    def _seed_succeeded(self, iid, jid, rung, delta=1.0,
                        action_name="reclaim"):
        gate = self.gate
        tok = gate.get_job(jid)["fencing_token"]
        rung_name = CANONICAL_LADDER[rung]
        att = gate.create_recovery_attempt(
            incident_id=iid, job_id=jid, fencing_token=tok,
            rung=rung, rung_name=rung_name,
            action={"name": action_name, "rung": rung,
                    "rung_name": rung_name},
            failure_class="LEASE_EXPIRED", success_criterion="s",
            failure_criterion="f", evidence_before={}, actor="test")
        self.assertTrue(
            gate.claim_recovery_attempt(att["attempt_id"],
                                        "seed-controller"))
        gate.complete_recovery_attempt(
            att["attempt_id"], decision="success",
            observed_effect="recovered", progress_delta=delta,
            resulting_state="SUCCEEDED", evidence_after={}, actor="test")
        return att

    def _advance_one_rung(self, pc, iid, jid, rung):
        """Seed two zero-progress failures at `rung`, reconcile one pass."""
        self._seed_failed(iid, jid, rung=rung, n=2,
                          action_name="restart" if rung >= 2 else "reclaim")
        return pc._reconcile_incident(pc.gate, iid)

    def _reconcile(self, pc, iid):
        return pc._reconcile_incident(pc.gate, iid)

    def _policy(self, gate, iid):
        return gate.get_recovery_policy(iid)

    def _ledger(self, gate, event_type, filt=None):
        rows = gate.store.conn.execute(
            "SELECT actor, payload FROM ledger WHERE event_type=?"
            " ORDER BY seq", (event_type,)).fetchall()
        evs = []
        for actor, payload in rows:
            p = json.loads(payload)
            p["_actor"] = actor
            evs.append(p)
        if filt is not None:
            evs = [e for e in evs if filt(e)]
        return evs

    def _race_fns(self, fns, timeout=60):
        """Run fns (each in its own thread) behind a barrier; re-raise the
        first thread error, if any."""
        n = len(fns)
        barrier = threading.Barrier(n)
        errors = []

        def run(fn):
            try:
                barrier.wait(timeout=15)
                fn()
            except Exception as exc:  # noqa: BLE001 - re-raised below
                errors.append(exc)

        threads = [threading.Thread(target=run, args=(fn,), daemon=True)
                   for fn in fns]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=timeout)
        self.assertEqual([t.is_alive() for t in threads], [False] * n,
                         "worker thread hung")
        if errors:
            raise errors[0]

    def _seed_stalled(self, jid="j-s", wid="w-s"):
        """Owned RUNNING job + STALLED verdict on a live lease: R8's
        naturalistic intake path (forced reclaim)."""
        gate = self.gate
        job = self._seed_owned(jid, wid)
        gate.record_watchdog_verdict(jid, job["fencing_token"], "STALLED",
                                     {"reason": "test-r9"}, "test-r9")
        return gate.get_job(jid)

    def _drive_until_policy_terminal(self, pc, gate, iid, timeout=20.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pc.evaluate()
            pol = gate.get_recovery_policy(iid)
            if pol is not None and pol["terminal_state"] is not None:
                return pol
            time.sleep(0.05)
        raise AssertionError(
            f"policy for {iid} never reached a terminal state")

# ---------------------------------------------------------------- ladder


class TestR901(RecoveryBase):
    def test_R9_01_new_incident_rung1(self):
        """A fresh incident converges on rung 1 "Retry with backoff": the
        policy row is created by evaluate() with the default budgets."""
        gate = self.gate
        self._seed_owned("j-901", "w-901")
        inc = self._seed_incident("j-901")
        iid = inc["incident_id"]
        pc = self._new_pc()
        pc.evaluate()
        pol = self._policy(gate, iid)
        self.assertIsNotNone(pol)
        self.assertEqual(pol["current_rung"], 1)
        self.assertEqual(pol["rung_name"], "Retry with backoff")
        self.assertEqual(pol["incident_budget"], 7)
        self.assertEqual(pol["remaining_budget"], 7)
        self.assertEqual(pol["attempt_count"], 0)
        self.assertEqual(pol["version"], 1)
        # R8 saw nothing actionable (live lease, no verdict): no attempt.
        self.assertEqual(gate.recovery_attempts_for(iid), [])


class TestR902(RecoveryBase):
    def test_R9_02_canonical_ladder_and_r8_mapping(self):
        """The five rung names are verbatim; R8's action-keyed canonical
        mapping is byte-identical (the only contract R9 trusts from R8)."""
        self.assertEqual(CANONICAL_LADDER, {
            1: "Retry with backoff",
            2: "Restart worker",
            3: "Replace / reassign",
            4: "Widen scope",
            5: "Replan / pause",
        })
        self.assertEqual(_CANONICAL_RUNGS, {
            "reclaim": (2, "re-claim/requeue"),
            "fence": (3, "replace/reassign"),
            "restart": (3, "replace/reassign"),
        })


class TestR903(RecoveryBase):
    def test_R9_03_determinism(self):
        """Same durable state -> two fresh PolicyControllers compute the
        identical select_rung decision; the provider returns the identical
        RungContext twice."""
        gate = self.gate
        self._seed_owned("j-903", "w-903")
        inc = self._seed_incident("j-903")
        iid = inc["incident_id"]
        pc1 = self._new_pc()
        self._reconcile(pc1, iid)
        pol = self._policy(gate, iid)
        args = dict(
            current_rung=pol["current_rung"],
            attempts_at_rung=json.loads(
                pol["per_rung_attempts"]).get("1", 0),
            per_rung_budgets={int(k): v for k, v in
                              json.loads(pol["per_rung_budgets"]).items()},
            incident_attempts=pol["attempt_count"],
            incident_budget=pol["incident_budget"],
            zero_progress_count=pol["zero_progress_count"])
        d1 = select_rung(**args)
        d2 = select_rung(**args)
        self.assertEqual(d1, d2)
        self.assertIsInstance(d1, RungDecision)
        # Two fresh controllers over the same durable row agree.
        pc2 = self._new_pc()
        pol2 = self._policy(pc2.gate, iid)
        self.assertEqual((pol2["current_rung"], pol2["rung_name"],
                          pol2["remaining_budget"]),
                         (pol["current_rung"], pol["rung_name"],
                          pol["remaining_budget"]))
        c1 = pc1._provider.select_rung(
            incident=inc, failure_class="LEASE_EXPIRED",
            action_name="reclaim", history=[])
        c2 = pc1._provider.select_rung(
            incident=inc, failure_class="LEASE_EXPIRED",
            action_name="reclaim", history=[])
        self.assertEqual((c1.rung, c1.name, c1.budget_context),
                         (c2.rung, c2.name, c2.budget_context))


class TestR904(RecoveryBase):
    def test_R9_04_rung1_repeat_within_budget(self):
        """Pure select_rung: 0 and 1 attempts against a rung-1 budget of 2
        continue at rung 1 (no advance, no terminal)."""
        budgets = {1: 2, 2: 2, 3: 3, 4: 0, 5: 0}
        for used in (0, 1):
            d = select_rung(current_rung=1, attempts_at_rung=used,
                            per_rung_budgets=budgets,
                            incident_attempts=used, incident_budget=7,
                            zero_progress_count=0)
            self.assertEqual(d.rung, 1)
            self.assertIsNone(d.terminal)
            self.assertFalse(d.advanced)


class TestR905(RecoveryBase):
    def test_R9_05_rung1_budget_exhausted_advances(self):
        """Pure select_rung: 2 attempts against a rung-1 budget of 2
        advances to rung 2 (never a terminal from the middle ladder)."""
        d = select_rung(current_rung=1, attempts_at_rung=2,
                        per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0},
                        incident_attempts=2, incident_budget=7,
                        zero_progress_count=0)
        self.assertEqual(d.rung, 2)
        self.assertIsNone(d.terminal)
        self.assertTrue(d.advanced)


class TestR906(RecoveryBase):
    def test_R9_06_r1_to_r2_integration(self):
        """Two FAILED rung-1 attempts (zero progress) -> rung 2
        "Restart worker"; exactly one recovery.policy_rung_advanced event."""
        gate = self.gate
        self._seed_owned("j-906", "w-906")
        inc = self._seed_incident("j-906")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._seed_failed(iid, "j-906", rung=1, n=2)
        out = self._reconcile(pc, iid)
        # The pass advanced to rung 2, and the new rung defers to the R8
        # intake path (owned job is not restart-executable).
        self.assertEqual(out["outcome"], "deferred-to-intake")
        pol = self._policy(gate, iid)
        self.assertEqual(pol["current_rung"], 2)
        self.assertEqual(pol["rung_name"], "Restart worker")
        self.assertEqual(pol["zero_progress_count"], 0)
        self.assertEqual(pol["attempt_count"], 2)
        self.assertEqual(pol["remaining_budget"], 5)
        evs = self._ledger(gate, "recovery.policy_rung_advanced",
                           lambda e: e.get("incident_id") == iid)
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["from_rung"], 1)
        self.assertEqual(evs[0]["to_rung"], 2)
        self.assertEqual(evs[0]["rung_name"], "Restart worker")
        self.assertIn("policy_version", evs[0])


class TestR907(RecoveryBase):
    def test_R9_07_r2_to_r3(self):
        """Two FAILED rung-2 attempts -> rung 3 "Replace / reassign"."""
        gate = self.gate
        self._seed_owned("j-907", "w-907")
        inc = self._seed_incident("j-907")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._advance_one_rung(pc, iid, "j-907", 1)
        self.assertEqual(self._policy(gate, iid)["current_rung"], 2)
        self._advance_one_rung(pc, iid, "j-907", 2)
        pol = self._policy(gate, iid)
        self.assertEqual(pol["current_rung"], 3)
        self.assertEqual(pol["rung_name"], "Replace / reassign")
        self.assertEqual(pol["attempt_count"], 4)


class TestR908(RecoveryBase):
    def test_R9_08_r3_to_r4_no_executor(self):
        """Two FAILED rung-3 attempts -> rung 4 "Widen scope"; rung 4 has no
        executor, so the pass reports "no-executor" without side effects."""
        gate = self.gate
        self._seed_owned("j-908", "w-908")
        inc = self._seed_incident("j-908")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._advance_one_rung(pc, iid, "j-908", 1)
        self._advance_one_rung(pc, iid, "j-908", 2)
        out = self._advance_one_rung(pc, iid, "j-908", 3)
        pol = self._policy(gate, iid)
        self.assertEqual(pol["current_rung"], 4)
        self.assertEqual(pol["rung_name"], "Widen scope")
        self.assertEqual(out["outcome"], "no-executor")
        self.assertEqual(len(gate.recovery_attempts_for(iid)), 6)


class TestR909(RecoveryBase):
    def test_R9_09_r4_to_r5_terminal_pauses_task(self):
        """From rung 4 the next reconcile applies terminal R5_TERMINAL: the
        incident is human-escalated and the AUTHORIZED task is paused."""
        gate = self.gate
        self._seed_owned("j-909", "w-909")
        inc = self._seed_incident("j-909")
        iid = inc["incident_id"]
        gate.transition_task("t", "AUTHORIZED", "test")
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=4,
            rung_name="Widen scope", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        pc = self._new_pc()
        out = self._reconcile(pc, iid)
        self.assertEqual(out["outcome"], "terminal")
        self.assertEqual(out["terminal_state"], T_R5_TERMINAL)
        pol = self._policy(gate, iid)
        self.assertEqual(pol["terminal_state"], T_R5_TERMINAL)
        self.assertEqual(pol["escalation_target"], "human")
        self.assertEqual(pol["escalation_state"], "terminal")
        inc2 = gate.get_recovery_incident(iid)
        self.assertEqual(inc2["outcome"], "escalated")
        self.assertEqual(inc2["escalated_to"], "human")
        self.assertEqual(gate.get_task("t")["status"], "PAUSED_FOR_HUMAN")
        evs = self._ledger(gate, "recovery.policy_terminal",
                           lambda e: e.get("incident_id") == iid)
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["terminal_state"], T_R5_TERMINAL)


class TestR910(RecoveryBase):
    def test_R9_10_per_rung_budget_race(self):
        """Two reconcilers racing over one new attempt consume it exactly
        once: attempt_count == 1, per-rung totals sum to 1."""
        gate = self.gate
        self._seed_owned("j-910", "w-910")
        inc = self._seed_incident("j-910")
        iid = inc["incident_id"]
        pc = self._new_pc()
        gate.create_recovery_attempt(
            incident_id=iid, job_id="j-910",
            fencing_token=gate.get_job("j-910")["fencing_token"],
            rung=1, rung_name="Retry with backoff",
            action={"name": "reclaim", "rung": 1,
                    "rung_name": "Retry with backoff"},
            failure_class="LEASE_EXPIRED", success_criterion="s",
            failure_criterion="f", evidence_before={}, actor="test")
        self._race_fns([
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid),
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid)])
        pol = self._policy(gate, iid)
        self.assertEqual(pol["attempt_count"], 1)
        self.assertEqual(pol["remaining_budget"], 6)
        per_rung = {int(k): v for k, v in
                    json.loads(pol["per_rung_attempts"]).items()}
        self.assertEqual(sum(per_rung.values()), 1)


class TestR911(RecoveryBase):
    def test_R9_11_incident_budget_race(self):
        """Four reconcilers racing over three new attempts: exactly-once
        consumption, remaining_budget == 4, never negative."""
        gate = self.gate
        self._seed_owned("j-911", "w-911")
        inc = self._seed_incident("j-911")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._seed_failed(iid, "j-911", rung=1, n=3)
        self._race_fns([
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid),
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid),
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid),
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid)])
        pol = self._policy(gate, iid)
        self.assertEqual(pol["attempt_count"], 3)
        self.assertEqual(pol["remaining_budget"], 4)
        self.assertGreaterEqual(pol["remaining_budget"], 0)
        per_rung = {int(k): v for k, v in
                    json.loads(pol["per_rung_attempts"]).items()}
        self.assertEqual(sum(per_rung.values()), 3)


class TestR912(RecoveryBase):
    def test_R9_12_two_controllers_race_evaluate(self):
        """Two PolicyControllers racing evaluate() over one new FAILED
        attempt: the attempt is consumed exactly once."""
        gate = self.gate
        self._seed_owned("j-912", "w-912")
        inc = self._seed_incident("j-912")
        iid = inc["incident_id"]
        self._seed_failed(iid, "j-912", rung=1, n=1)
        pc1 = self._new_pc()
        pc2 = self._new_pc()
        self._race_fns([pc1.evaluate, pc2.evaluate])
        pol = self._policy(gate, iid)
        self.assertEqual(pol["attempt_count"], 1)
        self.assertEqual(pol["remaining_budget"], 6)
        self.assertEqual(pol["zero_progress_count"], 1)


class TestR913(RecoveryBase):
    def test_R9_13_rung_transition_race(self):
        """zpc=2 durable, two reconcilers racing: exactly one rung advance
        (one version bump, one ledger event)."""
        gate = self.gate
        self._seed_owned("j-913", "w-913")
        inc = self._seed_incident("j-913")
        iid = inc["incident_id"]
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=1,
            rung_name="Retry with backoff", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        gate.cas_update_recovery_policy(
            iid, 1, {"zero_progress_count": 2,
                     "result_consumed_attempt_number": 10}, "test")
        pc = self._new_pc()
        self._race_fns([
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid),
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid)])
        pol = self._policy(gate, iid)
        self.assertEqual(pol["current_rung"], 2)
        self.assertEqual(pol["version"], 3)
        self.assertEqual(pol["zero_progress_count"], 0)
        evs = self._ledger(gate, "recovery.policy_rung_advanced",
                           lambda e: e.get("incident_id") == iid)
        self.assertEqual(len(evs), 1)

# ---------------------------------------------------------------- progress


class TestR914(RecoveryBase):
    def test_R9_14_zero_progress_increments_zpc(self):
        """One zero-progress FAILED attempt -> zero_progress_count == 1
        (progress evidence consumed from the attempt row, never redefined)."""
        gate = self.gate
        self._seed_owned("j-914", "w-914")
        inc = self._seed_incident("j-914")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._seed_failed(iid, "j-914", rung=1, n=1, delta=0.0)
        out = self._reconcile(pc, iid)
        self.assertEqual(out["outcome"], "no-execution")
        pol = self._policy(gate, iid)
        self.assertEqual(pol["zero_progress_count"], 1)
        self.assertEqual(pol["attempt_count"], 1)
        self.assertEqual(pol["current_rung"], 1)


class TestR915(RecoveryBase):
    def test_R9_15_two_zero_progress_escalate(self):
        """Two consecutive zero-progress attempts -> the policy escalates
        (advances the rung)."""
        gate = self.gate
        self._seed_owned("j-915", "w-915")
        inc = self._seed_incident("j-915")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._seed_failed(iid, "j-915", rung=1, n=2, delta=0.0)
        self._reconcile(pc, iid)
        pol = self._policy(gate, iid)
        self.assertEqual(pol["current_rung"], 2)
        self.assertEqual(pol["zero_progress_count"], 0)


class TestR916(RecoveryBase):
    def test_R9_16_progress_resets_zpc_no_rewind(self):
        """A successful attempt (positive progress delta) resets zpc and
        the ladder never rewinds: rung stays 2."""
        gate = self.gate
        self._seed_owned("j-916", "w-916")
        inc = self._seed_incident("j-916")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._advance_one_rung(pc, iid, "j-916", 1)
        self.assertEqual(self._policy(gate, iid)["current_rung"], 2)
        self._seed_succeeded(iid, "j-916", rung=2, delta=1.0,
                             action_name="restart")
        self._reconcile(pc, iid)
        pol = self._policy(gate, iid)
        self.assertEqual(pol["zero_progress_count"], 0)
        self.assertEqual(pol["current_rung"], 2)
        self.assertEqual(pol["attempt_count"], 3)


# ------------------------------------------------------- composition/auth


class TestR917(RecoveryBase):
    def test_R9_17a_policy_never_calls_r8_authorities(self):
        """AST: policy.py contains no call to reclaim_lease, fence_sweep,
        or start_worker. R9 decides; R8 acts."""
        called = _called_names(_policy_tree())
        for name in ("reclaim_lease", "fence_sweep", "start_worker"):
            self.assertNotIn(name, called,
                             f"policy.py must not call {name}")

    def test_R9_17b_ownerless_rung2_restart_goes_through_dispatch(self):
        """Behavioral: an ownerless job with policy at rung 2 -> evaluate()
        returns outcome "restart-dispatched" and the attempt row's
        action.name == "restart" (dispatched via pc.rc.dispatch_restart)."""
        gate = self.gate
        gate.create_job("j-917", "t", "s", "scheduler")
        inc = self._seed_incident("j-917")
        iid = inc["incident_id"]
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=2,
            rung_name="Restart worker", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        pc = self._new_pc(supervisor=self.sup)
        outcomes = pc.evaluate()
        dispatched = [o for o in outcomes
                      if o.get("outcome") == "restart-dispatched"]
        self.assertEqual(len(dispatched), 1,
                         f"expected one restart-dispatched, got {outcomes}")
        target_id = dispatched[0]["incident_id"]
        atts = gate.recovery_attempts_for(target_id)
        self.assertEqual(len(atts), 1)
        action = json.loads(atts[0]["action"])
        self.assertEqual(action["name"], "restart")
        self.assertEqual(atts[0]["rung"], 2)
        # R9 has no direct reclaim authority of its own.
        self.assertFalse(hasattr(pc, "reclaim_lease"))
        self.assertNotIn("reclaim_lease", _called_names(_policy_tree()))


class TestR918(RecoveryBase):
    def test_R9_18_attempt_rows_carry_rung_and_budget(self):
        """An attempt created through the naturalistic R8 intake carries the
        actual R9-selected rung and real budget context (no deferred_to)."""
        gate = self.gate
        self._seed_stalled("j-918", "w-918")
        pc = self._new_pc()
        outcomes = pc.evaluate()
        created = [o for o in outcomes
                   if o.get("outcome") == "attempt-created"]
        self.assertEqual(len(created), 1)
        iid = created[0]["incident_id"]
        atts = gate.recovery_attempts_for(iid)
        self.assertEqual(len(atts), 1)
        self.assertEqual(atts[0]["rung"], 1)
        self.assertEqual(atts[0]["rung_name"], "Retry with backoff")
        bc = json.loads(atts[0]["budget_context"])
        self.assertEqual(bc["remaining_budget"], 7)
        self.assertEqual(bc["incident_budget"], 7)
        self.assertEqual(bc["policy_version"], "r9-policy/v1")
        self.assertNotIn("deferred_to", bc)


class TestR919(RecoveryBase):
    def test_R9_19_recovery_never_completes_job(self):
        """STALLED + forced reclaim drives to RECOVERY_COMPLETE, and the
        job is never marked COMPLETE (that contract belongs to the
        artifact-commit path)."""
        gate = self.gate
        job = self._seed_stalled("j-919", "w-919")
        pc = self._new_pc()
        outcomes = pc.evaluate()
        iid = [o for o in outcomes
               if o.get("outcome") == "attempt-created"][0]["incident_id"]
        pol = self._drive_until_policy_terminal(pc, gate, iid)
        self.assertEqual(pol["terminal_state"], T_RECOVERY_COMPLETE)
        final = gate.get_job("j-919")
        self.assertNotEqual(final["status"], "COMPLETE")

# ------------------------------------------------- durability / restart


class TestR920(RecoveryBase):
    def test_R9_20_r5_terminal_sticky_across_restart(self):
        """R5_TERMINAL survives a controller restart: a fresh controller
        still reports terminal, creates no attempts, and the task stays
        paused."""
        gate = self.gate
        self._seed_owned("j-920", "w-920")
        inc = self._seed_incident("j-920")
        iid = inc["incident_id"]
        gate.transition_task("t", "AUTHORIZED", "test")
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=4,
            rung_name="Widen scope", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        pc1 = self._new_pc()
        out1 = self._reconcile(pc1, iid)
        self.assertEqual(out1["terminal_state"], T_R5_TERMINAL)
        pc2 = self._new_pc()
        out2 = pc2._reconcile_incident(pc2.gate, iid)
        self.assertEqual(out2["outcome"], "terminal")
        self.assertEqual(out2["terminal_state"], T_R5_TERMINAL)
        pc2.evaluate()
        self.assertEqual(gate.recovery_attempts_for(iid), [])
        self.assertEqual(self._policy(gate, iid)["terminal_state"],
                         T_R5_TERMINAL)
        self.assertEqual(gate.get_task("t")["status"], "PAUSED_FOR_HUMAN")


class TestR921(RecoveryBase):
    def test_R9_21_restart_preserves_rung(self):
        """Mid-ladder (rung 3): a fresh controller sees the same rung."""
        gate = self.gate
        self._seed_owned("j-921", "w-921")
        inc = self._seed_incident("j-921")
        iid = inc["incident_id"]
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=3,
            rung_name="Replace / reassign", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        pc1 = self._new_pc()
        out1 = self._reconcile(pc1, iid)
        self.assertEqual(out1["outcome"], "deferred-to-intake")
        pc2 = self._new_pc()
        out2 = self._reconcile(pc2, iid)
        self.assertEqual(out2["outcome"], "deferred-to-intake")
        self.assertEqual(self._policy(gate, iid)["current_rung"], 3)


class TestR922(RecoveryBase):
    def test_R9_22_restart_preserves_budget(self):
        """remaining_budget / attempt_count are identical after a controller
        restart (durable in the policy row, never recomputed)."""
        gate = self.gate
        self._seed_owned("j-922", "w-922")
        inc = self._seed_incident("j-922")
        iid = inc["incident_id"]
        pc1 = self._new_pc()
        self._seed_failed(iid, "j-922", rung=1, n=1)
        self._reconcile(pc1, iid)
        before = self._policy(gate, iid)
        pc2 = self._new_pc()
        self._reconcile(pc2, iid)
        after = self._policy(gate, iid)
        self.assertEqual((after["attempt_count"], after["remaining_budget"]),
                         (before["attempt_count"],
                          before["remaining_budget"]))
        self.assertEqual((after["attempt_count"], after["remaining_budget"]),
                         (1, 6))


class TestR923(RecoveryBase):
    def test_R9_23_restart_preserves_escalation(self):
        """Terminal + escalation fields survive a controller restart."""
        gate = self.gate
        self._seed_owned("j-923", "w-923")
        inc = self._seed_incident("j-923")
        iid = inc["incident_id"]
        gate.transition_task("t", "AUTHORIZED", "test")
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=4,
            rung_name="Widen scope", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        pc1 = self._new_pc()
        self._reconcile(pc1, iid)
        pc2 = self._new_pc()
        self._reconcile(pc2, iid)
        pol = self._policy(gate, iid)
        self.assertEqual(pol["terminal_state"], T_R5_TERMINAL)
        self.assertEqual(pol["escalation_state"], "terminal")
        self.assertEqual(pol["escalation_target"], "human")


# -------------------------------------------------------- crash boundaries


class TestR924(RecoveryBase):
    def test_R9_24_crash_after_attempt_creation(self):
        """C3/C4 — an attempt created and claimed via the gate (simulating a
        crash after R8's attempt creation but before any policy commit) is
        consumed exactly once by a fresh controller's evaluate()."""
        gate = self.gate
        self._seed_owned("j-924", "w-924")
        inc = self._seed_incident("j-924")
        iid = inc["incident_id"]
        att = gate.create_recovery_attempt(
            incident_id=iid, job_id="j-924",
            fencing_token=gate.get_job("j-924")["fencing_token"],
            rung=1, rung_name="Retry with backoff",
            action={"name": "reclaim", "rung": 1,
                    "rung_name": "Retry with backoff"},
            failure_class="LEASE_EXPIRED", success_criterion="s",
            failure_criterion="f", evidence_before={}, actor="test")
        self.assertTrue(gate.claim_recovery_attempt(
            att["attempt_id"], "crashed-controller"))
        # The "crashed" controller never reconciled; a fresh one takes over.
        pc = self._new_pc()
        pc.evaluate()
        pol = self._policy(gate, iid)
        self.assertEqual(pol["attempt_count"], 1)
        self.assertEqual(pol["remaining_budget"], 6)
        self.assertEqual(
            gate.get_recovery_attempt(att["attempt_id"])["attempt_state"],
            "RUNNING")


class TestR925(RecoveryBase):
    def test_R9_25_crash_after_dispatch(self):
        """C4/C5 — an attempt claimed then completed as FAILED via the gate
        (simulating a crash after R8's dispatch but before policy
        reconcile) is counted once: zpc == 1."""
        gate = self.gate
        self._seed_owned("j-925", "w-925")
        inc = self._seed_incident("j-925")
        iid = inc["incident_id"]
        att = gate.create_recovery_attempt(
            incident_id=iid, job_id="j-925",
            fencing_token=gate.get_job("j-925")["fencing_token"],
            rung=1, rung_name="Retry with backoff",
            action={"name": "reclaim", "rung": 1,
                    "rung_name": "Retry with backoff"},
            failure_class="LEASE_EXPIRED", success_criterion="s",
            failure_criterion="f", evidence_before={}, actor="test")
        gate.claim_recovery_attempt(att["attempt_id"], "crashed-controller")
        gate.complete_recovery_attempt(
            att["attempt_id"], decision="retry", observed_effect="none",
            progress_delta=0.0, resulting_state="FAILED",
            evidence_after={}, actor="test")
        pc = self._new_pc()
        pc.evaluate()
        pol = self._policy(gate, iid)
        self.assertEqual(pol["attempt_count"], 1)
        self.assertEqual(pol["zero_progress_count"], 1)
        evs = self._ledger(gate, "recovery.policy_results_consumed",
                           lambda e: e.get("incident_id") == iid)
        self.assertEqual(len(evs), 1)


class TestR926(RecoveryBase):
    def test_R9_26_crash_after_success(self):
        """C6 — the success result and incident outcome were committed before
        the crash: a fresh controller converges to RECOVERY_COMPLETE and
        creates no extra attempt."""
        gate = self.gate
        self._seed_owned("j-926", "w-926")
        inc = self._seed_incident("j-926")
        iid = inc["incident_id"]
        self._seed_succeeded(iid, "j-926", rung=1, delta=1.0)
        pc1 = self._new_pc()
        self._reconcile(pc1, iid)  # policy row exists before the crash
        # R8's success commit lands (incident outcome), then the crash.
        gate.set_incident_outcome(iid, "success", "r8 recovered", "test")
        pc2 = self._new_pc()
        pc2.evaluate()
        pol = self._policy(gate, iid)
        self.assertEqual(pol["terminal_state"], T_RECOVERY_COMPLETE)
        self.assertEqual(len(gate.recovery_attempts_for(iid)), 1)


class TestR927(RecoveryBase):
    def test_R9_27_crash_before_policy_commit(self):
        """C8 — calling _reconcile_incident twice in a row: the second pass
        is a no-op (version and zpc unchanged, decision recomputed
        identically)."""
        gate = self.gate
        self._seed_owned("j-927", "w-927")
        inc = self._seed_incident("j-927")
        iid = inc["incident_id"]
        self._seed_failed(iid, "j-927", rung=1, n=1)
        pc = self._new_pc()
        out1 = self._reconcile(pc, iid)
        pol1 = dict(self._policy(gate, iid))
        out2 = self._reconcile(pc, iid)
        pol2 = self._policy(gate, iid)
        self.assertEqual(pol2["version"], pol1["version"])
        self.assertEqual(pol2["zero_progress_count"],
                         pol1["zero_progress_count"])
        self.assertEqual(out1, out2)
        self.assertEqual(out2["outcome"], "no-execution")


class TestR928(RecoveryBase):
    def test_R9_28_corrupt_policy_row_fails_closed(self):
        """A hand-corrupted policy row (remaining_budget = -5) fails closed:
        outcome "blocked", incident escalated to human, no attempt ever
        created from it."""
        gate = self.gate
        self._seed_owned("j-928", "w-928")
        inc = self._seed_incident("j-928")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._reconcile(pc, iid)  # create the policy row
        with gate.store.write_txn() as (conn, _now):
            conn.execute("UPDATE recovery_policy SET remaining_budget=-5"
                         " WHERE incident_id=?", (iid,))
        # Single deterministic pass: outcome "blocked" exactly once.
        out = self._reconcile(pc, iid)
        self.assertEqual(out["outcome"], "blocked")
        self.assertIn("corrupt", out["detail"])
        inc2 = gate.get_recovery_incident(iid)
        self.assertEqual(inc2["outcome"], "escalated")
        self.assertEqual(inc2["escalated_to"], "human")
        self.assertEqual(gate.recovery_attempts_for(iid), [])
        self.assertEqual(self._policy(gate, iid)["attempt_count"], 0)


# -------------------------------------------------------- policy version


class TestR929(RecoveryBase):
    def test_R9_29_policy_version_persisted(self):
        """policy_version is written at creation and reconstructed by a new
        controller from the durable row."""
        gate = self.gate
        self._seed_owned("j-929", "w-929")
        inc = self._seed_incident("j-929")
        iid = inc["incident_id"]
        pc1 = self._new_pc()
        self._reconcile(pc1, iid)
        self.assertEqual(self._policy(gate, iid)["policy_version"],
                         "r9-policy/v1")
        pc2 = self._new_pc()
        out = self._reconcile(pc2, iid)  # would raise PolicyCorrupt if lost
        self.assertEqual(out["outcome"], "no-execution")
        self.assertEqual(self._policy(gate, iid)["policy_version"],
                         "r9-policy/v1")


class TestR930(RecoveryBase):
    def test_R9_30_version_mismatch_blocked(self):
        """A row written under "r9-policy/v0" is contradictory to this
        controller's "r9-policy/v1": blocked, human-escalated, no rung
        selection performed."""
        gate = self.gate
        self._seed_owned("j-930", "w-930")
        inc = self._seed_incident("j-930")
        iid = inc["incident_id"]
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v0", current_rung=1,
            rung_name="Retry with backoff", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        pc = self._new_pc()
        out = self._reconcile(pc, iid)
        self.assertEqual(out["outcome"], "blocked")
        self.assertIn("r9-policy/v0", out["detail"])
        self.assertIn("r9-policy/v1", out["detail"])
        inc2 = gate.get_recovery_incident(iid)
        self.assertEqual(inc2["outcome"], "escalated")
        self.assertEqual(inc2["escalated_to"], "human")
        pol = self._policy(gate, iid)
        self.assertIsNone(pol["terminal_state"])
        self.assertEqual(pol["current_rung"], 1)
        self.assertEqual(gate.recovery_attempts_for(iid), [])

# --------------------------------------------------- authority / static


class TestR931(RecoveryBase):
    def test_R9_31a_no_hidden_retry_loop(self):
        """AST: no `while` loop in policy.py may call
        create_recovery_attempt or dispatch_restart. The background cadence
        loop itself (no such calls) is allowed."""
        whiles = _while_called_names(_policy_tree())
        self.assertTrue(whiles, "expected at least the cadence while loop")
        for names in whiles:
            self.assertNotIn("create_recovery_attempt", names,
                             "hidden retry loop creating attempts")
            self.assertNotIn("dispatch_restart", names,
                             "hidden retry loop dispatching restarts")

    def test_R9_31b_one_evaluate_creates_at_most_one_attempt(self):
        """Behavioral: one pc.evaluate() creates at most one attempt per
        incident (the naturalistic intake path)."""
        gate = self.gate
        self._seed_stalled("j-931", "w-931")
        pc = self._new_pc()
        pc.evaluate()
        incs = gate.open_recovery_incidents()
        self.assertEqual(len(incs), 1)
        atts = gate.recovery_attempts_for(incs[0]["incident_id"])
        self.assertLessEqual(len(atts), 1)
        self.assertEqual(len(atts), 1)


class TestR932(RecoveryBase):
    def test_R9_32_cannot_directly_reclaim(self):
        """policy.py never calls reclaim_lease (AST) and the PolicyController
        exposes no reclaim_lease attribute (behavioral)."""
        self.assertNotIn("reclaim_lease", _called_names(_policy_tree()))
        pc = self._new_pc()
        self.assertFalse(hasattr(pc, "reclaim_lease"))


class TestR933(RecoveryBase):
    def test_R9_33_cannot_directly_fence(self):
        """AST: no fence_sweep, no os.kill/killpg, no subprocess, no signal
        anywhere in policy.py (no imports, no calls)."""
        tree = _policy_tree()
        called = _called_names(tree)
        for name in ("fence_sweep", "kill", "killpg", "Popen", "subprocess",
                     "signal"):
            self.assertNotIn(name, called,
                             f"policy.py must not reference {name}")
        imported = _imported_modules(tree)
        for mod in ("subprocess", "signal", "os"):
            self.assertNotIn(mod, imported,
                             f"policy.py must not import {mod}")


class TestR934(RecoveryBase):
    def test_R9_34_cannot_bypass_r5_completion(self):
        """AST: no commit_artifact/stage_artifact/verify_artifact/
        begin_commit/transition_job in policy.py; transition_task (the rung-5
        pause carve-out) IS present."""
        called = _called_names(_policy_tree())
        for name in ("commit_artifact", "stage_artifact", "verify_artifact",
                     "begin_commit", "transition_job"):
            self.assertNotIn(name, called,
                             f"policy.py must not call {name}")
        self.assertIn("transition_task", called,
                      "the rung-5 human-pause carve-out must be present")


class TestR935(RecoveryBase):
    def test_R9_35a_no_watchdog_verdict_recording(self):
        """AST: policy.py never records watchdog verdicts."""
        self.assertNotIn("record_watchdog_verdict",
                         _called_names(_policy_tree()))

    def test_R9_35b_progress_evidence_consumed_from_attempts(self):
        """Behavioral: zpc follows the attempt rows' progress_delta (0.0 ->
        increment, 1.0 -> reset), never a verdict the policy recorded."""
        gate = self.gate
        self._seed_owned("j-935", "w-935")
        inc = self._seed_incident("j-935")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._seed_failed(iid, "j-935", rung=1, n=1, delta=0.0)
        self._reconcile(pc, iid)
        self.assertEqual(self._policy(gate, iid)["zero_progress_count"], 1)
        self._seed_succeeded(iid, "j-935", rung=1, delta=1.0)
        self._reconcile(pc, iid)
        self.assertEqual(self._policy(gate, iid)["zero_progress_count"], 0)


class TestR936(RecoveryBase):
    def test_R9_36_full_r1_r8_composition(self):
        """Full naturalistic composition: STALLED verdict on a live lease ->
        forced reclaim -> success -> incident closed -> R9 converges to
        RECOVERY_COMPLETE. Exactly one rung-1 attempt (delta 1.0), budget
        6 remaining, job never COMPLETE."""
        gate = self.gate
        self._seed_stalled("j-936", "w-936")
        pc = self._new_pc()
        outcomes = pc.evaluate()
        created = [o for o in outcomes
                   if o.get("outcome") == "attempt-created"]
        self.assertEqual(len(created), 1)
        iid = created[0]["incident_id"]
        pol = self._drive_until_policy_terminal(pc, gate, iid)
        self.assertEqual(pol["terminal_state"], T_RECOVERY_COMPLETE)
        inc = gate.get_recovery_incident(iid)
        self.assertEqual(inc["outcome"], "success")
        atts = gate.recovery_attempts_for(iid)
        self.assertEqual(len(atts), 1)
        self.assertEqual(atts[0]["attempt_state"], "SUCCEEDED")
        self.assertEqual(atts[0]["progress_delta"], 1.0)
        self.assertEqual(atts[0]["rung"], 1)
        self.assertEqual(atts[0]["rung_name"], "Retry with backoff")
        self.assertEqual(pol["attempt_count"], 1)
        self.assertEqual(pol["remaining_budget"], 6)
        self.assertNotEqual(gate.get_job("j-936")["status"], "COMPLETE")


# ------------------------------------------------------------- migration


class TestR9M1(RecoveryBase):
    def test_R9_M1_v6_to_v7_migration(self):
        """Migrate a fresh DB through MIGRATIONS[:6], then the full migrate()
        applies exactly [7, 8, 9, 10, 11, 12] (8 = R10 scheduler-claim liveness
        beats, 9 = R11 desired-state reconciliation, 10 = R12
        circuit-breaker state + signals, 11 = R13 finalization runs,
        12 = R14 per-job artifact staging provenance); the
        recovery_policy table
        exists with every expected column and a policy row round-trips."""
        db = os.path.join(self.tmp, "mig.db")
        store = open_store(db)
        self._temp_stores.append(store)
        from axos.store import migrate as do_migrate
        do_migrate(store, MIGRATIONS[:6])
        ver = store.conn.execute(
            "SELECT MAX(version) FROM schema_migrations").fetchone()[0]
        self.assertEqual(ver, 6)
        applied = do_migrate(store)
        self.assertEqual(applied, [7, 8, 9, 10, 11, 12])
        cols = {r[1]: r[2] for r in
                store.conn.execute("PRAGMA table_info(recovery_policy)")}
        for c in ("incident_id", "policy_version", "version", "current_rung",
                  "rung_name", "incident_budget", "per_rung_budgets",
                  "attempt_count", "per_rung_attempts", "remaining_budget",
                  "consumed_attempt_number",
                  "result_consumed_attempt_number", "zero_progress_count",
                  "last_attempt_id", "last_attempt_result",
                  "escalation_state", "escalation_target", "terminal_state",
                  "superseded_by", "updated_at"):
            self.assertIn(c, cols)
        gate = TransitionGate(store)
        now = store.current_time()
        with store.write_txn() as (conn, _t):
            conn.execute(
                "INSERT INTO tasks(task_id, status, objective, budgets,"
                " created_at, updated_at)"
                " VALUES('t','PROPOSED','{}','{}',?,?)", (now, now))
            conn.execute(
                "INSERT INTO incidents(incident_id, scope, failure_class,"
                " signature, created_at, updated_at)"
                " VALUES('inc-r9','recovery','STALLED',"
                " '{\"job_id\":\"j\"}',?,?)", (now, now))
        gate.ensure_recovery_policy(
            "inc-r9", policy_version="r9-policy/v1", current_rung=1,
            rung_name="Retry with backoff", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        back = gate.get_recovery_policy("inc-r9")
        self.assertEqual(back["incident_id"], "inc-r9")
        self.assertEqual(back["current_rung"], 1)
        self.assertEqual(back["remaining_budget"], 7)
        self.assertEqual(json.loads(back["per_rung_budgets"]),
                         {"1": 2, "2": 2, "3": 3, "4": 0, "5": 0})


# ----------------------------------------------------------- race extras


class TestR9Race1(RecoveryBase):
    def test_R9_RACE_1_budget_double_consume(self):
        """One new attempt, two reconcilers racing: consumed exactly once
        (attempt_count == 1, remaining == 6)."""
        gate = self.gate
        self._seed_owned("j-ra1", "w-ra1")
        inc = self._seed_incident("j-ra1")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._seed_failed(iid, "j-ra1", rung=1, n=1)
        self._race_fns([
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid),
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid)])
        pol = self._policy(gate, iid)
        self.assertEqual(pol["attempt_count"], 1)
        self.assertEqual(pol["remaining_budget"], 6)


class TestR9Race2(RecoveryBase):
    def test_R9_RACE_2_rung_double_advance(self):
        """zpc=2 durable, two reconcilers racing: the ladder advances exactly
        one rung (2, not 3)."""
        gate = self.gate
        self._seed_owned("j-ra2", "w-ra2")
        inc = self._seed_incident("j-ra2")
        iid = inc["incident_id"]
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=1,
            rung_name="Retry with backoff", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        gate.cas_update_recovery_policy(
            iid, 1, {"zero_progress_count": 2,
                     "result_consumed_attempt_number": 10}, "test")
        pc = self._new_pc()
        self._race_fns([
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid),
            lambda: pc._reconcile_incident(pc._gate_for_thread(), iid)])
        pol = self._policy(gate, iid)
        self.assertEqual(pol["current_rung"], 2)
        self.assertEqual(pol["version"], 3)


class TestR9Race3(RecoveryBase):
    def test_R9_RACE_3_duplicate_attempt_race(self):
        """Two controllers racing evaluate() where R8 would create an
        attempt: exactly one attempt row exists afterwards."""
        gate = self.gate
        self._seed_stalled("j-ra3", "w-ra3")
        pc1 = self._new_pc()
        pc2 = self._new_pc()
        self._race_fns([pc1.evaluate, pc2.evaluate])
        incs = gate.open_recovery_incidents()
        self.assertEqual(len(incs), 1)
        atts = gate.recovery_attempts_for(incs[0]["incident_id"])
        self.assertEqual(len(atts), 1)


# ---------------------------------------------------------- crash extras


class TestR9C1(RecoveryBase):
    def test_R9_C1_crash_before_policy_evaluation(self):
        """C1 — a fresh controller over an open incident with no attempts
        writes only the policy row creation: no attempts, no consumption
        events, no terminal."""
        gate = self.gate
        self._seed_owned("j-c1", "w-c1")
        inc = self._seed_incident("j-c1")
        iid = inc["incident_id"]
        pc = self._new_pc()  # fresh: nothing evaluated before
        pc.evaluate()
        pol = self._policy(gate, iid)
        self.assertIsNotNone(pol)
        self.assertEqual(pol["current_rung"], 1)
        self.assertEqual(pol["version"], 1)
        self.assertIsNone(pol["terminal_state"])
        self.assertEqual(gate.recovery_attempts_for(iid), [])
        types = {r[0] for r in gate.store.conn.execute(
            "SELECT DISTINCT event_type FROM ledger"
            " WHERE event_type LIKE 'recovery.policy_%'").fetchall()}
        self.assertEqual(types, {"recovery.policy_created"})


class TestR9C2(RecoveryBase):
    def test_R9_C2_crash_after_policy_decision(self):
        """C2 — select_rung is pure: compute the decision, discard it, and
        recompute; the two decisions are identical."""
        args = dict(current_rung=2, attempts_at_rung=1,
                    per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0},
                    incident_attempts=3, incident_budget=7,
                    zero_progress_count=1)
        d1 = select_rung(**args)
        d2 = select_rung(**args)  # crash between the two; recompute
        self.assertEqual(d1, d2)
        self.assertEqual(d1.rung, 2)
        self.assertIsNone(d1.terminal)


class TestR9C7(RecoveryBase):
    def test_R9_C7_crash_after_failure_result(self):
        """C7 — a FAILED result committed before the crash is counted exactly
        once by the fresh controller: zpc == 1 and a single
        recovery.policy_results_consumed event."""
        gate = self.gate
        self._seed_owned("j-c7", "w-c7")
        inc = self._seed_incident("j-c7")
        iid = inc["incident_id"]
        self._seed_failed(iid, "j-c7", rung=1, n=1, delta=0.0)
        pc = self._new_pc()
        pc.evaluate()
        pol = self._policy(gate, iid)
        self.assertEqual(pol["attempt_count"], 1)
        self.assertEqual(pol["zero_progress_count"], 1)
        evs = self._ledger(gate, "recovery.policy_results_consumed",
                           lambda e: e.get("incident_id") == iid)
        self.assertEqual(len(evs), 1)


class TestR9C9(RecoveryBase):
    def test_R9_C9_crash_after_escalation_commit(self):
        """C9 — terminal applied, then the controller restarts: still
        terminal, no new attempts, exactly one recovery.policy_terminal
        ledger event."""
        gate = self.gate
        self._seed_owned("j-c9", "w-c9")
        inc = self._seed_incident("j-c9")
        iid = inc["incident_id"]
        gate.transition_task("t", "AUTHORIZED", "test")
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=4,
            rung_name="Widen scope", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        pc1 = self._new_pc()
        self._reconcile(pc1, iid)
        filt = lambda e: e.get("incident_id") == iid
        self.assertEqual(len(self._ledger(gate, "recovery.policy_terminal",
                                          filt)), 1)
        pc2 = self._new_pc()
        out = pc2._reconcile_incident(pc2.gate, iid)
        self.assertEqual(out["outcome"], "terminal")
        self.assertEqual(out["terminal_state"], T_R5_TERMINAL)
        self.assertEqual(len(self._ledger(gate, "recovery.policy_terminal",
                                          filt)), 1)
        self.assertEqual(gate.recovery_attempts_for(iid), [])


# ------------------------------------------------------- fix follow-ups


class TestR9ZB1(RecoveryBase):
    def test_R9_ZB1_partial_config_materializes_zero(self):
        """Missing per-rung keys materialize to 0 (skip), while unknown
        rungs, non-int/bool/negative caps, nonzero rung 4/5, and a
        non-positive incident budget are still rejected."""
        cfg = PolicyConfig(per_rung_max_attempts={1: 5},
                           incident_max_attempts=10)
        self.assertEqual(cfg.per_rung_max_attempts,
                         {1: 5, 2: 0, 3: 0, 4: 0, 5: 0})
        self.assertEqual(cfg.incident_max_attempts, 10)
        bad_budgets = ({6: 1}, {"1": 2}, {1: True}, {1: -1}, {1: 1.5},
                       {4: 1}, {5: 1})
        for bad in bad_budgets:
            with self.assertRaises(ValueError, msg=f"budgets={bad}"):
                PolicyConfig(per_rung_max_attempts=bad)
        for bad in (0, -3, True, 2.5):
            with self.assertRaises(ValueError, msg=f"incident={bad}"):
                PolicyConfig(incident_max_attempts=bad)


class TestR9ZB2(RecoveryBase):
    def test_R9_ZB2_zero_budget_rung_skipped(self):
        """A rung with zero budget is skipped without executing an attempt:
        pure select_rung advances, and the durable pass moves 2 -> 3 with no
        rung-2 attempt row."""
        d = select_rung(current_rung=2, attempts_at_rung=0,
                        per_rung_budgets={1: 2, 2: 0, 3: 3, 4: 0, 5: 0},
                        incident_attempts=2, incident_budget=7,
                        zero_progress_count=0)
        self.assertEqual(d.rung, 3)
        self.assertIsNone(d.terminal)
        self.assertTrue(d.advanced)
        gate = self.gate
        self._seed_owned("j-zb2", "w-zb2")
        inc = self._seed_incident("j-zb2")
        iid = inc["incident_id"]
        pc = self._new_pc(per_rung_max_attempts={1: 2, 2: 0, 3: 3})
        self._reconcile(pc, iid)
        pol = self._policy(gate, iid)
        self.assertEqual(json.loads(pol["per_rung_budgets"])["2"], 0)
        self._seed_failed(iid, "j-zb2", rung=1, n=2)
        self._reconcile(pc, iid)
        self.assertEqual(self._policy(gate, iid)["current_rung"], 2)
        self._reconcile(pc, iid)
        pol = self._policy(gate, iid)
        self.assertEqual(pol["current_rung"], 3)
        self.assertEqual(pol["rung_name"], "Replace / reassign")
        atts = gate.recovery_attempts_for(iid)
        self.assertEqual(len(atts), 2)  # no rung-2 attempt was executed
        self.assertTrue(all(a["rung"] == 1 for a in atts))
        evs = self._ledger(gate, "recovery.policy_rung_advanced",
                           lambda e: e.get("incident_id") == iid)
        self.assertTrue(any("0/0" in e.get("reason", "") for e in evs))


class TestR9BE1(RecoveryBase):
    def test_R9_BE1_zero_remaining_converges_terminal(self):
        """Zero remaining budget plus newly arrived R8 attempts converges to
        terminal BUDGET_EXHAUSTED (incident escalated) instead of deferring
        forever; a second pass still returns terminal."""
        gate = self.gate
        self._seed_owned("j-be1", "w-be1")
        inc = self._seed_incident("j-be1")
        iid = inc["incident_id"]
        pc = self._new_pc(incident_max_attempts=2)
        self._reconcile(pc, iid)  # policy row: incident budget 2
        self._seed_failed(iid, "j-be1", rung=1, n=2)
        # Simulate the consume commit landing while the terminal step is
        # still pending (crash/race between the two CAS steps).
        last_id = gate.recovery_attempts_for(iid)[-1]["attempt_id"]
        gate.consume_policy_attempts(iid, 1, upto_attempt_number=2,
                                     per_rung_attempts={"1": 2},
                                     last_attempt_id=last_id, actor="test")
        # R8's independent intake records a 3rd attempt directly.
        self._seed_failed(iid, "j-be1", rung=2, n=1, action_name="restart")
        out = self._reconcile(pc, iid)
        self.assertNotEqual(out["outcome"], "deferred")
        self.assertEqual(out["outcome"], "terminal")
        self.assertEqual(out["terminal_state"], T_BUDGET_EXHAUSTED)
        pol = self._policy(gate, iid)
        self.assertEqual(pol["terminal_state"], T_BUDGET_EXHAUSTED)
        inc2 = gate.get_recovery_incident(iid)
        self.assertEqual(inc2["outcome"], "escalated")
        self.assertEqual(inc2["escalated_to"], "human")
        out2 = self._reconcile(pc, iid)
        self.assertEqual(out2["outcome"], "terminal")
        self.assertEqual(out2["terminal_state"], T_BUDGET_EXHAUSTED)


class TestR9UN1(RecoveryBase):
    def test_R9_UN1_unnumbered_attempt_fails_closed(self):
        """An attempt row with NULL attempt_number (pre-numbering legacy
        API) cannot be accounted: the pass returns "blocked", the incident
        is human-escalated, and no policy attempt accounting happened."""
        gate = self.gate
        self._seed_owned("j-un1", "w-un1")
        inc = self._seed_incident("j-un1")
        iid = inc["incident_id"]
        gate.record_recovery_attempt(None, iid, 1, {"name": "reclaim"},
                                     "none", 0.0, 0.0, "retry", "test")
        pc = self._new_pc()
        out = self._reconcile(pc, iid)
        self.assertEqual(out["outcome"], "blocked")
        inc2 = gate.get_recovery_incident(iid)
        self.assertEqual(inc2["outcome"], "escalated")
        self.assertEqual(inc2["escalated_to"], "human")
        pol = self._policy(gate, iid)
        self.assertEqual(pol["attempt_count"], 0)
        self.assertEqual(self._ledger(
            gate, "recovery.policy_attempts_consumed",
            lambda e: e.get("incident_id") == iid), [])


class TestR9SC1(RecoveryBase):
    def test_R9_SC1_closed_stale_incident_converges(self):
        """A closed STALE_AUTHORITY restart incident for the job converges
        the original incident from its durable outcome (success ->
        RECOVERY_COMPLETE) instead of deferring forever."""
        gate = self.gate
        gate.create_job("j-sc1", "t", "s", "scheduler")  # ownerless
        inc = self._seed_incident("j-sc1", failure_class="STALLED")
        iid = inc["incident_id"]
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=2,
            rung_name="Restart worker", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        stale = gate.find_or_create_recovery_incident(
            "j-sc1", "STALE_AUTHORITY", {"job_id": "j-sc1", "note": "r8"},
            "test")
        gate.set_incident_outcome(stale["incident_id"], "success",
                                  "r8 resolved", "test")
        pc = self._new_pc()
        out = self._reconcile(pc, iid)
        self.assertNotEqual(out["outcome"], "deferred")
        self.assertEqual(out["outcome"], "terminal")
        self.assertEqual(out["terminal_state"], T_RECOVERY_COMPLETE)
        self.assertEqual(self._policy(gate, iid)["terminal_state"],
                         T_RECOVERY_COMPLETE)


class TestR9FK1(RecoveryBase):
    def test_R9_FK1_ensure_missing_incident_rejected(self):
        """ensure_recovery_policy on a nonexistent incident raises an
        informative TransitionRejected (FK violations are never masked as
        create-race disappearances)."""
        gate = self.gate
        with self.assertRaises(TransitionRejected) as ctx:
            gate.ensure_recovery_policy(
                "inc-missing", policy_version="r9-policy/v1",
                current_rung=1, rung_name="Retry with backoff",
                incident_budget=7,
                per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0},
                actor="test")
        self.assertIn("inc-missing", str(ctx.exception))
        self.assertIsNone(gate.get_recovery_policy("inc-missing"))


class TestR9FK2(RecoveryBase):
    def test_R9_FK2_budgets_not_cas_updatable(self):
        """per_rung_budgets is budget identity, set once at creation: any
        CAS rewrite attempt raises TransitionRejected; legitimate keys
        still CAS-update fine."""
        gate = self.gate
        self._seed_owned("j-fk2", "w-fk2")
        inc = self._seed_incident("j-fk2")
        iid = inc["incident_id"]
        gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=1,
            rung_name="Retry with backoff", incident_budget=7,
            per_rung_budgets={1: 2, 2: 2, 3: 3, 4: 0, 5: 0}, actor="test")
        with self.assertRaises(TransitionRejected):
            gate.cas_update_recovery_policy(
                iid, 1, {"per_rung_budgets": {"1": 9}}, "test")
        pol = gate.cas_update_recovery_policy(
            iid, 1, {"zero_progress_count": 1}, "test")
        self.assertEqual(pol["version"], 2)
        self.assertEqual(pol["zero_progress_count"], 1)
        self.assertEqual(json.loads(pol["per_rung_budgets"])["1"], 2)


class TestR9PB1(RecoveryBase):
    def test_R9_PB1_corrupt_budgets_fail_closed(self):
        """A hand-corrupted per_rung_budgets blob (invalid JSON) fails
        closed: the pass returns "blocked" and the incident is
        human-escalated — never a raw ValueError from the rung provider."""
        gate = self.gate
        self._seed_owned("j-pb1", "w-pb1")
        inc = self._seed_incident("j-pb1")
        iid = inc["incident_id"]
        pc = self._new_pc()
        self._reconcile(pc, iid)  # create the policy row
        with gate.store.write_txn() as (conn, _now):
            conn.execute("UPDATE recovery_policy SET per_rung_budgets="
                         "'not-json{{' WHERE incident_id=?", (iid,))
        out = self._reconcile(pc, iid)
        self.assertEqual(out["outcome"], "blocked")
        inc2 = gate.get_recovery_incident(iid)
        self.assertEqual(inc2["outcome"], "escalated")
        self.assertEqual(inc2["escalated_to"], "human")
        self.assertEqual(gate.recovery_attempts_for(iid), [])


if __name__ == "__main__":
    unittest.main()
