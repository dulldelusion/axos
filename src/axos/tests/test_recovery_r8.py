"""Phase 1C R8 — Recovery Controller & Recovery Contract Gate tests.

STANDARD: real SQLite/WAL; real subprocesses/process groups where the
action needs them (fence, restart); gate-level fixtures elsewhere. No
fake process objects, no in-memory model of the kernel. Deterministic
polling instead of blind sleeps; every spawned process is killed and
reaped in tearDown.

Test IDs:
  R8-01  controller refuses before READY
  R8-02  HEALTHY verdict -> no incident, attempt, reclaim, fence, restart
  R8-03  no verdict at all -> no incident
  R8-04  STALLED/DEAD + live lease -> incident + attempt#1 (reclaim),
         full Recovery Contract fields, canonical rung 2
  R8-05  reclaim verify: durable token bump/owner clear -> success,
         delta 1.0, incident closed
  R8-06  action runs but moves no evidence -> retry, attempt#2 allowed
  R8-07  two consecutive zero-progress -> escalate; incident
         escalated_to=r9-policy; no attempt#3 ever
  R8-08  I-18: success with progress_delta<=0 refused at the gate
  R8-09  token changed before dispatch -> stale attempt BLOCKED, no action
  R8-10  forced reclaim without DEAD/STALLED verdict -> authority-refused
  R8-11  incident identity (job_id, failure_class) stable across passes
  R8-12  concurrent creators -> exactly one attempt (loser observes)
  R8-13  concurrent claim -> exactly one claim holder (loser observes)
  R8-14  recovery.attempt/incident ledger evidence is durable
  R8-15  restart: replacement claims + progress -> success, delta 1.0
  R8-16  restart without progress -> retry (heartbeats are not progress)
  R8-17  restart refused when the job has a live owner
  R8-18  restart refused for terminal execution
  R8-19  restart dispatch is idempotent (no duplicate spawn after crash)
  R8-20  fence: lingering stale process killed by the R2 sweep -> success
  R8-21  fence when the process is already dead -> success (no-op)
  R8-22  token bumped between claim and dispatch -> stale BLOCKED
  R8-23  crash between claim and dispatch -> reconcile, single dispatch
  R8-24  crash after dispatch -> UNCERTAIN reconcile, no duplicate action
  R8-25  crash before creation -> exactly one attempt on re-evaluation
  R8-26  terminal execution never creates an incident
  R8-27  heartbeat-only replacement never counts as progress
  R8-28  action return code cannot fake recovery (mocked success ignored)
  R8-29  two controllers, one incident -> one attempt, one claim
  R8-30  unreadable verdict source -> fail-closed RecoveryError
  R8-31  RecoveryConfig rejects non-positive/non-finite values
  R8-32  background loop evaluates; errors are recorded, loop survives
  R8-33  R9 boundary: budget_context is the deferred-to-R9 marker only
  R8-34  full lifecycle: STALLED worker -> reclaim -> fence -> restart
  R8-35  after escalation the controller creates nothing more
  R8-R1  race: token bumped mid-dispatch -> R1 CAS rejects, authority-refused
  R8-R2  race: double completion of one attempt is impossible (CAS)
  R8-R3  race: peer completes during reconcile -> fail-closed, one record
  R8-M1  migration v5 -> v6 preserves legacy rows and I-18
"""
import json
import os
import signal
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace

from axos.exec.recovery import (CanonicalRungProvider, RecoveryConfig,  # noqa: E402
                                RecoveryController, RecoveryError,
                                RecoveryNotReady, RungContext)
from axos.exec.supervisor import Supervisor  # noqa: E402
from axos.store import (StoreError, TransitionGate, TransitionRejected,  # noqa: E402
                        open_store, migrate)
from axos.store.migrations import MIGRATIONS  # noqa: E402


def _proc_gone(pid: int) -> bool:
    """True when the process is dead — a zombie counts as dead."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    try:
        with open(f"/proc/{pid}/stat") as f:
            state = f.read().rsplit(")", 1)[1].split()[0]
        return state in ("Z", "X", "x")
    except Exception:
        return True


class RecoveryBase(unittest.TestCase):
    H = 0.4

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r8-")
        self.db = os.path.join(self.tmp, "t.db")
        self._sups: list[Supervisor] = []
        self._rcs: list[RecoveryController] = []
        self._temp_stores: list = []
        self.sup = self._new_sup(H=self.H, actor="test-r8")
        self.gate = self.sup.gate
        self.gate.create_task("t", {"objective": "x"}, {"usd": 1},
                              "scheduler")

    def tearDown(self):
        for rc in self._rcs:
            try:
                rc.close()
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
        s = Supervisor(self.db, actor=actor or f"test-r8-{len(self._sups)}",
                       heartbeat_interval_s=H or self.H)
        self._sups.append(s)
        return s

    def _new_rc(self, supervisor=None, readiness=None,
                **cfg_kw) -> RecoveryController:
        cfg = RecoveryConfig(
            observation_window_s=cfg_kw.get("observation_window_s", 0.4),
            evaluation_interval_s=cfg_kw.get("evaluation_interval_s", 0.2),
            claim_timeout_s=cfg_kw.get("claim_timeout_s", 1.0),
            restart_worker_duration_s=cfg_kw.get(
                "restart_worker_duration_s", 30.0),
            restart_worker_ttl_s=cfg_kw.get("restart_worker_ttl_s", 10.0),
            restart_worker_hb_interval_s=cfg_kw.get(
                "restart_worker_hb_interval_s", 0.2),
        )
        kw: dict = {}
        if supervisor is not None:
            kw["supervisor"] = supervisor
        elif readiness is not None:
            kw["readiness"] = readiness
        else:
            kw["readiness"] = lambda: True
        rc = RecoveryController(self.db, cfg,
                                actor=f"test-rc-{len(self._rcs)}", **kw)
        self._rcs.append(rc)
        return rc

    def _temp_gate(self) -> TransitionGate:
        store = open_store(self.db)
        migrate(store)
        self._temp_stores.append(store)
        return TransitionGate(store)

    def _wait(self, pred, timeout=15.0, interval=0.02):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("timed out waiting for condition")

    def _wait_claimed(self, gate, jid, wid, timeout=15.0):
        def _p():
            try:
                j = gate.get_job(jid)
            except Exception:
                return False
            return (j["owner_worker_id"] == wid
                    and j["status"] in ("CLAIMED", "RUNNING"))
        self._wait(_p, timeout)
        return gate.get_job(jid)

    def _gate_claim_running(self, gate, jid, wid, ttl=60.0):
        """Gate-level RUNNING job with a worker row but no process."""
        gate.create_job(jid, "t", "s", "scheduler")
        self.sup._ensure_worker_row(wid)
        self.assertTrue(gate.claim_job(jid, wid, ttl, "scheduler"))
        gate.transition_job(jid, "RUNNING", f"worker:{wid}")
        return gate.get_job(jid)

    def _verdict(self, gate, jid, tok, verdict):
        return gate.record_watchdog_verdict(jid, tok, verdict,
                                            {"reason": "test-r8"},
                                            "test-r8")

    def _attempts(self, gate, incident_id):
        return gate.recovery_attempts_for(incident_id)

    def _drive_until_terminal(self, rc, gate, attempt_id, timeout=12.0):
        """Poll rc.evaluate() until the attempt reaches a terminal state."""
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            rc.evaluate()
            last = gate.get_recovery_attempt(attempt_id)
            if last["attempt_state"] in ("SUCCEEDED", "FAILED", "BLOCKED"):
                return last
            time.sleep(0.05)
        raise AssertionError(
            f"attempt {attempt_id} never reached terminal state;"
            f" last={last['attempt_state'] if last else None}")

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

    def _incident(self, gate, incident_id):
        return gate.get_recovery_incident(incident_id)


# ------------------------------------------------------------------- R8-01
class TestR801(RecoveryBase):
    def test_R8_01_refuses_before_ready(self):
        """RecoveryNotReady before the runtime reports READY (R6)."""
        rc = self._new_rc(readiness=lambda: False)
        with self.assertRaises(RecoveryNotReady):
            rc.evaluate()
        # Nothing was written: no incidents, no attempts.
        self.assertEqual(self.gate.open_recovery_incidents(), [])


# ------------------------------------------------------------------- R8-02
class TestR802(RecoveryBase):
    def test_R8_02_healthy_produces_nothing(self):
        """A HEALTHY verdict means no incident, attempt, reclaim, fence,
        or restart — the controller never acts on healthy execution."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-healthy", "w-healthy")
        self._verdict(gate, "j-healthy", job["fencing_token"], "HEALTHY")
        rc = self._new_rc()
        outcomes = rc.evaluate()
        self.assertEqual(outcomes, [])
        self.assertEqual(gate.open_recovery_incidents(), [])
        self.assertEqual(
            self._ledger(gate, "job.lease_reclaimed"), [])
        self.assertEqual(
            self._ledger(gate, "recovery.attempt_created"), [])


# ------------------------------------------------------------------- R8-03
class TestR803(RecoveryBase):
    def test_R8_03_no_verdict_produces_nothing(self):
        """No verdict at all (and a live lease) -> no recovery incident."""
        gate = self._temp_gate()
        self._gate_claim_running(gate, "j-noverdict", "w-noverdict")
        rc = self._new_rc()
        self.assertEqual(rc.evaluate(), [])
        self.assertEqual(gate.open_recovery_incidents(), [])


# ------------------------------------------------------------------- R8-04
class TestR804(RecoveryBase):
    def _stalled(self, gate=None):
        gate = gate or self._temp_gate()
        job = self._gate_claim_running(gate, "j-r804", "w-r804")
        self._verdict(gate, "j-r804", job["fencing_token"], "DEAD")
        return gate, job

    def test_R8_04_incident_and_attempt_with_contract_fields(self):
        """DEAD + live lease -> one incident, attempt#1 (reclaim), the
        full Recovery Contract fields, canonical rung 2."""
        gate, job = self._stalled()
        rc = self._new_rc()
        outcomes = rc.evaluate()
        created = [o for o in outcomes if o.get("outcome")
                   == "attempt-created"]
        self.assertEqual(len(created), 1)
        incident_id = created[0]["incident_id"]
        inc = self._incident(gate, incident_id)
        self.assertEqual(inc["scope"], "recovery")
        self.assertEqual(inc["failure_class"], "DEAD")
        sig = json.loads(inc["signature"])
        self.assertEqual(sig["job_id"], "j-r804")
        self.assertEqual(sig["failure_class"], "DEAD")
        self.assertIsNone(inc["outcome"])
        atts = self._attempts(gate, incident_id)
        self.assertEqual(len(atts), 1)
        att = atts[0]
        # Contract fields.
        self.assertEqual(att["attempt_number"], 1)
        self.assertEqual(att["attempt_state"], "CREATED")
        self.assertEqual(att["job_id"], "j-r804")
        self.assertEqual(att["fencing_token"], job["fencing_token"])
        self.assertEqual(att["rung"], 2)
        self.assertEqual(att["rung_name"], "re-claim/requeue")
        action = json.loads(att["action"])
        self.assertEqual(action["name"], "reclaim")
        self.assertEqual(att["failure_class"], "DEAD")
        self.assertTrue(att["success_criterion"])
        self.assertTrue(att["failure_criterion"])
        before = json.loads(att["evidence_before"])
        self.assertIn("progress_updated_at", before)
        self.assertIn("verified_artifacts", before)
        self.assertIn("latest_known_good", before)
        self.assertTrue(att["idempotency_key"])
        budget = json.loads(att["budget_context"])
        self.assertEqual(budget["deferred_to"], "r9-policy")


# ------------------------------------------------------------------- R8-05
class TestR805(RecoveryBase):
    def test_R8_05_reclaim_verified_success(self):
        """Forced reclaim through R1, then the window: durable token
        bump + owner clear -> success, delta 1.0, incident closed."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r805", "w-r805")
        self._verdict(gate, "j-r805", job["fencing_token"], "DEAD")
        rc = self._new_rc()
        rc.evaluate()  # intake: attempt#1 CREATED
        inc = gate.open_recovery_incidents()[0]
        att = self._attempts(gate, inc["incident_id"])[0]
        rc.evaluate()  # drive: claim + dispatch (forced reclaim via R1)
        d = gate.get_recovery_attempt(att["attempt_id"])
        self.assertEqual(d["attempt_state"], "RUNNING")
        self.assertIsNotNone(d["verify_after"])
        term = self._drive_until_terminal(rc, gate, att["attempt_id"])
        self.assertEqual(term["attempt_state"], "SUCCEEDED")
        self.assertEqual(term["decision"], "success")
        self.assertEqual(term["progress_delta"], 1.0)
        after = json.loads(term["evidence_after"])
        self.assertGreater(after["fencing_token"],
                           job["fencing_token"])
        self.assertIsNone(after["owner_worker_id"])
        inc2 = self._incident(gate, inc["incident_id"])
        self.assertEqual(inc2["outcome"], "success")
        # The reclaim went through R1 with the controller actor.
        reclaims = self._ledger(
            gate, "job.lease_reclaimed",
            lambda e: e.get("job_id") == "j-r805")
        self.assertEqual(len(reclaims), 1)
        self.assertEqual(reclaims[0]["_actor"], "recovery-controller")
        self.assertTrue(reclaims[0]["forced"])


# ------------------------------------------------------------------- R8-06
class TestR806(RecoveryBase):
    def test_R8_06_zero_progress_is_retry_not_success(self):
        """The action "runs" but moves no authoritative evidence: the
        attempt completes as retry (FAILED), never success — and a second
        attempt is still allowed."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r806", "w-r806")
        self._verdict(gate, "j-r806", job["fencing_token"], "DEAD")
        rc = self._new_rc()

        def fake_reclaim(job_id, **kw):
            # Lies about success without touching durable state.
            return {"fencing_token": kw["expected_token"] + 1,
                    "status": "PENDING", "owner_worker_id": None}

        with mock.patch.object(rc.gate, "reclaim_lease",
                               side_effect=fake_reclaim):
            rc.evaluate()  # intake
            inc = gate.open_recovery_incidents()[0]
            att = self._attempts(gate, inc["incident_id"])[0]
            rc.evaluate()  # claim + dispatch (mocked)
            term = self._drive_until_terminal(rc, gate,
                                              att["attempt_id"])
        self.assertEqual(term["attempt_state"], "FAILED")
        self.assertEqual(term["decision"], "retry")
        self.assertEqual(term["progress_delta"], 0.0)
        # Durable state truly unchanged: the mock moved nothing.
        self.assertEqual(gate.get_job("j-r806")["fencing_token"],
                         job["fencing_token"])
        # The two-attempt bound still permits attempt#2: the trailing
        # intake in the pass that completed attempt#1 created it.
        atts = self._attempts(gate, inc["incident_id"])
        self.assertEqual(len(atts), 2)
        a2 = [a for a in atts if a["attempt_number"] == 2][0]
        self.assertEqual(a2["attempt_state"], "CREATED")
        self.assertEqual(json.loads(a2["action"])["name"], "reclaim")


# ------------------------------------------------------------------- R8-07
class TestR807(RecoveryBase):
    def test_R8_07_two_zero_progress_escalates(self):
        """Two consecutive zero-progress attempts -> escalate: the second
        attempt completes once as escalate with escalation_target
        r9-policy, the incident is durably escalated, and no attempt#3 is
        ever created."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r807", "w-r807")
        self._verdict(gate, "j-r807", job["fencing_token"], "DEAD")
        rc = self._new_rc()

        def fake_reclaim(job_id, **kw):
            return {"fencing_token": kw["expected_token"] + 1,
                    "status": "PENDING", "owner_worker_id": None}

        with mock.patch.object(rc.gate, "reclaim_lease",
                               side_effect=fake_reclaim):
            rc.evaluate()
            inc = gate.open_recovery_incidents()[0]
            a1 = self._attempts(gate, inc["incident_id"])[0]
            rc.evaluate()
            t1 = self._drive_until_terminal(rc, gate, a1["attempt_id"])
            self.assertEqual(t1["decision"], "retry")
            rc.evaluate()  # intake: attempt#2
            a2 = [a for a in self._attempts(gate, inc["incident_id"])
                  if a["attempt_number"] == 2][0]
            rc.evaluate()  # claim + dispatch
            t2 = self._drive_until_terminal(rc, gate, a2["attempt_id"])
        self.assertEqual(t2["attempt_state"], "FAILED")
        self.assertEqual(t2["decision"], "escalate")
        self.assertEqual(t2["escalation_target"], "r9-policy")
        self.assertEqual(t2["progress_delta"], 0.0)
        inc2 = self._incident(gate, inc["incident_id"])
        self.assertEqual(inc2["escalated_to"], "r9-policy")
        # The controller stops: no attempt#3, ever.
        for _ in range(3):
            out = rc.evaluate()
            self.assertFalse(
                [o for o in out if o.get("outcome")
                 == "attempt-created"],
                f"attempt#3 must never be created: {out}")
        self.assertEqual(
            len(self._attempts(gate, inc["incident_id"])), 2)


# ------------------------------------------------------------------- R8-08
class TestR808(RecoveryBase):
    def test_R8_08_i18_success_requires_positive_delta(self):
        """The gate refuses decision=success with progress_delta<=0 —
        zero progress can never be recorded as success (I-18)."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r808", "w-r808")
        inc = gate.find_or_create_recovery_incident(
            "j-r808", "DEAD", {"job_id": "j-r808"}, "test")
        att = gate.create_recovery_attempt(
            incident_id=inc["incident_id"], job_id="j-r808",
            fencing_token=job["fencing_token"], rung=2,
            rung_name="re-claim/requeue",
            action={"name": "reclaim", "rung": 2,
                    "rung_name": "re-claim/requeue"},
            failure_class="DEAD", success_criterion="s",
            failure_criterion="f", evidence_before={},
            actor="test")
        gate.claim_recovery_attempt(att["attempt_id"], "c1")
        with self.assertRaises(TransitionRejected):
            gate.complete_recovery_attempt(
                att["attempt_id"], decision="success",
                observed_effect="nothing moved", progress_delta=0.0,
                resulting_state="s", evidence_after={}, actor="test")
        # ...and with a negative delta too.
        with self.assertRaises(TransitionRejected):
            gate.complete_recovery_attempt(
                att["attempt_id"], decision="success",
                observed_effect="nothing moved", progress_delta=-1.0,
                resulting_state="s", evidence_after={}, actor="test")
        # Positive delta succeeds.
        row = gate.complete_recovery_attempt(
            att["attempt_id"], decision="success",
            observed_effect="token moved", progress_delta=1.0,
            resulting_state="s", evidence_after={}, actor="test")
        self.assertEqual(row["attempt_state"], "SUCCEEDED")


# ------------------------------------------------------------------- R8-09
class TestR809(RecoveryBase):
    def test_R8_09_stale_token_blocked_before_dispatch(self):
        """The token changes between attempt creation and dispatch: the
        attempt is BLOCKED as stale and the controller performs no
        external action."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r809", "w-r809")
        self._verdict(gate, "j-r809", job["fencing_token"], "DEAD")
        rc = self._new_rc()
        rc.evaluate()  # intake: attempt#1 binds token N
        inc = gate.open_recovery_incidents()[0]
        att = self._attempts(gate, inc["incident_id"])[0]
        # Someone else reclaims first: the token moves without us.
        gate.reclaim_lease("j-r809", actor="operator", reason="test",
                           expected_owner="w-r809",
                           expected_token=job["fencing_token"],
                           force=True, verdict="DEAD",
                           incident_id="other")
        out = rc.evaluate()  # drive: claim, then the token check fires
        drive = [o for o in out if o.get("attempt_id")
                 == att["attempt_id"]][0]
        self.assertEqual(drive["outcome"], "blocked")
        term = gate.get_recovery_attempt(att["attempt_id"])
        self.assertEqual(term["attempt_state"], "BLOCKED")
        self.assertIn("token changed", term["observed_effect"])
        # The controller dispatched nothing: only the operator's reclaim.
        reclaims = self._ledger(gate, "job.lease_reclaimed")
        self.assertEqual(len(reclaims), 1)
        self.assertEqual(reclaims[0]["_actor"], "operator")
        inc2 = self._incident(gate, inc["incident_id"])
        self.assertEqual(inc2["outcome"], "stopped")


# ------------------------------------------------------------------- R8-10
class TestR810(RecoveryBase):
    def test_R8_10_forced_reclaim_needs_dead_or_stalled(self):
        """A forced reclaim for a non-DEAD/STALLED failure class is
        refused by the controller: authority-refused, zero progress."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r810", "w-r810")
        # LEASE_EXPIRED with a live lease: routine reclaim is impossible
        # (lease not expired), forced reclaim is refused (not
        # DEAD/STALLED).
        inc = gate.find_or_create_recovery_incident(
            "j-r810", "LEASE_EXPIRED", {"job_id": "j-r810"}, "test")
        att = gate.create_recovery_attempt(
            incident_id=inc["incident_id"], job_id="j-r810",
            fencing_token=job["fencing_token"], rung=2,
            rung_name="re-claim/requeue",
            action={"name": "reclaim", "rung": 2,
                    "rung_name": "re-claim/requeue"},
            failure_class="LEASE_EXPIRED", success_criterion="s",
            failure_criterion="f", evidence_before={},
            actor="test")
        rc = self._new_rc()
        gate.claim_recovery_attempt(att["attempt_id"], rc.controller_id)
        out = rc.evaluate()
        done = [o for o in out if o.get("attempt_id")
                == att["attempt_id"]][0]
        self.assertEqual(done["outcome"], "completed")
        self.assertEqual(done["decision"], "retry")
        self.assertEqual(done["note"], "authority-refused")
        self.assertEqual(done["progress_delta"], 0.0)
        term = gate.get_recovery_attempt(att["attempt_id"])
        self.assertEqual(term["attempt_state"], "FAILED")
        # Durable state untouched.
        self.assertEqual(gate.get_job("j-r810")["fencing_token"],
                         job["fencing_token"])


# ------------------------------------------------------------------- R8-11
class TestR811(RecoveryBase):
    def test_R8_11_incident_identity_stable(self):
        """Repeated evaluations resolve to the same incident: identity is
        (job_id, failure_class), never controller memory."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r811", "w-r811")
        self._verdict(gate, "j-r811", job["fencing_token"], "STALLED")
        rc = self._new_rc()
        rc.evaluate()
        inc1 = gate.open_recovery_incidents()[0]["incident_id"]
        # A second controller, fresh process state, same verdict.
        rc2 = self._new_rc()
        rc2.evaluate()
        incs = gate.open_recovery_incidents()
        self.assertEqual(len(incs), 1)
        self.assertEqual(incs[0]["incident_id"], inc1)
        # And still exactly one attempt (the open one suppresses dupes).
        self.assertEqual(len(self._attempts(gate, inc1)), 1)


# ------------------------------------------------------------------- R8-12
class TestR812(RecoveryBase):
    def test_R8_12_concurrent_creators_single_attempt(self):
        """Two controllers racing intake produce exactly one attempt row;
        the loser observes the winner (UNIQUE + in-txn re-read)."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r812", "w-r812")
        self._verdict(gate, "j-r812", job["fencing_token"], "DEAD")
        rc1 = self._new_rc()
        rc2 = self._new_rc()
        barrier = threading.Barrier(2)
        results: dict = {}

        def run(name, rc):
            barrier.wait(timeout=10)
            try:
                results[name] = rc.evaluate()
            except Exception as exc:  # record, don't kill the thread
                results[name] = exc

        t1 = threading.Thread(target=run, args=("a", rc1))
        t2 = threading.Thread(target=run, args=("b", rc2))
        t1.start()
        t2.start()
        t1.join(timeout=20)
        t2.join(timeout=20)
        self.assertFalse(t1.is_alive() or t2.is_alive())
        for v in results.values():
            self.assertNotIsInstance(v, Exception)
        inc = gate.open_recovery_incidents()[0]
        atts = self._attempts(gate, inc["incident_id"])
        self.assertEqual(len(atts), 1)
        self.assertEqual(atts[0]["attempt_number"], 1)
        ids = set()
        for v in results.values():
            for o in v:
                if o.get("outcome") == "attempt-created":
                    ids.add(o["attempt_id"])
        self.assertEqual(ids, {atts[0]["attempt_id"]})


# ------------------------------------------------------------------- R8-13
class TestR813(RecoveryBase):
    def test_R8_13_concurrent_claim_single_holder(self):
        """Two controllers racing the claim: exactly one holder, the
        loser observes 'lost the claim race' and dispatches nothing."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r813", "w-r813")
        self._verdict(gate, "j-r813", job["fencing_token"], "DEAD")
        rc1 = self._new_rc()
        rc1.evaluate()  # intake only: attempt is CREATED, unclaimed
        inc = gate.open_recovery_incidents()[0]
        att = self._attempts(gate, inc["incident_id"])[0]
        rc2 = self._new_rc()
        barrier = threading.Barrier(2)
        results: dict = {}

        def run(name, rc):
            barrier.wait(timeout=10)
            try:
                results[name] = rc.evaluate()
            except Exception as exc:
                results[name] = exc

        # Part 1 (deterministic): the claim CAS itself. Two threads race
        # claim_recovery_attempt directly — exactly one wins, regardless
        # of interleaving. (Built manually via the gate: no evaluate(),
        # so Part 0's attempt is untouched. Thread-local gates: sqlite3
        # connections are thread-bound.)
        gate2 = self._temp_gate()
        job2 = self._gate_claim_running(gate2, "j-r813b", "w-r813b")
        self._verdict(gate2, "j-r813b", job2["fencing_token"], "DEAD")
        inc2 = gate2.find_or_create_recovery_incident(
            "j-r813b", "DEAD",
            {"job_id": "j-r813b",
             "fencing_token": job2["fencing_token"],
             "observed_at": gate2.store.current_time()},
            "test")
        att2 = gate2.create_recovery_attempt(
            incident_id=inc2["incident_id"], job_id="j-r813b",
            fencing_token=job2["fencing_token"], rung=2,
            rung_name="re-claim/requeue",
            action={"name": "reclaim", "rung": 2,
                    "rung_name": "re-claim/requeue"},
            failure_class="DEAD",
            success_criterion="reclaim verified",
            failure_criterion="reclaim produced no progress",
            evidence_before={}, actor="test")
        self.assertEqual(att2["attempt_state"], "CREATED")
        rc3 = self._new_rc()  # for thread-local gates only; no evaluate()
        barrier2 = threading.Barrier(2)
        claim_results: dict = {}

        def race_claim(name, cid):
            barrier2.wait(timeout=10)
            try:
                g = rc3._gate_for_thread()
                claim_results[name] = g.claim_recovery_attempt(
                    att2["attempt_id"], cid)
            except Exception as exc:  # record, don't kill the thread
                claim_results[name] = exc

        ct1 = threading.Thread(target=race_claim,
                               args=("a", "controller-A"))
        ct2 = threading.Thread(target=race_claim,
                               args=("b", "controller-B"))
        ct1.start()
        ct2.start()
        ct1.join(timeout=20)
        ct2.join(timeout=20)
        self.assertFalse(ct1.is_alive() or ct2.is_alive())
        wins = [v for v in claim_results.values() if v is True]
        self.assertEqual(len(wins), 1)
        self.assertTrue(all(v is False or v is True
                            for v in claim_results.values()))
        # Part 2 (evaluate level): the full race. Which interleaving
        # occurs is timing-dependent — the loser either loses the claim
        # race or observes the peer's in-flight attempt. Both are
        # correct; the invariants below hold in every interleaving.
        t1 = threading.Thread(target=run, args=("a", rc1))
        t2 = threading.Thread(target=run, args=("b", rc2))
        t1.start()
        t2.start()
        t1.join(timeout=20)
        t2.join(timeout=20)
        self.assertFalse(t1.is_alive() or t2.is_alive())
        for v in results.values():
            self.assertNotIsInstance(v, Exception)
        row = gate.get_recovery_attempt(att["attempt_id"])
        self.assertIn(row["controller_id"],
                      {rc1.controller_id, rc2.controller_id})
        # Exactly one external action: one reclaim in the ledger.
        reclaims = self._ledger(
            gate, "job.lease_reclaimed",
            lambda e: e.get("job_id") == "j-r813")
        self.assertEqual(len(reclaims), 1)
        dispatched = [o for v in results.values() for o in v
                      if o.get("outcome") == "dispatched"]
        self.assertEqual(len(dispatched), 1)
        # Any loser outcome is one of the correct observations: it lost
        # the claim race, saw the peer's in-flight attempt, or found the
        # peer's dispatched attempt still inside its observation window
        # (a timing-dependent but legitimate interleaving — the loser
        # still dispatches nothing).
        for o in [o for v in results.values() for o in v
                  if o.get("outcome") == "observed"]:
            self.assertTrue(
                "lost the claim race" in o.get("detail", "")
                or "peer" in o.get("detail", "")
                or "observation window not elapsed" in o.get("detail", ""),
                o.get("detail"))


# ------------------------------------------------------------------- R8-14
class TestR814(RecoveryBase):
    def test_R8_14_durable_ledger_evidence(self):
        """The attempt lifecycle is durably journaled: incident.created,
        recovery.attempt_created, _claimed, _dispatched, recovery.attempt
        (terminal) — all bound to the attempt/incident ids."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r814", "w-r814")
        self._verdict(gate, "j-r814", job["fencing_token"], "DEAD")
        rc = self._new_rc()
        rc.evaluate()
        inc = gate.open_recovery_incidents()[0]
        att = self._attempts(gate, inc["incident_id"])[0]
        aid = att["attempt_id"]
        rc.evaluate()
        self._drive_until_terminal(rc, gate, aid)
        inc_created = self._ledger(
            gate, "incident.created",
            lambda e: e.get("incident_id") == inc["incident_id"])
        self.assertEqual(len(inc_created), 1)
        for et in ("recovery.attempt_created", "recovery.attempt_claimed",
                   "recovery.attempt_dispatched"):
            evs = self._ledger(gate, et,
                               lambda e: e.get("attempt_id") == aid)
            self.assertEqual(len(evs), 1, et)
        terminal = self._ledger(
            gate, "recovery.attempt",
            lambda e: e.get("attempt_id") == aid
            and e.get("decision") == "success")
        self.assertEqual(len(terminal), 1)
        self.assertEqual(terminal[0]["progress_delta"], 1.0)


# ------------------------------------------------------------------- R8-15
class TestR815(RecoveryBase):
    def test_R8_15_restart_with_progress_succeeds(self):
        """dispatch_restart: the replacement worker claims under the
        current token and durable progress advances -> success, 1.0."""
        sup = self.sup
        gate = self._temp_gate()
        gate.create_job("j-r815", "t", "s", "scheduler")
        rc = self._new_rc(supervisor=sup)
        out = rc.dispatch_restart("j-r815")
        self.assertEqual(out["outcome"], "dispatched")
        self.assertEqual(out["action"], "restart")
        wid = out["dispatch"]["replacement_worker_id"]
        # The replacement claims its own lease through the gate.
        job = self._wait_claimed(gate, "j-r815", wid, timeout=15.0)
        tok = job["fencing_token"]
        # Durable progress through the gate (what a real worker reports).
        gate.update_job_progress("j-r815", wid, tok, 1.0, 10.0,
                                 "test-progress")
        term = self._drive_until_terminal(rc, gate,
                                          out["attempt_id"])
        self.assertEqual(term["attempt_state"], "SUCCEEDED")
        self.assertEqual(term["decision"], "success")
        self.assertEqual(term["progress_delta"], 1.0)
        action = json.loads(term["action"])
        self.assertEqual(action["rung"], 3)
        self.assertEqual(action["rung_name"], "replace/reassign")
        # No fencing-token games: the token never moved for a restart.
        self.assertEqual(gate.get_job("j-r815")["fencing_token"], tok)


# ------------------------------------------------------------------- R8-16
class TestR816(RecoveryBase):
    def test_R8_16_restart_without_progress_is_retry(self):
        """A replacement that heartbeats but never advances durable
        progress is zero progress -> retry, never success."""
        sup = self.sup
        gate = self._temp_gate()
        gate.create_job("j-r816", "t", "s", "scheduler")
        rc = self._new_rc(supervisor=sup)
        out = rc.dispatch_restart("j-r816")
        wid = out["dispatch"]["replacement_worker_id"]
        self._wait_claimed(gate, "j-r816", wid, timeout=15.0)
        # Heartbeats flow (liveness), but no progress is ever reported.
        term = self._drive_until_terminal(rc, gate, out["attempt_id"],
                                          timeout=15.0)
        self.assertEqual(term["attempt_state"], "FAILED")
        self.assertEqual(term["decision"], "retry")
        self.assertEqual(term["progress_delta"], 0.0)


# ------------------------------------------------------------------- R8-17
class TestR817(RecoveryBase):
    def test_R8_17_restart_refused_with_live_owner(self):
        """dispatch_restart refuses a job that still has an owner:
        restart never inherits a live lease."""
        gate = self._temp_gate()
        self._gate_claim_running(gate, "j-r817", "w-r817")
        rc = self._new_rc(supervisor=self.sup)
        with self.assertRaises(RecoveryError):
            rc.dispatch_restart("j-r817")
        self.assertEqual(
            len(self._ledger(gate, "recovery.attempt_created")), 0)


# ------------------------------------------------------------------- R8-18
class TestR818(RecoveryBase):
    def test_R8_18_restart_refused_for_terminal(self):
        """dispatch_restart refuses terminal execution: recovery never
        resurrects terminal state."""
        gate = self._temp_gate()
        gate.create_job("j-r818", "t", "s", "scheduler")
        # Sanctioned terminal path (F1: no raw store.conn writes):
        # PENDING -> CLAIMED -> FAILED.
        gate.claim_job("j-r818", "w-r818", 30.0, "scheduler")
        gate.transition_job("j-r818", "FAILED", "scheduler",
                            reason="r8 test: terminal fixture")
        rc = self._new_rc(supervisor=self.sup)
        with self.assertRaises(RecoveryError):
            rc.dispatch_restart("j-r818")


# ------------------------------------------------------------------- R8-19
class TestR819(RecoveryBase):
    def test_R8_19_restart_dispatch_idempotent(self):
        """A crash between dispatch and verify must not spawn a second
        replacement: the idempotency key binds the attempt."""
        sup = self.sup
        gate = self._temp_gate()
        gate.create_job("j-r819", "t", "s", "scheduler")
        rc = self._new_rc(supervisor=sup)
        out = rc.dispatch_restart("j-r819")
        wid = out["dispatch"]["replacement_worker_id"]
        job = self._wait_claimed(gate, "j-r819", wid, timeout=15.0)
        self.assertEqual(job["owner_worker_id"], wid)
        # Simulate the crash-recovery re-drive on the same RUNNING
        # attempt: dispatch again.
        att = gate.get_recovery_attempt(out["attempt_id"])
        again = rc._dispatch(gate, att, gate.store.current_time())
        self.assertEqual(again["outcome"], "dispatched")
        self.assertEqual(again["dispatch"]["replacement_worker_id"], wid)
        self.assertIn("not duplicated",
                      again["dispatch"].get("note", ""))
        # Exactly one worker identity was ever spawned for the job.
        procs = [w for w in sup._procs if w == wid]
        self.assertEqual(len(procs), 1)


# ------------------------------------------------------------------- R8-20
class TestR820(RecoveryBase):
    def test_R8_20_fence_kills_lingering_process(self):
        """A stale but living worker process after reclaim -> the next
        pass creates a fence attempt; the R2 sweep kills it; verification
        sees the process dead -> success."""
        sup = self._new_sup(H=self.H, actor="test-r8-fence")
        sup._sweep_stop.set()  # no background sweeps: explicit only
        self.sup._sweep_stop.set()  # the setUp supervisor shares the DB
        gate = sup.gate
        gate.create_job("j-r820", "t", "s", "scheduler")
        proc_id = sup.start_worker(
            "w-r820", "j-r820",
            {"kind": "wedged", "duration_s": 60.0},
            ttl_s=1.0, hb_interval_s=0.2, renew=False)
        # "wedged" never heartbeats and never exits on its own: after the
        # reclaim its process lingers until the R2 sweep kills it.
        job = self._wait_claimed(gate, "j-r820", "w-r820")
        pid = sup._procs["w-r820"].popen.pid
        # Lease expires while the process keeps living.
        self._wait(lambda: any(
            e["job_id"] == "j-r820"
            for e in gate.observe_expired_leases()), timeout=10.0)
        rc = self._new_rc(supervisor=sup)
        rc.evaluate()  # intake: reclaim attempt#1 (LEASE_EXPIRED)
        inc = gate.open_recovery_incidents()[0]
        a1 = self._attempts(gate, inc["incident_id"])[0]
        rc.evaluate()  # claim + dispatch: routine reclaim via R1
        t1 = self._drive_until_terminal(rc, gate, a1["attempt_id"])
        self.assertEqual(t1["decision"], "success")
        # The process lingers: logically fenced, physically alive.
        self.assertFalse(_proc_gone(pid))
        # The incident stays open for the fence.
        self.assertIsNone(
            self._incident(gate, inc["incident_id"])["outcome"])
        rc.evaluate()  # intake: ownerless + lingering -> fence attempt#2
        a2 = [a for a in self._attempts(gate, inc["incident_id"])
              if a["attempt_number"] == 2]
        self.assertEqual(len(a2), 1)
        self.assertEqual(json.loads(a2[0]["action"])["name"], "fence")
        rc.evaluate()  # claim + dispatch: the R2 sweep
        t2 = self._drive_until_terminal(rc, gate, a2[0]["attempt_id"])
        self.assertEqual(t2["attempt_state"], "SUCCEEDED")
        self.assertEqual(t2["decision"], "success")
        self.assertEqual(t2["progress_delta"], 1.0)
        # The stale process is really dead.
        self._wait(lambda: _proc_gone(pid), timeout=10.0)
        # The R2 mechanism (not the controller) did the killing.
        fence_evs = self._ledger(
            gate, "worker.fence_enforced",
            lambda e: e.get("worker_id") == "w-r820")
        self.assertEqual(len(fence_evs), 1)
        # Incident closed: nothing lingers anymore.
        self.assertEqual(
            self._incident(gate, inc["incident_id"])["outcome"],
            "success")


# ------------------------------------------------------------------- R8-21
class TestR821(RecoveryBase):
    def test_R8_21_fence_already_dead_still_success(self):
        """The stale process dies on its own between attempt creation and
        dispatch: the sweep is a no-op and verification still succeeds —
        the desired end state holds."""
        sup = self._new_sup(H=self.H, actor="test-r8-fence2")
        sup._sweep_stop.set()
        self.sup._sweep_stop.set()  # the setUp supervisor shares the DB
        gate = sup.gate
        gate.create_job("j-r821", "t", "s", "scheduler")
        sup.start_worker("w-r821", "j-r821",
                         {"kind": "wedged", "duration_s": 60.0},
                         ttl_s=1.0, hb_interval_s=0.2, renew=False)
        # "wedged" lingers after the reclaim (never exits on its own),
        # so the fence attempt is created; the test then kills it
        # manually before dispatch.
        self._wait_claimed(gate, "j-r821", "w-r821")
        pid = sup._procs["w-r821"].popen.pid
        self._wait(lambda: any(
            e["job_id"] == "j-r821"
            for e in gate.observe_expired_leases()), timeout=10.0)
        rc = self._new_rc(supervisor=sup)
        rc.evaluate()
        inc = gate.open_recovery_incidents()[0]
        a1 = self._attempts(gate, inc["incident_id"])[0]
        rc.evaluate()
        self._drive_until_terminal(rc, gate, a1["attempt_id"])
        # a2 was created by the trailing intake (CREATED, undispatched).
        # The process dies on its own BEFORE the controller's sweep.
        a2 = [a for a in self._attempts(gate, inc["incident_id"])
              if a["attempt_number"] == 2][0]
        self.assertEqual(a2["attempt_state"], "CREATED")
        os.killpg(pid, signal.SIGKILL)
        self._wait(lambda: _proc_gone(pid), timeout=10.0)
        rc.evaluate()  # claim + dispatch: sweep finds it already dead
        t2 = self._drive_until_terminal(rc, gate, a2["attempt_id"])
        self.assertEqual(t2["attempt_state"], "SUCCEEDED")
        self.assertEqual(t2["progress_delta"], 1.0)


# ------------------------------------------------------------------- R8-22
class TestR822(RecoveryBase):
    def test_R8_22_token_changed_before_dispatch_blocked(self):
        """Same as R8-09 at the dispatch boundary: covered there; this
        variant bumps the token between claim and the R1 call via a
        second gate handle (mid-dispatch race on the check itself)."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r822", "w-r822")
        self._verdict(gate, "j-r822", job["fencing_token"], "DEAD")
        rc = self._new_rc()
        rc.evaluate()  # intake
        inc = gate.open_recovery_incidents()[0]
        att = self._attempts(gate, inc["incident_id"])[0]
        other = self._temp_gate()
        # NOTE: rc drives through rc.gate (its own handle on the same
        # DB), so the mock must patch there, not the test's gate.
        real_claim = rc.gate.claim_recovery_attempt

        def claim_then_bump(attempt_id, controller_id):
            ok = real_claim(attempt_id, controller_id)
            if ok:
                other.reclaim_lease(
                    "j-r822", actor="operator", reason="race",
                    expected_owner="w-r822",
                    expected_token=job["fencing_token"],
                    force=True, verdict="DEAD", incident_id="race")
            return ok

        with mock.patch.object(rc.gate, "claim_recovery_attempt",
                               side_effect=claim_then_bump):
            out = rc.evaluate()
        drive = [o for o in out if o.get("attempt_id")
                 == att["attempt_id"]][0]
        self.assertEqual(drive["outcome"], "blocked")
        term = gate.get_recovery_attempt(att["attempt_id"])
        self.assertEqual(term["attempt_state"], "BLOCKED")


# ------------------------------------------------------------------- R8-23
class TestR823(RecoveryBase):
    def test_R8_23_crash_between_claim_and_dispatch(self):
        """Claimed by a dead controller, no dispatch record: the
        reconciler finds no action effect, releases the claim, and the
        next pass dispatches exactly once."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r823", "w-r823")
        self._verdict(gate, "j-r823", job["fencing_token"], "DEAD")
        rc = self._new_rc(claim_timeout_s=0.05)
        rc.evaluate()  # intake: CREATED
        inc = gate.open_recovery_incidents()[0]
        att = self._attempts(gate, inc["incident_id"])[0]
        # The crash: a claim with no dispatch, holder never returns.
        gate.claim_recovery_attempt(att["attempt_id"], "dead-controller")
        time.sleep(0.1)  # let the dead claim go stale
        out = rc.evaluate()  # reconcile -> no effect -> release
        rec = [o for o in out if o.get("attempt_id")
               == att["attempt_id"]][0]
        self.assertEqual(rec["outcome"], "reconciled")
        self.assertEqual(
            gate.get_recovery_attempt(
                att["attempt_id"])["attempt_state"], "CREATED")
        rc.evaluate()  # claim + dispatch exactly once
        self._drive_until_terminal(rc, gate, att["attempt_id"])
        reclaims = self._ledger(
            gate, "job.lease_reclaimed",
            lambda e: e.get("job_id") == "j-r823")
        self.assertEqual(len(reclaims), 1)


# ------------------------------------------------------------------- R8-24
class TestR824(RecoveryBase):
    def test_R8_24_crash_after_dispatch_reconciles(self):
        """Dispatched, then the controller crashes: UNCERTAIN
        reconciliation finds the durable effect, verifies without
        re-dispatching, and the action ran exactly once."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r824", "w-r824")
        self._verdict(gate, "j-r824", job["fencing_token"], "DEAD")
        rc = self._new_rc()
        rc.evaluate()  # intake
        inc = gate.open_recovery_incidents()[0]
        att = self._attempts(gate, inc["incident_id"])[0]
        rc.evaluate()  # claim + dispatch (real reclaim via R1)
        d = gate.get_recovery_attempt(att["attempt_id"])
        self.assertEqual(d["attempt_state"], "RUNNING")
        # The crash: reconstruction marks the attempt UNCERTAIN.
        gate.mark_recovery_attempt_uncertain(att["attempt_id"], "test",
                                             "test")
        rc2 = self._new_rc()  # a fresh controller after the crash
        term = self._drive_until_terminal(rc2, gate, att["attempt_id"])
        self.assertEqual(term["attempt_state"], "SUCCEEDED")
        self.assertEqual(term["decision"], "success")
        reclaims = self._ledger(
            gate, "job.lease_reclaimed",
            lambda e: e.get("job_id") == "j-r824")
        self.assertEqual(len(reclaims), 1)


# ------------------------------------------------------------------- R8-25
class TestR825(RecoveryBase):
    def test_R8_25_crash_before_creation_single_attempt(self):
        """The controller dies before creating the attempt: on
        re-evaluation exactly one attempt exists — creation is the
        idempotent edge."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r825", "w-r825")
        self._verdict(gate, "j-r825", job["fencing_token"], "DEAD")
        rc = self._new_rc()
        real_create = rc.gate.create_recovery_attempt
        calls = {"n": 0}

        def crash_once(**kw):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("simulated crash before creation")
            return real_create(**kw)

        with mock.patch.object(rc.gate, "create_recovery_attempt",
                               side_effect=crash_once):
            with self.assertRaises(RuntimeError):
                rc.evaluate()
        # Nothing was created by the crashed pass.
        inc = gate.open_recovery_incidents()[0]
        self.assertEqual(self._attempts(gate, inc["incident_id"]), [])
        # Re-evaluation creates exactly one attempt.
        rc.evaluate()
        atts = self._attempts(gate, inc["incident_id"])
        self.assertEqual(len(atts), 1)
        self.assertEqual(atts[0]["attempt_number"], 1)


# ------------------------------------------------------------------- R8-26
class TestR826(RecoveryBase):
    def test_R8_26_terminal_execution_never_recovered(self):
        """Terminal jobs never become recovery incidents — not via
        intake, and the intake guard refuses them directly."""
        gate = self._temp_gate()
        gate.create_job("j-r826", "t", "s", "scheduler")
        # Sanctioned terminal path (F1: no raw store.conn writes):
        # PENDING -> CLAIMED -> FAILED.
        gate.claim_job("j-r826", "w-r826", 30.0, "scheduler")
        gate.transition_job("j-r826", "FAILED", "scheduler",
                            reason="r8 test: terminal fixture")
        rc = self._new_rc()
        self.assertEqual(rc.evaluate(), [])
        self.assertEqual(gate.open_recovery_incidents(), [])
        # The guard itself, called directly with a fabricated candidate.
        cand = {"job_id": "j-r826", "fencing_token": 1,
                "failure_class": "DEAD", "detection": {}}
        res = rc._intake(gate, cand, gate.store.current_time())
        self.assertEqual(res["outcome"], "no-incident")


# ------------------------------------------------------------------- R8-27
class TestR827(RecoveryBase):
    def test_R8_27_heartbeat_only_never_progress(self):
        """A live, heartbeating replacement with zero durable progress is
        zero progress: the restart attempt retries, never succeeds."""
        sup = self.sup
        gate = self._temp_gate()
        gate.create_job("j-r827", "t", "s", "scheduler")
        rc = self._new_rc(supervisor=sup)
        out = rc.dispatch_restart("j-r827")
        wid = out["dispatch"]["replacement_worker_id"]
        job = self._wait_claimed(gate, "j-r827", wid, timeout=15.0)
        # Prove liveness flowed: wait for at least one ingested heartbeat
        # (ingestion is async after the claim).
        self._wait(lambda: gate.store.conn.execute(
            "SELECT COUNT(*) FROM heartbeats WHERE worker_id=?",
            (wid,)).fetchone()[0] > 0, timeout=15.0)
        beats = gate.store.conn.execute(
            "SELECT COUNT(*) FROM heartbeats WHERE worker_id=?",
            (wid,)).fetchone()[0]
        self.assertGreater(beats, 0)
        # ...but durable progress never advanced.
        self.assertIsNone(
            gate.get_job("j-r827")["progress_updated_at"])
        term = self._drive_until_terminal(rc, gate, out["attempt_id"],
                                          timeout=15.0)
        self.assertEqual(term["decision"], "retry")
        self.assertEqual(term["progress_delta"], 0.0)


# ------------------------------------------------------------------- R8-28
class TestR828(RecoveryBase):
    def test_R8_28_action_return_code_cannot_fake_recovery(self):
        """The authority's return value is not evidence: a reclaim that
        reports success without moving durable state verifies as zero
        progress. Verification reads the store, never the return code."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r828", "w-r828")
        self._verdict(gate, "j-r828", job["fencing_token"], "DEAD")
        rc = self._new_rc()

        def lying_reclaim(job_id, **kw):
            return {"fencing_token": kw["expected_token"] + 1,
                    "status": "PENDING", "owner_worker_id": None,
                    "job_id": job_id}

        with mock.patch.object(rc.gate, "reclaim_lease",
                               side_effect=lying_reclaim):
            rc.evaluate()
            inc = gate.open_recovery_incidents()[0]
            att = self._attempts(gate, inc["incident_id"])[0]
            rc.evaluate()
            dispatched = gate.get_recovery_attempt(att["attempt_id"])
            # The lie IS recorded in the dispatch evidence...
            dispatch = json.loads(dispatched["action"])["dispatch"]
            self.assertEqual(dispatch["token_after"],
                             job["fencing_token"] + 1)
            term = self._drive_until_terminal(rc, gate,
                                              att["attempt_id"])
        # ...but verification measured the real store: zero progress.
        self.assertEqual(term["decision"], "retry")
        self.assertEqual(term["progress_delta"], 0.0)
        after = json.loads(term["evidence_after"])
        self.assertEqual(after["fencing_token"], job["fencing_token"])
        self.assertIsNone(
            self._incident(gate, inc["incident_id"])["outcome"])


# ------------------------------------------------------------------- R8-29
class TestR829(RecoveryBase):
    def test_R8_29_two_controllers_one_incident(self):
        """Two controllers driving the same failure end to end: exactly
        one attempt row, exactly one claim, exactly one external action."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r829", "w-r829")
        self._verdict(gate, "j-r829", job["fencing_token"], "DEAD")
        rc1 = self._new_rc()
        rc2 = self._new_rc()
        barrier = threading.Barrier(2)
        results: dict = {}

        def run(name, rc):
            barrier.wait(timeout=10)
            try:
                # Two full passes: intake race, then claim race.
                results[name] = (rc.evaluate(), rc.evaluate())
            except Exception as exc:
                results[name] = exc

        t1 = threading.Thread(target=run, args=("a", rc1))
        t2 = threading.Thread(target=run, args=("b", rc2))
        t1.start()
        t2.start()
        t1.join(timeout=30)
        t2.join(timeout=30)
        self.assertFalse(t1.is_alive() or t2.is_alive())
        for v in results.values():
            self.assertNotIsInstance(v, Exception)
        inc = gate.open_recovery_incidents()[0]
        atts = self._attempts(gate, inc["incident_id"])
        self.assertEqual(len(atts), 1)
        row = gate.get_recovery_attempt(atts[0]["attempt_id"])
        self.assertIn(row["controller_id"],
                      {rc1.controller_id, rc2.controller_id})
        reclaims = self._ledger(
            gate, "job.lease_reclaimed",
            lambda e: e.get("job_id") == "j-r829")
        self.assertEqual(len(reclaims), 1)


# ------------------------------------------------------------------- R8-30
class TestR830(RecoveryBase):
    def test_R8_30_unreadable_verdicts_fail_closed(self):
        """When the R7 verdict source is unreadable, evaluation raises
        RecoveryError and writes nothing — fail-closed, no guessing."""
        gate = self._temp_gate()
        self._gate_claim_running(gate, "j-r830", "w-r830", ttl=0.05)
        self._wait(lambda: any(
            e["job_id"] == "j-r830"
            for e in gate.observe_expired_leases()), timeout=10.0)
        rc = self._new_rc()
        with mock.patch.object(
                TransitionGate, "latest_watchdog_verdict",
                side_effect=RuntimeError("verdict store down")):
            with self.assertRaises(RecoveryError):
                rc.evaluate()
        self.assertEqual(gate.open_recovery_incidents(), [])
        self.assertEqual(
            self._ledger(gate, "recovery.attempt_created"), [])


# ------------------------------------------------------------------- R8-31
class TestR831(RecoveryBase):
    def test_R8_31_config_validation(self):
        """RecoveryConfig rejects zero/negative/NaN/inf/bool/non-numeric
        timings — all controller timing is constructor-injected."""
        good = dict(observation_window_s=0.4, evaluation_interval_s=0.2,
                    claim_timeout_s=1.0)
        RecoveryConfig(**good)  # does not raise
        for name in good:
            for bad in (0, -1.0, float("nan"), float("inf"),
                        float("-inf"), True, "0.4", None):
                kw = dict(good)
                kw[name] = bad
                with self.assertRaises((ValueError, TypeError), msg=name):
                    RecoveryConfig(**kw)
        with self.assertRaises(TypeError):
            RecoveryController(self.db, {"not": "a config"},
                               readiness=lambda: True)


# ------------------------------------------------------------------- R8-32
class TestR832(RecoveryBase):
    def test_R8_32_background_loop_survives_errors(self):
        """The background loop evaluates on cadence; an evaluation error
        is recorded in _errors and the loop keeps running."""
        rc = self._new_rc(evaluation_interval_s=0.1)
        rc.start()
        try:
            self._wait(lambda: rc._thread is not None
                       and rc._thread.is_alive(), timeout=5.0)
            real = rc.evaluate
            calls = {"n": 0}

            def flaky():
                calls["n"] += 1
                if calls["n"] == 1:
                    raise RuntimeError("boom")
                return real()

            with mock.patch.object(rc, "evaluate", side_effect=flaky):
                self._wait(lambda: len(rc._errors) >= 1, timeout=5.0)
            self.assertTrue(rc._thread.is_alive())
            self.assertIn("boom", rc._errors[0]["error"])
        finally:
            rc.stop()
        self._wait(lambda: not rc._thread.is_alive(), timeout=5.0)


# ------------------------------------------------------------------- R8-33
class TestR833(RecoveryBase):
    def test_R8_33_r9_boundary_is_deferred_marker(self):
        """R8 records the canonical rung and a deferred-to-R9 budget
        marker — no budgets, no ladder traversal, no retry arithmetic."""
        provider = CanonicalRungProvider()
        ctx = provider.select_rung(
            incident={}, failure_class="DEAD", action_name="reclaim",
            history=[])
        self.assertIsInstance(ctx, RungContext)
        self.assertEqual((ctx.rung, ctx.name), (2, "re-claim/requeue"))
        self.assertEqual(ctx.budget_context["deferred_to"], "r9-policy")
        self.assertEqual(ctx.budget_context["r8_budget_policy"], "none")
        ctx3 = provider.select_rung(
            incident={}, failure_class="DEAD", action_name="fence",
            history=[])
        self.assertEqual((ctx3.rung, ctx3.name), (3, "replace/reassign"))
        # The controller exposes no retry-budget/ladder knobs.
        rc = self._new_rc()
        for attr in ("max_retries", "retry_budget", "ladder",
                     "loop_breaker"):
            self.assertFalse(hasattr(rc, attr), attr)
        # The stored attempt carries only the marker.
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r833", "w-r833")
        self._verdict(gate, "j-r833", job["fencing_token"], "DEAD")
        rc.evaluate()
        inc = gate.open_recovery_incidents()[0]
        att = self._attempts(gate, inc["incident_id"])[0]
        budget = json.loads(att["budget_context"])
        self.assertEqual(set(budget),
                         {"r8_budget_policy", "deferred_to", "note"})


# ------------------------------------------------------------------- R8-34
class TestR834(RecoveryBase):
    def test_R8_34_full_lifecycle(self):
        """Expired lease -> routine reclaim -> lingering process fenced
        -> replacement restarted with progress -> incident success."""
        sup = self._new_sup(H=self.H, actor="test-r8-e2e")
        sup._sweep_stop.set()
        self.sup._sweep_stop.set()  # the setUp supervisor shares the DB
        gate = sup.gate
        gate.create_job("j-r834", "t", "s", "scheduler")
        sup.start_worker("w-r834", "j-r834",
                         {"kind": "wedged", "duration_s": 60.0},
                         ttl_s=1.0, hb_interval_s=0.2, renew=False)
        # "wedged" lingers after the reclaim (never exits on its own).
        job = self._wait_claimed(gate, "j-r834", "w-r834")
        pid = sup._procs["w-r834"].popen.pid
        # Lease expires while the process keeps living.
        self._wait(lambda: any(
            e["job_id"] == "j-r834"
            for e in gate.observe_expired_leases()), timeout=10.0)
        rc = self._new_rc(supervisor=sup)
        # Attempt#1: routine reclaim through R1.
        rc.evaluate()
        inc = gate.open_recovery_incidents()[0]
        a1 = self._attempts(gate, inc["incident_id"])[0]
        rc.evaluate()
        t1 = self._drive_until_terminal(rc, gate, a1["attempt_id"])
        self.assertEqual(t1["decision"], "success")
        self.assertFalse(_proc_gone(pid))  # still physically alive
        # Attempt#2: fence the lingering process through the R2 sweep.
        rc.evaluate()
        a2 = [a for a in self._attempts(gate, inc["incident_id"])
              if a["attempt_number"] == 2][0]
        self.assertEqual(json.loads(a2["action"])["name"], "fence")
        rc.evaluate()
        t2 = self._drive_until_terminal(rc, gate, a2["attempt_id"])
        self.assertEqual(t2["decision"], "success")
        self._wait(lambda: _proc_gone(pid), timeout=10.0)
        # The fence proved the worker dead; R9's reconciler would now
        # resolve UNCERTAIN -> PENDING. The test performs that
        # sanctioned transition explicitly so the restart entry point
        # can be exercised.
        gate.transition_job("j-r834", "PENDING", "test",
                            reason="r8 test: fenced worker confirmed dead")
        # Attempt#3: explicit restart through the execution substrate.
        out = rc.dispatch_restart("j-r834")
        wid = out["dispatch"]["replacement_worker_id"]
        rjob = self._wait_claimed(gate, "j-r834", wid, timeout=15.0)
        gate.update_job_progress("j-r834", wid, rjob["fencing_token"],
                                 2.0, 10.0, "test-progress")
        t3 = self._drive_until_terminal(rc, gate, out["attempt_id"])
        self.assertEqual(t3["decision"], "success")
        self.assertEqual(t3["progress_delta"], 1.0)
        # Two attempts on the original incident (reclaim + fence); the
        # restart went to its own explicit STALE_AUTHORITY incident.
        atts = self._attempts(gate, inc["incident_id"])
        self.assertEqual(len(atts), 2)
        self.assertTrue(all(a["attempt_state"] == "SUCCEEDED"
                            for a in atts))
        inc1 = self._incident(gate, inc["incident_id"])
        self.assertEqual(inc1["outcome"], "success")


# ------------------------------------------------------------------- R8-35
class TestR835(RecoveryBase):
    def test_R8_35_escalated_incident_creates_nothing(self):
        """After escalation the controller keeps evaluating but never
        creates another attempt and never touches R1 again."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r835", "w-r835")
        self._verdict(gate, "j-r835", job["fencing_token"], "DEAD")
        rc = self._new_rc()

        def fake_reclaim(job_id, **kw):
            return {"fencing_token": kw["expected_token"] + 1,
                    "status": "PENDING", "owner_worker_id": None}

        with mock.patch.object(rc.gate, "reclaim_lease",
                               side_effect=fake_reclaim):
            rc.evaluate()
            inc = gate.open_recovery_incidents()[0]
            a1 = self._attempts(gate, inc["incident_id"])[0]
            rc.evaluate()
            self._drive_until_terminal(rc, gate, a1["attempt_id"])
            rc.evaluate()
            a2 = [a for a in self._attempts(gate, inc["incident_id"])
                  if a["attempt_number"] == 2][0]
            rc.evaluate()
            t2 = self._drive_until_terminal(rc, gate, a2["attempt_id"])
            self.assertEqual(t2["decision"], "escalate")
        # The mock is gone: real R1 is available again — still nothing.
        for _ in range(3):
            out = rc.evaluate()
            self.assertFalse(
                [o for o in out if o.get("outcome")
                 == "attempt-created"])
        self.assertEqual(
            len(self._attempts(gate, inc["incident_id"])), 2)
        self.assertEqual(
            self._ledger(gate, "job.lease_reclaimed"), [])


# ------------------------------------------------------------------- R8-R1
class TestR8R1(RecoveryBase):
    def test_R8_R1_token_race_mid_dispatch(self):
        """The token moves between the controller's check and the R1
        call: R1's compare-and-swap rejects the stale token and the
        attempt records authority-refused (zero progress, no lie)."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r8r1", "w-r8r1")
        self._verdict(gate, "j-r8r1", job["fencing_token"], "DEAD")
        rc = self._new_rc()
        other = self._temp_gate()
        real_reclaim = rc.gate.reclaim_lease

        def racy_reclaim(job_id, **kw):
            # Someone else's reclaim lands first...
            other.reclaim_lease(
                job_id, actor="operator", reason="r8-r1-race",
                expected_owner=kw["expected_owner"],
                expected_token=kw["expected_token"],
                force=True, verdict="DEAD", incident_id="race")
            # ...so ours hits the CAS with a stale token.
            return real_reclaim(job_id, **kw)

        rc.evaluate()
        inc = gate.open_recovery_incidents()[0]
        att = self._attempts(gate, inc["incident_id"])[0]
        with mock.patch.object(rc.gate, "reclaim_lease",
                               side_effect=racy_reclaim):
            rc.evaluate()  # claim; dispatch hits the race
            term = self._drive_until_terminal(rc, gate,
                                              att["attempt_id"])
        self.assertEqual(term["decision"], "retry")
        self.assertEqual(term["progress_delta"], 0.0)
        done_ev = [o for o in self._ledger(
            gate, "recovery.attempt",
            lambda e: e.get("attempt_id") == att["attempt_id"])]
        self.assertTrue(done_ev)
        # The token moved exactly once (the operator's reclaim); the
        # controller's stale call changed nothing.
        self.assertEqual(gate.get_job("j-r8r1")["fencing_token"],
                         job["fencing_token"] + 1)
        reclaims = self._ledger(gate, "job.lease_reclaimed")
        self.assertEqual(len(reclaims), 1)
        self.assertEqual(reclaims[0]["_actor"], "operator")


# ------------------------------------------------------------------- R8-R2
class TestR8R2(RecoveryBase):
    def test_R8_R2_double_completion_impossible(self):
        """Terminal completion is a CAS: completing an already-terminal
        attempt raises; the ledger shows exactly one terminal record."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r8r2", "w-r8r2")
        inc = gate.find_or_create_recovery_incident(
            "j-r8r2", "DEAD", {"job_id": "j-r8r2"}, "test")
        att = gate.create_recovery_attempt(
            incident_id=inc["incident_id"], job_id="j-r8r2",
            fencing_token=job["fencing_token"], rung=2,
            rung_name="re-claim/requeue",
            action={"name": "reclaim", "rung": 2,
                    "rung_name": "re-claim/requeue"},
            failure_class="DEAD", success_criterion="s",
            failure_criterion="f", evidence_before={},
            actor="test")
        gate.claim_recovery_attempt(att["attempt_id"], "c1")
        gate.transition_recovery_attempt(att["attempt_id"], ("RUNNING",),
                                         "VERIFYING", "test")
        gate.complete_recovery_attempt(
            att["attempt_id"], decision="retry",
            observed_effect="zero", progress_delta=0.0,
            resulting_state="s", evidence_after={}, actor="test")
        with self.assertRaises(TransitionRejected):
            gate.complete_recovery_attempt(
                att["attempt_id"], decision="retry",
                observed_effect="zero again", progress_delta=0.0,
                resulting_state="s", evidence_after={}, actor="test")
        with self.assertRaises(TransitionRejected):
            gate.transition_recovery_attempt(att["attempt_id"],
                                             ("RUNNING", "VERIFYING"),
                                             "VERIFYING", "test")
        terminal = self._ledger(
            gate, "recovery.attempt",
            lambda e: e.get("attempt_id") == att["attempt_id"])
        self.assertEqual(len(terminal), 1)


# ------------------------------------------------------------------- R8-R3
class TestR8R3(RecoveryBase):
    def test_R8_R3_peer_completes_during_reconcile(self):
        """A peer completes the attempt while we reconcile: our
        reconcile fails closed (TransitionRejected) and no second
        terminal record exists."""
        gate = self._temp_gate()
        job = self._gate_claim_running(gate, "j-r8r3", "w-r8r3")
        inc = gate.find_or_create_recovery_incident(
            "j-r8r3", "DEAD", {"job_id": "j-r8r3"}, "test")
        att = gate.create_recovery_attempt(
            incident_id=inc["incident_id"], job_id="j-r8r3",
            fencing_token=job["fencing_token"], rung=2,
            rung_name="re-claim/requeue",
            action={"name": "reclaim", "rung": 2,
                    "rung_name": "re-claim/requeue"},
            failure_class="DEAD", success_criterion="s",
            failure_criterion="f", evidence_before={},
            actor="test")
        rc = self._new_rc()
        gate.claim_recovery_attempt(att["attempt_id"], rc.controller_id)
        gate.mark_recovery_attempt_uncertain(att["attempt_id"], "test",
                                             "test")
        # The peer's completion lands first.
        other = self._temp_gate()
        other.complete_recovery_attempt(
            att["attempt_id"], decision="retry",
            observed_effect="peer saw zero progress",
            progress_delta=0.0, resulting_state="s",
            evidence_after={}, actor="peer")
        with self.assertRaises(TransitionRejected):
            rc._reconcile(gate, gate.get_recovery_attempt(
                att["attempt_id"]), gate.store.current_time())
        terminal = self._ledger(
            gate, "recovery.attempt",
            lambda e: e.get("attempt_id") == att["attempt_id"])
        self.assertEqual(len(terminal), 1)
        self.assertEqual(terminal[0]["_actor"], "peer")


# ------------------------------------------------------------------- R8-M1
class TestR8M1(RecoveryBase):
    def test_R8_M1_v5_to_v6_migration(self):
        """v6 applies forward-only on a v5 database: legacy
        recovery_attempts rows survive byte-identical, the new columns
        and the UNIQUE(incident_id, attempt_number) index appear, and
        the I-18 CHECK still rejects success-without-progress."""
        import sqlite3
        db = os.path.join(self.tmp, "mig.db")
        store = open_store(db)
        self._temp_stores.append(store)
        migrate(store, MIGRATIONS[:5])
        ver = store.conn.execute(
            "SELECT MAX(version) FROM schema_migrations").fetchone()[0]
        self.assertEqual(ver, 5)
        now = store.current_time()
        with store.write_txn() as (conn, _):
                    conn.execute(
                "INSERT INTO tasks(task_id, status, objective, budgets,"
                " created_at, updated_at)"
                " VALUES('t','ACTIVE','x','{}',?,?)", (now, now))
                    conn.execute(
                "INSERT INTO incidents(incident_id, scope, failure_class,"
                " signature, created_at, updated_at)"
                " VALUES('inc-legacy','ops','DEAD','{\"a\":1}',?,?)",
                (now, now))
                    conn.execute(
                "INSERT INTO recovery_attempts(attempt_id, incident_id, rung,"
                " action, observed_effect, progress_delta,"
                " output_health_delta, decision, actor, recorded_at)"
                " VALUES('att-legacy','inc-legacy',2,'{\"name\":\"reclaim\"}',"
                " 'none',0.0,0.0,'retry','old',?)", (now,))
        applied = migrate(store, MIGRATIONS[:6])
        self.assertEqual(applied, [6])
        row = store.conn.execute(
            "SELECT * FROM recovery_attempts"
            " WHERE attempt_id='att-legacy'").fetchone()
        row = dict(row)
        # Legacy columns byte-identical.
        self.assertEqual(row["incident_id"], "inc-legacy")
        self.assertEqual(row["rung"], 2)
        self.assertEqual(row["decision"], "retry")
        self.assertEqual(row["actor"], "old")
        # New columns exist (NULL for legacy rows).
        cols = {r[1] for r in store.conn.execute(
            "PRAGMA table_info(recovery_attempts)")}
        for c in ("job_id", "fencing_token", "attempt_number",
                  "attempt_state", "idempotency_key", "budget_context",
                  "claimed_at", "evidence_before", "evidence_after",
                  "escalation_target"):
            self.assertIn(c, cols)
        idx = [r[0] for r in store.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'")]
        self.assertIn("recovery_attempts_incident_number", idx)
        # I-18 still enforced after v6.
        with self.assertRaises(sqlite3.IntegrityError):
            with store.write_txn() as (conn2, _):
                conn2.execute(
                    "INSERT INTO recovery_attempts(attempt_id, incident_id,"
                    " rung, action, observed_effect, progress_delta,"
                    " output_health_delta, decision, actor, recorded_at)"
                    " VALUES('att-bad','inc-legacy',2,'{}','none',0.0,0.0,"
                    " 'success','old',?)", (now,))
        # Forward-only: re-running applies nothing.
        self.assertEqual(migrate(store, MIGRATIONS[:6]), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
