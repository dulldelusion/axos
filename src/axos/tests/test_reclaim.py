"""Phase 1C R1 — lease reclaim primitive tests.

STANDARD: no mocks for the critical guarantees. Real SQLite/WAL files,
real concurrent writers, real transaction rollback. The single exception
is R1-13, which injects a failure *inside* the reclaim transaction to prove
the rollback leaves no half-reclaimed state.

Test IDs:
  R1-01..03  successful expired reclaim from CLAIMED / RUNNING / COMMITTING
  R1-04      non-expired lease rejected, zero mutation
  R1-05      stale token rejected
  R1-06      wrong owner rejected
  R1-07      concurrent reclaim: exactly one wins, one token increment
  R1-08      old owner cannot commit after reclaim (fenced out)
  R1-09      old owner cannot renew after reclaim
  R1-10      old owner heartbeat rejected after reclaim
  R1-11      old owner cannot update progress after reclaim
  R1-12      token monotonicity across claim/reclaim cycles
  R1-13      crash/rollback safety: injected failure -> no partial state
  R1-14      store-time authority: worker timestamps never decide expiry
  R1-15..22  failure semantics: actors, terminal/ownerless states,
              forced-reclaim evidence, double reclaim, UNCERTAIN
"""
import json
import os
import shutil
import sys
import tempfile
import threading
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace

from axos.store import (
    open_store, migrate, TransitionGate,
    TransitionRejected, LeaseError,
)
from axos.tests.r5_helpers import r5_complete


class FakeClock:
    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class ReclaimTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r1-")
        self.db_path = os.path.join(self.tmp, "axos.db")
        self.clock = FakeClock()
        self.store = open_store(self.db_path, clock=self.clock)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        self.gate.create_task("t1", {"objective": "r1"}, {"usd": 1},
                              "scheduler")

    def tearDown(self):
        try:
            self.store.close()
        finally:
            shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- helpers
    def _mk_job(self, jid, state="CLAIMED", owner="w1", ttl=60.0):
        """Create a job, claim it, and move it to `state` (lease intact)."""
        self.gate.create_job(jid, "t1", "stage-a", "scheduler")
        self.assertTrue(self.gate.claim_job(jid, owner, ttl, "scheduler"))
        if state in ("RUNNING", "COMMITTING"):
            self.gate.transition_job(jid, "RUNNING", "scheduler")
        if state == "COMMITTING":
            self.gate.transition_job(jid, "COMMITTING", "scheduler")
        return self.gate.get_job(jid)

    def _reclaim(self, jid, token, actor="reconciler", owner="w1",
                 **kw):
        return self.gate.reclaim_lease(
            jid, actor=actor, reason="lease expired",
            expected_owner=owner, expected_token=token, **kw)

    def _snap(self, jid):
        j = self.gate.get_job(jid)
        return (j["status"], j["owner_worker_id"], j["fencing_token"],
                j["lease_acquired_at"], j["lease_expires_at"])

    def _count(self, event_type, jid=None):
        q = "SELECT COUNT(*) FROM ledger WHERE event_type=?" + \
            (" AND json_extract(payload,'$.job_id')=?" if jid else "")
        params = (event_type, jid) if jid else (event_type,)
        return self.store.conn.execute(q, params).fetchone()[0]

    def _payload(self, event_type, jid):
        row = self.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type=?"
            " AND json_extract(payload,'$.job_id')=? ORDER BY seq DESC LIMIT 1",
            (event_type, jid)).fetchone()
        return json.loads(row[0])

    def _assert_chain(self):
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)

    # ------------------------------------------- R1-01/02/03 happy paths
    def test_R1_01_expired_claimed_reclaim(self):
        j = self._mk_job("j1", state="CLAIMED", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        out = self._reclaim("j1", tok)
        self.assertEqual(out["status"], "PENDING")
        self.assertIsNone(out["owner_worker_id"])
        self.assertIsNone(out["lease_expires_at"])
        self.assertEqual(out["fencing_token"], tok + 1)
        p = self._payload("job.lease_reclaimed", "j1")
        self.assertEqual(p["prev_owner"], "w1")
        self.assertEqual(p["prev_token"], tok)
        self.assertEqual(p["new_token"], tok + 1)
        self.assertEqual(p["prev_state"], "CLAIMED")
        self.assertEqual(p["new_state"], "PENDING")
        self.assertEqual(p["reason"], "lease expired")
        self.assertFalse(p["forced"])
        self.assertEqual(self._count("job.uncertain_opened", "j1"), 0)
        self._assert_chain()

    def test_R1_02_expired_running_reclaim(self):
        j = self._mk_job("j1", state="RUNNING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        out = self._reclaim("j1", tok)
        self.assertEqual(out["status"], "UNCERTAIN")
        self.assertIsNone(out["owner_worker_id"])
        self.assertEqual(out["fencing_token"], tok + 1)
        p = self._payload("job.lease_reclaimed", "j1")
        self.assertEqual((p["prev_state"], p["new_state"]),
                         ("RUNNING", "UNCERTAIN"))
        self.assertEqual(self._count("job.uncertain_opened", "j1"), 1)
        u = self._payload("job.uncertain_opened", "j1")
        self.assertEqual(u["cause"], "lease_reclaimed")
        self.assertEqual(u["prev_owner"], "w1")
        self._assert_chain()

    def test_R1_03_expired_committing_reclaim(self):
        j = self._mk_job("j1", state="COMMITTING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        out = self._reclaim("j1", tok)
        self.assertEqual(out["status"], "UNCERTAIN")
        self.assertIsNone(out["owner_worker_id"])
        self.assertEqual(out["fencing_token"], tok + 1)
        self._assert_chain()

    # ------------------------------------------------- R1-04/05/06 rejects
    def test_R1_04_non_expired_lease_rejected(self):
        j = self._mk_job("j1", state="RUNNING", ttl=600.0)
        tok = j["fencing_token"]
        before = self._snap("j1")
        n_events = self._count("job.lease_reclaimed", "j1")
        with self.assertRaises(LeaseError):
            self._reclaim("j1", tok)
        self.assertEqual(self._snap("j1"), before)  # zero mutation
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), n_events)
        self._assert_chain()

    def test_R1_05_stale_token_rejected(self):
        j = self._mk_job("j1", state="RUNNING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        before = self._snap("j1")
        with self.assertRaises(LeaseError):
            self._reclaim("j1", tok - 1)
        with self.assertRaises(LeaseError):
            self._reclaim("j1", tok + 1)
        self.assertEqual(self._snap("j1"), before)
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 0)
        self._assert_chain()

    def test_R1_06_wrong_owner_rejected(self):
        j = self._mk_job("j1", state="RUNNING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        before = self._snap("j1")
        with self.assertRaises(LeaseError):
            self._reclaim("j1", tok, owner="w2")
        self.assertEqual(self._snap("j1"), before)
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 0)
        self._assert_chain()

    # ------------------------------------------------- R1-07 concurrency
    def test_R1_07_concurrent_reclaim_exactly_one_wins(self):
        j = self._mk_job("j1", state="RUNNING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        barrier = threading.Barrier(2)
        results = []

        def contender():
            st = open_store(self.db_path, clock=self.clock)
            g = TransitionGate(st)
            barrier.wait()
            try:
                g.reclaim_lease("j1", actor="reconciler",
                                reason="race", expected_owner="w1",
                                expected_token=tok)
                results.append("ok")
            except Exception as e:  # noqa: BLE001 - loser must only reject
                results.append(type(e).__name__)
            finally:
                st.close()

        ts = [threading.Thread(target=contender) for _ in range(2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(sorted(results), ["TransitionRejected", "ok"],
                         f"results={results}")
        job = self.gate.get_job("j1")
        self.assertEqual(job["fencing_token"], tok + 1)  # one increment
        self.assertIsNone(job["owner_worker_id"])        # one revocation
        self.assertEqual(job["status"], "UNCERTAIN")     # one transition
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 1)
        self._assert_chain()

    # ---------------------------------- R1-08/09/10/11 old owner fenced out
    def _reclaimed(self, jid="j1", state="RUNNING"):
        j = self._mk_job(jid, state=state, ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        self._reclaim(jid, tok)
        return tok

    def test_R1_08_old_owner_cannot_commit_after_reclaim(self):
        tok = self._reclaimed()
        with self.assertRaises(LeaseError):
            r5_complete(self.gate, job_id="j1", worker_id="w1",
                        fencing_token=tok, task_id="t1", outcome="SUCCESS",
                        evidence={"out": 1}, actor="worker:w1")
        job = self.gate.get_job("j1")
        self.assertEqual(job["status"], "UNCERTAIN")  # no completion
        self.assertIsNone(job["commit_outcome"])      # no result mutation
        self.assertEqual(self._count("job.committed", "j1"), 0)
        self._assert_chain()

    def test_R1_09_old_owner_cannot_renew_after_reclaim(self):
        tok = self._reclaimed()
        self.assertFalse(
            self.gate.renew_lease("j1", "w1", tok, 60.0, "worker:w1"))
        job = self.gate.get_job("j1")
        self.assertIsNone(job["owner_worker_id"])
        self.assertIsNone(job["lease_expires_at"])
        self._assert_chain()

    def test_R1_10_old_owner_heartbeat_rejected_after_reclaim(self):
        tok = self._reclaimed()
        self.gate.create_worker("w1", "test")
        with self.assertRaises(LeaseError):
            self.gate.ingest_heartbeat("w1", "proc-1", "j1", tok, 0,
                                       "RUNNING", None, "worker:w1")
        self.assertEqual(self.gate.heartbeats_for("w1"), [])
        self._assert_chain()

    def test_R1_11_old_owner_cannot_update_progress_after_reclaim(self):
        tok = self._reclaimed()
        with self.assertRaises(TransitionRejected):
            self.gate.update_job_progress("j1", "w1", tok, 5.0, 10.0,
                                          "worker:w1")
        job = self.gate.get_job("j1")
        self.assertEqual(job["progress_done"], 0.0)  # schema default: untouched
        self._assert_chain()

    # ------------------------------------------------- R1-12 monotonicity
    def test_R1_12_token_monotonicity_across_cycles(self):
        seen = []
        self.gate.create_job("j1", "t1", "stage-a", "scheduler")
        for _ in range(3):
            self.assertTrue(self.gate.claim_job("j1", "w1", 60.0,
                                                "scheduler"))
            tok = self.gate.get_job("j1")["fencing_token"]
            seen.append(("claim", tok))
            self.clock.advance(120.0)
            out = self._reclaim("j1", tok)
            seen.append(("reclaim", out["fencing_token"]))
            # PENDING again so the next cycle can claim
            self.assertEqual(out["status"], "PENDING")
        toks = [t for _, t in seen]
        self.assertEqual(toks, [1, 2, 3, 4, 5, 6])
        self.assertEqual(len(set(toks)), len(toks))  # no reuse, no dupes

    # ------------------------------------------------- R1-13 crash safety
    def test_R1_13_injected_failure_rolls_back_cleanly(self):
        j = self._mk_job("j1", state="RUNNING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        before = self._snap("j1")
        n_events = self.store.conn.execute(
            "SELECT COUNT(*) FROM ledger").fetchone()[0]
        with mock.patch.object(
                self.gate, "_append_event",
                side_effect=RuntimeError("injected mid-txn failure")):
            with self.assertRaises(RuntimeError):
                self._reclaim("j1", tok)
        # owner / token / state / ledger mutually consistent: nothing moved
        self.assertEqual(self._snap("j1"), before)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM ledger")
            .fetchone()[0], n_events)
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 0)
        self._assert_chain()

    # ------------------------------------------------- R1-14 store time
    def test_R1_14_worker_timestamps_never_decide_reclaim(self):
        # future worker timestamp must not make a live lease reclaimable
        self.gate.create_job("j1", "t1", "stage-a", "scheduler")
        self.assertTrue(self.gate.claim_job(
            "j1", "w1", 100.0, "scheduler", worker_reported_ts=99_999_999.0))
        tok = self.gate.get_job("j1")["fencing_token"]
        self.clock.advance(50.0)  # store time: lease live
        with self.assertRaises(LeaseError):
            self._reclaim("j1", tok)
        # past worker timestamp must not block reclaim of an expired lease
        self.gate.create_job("j2", "t1", "stage-a", "scheduler")
        self.assertTrue(self.gate.claim_job(
            "j2", "w1", 100.0, "scheduler", worker_reported_ts=1.0))
        tok2 = self.gate.get_job("j2")["fencing_token"]
        self.clock.advance(110.0)  # store time: j2's lease expired
        out = self._reclaim("j2", tok2)
        self.assertEqual(out["status"], "PENDING")
        self.assertEqual(out["fencing_token"], tok2 + 1)
        self._assert_chain()

    # --------------------------------- R1-15..22 failure semantics extras
    def test_R1_15_supervisor_is_not_a_reclaim_authority(self):
        j = self._mk_job("j1", state="RUNNING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        before = self._snap("j1")
        with self.assertRaises(TransitionRejected):
            self._reclaim("j1", tok, actor="supervisor")
        self.assertEqual(self._snap("j1"), before)

    def test_R1_16_worker_actor_rejected(self):
        j = self._mk_job("j1", state="RUNNING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        with self.assertRaises(TransitionRejected):
            self._reclaim("j1", tok, actor="worker:w1")
        self.assertEqual(self._snap("j1")[0], "RUNNING")

    def test_R1_17_terminal_job_reclaim_rejected(self):
        self.gate.create_job("j1", "t1", "stage-a", "scheduler")
        self.assertTrue(self.gate.claim_job("j1", "w1", 600.0, "scheduler"))
        self.gate.transition_job("j1", "RUNNING", "scheduler")
        tok = self.gate.get_job("j1")["fencing_token"]
        r5_complete(self.gate, job_id="j1", worker_id="w1",
                    fencing_token=tok, task_id="t1", outcome="SUCCESS",
                    evidence={}, actor="worker:w1")
        before = self._snap("j1")
        with self.assertRaises(TransitionRejected):
            self._reclaim("j1", tok)
        self.assertEqual(self._snap("j1"), before)
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 0)

    def test_R1_18_ownerless_and_double_reclaim_rejected(self):
        # never-claimed job: nothing to revoke
        self.gate.create_job("j9", "t1", "stage-a", "scheduler")
        with self.assertRaises(TransitionRejected):
            self.gate.reclaim_lease("j9", actor="reconciler",
                                    reason="x", expected_owner="w1",
                                    expected_token=1)
        # second reclaim after a success: ownerless, never a re-increment
        j = self._mk_job("j1", state="RUNNING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        out = self._reclaim("j1", tok)
        new_tok = out["fencing_token"]
        with self.assertRaises(TransitionRejected):
            self.gate.reclaim_lease("j1", actor="reconciler", reason="x",
                                    expected_owner="w1",
                                    expected_token=new_tok)
        self.assertEqual(self.gate.get_job("j1")["fencing_token"], new_tok)
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 1)

    def test_R1_19_forced_reclaim_of_live_lease_with_verdict(self):
        j = self._mk_job("j1", state="RUNNING", ttl=600.0)  # lease LIVE
        tok = j["fencing_token"]
        out = self.gate.reclaim_lease(
            "j1", actor="recovery-controller", reason="watchdog STALLED",
            expected_owner="w1", expected_token=tok,
            force=True, verdict="STALLED", incident_id="inc-1")
        self.assertEqual(out["status"], "UNCERTAIN")
        self.assertIsNone(out["owner_worker_id"])
        self.assertEqual(out["fencing_token"], tok + 1)
        p = self._payload("job.lease_reclaimed", "j1")
        self.assertTrue(p["forced"])
        self.assertEqual(p["verdict"], "STALLED")
        self.assertEqual(p["incident_id"], "inc-1")
        self._assert_chain()

    def test_R1_20_forced_reclaim_requires_verdict_and_incident(self):
        j = self._mk_job("j1", state="RUNNING", ttl=600.0)
        tok = j["fencing_token"]
        before = self._snap("j1")
        kw = dict(actor="recovery-controller", reason="x",
                  expected_owner="w1", expected_token=tok)
        with self.assertRaises(TransitionRejected):  # no verdict
            self.gate.reclaim_lease("j1", force=True, incident_id="inc-1",
                                    **kw)
        with self.assertRaises(TransitionRejected):  # bad verdict
            self.gate.reclaim_lease("j1", force=True, verdict="UNKNOWN",
                                    incident_id="inc-1", **kw)
        with self.assertRaises(TransitionRejected):  # no incident
            self.gate.reclaim_lease("j1", force=True, verdict="DEAD", **kw)
        self.assertEqual(self._snap("j1"), before)
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 0)

    def test_R1_21_second_reclaim_with_old_token_rejected(self):
        # the contract's §9 example: old token must never re-revoke
        j = self._mk_job("j1", state="RUNNING", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        out = self._reclaim("j1", tok)
        self.assertEqual(out["fencing_token"], tok + 1)
        # The stale token can never re-revoke: the job is UNCERTAIN now, so
        # the state check fires first — either way it must REJECT with zero
        # mutation (the stale-token path itself is proven in R1-05).
        with self.assertRaises((LeaseError, TransitionRejected)):
            self._reclaim("j1", tok)  # stale token: reject, no mutation
        job = self.gate.get_job("j1")
        self.assertEqual(job["fencing_token"], tok + 1)
        self.assertIsNone(job["owner_worker_id"])
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 1)

    def test_R1_22_uncertain_job_reclaim_rejected(self):
        tok = self._reclaimed()  # RUNNING -> UNCERTAIN
        with self.assertRaises(TransitionRejected):
            self.gate.reclaim_lease("j1", actor="reconciler",
                                    reason="x", expected_owner="w1",
                                    expected_token=tok + 1)
        self.assertEqual(self.gate.get_job("j1")["status"], "UNCERTAIN")

    def test_R1_23_unknown_job_rejected(self):
        with self.assertRaises(TransitionRejected):
            self.gate.reclaim_lease("nope", actor="reconciler", reason="x",
                                    expected_owner="w1", expected_token=1)


if __name__ == "__main__":
    unittest.main()
