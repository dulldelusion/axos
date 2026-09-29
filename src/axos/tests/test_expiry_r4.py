"""Phase 1C R4 — lease expiry detection & fencing evidence.

STANDARD: real SQLite/WAL files, real transactions, real concurrency
(threads with one Store per thread, exactly like production components),
real worker subprocesses via the supervisor, and the real store clock
(FakeClock only where determinism needs it). No mocks except the
injected-failure rollback test, which must raise *inside* the gate's
transaction to prove atomicity — the single sanctioned exception.

R4 answers "has the durable lease actually expired?" via
TransitionGate.observe_expired_leases(): a read-only, deterministic
observer over the current durable row. It is advisory evidence, never
authorization: the only mutation path stays R1's reclaim_lease(), which
revalidates owner/token/expiry atomically inside its own transaction.

Test IDs:
  R4-01  unexpired lease: observed empty, row untouched
  R4-02  exact expiry boundary: lease_expires_at == now is expired
  R4-03  expired lease detected, evidence reconstructs the observation
  R4-04  future worker timestamp cannot prevent expiry
  R4-05  heartbeat without renewal leaves the lease expired
  R4-06  legitimate renewal before expiry prevents false expiry
  R4-07  renewal race: a successful renewal is never falsely reclaimed
  R4-08  reclaim race: two contenders, exactly one authoritative mutation
  R4-09  repeated observation is idempotent, no duplicate mutations
  R4-10  stale owner/token evidence cannot act after a token change
  R4-11  multiple jobs: one expired job does not affect unrelated jobs
  R4-12  state filtering: non-executing states are never expiry candidates
  R4-13  R3 compatibility: heartbeat activity never renews the lease
  R4-14  R2 compatibility: expiry -> R1 reclaim -> R2 fence-sweep lifecycle
  R4-15  authoritative clock: wrong worker/local clocks cannot decide expiry
  R4-16  rollback: injected failure leaves no partial expiry/reclaim mutation
  R4-17  no duplicate reclaim authority (static + audit proof)
  R4-18  supervisor separation: R4 never terminates a process group
Audit:
  R4-A1   audit 09 R4 checks (A10a-A10g) present and green
"""
import inspect
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace
AXOS_DIR = os.path.dirname(HERE)

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


class ExpiryR4Test(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r4-")
        self.db_path = os.path.join(self.tmp, "axos.db")
        self.clock = FakeClock()
        self.store = open_store(self.db_path, clock=self.clock)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        self._sups = []

    def tearDown(self):
        for s in self._sups:
            try:
                for wid in list(s._procs):
                    try:
                        s.kill_worker(wid)
                    except Exception:
                        pass
                s.close()
            except Exception:
                pass
        try:
            self.store.close()
        except Exception:
            pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- helpers
    def _task(self):
        try:
            self.gate.create_task("t", {"objective": "x"}, {"usd": 1},
                                  "scheduler")
        except Exception:
            pass

    def _mk(self, jid, wid, state="RUNNING", ttl=60.0):
        self._task()
        self.gate.create_job(jid, "t", "s", "scheduler")
        self.gate.create_worker(wid, "test")
        assert self.gate.claim_job(jid, wid, ttl, "scheduler")
        if state != "CLAIMED":
            self.gate.transition_job(jid, state, f"worker:{wid}")
        return self.gate.get_job(jid)

    def _routine_reclaim(self, jid, owner, token):
        return self.gate.reclaim_lease(
            jid, actor="recovery-controller", reason="r4 test",
            expected_owner=owner, expected_token=token)

    def _snap(self, jid):
        return dict(self.gate.get_job(jid))

    def _ledger_count(self):
        return self.store.conn.execute(
            "SELECT COUNT(*) FROM ledger").fetchone()[0]

    def _count(self, event_type, jid):
        return self.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?"
            " AND json_extract(payload,'$.job_id')=?",
            (event_type, jid)).fetchone()[0]

    def _wait_for(self, pred, timeout=20.0, interval=0.1):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("wait_for timed out")

    def _by_id(self, jid):
        return {e["job_id"]: e
                for e in self.gate.observe_expired_leases()}.get(jid)

    # ------------------------------------------- R4-01 unexpired untouched
    def test_R4_01_unexpired_lease_not_detected(self):
        j = self._mk("j1", "w1", ttl=60.0)
        before = self._snap("j1")
        self.assertEqual(self.gate.observe_expired_leases(), [])
        self.assertEqual(self.gate.expired_leases(), [])
        # observation touched nothing
        self.assertEqual(self._snap("j1"), before)
        # still live just before expiry: advance to 1s before the deadline
        self.clock.t = j["lease_expires_at"] - 1.0
        self.assertEqual(self.gate.observe_expired_leases(), [])
        self.assertEqual(self._snap("j1"), before)

    # -------------------------------------- R4-02 exact boundary inclusive
    def test_R4_02_exact_expiry_boundary_is_expired(self):
        j = self._mk("j1", "w1", ttl=60.0)
        exp = j["lease_expires_at"]
        # just before: not expired
        self.clock.t = exp - 0.001
        self.assertEqual(self.gate.observe_expired_leases(), [])
        # exactly at: expired (predicate is <=, per contract)
        self.clock.t = exp
        ev = self.gate.observe_expired_leases()
        self.assertEqual([e["job_id"] for e in ev], ["j1"])
        self.assertEqual(ev[0]["observed_at"], exp)
        self.assertEqual(ev[0]["lease_expires_at"], exp)
        # just after: still expired
        self.clock.t = exp + 100.0
        self.assertEqual([e["job_id"] for e in
                          self.gate.observe_expired_leases()], ["j1"])

    # --------------------------------- R4-03 evidence reconstructs the fact
    def test_R4_03_expired_lease_detected_with_evidence(self):
        j = self._mk("j1", "w1", ttl=60.0)
        self.clock.advance(120.0)
        now = self.store.current_time()
        evs = self.gate.observe_expired_leases()
        self.assertEqual(len(evs), 1)
        ev = evs[0]
        # everything needed to reconstruct the observation + hand to R1
        self.assertEqual(ev["job_id"], "j1")
        self.assertEqual(ev["owner_worker_id"], "w1")
        self.assertEqual(ev["fencing_token"], j["fencing_token"])
        self.assertEqual(ev["status"], "RUNNING")
        self.assertEqual(ev["lease_acquired_at"], j["lease_acquired_at"])
        self.assertEqual(ev["lease_expires_at"], j["lease_expires_at"])
        self.assertEqual(ev["observed_at"], now)
        self.assertLessEqual(ev["lease_expires_at"], ev["observed_at"])
        self.assertIn("owner_worker_id IS NOT NULL", ev["predicate"])
        self.assertIn("lease_expires_at <= :now", ev["predicate"])
        self.assertIn("CLAIMED", ev["predicate"])
        self.assertIn("j1", ev["reason"])
        self.assertIn("w1", ev["reason"])
        # no worker-supplied timestamp anywhere in the evidence
        self.assertNotIn("worker_reported_ts", ev)
        # legacy projection carries the same predicate
        legacy = self.gate.expired_leases()
        self.assertEqual(len(legacy), 1)
        self.assertEqual(set(legacy[0]),
                         {"job_id", "owner_worker_id", "fencing_token",
                          "lease_expires_at"})
        self.assertEqual(legacy[0]["job_id"], "j1")

    # --------------------------- R4-04 future worker ts cannot save a lease
    def test_R4_04_future_worker_timestamp_cannot_prevent_expiry(self):
        j = self._mk("j1", "w1", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)  # lease expired per store time
        # heartbeat carrying a worker timestamp far in the future
        self.gate.ingest_heartbeat("w1", "p1", "j1", tok, 0, "RUNNING",
                                    "op", "worker:w1",
                                    worker_reported_ts=9_999_999_999.0)
        ev = self._by_id("j1")
        self.assertIsNotNone(ev, "expired lease must still be detected")
        self.assertEqual(ev["observed_at"], self.store.current_time())
        # ... and R1 still reclaims it: the worker timestamp bought nothing
        out = self._routine_reclaim("j1", "w1", tok)
        self.assertIsNone(out["owner_worker_id"])

    # ----------------------- R4-05 heartbeat without renewal changes nothing
    def test_R4_05_heartbeat_without_renewal_leaves_lease_expired(self):
        j = self._mk("j1", "w1", ttl=60.0)
        tok = j["fencing_token"]
        exp_before = j["lease_expires_at"]
        self.clock.advance(120.0)
        for s in range(5):
            self.gate.ingest_heartbeat("w1", "p1", "j1", tok, s, "RUNNING",
                                        "op", "worker:w1")
        after = self.gate.get_job("j1")
        self.assertEqual(after["lease_expires_at"], exp_before)
        self.assertEqual(after["fencing_token"], tok)
        self.assertIsNotNone(self._by_id("j1"))

    # ----------------- R4-06 legitimate renewal prevents false expiry
    def test_R4_06_legitimate_renewal_prevents_false_expiry(self):
        j = self._mk("j1", "w1", ttl=60.0)
        tok = j["fencing_token"]
        exp = j["lease_expires_at"]
        self.clock.t = exp - 30.0  # lease still live
        self.assertTrue(
            self.gate.renew_lease("j1", "w1", tok, 60.0, "worker:w1"))
        new_exp = self.gate.get_job("j1")["lease_expires_at"]
        self.assertGreater(new_exp, exp)
        # the renewed lease is not flagged while live ...
        self.assertEqual(self.gate.observe_expired_leases(), [])
        # ... but expiry is still detected once the renewed lease lapses
        self.clock.t = new_exp
        self.assertIsNotNone(self._by_id("j1"))
        # D2: an expired lease cannot be resurrected by renewal
        self.assertFalse(
            self.gate.renew_lease("j1", "w1", tok, 60.0, "worker:w1"))
        self.assertIsNotNone(self._by_id("j1"))

    # --------------------------------- R4-07 renewal race: never false-reclaim
    def test_R4_07_renewal_race_no_false_reclaim(self):
        # Deterministic half: a renewal that commits while the lease is
        # live moves the deadline; detection honors the renewed deadline,
        # and R1 revalidates expiry atomically — acting on owner/token
        # evidence while the (renewed) lease is live is rejected with
        # zero mutation. A successful renewal is never falsely reclaimed.
        j = self._mk("j1", "w1", ttl=60.0)
        tok = j["fencing_token"]
        exp = j["lease_expires_at"]
        self.clock.t = exp - 1.0
        self.assertTrue(
            self.gate.renew_lease("j1", "w1", tok, 600.0, "worker:w1"))
        renewed_exp = self.gate.get_job("j1")["lease_expires_at"]
        # at the ORIGINAL deadline the lease is live: no false expiry ...
        self.clock.t = exp
        self.assertEqual(self.gate.observe_expired_leases(), [])
        # ... and a reclaim attempt on the renewed (live) lease is
        # rejected by R1's in-transaction expiry check, mutating nothing
        before = self._snap("j1")
        n_ledger = self._ledger_count()
        with self.assertRaises(LeaseError):
            self._routine_reclaim("j1", "w1", tok)
        self.assertEqual(self._snap("j1"), before)
        self.assertEqual(self._ledger_count(), n_ledger)
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 0)
        # once the renewed lease genuinely lapses, expiry is detected
        self.clock.t = renewed_exp
        self.assertIsNotNone(self._by_id("j1"))

        # Threaded half: real clock, real SQLite locking. Renewers hammer
        # renew_lease while observers hammer observe -> routine reclaim.
        # Invariants, whatever the interleaving:
        #   * at most one successful reclaim per job (exactly-once);
        #   * every rejection is a clean LeaseError/TransitionRejected;
        #   * ledger reclaim events == successful reclaims (no duplicates);
        #   * after a reclaim, the old token can never renew again.
        import collections
        st0 = open_store(self.db_path)  # real clock
        try:
            g0 = TransitionGate(st0)
            jobs = {}
            for i in range(3):
                jid, wid = f"rj{i}", f"rw{i}"
                g0.create_job(jid, "t", "s", "scheduler")
                g0.create_worker(wid, "test")
                assert g0.claim_job(jid, wid, 0.4, "scheduler")
                g0.transition_job(jid, "RUNNING", f"worker:{wid}")
                jobs[jid] = (wid, g0.get_job(jid)["fencing_token"])
            stop = threading.Event()
            errors = []
            lock = threading.Lock()
            reclaims_ok = collections.Counter()

            def renewer(jid, wid, tok):
                st = open_store(self.db_path)
                g = TransitionGate(st)
                try:
                    while not stop.is_set():
                        try:
                            g.renew_lease(jid, wid, tok, 0.4, f"worker:{wid}")
                        except Exception as e:  # noqa: BLE001
                            with lock:
                                errors.append(("renew", jid, repr(e)))
                            break
                        time.sleep(0.005)
                finally:
                    st.close()

            def observer():
                st = open_store(self.db_path)
                g = TransitionGate(st)
                try:
                    while not stop.is_set():
                        try:
                            evs = g.observe_expired_leases()
                        except Exception as e:  # noqa: BLE001
                            with lock:
                                errors.append(("observe", repr(e)))
                            break
                        for ev in evs:
                            if ev["job_id"] not in jobs:
                                continue
                            try:
                                g.reclaim_lease(
                                    ev["job_id"], actor="recovery-controller",
                                    reason="r4-07 race",
                                    expected_owner=ev["owner_worker_id"],
                                    expected_token=ev["fencing_token"])
                            except (LeaseError, TransitionRejected):
                                pass  # clean rejection: stale or raced
                            except Exception as e:  # noqa: BLE001
                                with lock:
                                    errors.append(
                                        ("reclaim", ev["job_id"], repr(e)))
                                break
                            else:
                                with lock:
                                    reclaims_ok[ev["job_id"]] += 1
                        time.sleep(0.005)
                finally:
                    st.close()

            threads = ([threading.Thread(target=renewer, args=(jid, wid, tok))
                        for jid, (wid, tok) in jobs.items()]
                       + [threading.Thread(target=observer)
                          for _ in range(2)])
            for t in threads:
                t.start()
            time.sleep(2.5)
            stop.set()
            for t in threads:
                t.join(timeout=20.0)
            self.assertFalse(any(t.is_alive() for t in threads),
                             "race threads did not finish")
        finally:
            st0.close()
        self.assertEqual(errors, [], f"unexpected errors: {errors}")
        for jid, (wid, tok) in jobs.items():
            self.assertLessEqual(reclaims_ok[jid], 1,
                                 f"{jid}: more than one reclaim succeeded")
            self.assertEqual(self._count("job.lease_reclaimed", jid),
                             reclaims_ok[jid],
                             f"{jid}: duplicate reclaim ledger events")
            if reclaims_ok[jid] == 1:
                fin = self.gate.get_job(jid)
                self.assertEqual(fin["fencing_token"], tok + 1)
                self.assertIsNone(fin["owner_worker_id"])
                self.assertEqual(fin["status"], "UNCERTAIN")
                # the pre-reclaim token is dead: no post-reclaim renewal
                self.assertFalse(
                    self.gate.renew_lease(jid, wid, tok, 0.4,
                                          f"worker:{wid}"))

    # ------------------ R4-08 reclaim race: exactly one mutation wins
    def test_R4_08_reclaim_race_exactly_one_mutation(self):
        j = self._mk("j1", "w1", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        ev = self._by_id("j1")
        self.assertIsNotNone(ev)
        barrier = threading.Barrier(2)
        results = []

        def contender():
            st = open_store(self.db_path, clock=self.clock)
            g = TransitionGate(st)
            barrier.wait()
            try:
                g.reclaim_lease("j1", actor="reconciler", reason="r4-08 race",
                                expected_owner=ev["owner_worker_id"],
                                expected_token=ev["fencing_token"])
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
        # winner reclaims; loser sees the ownerless row -> TransitionRejected
        self.assertEqual(sorted(results), ["TransitionRejected", "ok"],
                         f"results={results}")
        fin = self.gate.get_job("j1")
        self.assertEqual(fin["fencing_token"], tok + 1)  # one increment
        self.assertIsNone(fin["owner_worker_id"])        # one revocation
        self.assertEqual(fin["status"], "UNCERTAIN")     # one transition
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 1)
        # the reclaimed job is no longer an expiry candidate
        self.assertIsNone(self._by_id("j1"))

    # ---------------------- R4-09 repeated observation is idempotent
    def test_R4_09_repeated_observation_idempotent(self):
        j = self._mk("j1", "w1", ttl=60.0)
        self.clock.advance(120.0)
        before = self._snap("j1")
        n_ledger = self._ledger_count()
        seen = [self.gate.observe_expired_leases() for _ in range(4)]
        for evs in seen:
            self.assertEqual(len(evs), 1)
            self.assertEqual(evs[0]["job_id"], "j1")
        self.assertEqual(seen[0], seen[1])
        self.assertEqual(seen[1], seen[2])
        self.assertEqual(seen[2], seen[3])
        # no mutation whatsoever: row byte-identical, no ledger noise,
        # no token bump, no reclaim event
        self.assertEqual(self._snap("j1"), before)
        self.assertEqual(self._ledger_count(), n_ledger)
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 0)
        self.assertEqual(self.gate.get_job("j1")["fencing_token"],
                         j["fencing_token"])

    # ------------------ R4-10 stale owner/token evidence cannot act
    def test_R4_10_stale_owner_token_evidence_rejected(self):
        j = self._mk("j1", "w1", ttl=60.0)
        tok1 = j["fencing_token"]
        self.clock.advance(120.0)
        ev_old = self._by_id("j1")
        self.assertEqual(ev_old["owner_worker_id"], "w1")
        self.assertEqual(ev_old["fencing_token"], tok1)
        # intervening authority change: routine reclaim, then re-claim by w2
        self._routine_reclaim("j1", "w1", tok1)
        self.gate.create_worker("w2", "test")
        self.gate.transition_job("j1", "PENDING", "recovery-controller")
        self.assertTrue(self.gate.claim_job("j1", "w2", 60.0, "scheduler"))
        tok2 = self.gate.get_job("j1")["fencing_token"]
        self.assertNotEqual(tok2, tok1)
        before = self._snap("j1")
        # acting on the OLD evidence: owner mismatch -> rejected
        with self.assertRaises((LeaseError, TransitionRejected)):
            self._routine_reclaim("j1", ev_old["owner_worker_id"],
                                  ev_old["fencing_token"])
        # (w2, old token): stale token -> LeaseError
        with self.assertRaises(LeaseError):
            self._routine_reclaim("j1", "w2", tok1)
        self.assertEqual(self._snap("j1"), before)
        # fresh evidence after the new lease lapses works
        self.clock.advance(120.0)
        ev_new = self._by_id("j1")
        self.assertEqual(ev_new["owner_worker_id"], "w2")
        self.assertEqual(ev_new["fencing_token"], tok2)
        out = self._routine_reclaim("j1", "w2", tok2)
        self.assertIsNone(out["owner_worker_id"])

    # ----------------- R4-11 one expired job does not affect others
    def test_R4_11_multiple_jobs_isolation(self):
        e1 = self._mk("j1", "w1", ttl=60.0)   # will expire
        e2 = self._mk("j2", "w2", ttl=600.0)  # stays live
        e3 = self._mk("j3", "w3", ttl=60.0)   # will expire
        snap2, snap3 = self._snap("j2"), self._snap("j3")
        self.clock.advance(120.0)
        flagged = {e["job_id"] for e in self.gate.observe_expired_leases()}
        self.assertEqual(flagged, {"j1", "j3"})
        ev1 = self._by_id("j1")
        self._routine_reclaim("j1", ev1["owner_worker_id"],
                              ev1["fencing_token"])
        # j2 untouched (still live, still owned); j3 still flagged, unmutated
        fin2 = self.gate.get_job("j2")
        self.assertEqual(fin2["owner_worker_id"], "w2")
        self.assertEqual(fin2["lease_expires_at"], e2["lease_expires_at"])
        self.assertEqual(fin2["fencing_token"], e2["fencing_token"])
        self.assertEqual(self._snap("j3"), snap3)
        self.assertIsNotNone(self._by_id("j3"))
        self.assertIsNone(self._by_id("j2"))

    # ----------------------- R4-12 state filtering: only live executions
    def test_R4_12_state_filtering(self):
        self._mk("jr", "wr", ttl=60.0)                      # RUNNING: flagged
        self._mk("jc", "wc", ttl=60.0, state="CLAIMED")      # CLAIMED: flagged
        jp = self._mk("jp", "wp", ttl=60.0)                 # -> PENDING
        self.gate.transition_job("jp", "PENDING", "test")
        jc2 = self._mk("jc2", "wc2", ttl=60.0)              # -> COMPLETE
        r5_complete(self.gate, job_id="jc2", worker_id="wc2",
                    fencing_token=jc2["fencing_token"], task_id="t",
                    outcome="SUCCESS", evidence={},
                    actor="worker:wc2")
        jf = self._mk("jf", "wf", ttl=60.0)                 # -> FAILED
        self.gate.transition_job("jf", "FAILED", "test")
        jb = self._mk("jb", "wb", ttl=60.0)                 # -> BLOCKED
        self.gate.transition_job("jb", "FAILED", "test")
        self.gate.transition_job("jb", "BLOCKED", "test")
        jq = self._mk("jq", "wq", ttl=60.0)                 # -> QUARANTINED
        self.gate.transition_job("jq", "FAILED", "test")
        self.gate.transition_job("jq", "QUARANTINED", "test")
        ju = self._mk("ju", "wu", ttl=60.0)                 # -> UNCERTAIN
        self.clock.advance(120.0)  # every lease above is now past expiry
        self._routine_reclaim("ju", "wu", ju["fencing_token"])
        flagged = {e["job_id"] for e in self.gate.observe_expired_leases()}
        self.assertEqual(flagged, {"jr", "jc"})
        # prove the filter did real work: the excluded jobs still carry a
        # stale owner + an expired lease timestamp — status alone excludes
        now = self.store.current_time()
        for jid in ("jp", "jc2", "jf", "jb", "jq"):
            row = self.gate.get_job(jid)
            self.assertIsNotNone(row["owner_worker_id"], jid)
            self.assertIsNotNone(row["lease_expires_at"], jid)
            self.assertLessEqual(row["lease_expires_at"], now, jid)
        self.assertIsNone(self.gate.get_job("ju")["owner_worker_id"])

    # -------------------- R4-13 R3 compatibility: heartbeats never renew
    def test_R4_13_heartbeat_activity_never_renews_lease(self):
        j = self._mk("j1", "w1", ttl=60.0)
        tok = j["fencing_token"]
        exp_before = j["lease_expires_at"]
        self.clock.advance(120.0)
        # plain liveness heartbeats: accepted, lease untouched, still expired
        for s in range(3):
            self.gate.ingest_heartbeat("w1", "p1", "j1", tok, s, "RUNNING",
                                        "op", "worker:w1",
                                        worker_reported_ts=5_000_000.0)
        # heartbeat WITH informational progress on an expired lease is
        # rejected by the existing R3 rule (progress needs a live lease);
        # the rejection changes nothing about the lease either
        with self.assertRaises(LeaseError):
            self.gate.ingest_heartbeat("w1", "p1", "j1", tok, 3, "RUNNING",
                                        "op", "worker:w1",
                                        progress_done=9.0, progress_total=10.0)
        after = self.gate.get_job("j1")
        self.assertEqual(after["lease_expires_at"], exp_before)
        self.assertEqual(after["fencing_token"], tok)
        self.assertEqual(after["owner_worker_id"], "w1")
        self.assertIsNotNone(self._by_id("j1"))
        # heartbeats_for still shows the liveness evidence that was accepted
        self.assertEqual(len(self.gate.heartbeats_for("w1")), 3)

    # ------ R4-14 lifecycle: R3 evidence -> R4 expiry -> R1 reclaim -> R2 kill
    def test_R4_14_expiry_to_reclaim_to_r2_fencing(self):
        from axos.exec.supervisor import Supervisor
        self._task()
        self.gate.create_job("j14", "t", "s", "scheduler")
        sup = Supervisor(self.db_path, actor="test-r4",
                         heartbeat_interval_s=0.2)
        self._sups.append(sup)
        proc_before = None
        sup.start_worker("w14", "j14",
                         {"kind": "heartbeat_loop", "duration_s": 60.0},
                         ttl_s=1.0, hb_interval_s=0.1, renew=False)
        self._wait_for(
            lambda: self.gate.get_job("j14")["owner_worker_id"] == "w14",
            timeout=20.0)
        # R3 evidence flows while the lease is live
        self._wait_for(lambda: len(self.gate.heartbeats_for("w14")) >= 1,
                       timeout=20.0)
        # the lease expires naturally on the real clock; R4 observes it.
        # observation alone must not disturb the worker: no reclaim event,
        # no fencing event, owner/token/lease untouched.
        def observed():
            return self._by_id("j14") is not None
        self._wait_for(observed, timeout=20.0)
        ev = self._by_id("j14")
        snap = self._snap("j14")
        self.assertEqual(self._count("job.lease_reclaimed", "j14"), 0)
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type="
                "'worker.fence_enforced'").fetchone()[0], 0)
        n_hb_old_token_before = self.store.conn.execute(
            "SELECT COUNT(*) FROM heartbeats WHERE job_id='j14'"
            " AND fencing_token=?", (snap["fencing_token"],)).fetchone()[0]
        self.assertGreaterEqual(n_hb_old_token_before, 1)
        # R1 routine reclaim on the observed evidence (the ONLY mutation)
        out = self._routine_reclaim("j14", ev["owner_worker_id"],
                                    ev["fencing_token"])
        self.assertIsNone(out["owner_worker_id"])
        self.assertEqual(out["fencing_token"], snap["fencing_token"] + 1)
        self.assertEqual(out["status"], "UNCERTAIN")
        # R2 fence sweep observes the authority loss and terminates the
        # old process group; the fencing evidence is journaled
        def enforced():
            return self.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type="
                "'worker.fence_enforced' AND json_extract(payload,"
                "'$.worker_id')='w14'").fetchone()[0] > 0
        self._wait_for(enforced, timeout=20.0)
        # the old token is dead: heartbeats still arriving with it are
        # rejected, never inserted — the durable count cannot grow
        time.sleep(1.0)
        n_hb_old_token_after = self.store.conn.execute(
            "SELECT COUNT(*) FROM heartbeats WHERE job_id='j14'"
            " AND fencing_token=?", (snap["fencing_token"],)).fetchone()[0]
        self.assertEqual(n_hb_old_token_after, n_hb_old_token_before)

    # ----------------- R4-15 authoritative clock beats worker/local clocks
    def test_R4_15_authoritative_clock_decides_expiry(self):
        j = self._mk("j1", "w1", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        # worker timestamps wrong in both directions change nothing
        self.gate.ingest_heartbeat("w1", "p1", "j1", tok, 0, "RUNNING",
                                    "op", "worker:w1",
                                    worker_reported_ts=1e15)
        self.gate.ingest_heartbeat("w1", "p1", "j1", tok, 1, "RUNNING",
                                    "op", "worker:w1",
                                    worker_reported_ts=0.0)
        evs = self.gate.observe_expired_leases()
        self.assertEqual(len(evs), 1)
        ev = evs[0]
        # observed_at is the authoritative store clock — provably not
        # derived from anything the worker supplied
        self.assertEqual(ev["observed_at"], self.store.current_time())
        self.assertNotEqual(ev["observed_at"], 1e15)
        self.assertNotEqual(ev["observed_at"], 0.0)
        self.assertNotIn("worker_reported_ts", ev)
        # liveness (last_seen_at) does not rescue the lease either
        wrow = self.store.conn.execute(
            "SELECT last_seen_at FROM workers WHERE worker_id='w1'"
        ).fetchone()
        self.assertIsNotNone(wrow["last_seen_at"])
        self.assertIsNotNone(self._by_id("j1"))

    # ----------------------- R4-16 rollback: no partial expiry mutation
    def test_R4_16_injected_failure_rolls_back_cleanly(self):
        j = self._mk("j1", "w1", ttl=60.0)
        tok = j["fencing_token"]
        self.clock.advance(120.0)
        ev = self.gate.observe_expired_leases()
        self.assertEqual(len(ev), 1)
        before = self._snap("j1")
        n_ledger = self._ledger_count()
        with mock.patch.object(
                self.gate, "_append_event",
                side_effect=RuntimeError("injected mid-txn failure")):
            with self.assertRaises(RuntimeError):
                self._routine_reclaim("j1", "w1", tok)
        # owner / token / state / ledger mutually consistent: nothing moved
        self.assertEqual(self._snap("j1"), before)
        self.assertEqual(self._ledger_count(), n_ledger)
        self.assertEqual(self._count("job.lease_reclaimed", "j1"), 0)
        # re-observation returns the identical evidence: the failed
        # reclaim left no half-reclaimed state behind
        self.assertEqual(self.gate.observe_expired_leases(), ev)

    # ----------------- R4-17 no duplicate reclaim authority (static proof)
    def test_R4_17_no_duplicate_reclaim_authority(self):
        import ast
        from axos.store import gate as gate_mod

        def body_src(fn):
            import textwrap
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if (node.body and isinstance(node.body[0], ast.Expr)
                            and isinstance(node.body[0].value, ast.Constant)
                            and isinstance(node.body[0].value.value, str)):
                        node.body = node.body[1:]
                    return ast.unparse(node)
            raise AssertionError("no body")
        # docstrings legitimately NAME reclaim_lease to document the R1
        # boundary; the proof is about the method BODIES (same as audit
        # A10a/A10b)
        src = body_src(gate_mod.TransitionGate.observe_expired_leases)
        for kw in ("INSERT", "UPDATE", "DELETE", "reclaim_lease",
                   "write_txn"):
            self.assertNotIn(kw, src,
                             f"R4 detector must not contain {kw!r}")
        # gate-wide: the only ownership-revoking mutation (clear owner +
        # bump token + PENDING/UNCERTAIN) is reclaim_lease; the voluntary
        # release primitive is the only other owner-clearing path and it
        # is not a reclaim (no token bump, no state transition to
        # PENDING/UNCERTAIN, owner-initiated).
        import re
        clearers = set()
        bumpers = set()
        for name in dir(TransitionGate):
            fn = getattr(TransitionGate, name, None)
            if not callable(fn):
                continue
            try:
                s = inspect.getsource(fn)
            except (OSError, TypeError):
                continue
            if "owner_worker_id=NULL" in s:
                clearers.add(name)
            if re.search(r"new_token\s*=\s*int\(cur_tok\)\s*\+\s*1", s):
                bumpers.add(name)
        self.assertEqual(clearers, {"reclaim_lease", "release_lease"},
                         f"owner-clearing methods: {clearers}")
        self.assertEqual(bumpers, {"reclaim_lease"},
                         f"token-bumping methods: {bumpers}")
        self.assertNotIn("reclaim_lease", src)

    # ---------------- R4-18 supervisor separation: R4 never kills processes
    def test_R4_18_r4_never_terminates_process_groups(self):
        import ast
        from axos.store import gate as gate_mod

        def body_src(fn):
            import textwrap
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if (node.body and isinstance(node.body[0], ast.Expr)
                            and isinstance(node.body[0].value, ast.Constant)
                            and isinstance(node.body[0].value.value, str)):
                        node.body = node.body[1:]
                    return ast.unparse(node)
            raise AssertionError("no body")
        src = (body_src(gate_mod.TransitionGate.observe_expired_leases)
               + body_src(gate_mod.TransitionGate.expired_leases))
        for kw in ("killpg", "SIGTERM", "SIGKILL", "terminate", "Popen",
                   "supervisor", "os.kill"):
            self.assertNotIn(kw, src,
                             f"R4 detector must not reference {kw!r}")
        # runtime: observation alone never disturbs a live worker process
        from axos.exec.supervisor import Supervisor
        self._task()
        self.gate.create_job("j18", "t", "s", "scheduler")
        sup = Supervisor(self.db_path, actor="test-r4",
                         heartbeat_interval_s=0.2)
        self._sups.append(sup)
        sup.start_worker("w18", "j18",
                         {"kind": "heartbeat_loop", "duration_s": 60.0},
                         ttl_s=60.0, hb_interval_s=0.1)
        self._wait_for(
            lambda: self.gate.get_job("j18")["owner_worker_id"] == "w18",
            timeout=20.0)
        self._wait_for(lambda: len(self.gate.heartbeats_for("w18")) >= 2,
                       timeout=20.0)
        # repeated observation of a live lease: empty, and the worker's
        # process group is untouched (still heartbeating afterwards)
        n0 = len(self.gate.heartbeats_for("w18"))
        for _ in range(3):
            self.assertEqual(self.gate.observe_expired_leases(), [])
        self._wait_for(
            lambda: len(self.gate.heartbeats_for("w18")) > n0, timeout=20.0)

    # --------------------------------------- R4-A1 audit 09 R4 checks green
    def test_R4_A1_audit_09_r4_checks_green(self):
        r = subprocess.run(
            [sys.executable, os.path.join(AXOS_DIR, "audit",
                                          "09_authority_audit.py")],
            capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 0,
                         f"audit 09 failed:\n{r.stdout}\n{r.stderr}")
        for tag in ("A10a", "A10b", "A10c", "A10d", "A10e", "A10f", "A10g"):
            self.assertIn(tag, r.stdout, f"{tag} missing from audit output")


if __name__ == "__main__":
    unittest.main(verbosity=2)
