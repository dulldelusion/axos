"""Phase 1B — execution substrate tests.

STANDARD: no mocks for the critical guarantees. Real SQLite files, real
subprocesses, real SIGTERM/SIGKILL, real lease expiry, real concurrent
races. Deterministic gate-level tests sit alongside adversarial
process-level fault injection.

Test IDs:
  FENCE-A/B/C/D*  fencing scenarios (gate-level and worker-level)
  FENCE-TOCTOU    concurrent commit race: stale token can never win
  FENCE-RACE      concurrent claim race: exactly one winner
  FENCE-SIX       commit validates job/worker/token/expiry/state/outcome
  RENEW           lease renewal keeps a long job authorized (sanctioned path)
  HB-*            heartbeat ingestion semantics
  IDEM-*          idempotency under duplicate delivery
  PROC-*          real process lifecycle (exit codes, signals, restart)
  CRASH-1..10     worker/supervisor crash fault injection (brief section 12)
  SUP-CRASH       supervisor SIGKILL: state intact, reconstructed from store
  OBS             observability: full story reconstructible from the store
  IDENT           identity model: worker/proc/lease/fencing distinction
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

WORKSPACE_ROOT = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))  # ~/workspace
sys.path.insert(0, WORKSPACE_ROOT)

from axos.store import (TransitionGate, TransitionRejected, LeaseError,
                        open_store)
from axos.exec.supervisor import Supervisor
from axos.exec.identity import new_proc_id, LeaseRef
from axos.tests.r5_helpers import r5_complete


def _event_count(gate, event_type, worker_id=None):
    q = ("SELECT COUNT(*) FROM ledger WHERE event_type=?"
         + (" AND json_extract(payload,'$.worker_id')=?" if worker_id else ""))
    params = (event_type, worker_id) if worker_id else (event_type,)
    return gate.store.conn.execute(q, params).fetchone()[0]


class ExecBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos1b-")
        self.db = os.path.join(self.tmp, "t.db")
        self.sup = Supervisor(self.db, actor="test-supervisor")
        self.gate = self.sup.gate
        self._sups = [self.sup]
        self._drivers = []

    def tearDown(self):
        for d in self._drivers:
            try:
                if d.poll() is None:
                    d.kill()
            except Exception:
                pass
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

    def new_supervisor(self, actor="test-supervisor-2"):
        s = Supervisor(self.db, actor=actor)
        self._sups.append(s)
        return s

    def make_job(self, jid):
        try:
            self.gate.create_task("t", {"objective": "x"}, {"usd": 1},
                                  "scheduler")
        except Exception:
            pass
        self.gate.create_job(jid, "t", "s", "scheduler")
        return jid

    def wait_for(self, pred, timeout=20.0, interval=0.1):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("wait_for timed out")

    def wait_reaped(self, sup, wid, timeout=20.0):
        """Pump observe() until the supervisor has reaped the worker."""
        def _poll():
            sup.observe()
            return wid in sup._reaped
        self.wait_for(_poll, timeout)
        return sup._reaped[wid]

    def start_driver(self, scenario, ready_name="ready"):
        ready = os.path.join(self.tmp, ready_name)
        ready2 = os.path.join(self.tmp, "ready2") \
            if scenario == "restart_midway" else None
        env = os.environ.copy()
        env["PYTHONPATH"] = WORKSPACE_ROOT + os.pathsep + env.get(
            "PYTHONPATH", "")
        cmd = [sys.executable,
               os.path.join(WORKSPACE_ROOT, "axos", "tests", "_supdrv.py"),
               "--db", self.db, "--scenario", scenario, "--ready", ready]
        if ready2:
            cmd += ["--ready2", ready2]
        proc = subprocess.Popen(cmd, env=env, start_new_session=True,
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
        self._drivers.append(proc)
        self.wait_for(lambda: os.path.exists(ready), timeout=20.0)
        return proc, ready, ready2


# ============================================================ fencing: gate
class FencingGateTest(ExecBase):
    def test_FENCE_A_normal_execution(self):
        """claim -> execute -> commit: SUCCESS, single atomic commit."""
        j = self.make_job("jA")
        self.assertTrue(self.gate.claim_job(j, "wA", 60.0, "scheduler"))
        tok = self.gate.get_job(j)["fencing_token"]
        self.gate.transition_job(j, "RUNNING", "worker:wA")
        row = r5_complete(self.gate, job_id=j, worker_id="wA",
                          fencing_token=tok, task_id="t", outcome="SUCCESS",
                          evidence={"exit_code": 0}, actor="worker:wA")
        self.assertEqual(row["status"], "COMPLETE")
        self.assertEqual(row["commit_outcome"], "SUCCESS")
        self.assertEqual(_event_count(self.gate, "job.committed"), 1)

    def test_FENCE_B_lease_expiry_rejects_commit(self):
        """Claim, let the lease expire, then commit: REJECTED, job untouched."""
        j = self.make_job("jB")
        self.assertTrue(self.gate.claim_job(j, "wB", 1.0, "scheduler"))
        tok = self.gate.get_job(j)["fencing_token"]
        # The worker started executing before the lease lapsed (R5: staging
        # requires RUNNING, as in the real worker).
        self.gate.transition_job(j, "RUNNING", "worker:wB")
        before = dict(self.gate.get_job(j))
        time.sleep(1.3)  # real expiry by store time
        with self.assertRaises(LeaseError):
            r5_complete(self.gate, job_id=j, worker_id="wB",
                        fencing_token=tok, task_id="t", outcome="SUCCESS",
                        evidence={}, actor="worker:wB")
        after = self.gate.get_job(j)
        self.assertEqual(after["status"], "RUNNING")  # not terminal
        self.assertIsNone(after["commit_outcome"])
        self.assertEqual(after["progress_done"], before["progress_done"])
        self.assertEqual(_event_count(self.gate, "job.committed"), 0)

    def test_FENCE_C_replacement_worker(self):
        """A claims (token 1), loses authority; B claims (token 2).
        A's commit REJECTED; B's commit SUCCESS."""
        j = self.make_job("jC")
        self.assertTrue(self.gate.claim_job(j, "wA", 1.0, "scheduler"))
        tok_a = self.gate.get_job(j)["fencing_token"]
        time.sleep(1.3)
        # Operator reset (sanctioned edge; recovery policy owns this in 1C).
        self.gate.transition_job(j, "PENDING", "test")
        self.assertTrue(self.gate.claim_job(j, "wB", 60.0, "scheduler"))
        tok_b = self.gate.get_job(j)["fencing_token"]
        self.assertGreater(tok_b, tok_a)
        with self.assertRaises(LeaseError):
            r5_complete(self.gate, job_id=j, worker_id="wA",
                        fencing_token=tok_a, task_id="t", outcome="SUCCESS",
                        evidence={}, actor="worker:wA")
        self.gate.transition_job(j, "RUNNING", "worker:wB")
        row = r5_complete(self.gate, job_id=j, worker_id="wB",
                          fencing_token=tok_b, task_id="t", outcome="SUCCESS",
                          evidence={"by": "wB"}, actor="worker:wB")
        self.assertEqual(row["status"], "COMPLETE")
        evs = self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='job.committed'"
        ).fetchall()
        self.assertEqual(len(evs), 1)
        self.assertEqual(json.loads(evs[0]["payload"])["fencing_token"],
                         tok_b)

    def test_FENCE_D_stale_token_after_takeover(self):
        """A dies; B takes over with a newer token; A's delayed commit with
        the old token is REJECTED."""
        j = self.make_job("jD")
        self.assertTrue(self.gate.claim_job(j, "wA", 1.0, "scheduler"))
        tok_a = self.gate.get_job(j)["fencing_token"]
        time.sleep(1.3)
        self.gate.transition_job(j, "PENDING", "test")
        self.assertTrue(self.gate.claim_job(j, "wB", 60.0, "scheduler"))
        tok_b = self.gate.get_job(j)["fencing_token"]
        # The delayed result from A's dead process instance arrives now.
        with self.assertRaises(LeaseError):
            r5_complete(self.gate, job_id=j, worker_id="wA",
                        fencing_token=tok_a, task_id="t", outcome="SUCCESS",
                        evidence={"late": True}, actor="worker:wA")
        self.gate.transition_job(j, "RUNNING", "worker:wB")
        row = r5_complete(self.gate, job_id=j, worker_id="wB",
                          fencing_token=tok_b, task_id="t", outcome="SUCCESS",
                          evidence={}, actor="worker:wB")
        self.assertEqual(row["commit_outcome"], "SUCCESS")

    def test_FENCE_SIX_commit_validates_everything(self):
        j = self.make_job("jS")
        self.assertTrue(self.gate.claim_job(j, "wS", 60.0, "scheduler"))
        tok = self.gate.get_job(j)["fencing_token"]
        # 1. job identity: unknown job
        with self.assertRaises(TransitionRejected):
            r5_complete(self.gate, job_id="nope", worker_id="wS",
                        fencing_token=tok, task_id="t", outcome="SUCCESS",
                        evidence={}, actor="worker:wS")
        # The worker started executing (R5 stages artifacts from RUNNING).
        self.gate.transition_job(j, "RUNNING", "worker:wS")
        # 2. worker identity: wrong owner
        with self.assertRaises(LeaseError):
            r5_complete(self.gate, job_id=j, worker_id="intruder",
                        fencing_token=tok, task_id="t", outcome="SUCCESS",
                        evidence={}, actor="worker:intruder")
        # 3. fencing token: stale
        with self.assertRaises(LeaseError):
            r5_complete(self.gate, job_id=j, worker_id="wS",
                        fencing_token=tok + 99, task_id="t",
                        outcome="SUCCESS", evidence={},
                        actor="worker:wS")
        # 4. malformed token
        with self.assertRaises(LeaseError):
            r5_complete(self.gate, job_id=j, worker_id="wS",
                        fencing_token="abc", task_id="t", outcome="SUCCESS",
                        evidence={}, actor="worker:wS")
        # 5. outcome must be known
        with self.assertRaises(TransitionRejected):
            r5_complete(self.gate, job_id=j, worker_id="wS",
                        fencing_token=tok, task_id="t", outcome="MAYBE",
                        evidence={}, actor="worker:wS")
        # 6. state: PENDING job cannot commit even for its (former) owner.
        j2 = self.make_job("jS2")
        self.gate.claim_job(j2, "wS", 60.0, "scheduler")
        tok2 = self.gate.get_job(j2)["fencing_token"]
        self.gate.transition_job(j2, "PENDING", "test")
        with self.assertRaises(TransitionRejected):
            r5_complete(self.gate, job_id=j2, worker_id="wS",
                        fencing_token=tok2, task_id="t", outcome="SUCCESS",
                        evidence={}, actor="worker:wS")
        # lease validity is covered by FENCE-B; the valid commit still works.
        row = r5_complete(self.gate, job_id=j, worker_id="wS",
                          fencing_token=tok, task_id="t", outcome="FAILURE",
                          evidence={"reason": "t"}, actor="worker:wS")
        self.assertEqual(row["status"], "FAILED")

    def test_FENCE_RACE_concurrent_claim_single_winner(self):
        """N threads race to claim one PENDING job: exactly one wins, the
        fencing token advances exactly once."""
        import threading
        j = self.make_job("jRace")
        winners = []
        lock = threading.Lock()

        def claim(i):
            st = open_store(self.db)
            try:
                g = TransitionGate(st)
                if g.claim_job(j, f"wR{i}", 60.0, f"racer-{i}"):
                    with lock:
                        winners.append(f"wR{i}")
            finally:
                st.close()

        threads = [threading.Thread(target=claim, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(winners), 1, f"winners={winners}")
        job = self.gate.get_job(j)
        self.assertEqual(job["owner_worker_id"], winners[0])
        self.assertEqual(job["fencing_token"], 1)
        # The losers' commits are all rejected (never the owner, wrong token).
        for i in range(8):
            if f"wR{i}" != winners[0]:
                with self.assertRaises(LeaseError):
                    r5_complete(self.gate, job_id=j, worker_id=f"wR{i}",
                                fencing_token=1, task_id="t",
                                outcome="SUCCESS", evidence={},
                                actor="worker")

    def test_FENCE_TOCTOU_stale_can_never_win_the_race(self):
        """Hammer the R5 artifact commit protocol from threads with a stale
        and a current token: the stale token can never commit, the current
        token commits exactly once, and no contradictory state is ever
        observable.

        Each thread holds its own Store (own connection), the way real
        worker processes do — BEGIN IMMEDIATE serializes the writers."""
        from axos.store import open_store
        for trial in range(10):
            j = self.make_job(f"jT{trial}")
            self.gate.claim_job(j, "wT", 60.0, "scheduler")
            self.gate.transition_job(j, "RUNNING", "worker:wT")
            tok = self.gate.get_job(j)["fencing_token"]
            results = {}

            def stale():
                g = TransitionGate(open_store(self.db))
                try:
                    r5_complete(g, job_id=j, worker_id="wT",
                                fencing_token=tok + 1000, task_id="t",
                                outcome="SUCCESS", evidence={},
                                actor="worker:wT")
                    results["stale"] = "COMMITTED?!"
                except LeaseError:
                    results["stale"] = "rejected"
                finally:
                    g.store.close()

            def legit():
                g = TransitionGate(open_store(self.db))
                try:
                    r5_complete(g, job_id=j, worker_id="wT",
                                fencing_token=tok, task_id="t",
                                outcome="SUCCESS", evidence={},
                                actor="worker:wT")
                    results["legit"] = "committed"
                except (LeaseError, TransitionRejected) as e:
                    results["legit"] = f"rejected({e})"
                finally:
                    g.store.close()

            t1, t2 = threading.Thread(target=stale), threading.Thread(
                target=legit)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            self.assertEqual(results["stale"], "rejected",
                             f"trial {trial}: stale token committed!")
            row = self.gate.get_job(j)
            self.assertEqual(row["status"], "COMPLETE")
            self.assertEqual(row["commit_outcome"], "SUCCESS")

    def test_FENCE_TOCTOU_duplicate_same_token_is_safe(self):
        """Two concurrent commits with the SAME valid token: one commits,
        the other is an idempotent no-op. Never an error, never a duplicate
        ledger event, never contradictory state."""
        from axos.store import open_store
        j = self.make_job("jDup")
        self.gate.claim_job(j, "wD", 60.0, "scheduler")
        self.gate.transition_job(j, "RUNNING", "worker:wD")
        tok = self.gate.get_job(j)["fencing_token"]
        errors = []

        def commit():
            g = TransitionGate(open_store(self.db))
            try:
                r5_complete(g, job_id=j, worker_id="wD", fencing_token=tok,
                            task_id="t", outcome="SUCCESS", evidence={},
                            actor="worker:wD")
            except Exception as e:  # noqa: BLE001 - must not happen
                errors.append(e)
            finally:
                g.store.close()

        ts = [threading.Thread(target=commit) for _ in range(8)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(errors, [])
        self.assertEqual(_event_count(self.gate, "job.committed"), 1)
        row = self.gate.get_job(j)
        self.assertEqual((row["status"], row["commit_outcome"]),
                         ("COMPLETE", "SUCCESS"))


# ================================================================ heartbeats
class HeartbeatTest(ExecBase):
    def _setup_claimed(self, jid="jH", wid="wH", ttl=60.0):
        j = self.make_job(jid)
        self.gate.create_worker(wid, "scheduler")
        self.assertTrue(self.gate.claim_job(j, wid, ttl, "scheduler"))
        tok = self.gate.get_job(j)["fencing_token"]
        return j, wid, tok

    def test_HB_accepted_with_store_time(self):
        """Heartbeat is durable evidence with store-time ts; the worker's
        timestamp is informational and never extends the lease."""
        j, wid, tok = self._setup_claimed()
        before_expiry = self.gate.get_job(j)["lease_expires_at"]
        hb = self.gate.ingest_heartbeat(wid, "proc-1", j, tok, 0, "RUNNING",
                                        "synth", "worker:wH",
                                        worker_reported_ts=9_999_999_999.0)
        self.assertEqual(hb["hb_seq"], 0)
        self.assertEqual(hb["worker_reported_ts"], 9_999_999_999.0)
        # ts is store time, not the worker's fantasy clock.
        self.assertLess(abs(hb["ts"] - self.gate.store.current_time()), 5.0)
        self.assertNotEqual(hb["ts"], 9_999_999_999.0)
        # Authority untouched: lease expiry unchanged, no ledger spam.
        self.assertEqual(self.gate.get_job(j)["lease_expires_at"],
                         before_expiry)
        self.assertEqual(_event_count(self.gate, "job.committed"), 0)

    def test_HB_duplicate_seq_rejected_without_mutation(self):
        j, wid, tok = self._setup_claimed()
        self.gate.ingest_heartbeat(wid, "proc-1", j, tok, 0, "RUNNING",
                                   "synth", "worker:wH")
        n0 = len(self.gate.heartbeats_for(wid))
        with self.assertRaises(TransitionRejected):
            self.gate.ingest_heartbeat(wid, "proc-1", j, tok, 0, "RUNNING",
                                       "synth", "worker:wH")
        with self.assertRaises(TransitionRejected):
            self.gate.ingest_heartbeat(wid, "proc-1", j, tok, -1, "RUNNING",
                                       "synth", "worker:wH")
        self.assertEqual(len(self.gate.heartbeats_for(wid)), n0)
        # ...but the next sequence is still accepted (no poison).
        self.gate.ingest_heartbeat(wid, "proc-1", j, tok, 1, "RUNNING",
                                   "synth", "worker:wH")
        self.assertEqual(len(self.gate.heartbeats_for(wid)), n0 + 1)

    def test_HB_stale_token_rejected(self):
        """After a newer fencing token exists, heartbeats with the old token
        are rejected — a live process is not an authorized process."""
        j, wid, tok_a = self._setup_claimed()
        time.sleep(0.05)
        # New epoch: the owner releases, the job resets, wH2 claims.
        self.assertTrue(self.gate.release_lease(j, wid, tok_a, "worker:wH"))
        self.gate.transition_job(j, "PENDING", "test")
        self.gate.create_worker("wH2", "scheduler")
        self.assertTrue(self.gate.claim_job(j, "wH2", 60.0, "scheduler"))
        tok_b = self.gate.get_job(j)["fencing_token"]
        self.assertGreater(tok_b, tok_a)
        with self.assertRaises(LeaseError):
            self.gate.ingest_heartbeat(wid, "proc-1", j, tok_a, 0,
                                       "RUNNING", "synth", "worker:wH")
        # And a heartbeat from a non-owner is rejected too.
        with self.assertRaises(LeaseError):
            self.gate.ingest_heartbeat(wid, "proc-1", j, tok_b, 0,
                                       "RUNNING", "synth", "worker:wH")

    def test_HB_unknown_worker_rejected(self):
        with self.assertRaises(TransitionRejected):
            self.gate.ingest_heartbeat("ghost", "proc-1", None, None, 0,
                                       "RUNNING", "synth", "worker:ghost")

    def test_HB_seq_scoped_per_proc_instance(self):
        """A restarted process instance (new proc_id) starts its sequence
        over — sequences identify a process instance, not a worker."""
        j, wid, tok = self._setup_claimed()
        self.gate.ingest_heartbeat(wid, "proc-1", j, tok, 0, "RUNNING",
                                   "s", "worker:wH")
        self.gate.ingest_heartbeat(wid, "proc-1", j, tok, 1, "RUNNING",
                                   "s", "worker:wH")
        self.gate.ingest_heartbeat(wid, "proc-2", j, tok, 0, "RUNNING",
                                   "s", "worker:wH")
        self.assertEqual(len(self.gate.heartbeats_for(wid, "proc-1")), 2)
        self.assertEqual(len(self.gate.heartbeats_for(wid, "proc-2")), 1)


# =============================================================== idempotency
class IdempotencyTest(ExecBase):
    def test_IDEM_duplicate_completion_same_outcome_is_noop(self):
        j = self.make_job("jI")
        self.gate.claim_job(j, "wI", 60.0, "scheduler")
        self.gate.transition_job(j, "RUNNING", "worker:wI")
        tok = self.gate.get_job(j)["fencing_token"]
        r1 = r5_complete(self.gate, job_id=j, worker_id="wI",
                         fencing_token=tok, task_id="t", outcome="SUCCESS",
                         evidence={"n": 1}, actor="worker:wI")
        r2 = r5_complete(self.gate, job_id=j, worker_id="wI",
                         fencing_token=tok, task_id="t", outcome="SUCCESS",
                         evidence={"n": 2}, actor="worker:wI")
        self.assertEqual(r1["status"], r2["status"], "COMPLETE")
        # Evidence is NOT overwritten by the duplicate.
        self.assertEqual(json.loads(r2["commit_evidence"]), {"n": 1})
        self.assertEqual(_event_count(self.gate, "job.committed"), 1)

    def test_IDEM_contradictory_duplicate_completion_rejected(self):
        j = self.make_job("jI2")
        self.gate.claim_job(j, "wI", 60.0, "scheduler")
        self.gate.transition_job(j, "RUNNING", "worker:wI")
        tok = self.gate.get_job(j)["fencing_token"]
        r5_complete(self.gate, job_id=j, worker_id="wI", fencing_token=tok,
                    task_id="t", outcome="SUCCESS", evidence={},
                    actor="worker:wI")
        with self.assertRaises(TransitionRejected):
            r5_complete(self.gate, job_id=j, worker_id="wI",
                        fencing_token=tok, task_id="t", outcome="FAILURE",
                        evidence={}, actor="worker:wI")
        row = self.gate.get_job(j)
        self.assertEqual((row["status"], row["commit_outcome"]),
                         ("COMPLETE", "SUCCESS"))

    def test_IDEM_repeated_stop_kill_reap(self):
        j = self.make_job("jI3")
        proc_id = self.sup.start_worker(
            "wI3", j, {"kind": "hang"}, ttl_s=60.0, hb_interval_s=0.2)
        self.assertTrue(self.wait_for(
            lambda: len(self.gate.heartbeats_for("wI3")) >= 1))
        r1 = self.sup.stop_worker("wI3", timeout=5.0)
        self.assertEqual(r1["proc_id"], proc_id)
        # Duplicate delivery: same cached record, no extra milestones.
        r2 = self.sup.stop_worker("wI3", timeout=5.0)
        r3 = self.sup.kill_worker("wI3")
        r4 = self.sup.reap("wI3")
        self.assertEqual(r1, r2)
        self.assertEqual(r1, r3)
        self.assertEqual(r1, r4)
        n = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type='worker.proc_reaped'"
            " AND json_extract(payload,'$.proc_id')=?",
            (proc_id,)).fetchone()[0]
        self.assertEqual(n, 1)
        # The job was never committed by a stopped process.
        self.assertNotIn(self.gate.get_job(j)["status"],
                         ("COMPLETE", "FAILED"))

    def test_IDEM_repeated_restart_mints_fresh_proc_ids(self):
        j = self.make_job("jI4")
        p1 = self.sup.start_worker("wI4", j, {"kind": "hang"}, ttl_s=60.0,
                                   hb_interval_s=0.2)
        self.assertTrue(self.wait_for(
            lambda: len(self.gate.heartbeats_for("wI4")) >= 1))
        p2 = self.sup.restart_worker("wI4", j, {"kind": "hang"}, ttl_s=60.0,
                                     hb_interval_s=0.2)
        p3 = self.sup.restart_worker("wI4", j, {"kind": "hang"}, ttl_s=60.0,
                                     hb_interval_s=0.2)
        self.assertEqual(len({p1, p2, p3}), 3)
        spawns = self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='worker.proc_spawned'"
            " AND json_extract(payload,'$.worker_id')='wI4'").fetchall()
        self.assertEqual(len(spawns), 3)
        self.assertEqual(len({json.loads(r["payload"])["proc_id"]
                              for r in spawns}), 3)


# ================================================== fencing: worker processes
class FencingWorkerTest(ExecBase):
    """Scenarios B/C/D with real subprocesses, real expiry, real kills."""

    def test_FENCE_B_worker_commit_after_expiry_rejected(self):
        j = self.make_job("jWB")
        self.sup.start_worker(
            "wWB", j,
            {"kind": "expire_then_commit", "sleep_s": 4.0},
            ttl_s=2.0, hb_interval_s=0.3, renew=False)
        rec = self.wait_reaped(self.sup, "wWB", timeout=20.0)
        # The worker attempted the commit and the gate refused it.
        self.assertEqual(rec["returncode"], 5)
        job = self.gate.get_job(j)
        self.assertEqual(job["status"], "RUNNING")  # active, not terminal
        self.assertIsNone(job["commit_outcome"])
        rej = self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='job.commit_rejected'"
        ).fetchall()
        self.assertEqual(len(rej), 1)
        self.assertIn("expired", json.loads(rej[0]["payload"])["reason"])
        # The lease shows as expired by store time — recovery input, not ours.
        self.assertTrue(any(e["job_id"] == j
                            for e in self.gate.expired_leases()))

    def test_FENCE_C_worker_replacement_end_to_end(self):
        """A (token 1) sleeps past expiry; B claims (token 2) and commits
        SUCCESS. A can never commit on the old token: either its commit
        is rejected by the gate (5, cooperative attempt) or the R2 sweep
        SIGTERMs its process group while it sleeps (143, enforced).
        The gate-rejection path is covered deterministically by the R1
        reclaim tests; here both outcomes prove A never commits."""
        j = self.make_job("jWC")
        self.sup.start_worker(
            "wA", j, {"kind": "expire_then_commit", "sleep_s": 5.0},
            ttl_s=3.0, hb_interval_s=0.3, renew=False)
        tok_a = self.wait_for(
            lambda: self.gate.get_job(j)["fencing_token"] or None,
            timeout=10.0)
        # Wait for A's lease to expire by store time, then reset + replace.
        self.wait_for(lambda: any(e["job_id"] == j
                                  for e in self.gate.expired_leases()),
                      timeout=10.0)
        self.gate.transition_job(j, "PENDING", "test")
        self.sup.start_worker(
            "wB", j, {"kind": "success_delayed", "duration_s": 3.0,
                      "steps": 3},
            ttl_s=60.0, hb_interval_s=0.3)
        rec_b = self.wait_reaped(self.sup, "wB", timeout=20.0)
        rec_a = self.wait_reaped(self.sup, "wA", timeout=20.0)
        # A never commits on the stale token: gate rejection (5) or R2
        # process-group enforcement (143) — both are fencing working.
        self.assertIn(rec_a["returncode"], (5, 143))
        self.assertEqual(rec_b["returncode"], 0)  # committed SUCCESS
        job = self.gate.get_job(j)
        self.assertEqual((job["status"], job["commit_outcome"]),
                         ("COMPLETE", "SUCCESS"))
        committed = self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='job.committed'"
        ).fetchall()
        self.assertEqual(len(committed), 1)
        payload = json.loads(committed[0]["payload"])
        self.assertEqual(payload["worker_id"], "wB")
        self.assertGreater(payload["fencing_token"], tok_a)

    def test_FENCE_D_sigkill_then_delayed_commit_rejected(self):
        """A is SIGKILLed mid-execution; B takes over with a newer token;
        A's old token can never commit afterwards."""
        j = self.make_job("jWD")
        self.sup.start_worker("wA", j, {"kind": "hang"}, ttl_s=3.0,
                              hb_interval_s=0.2)
        self.assertTrue(self.wait_for(
            lambda: len(self.gate.heartbeats_for("wA")) >= 2))
        tok_a = self.gate.get_job(j)["fencing_token"]
        proc_a = self.sup._procs["wA"].proc_id
        # Sudden death — not supervisor-initiated.
        self.sup._procs["wA"].popen.kill()
        rec = self.wait_reaped(self.sup, "wA", timeout=10.0)
        self.assertEqual(rec["signal"], 9)
        self.assertEqual(self.sup._read_worker("wA")["status"], "SUSPECT")
        self.wait_for(lambda: any(e["job_id"] == j
                                  for e in self.gate.expired_leases()),
                      timeout=10.0)
        self.gate.transition_job(j, "PENDING", "test")
        self.sup.start_worker("wB", j, {"kind": "success_immediate"},
                              ttl_s=60.0, hb_interval_s=0.2)
        rec_b = self.wait_reaped(self.sup, "wB", timeout=20.0)
        self.assertEqual(rec_b["returncode"], 0)
        self.assertNotEqual(rec_b["proc_id"], proc_a)
        # The delayed result from A's dead process instance arrives now.
        with self.assertRaises(LeaseError):
            r5_complete(self.gate, job_id=j, worker_id="wA",
                        fencing_token=tok_a, task_id="t", outcome="SUCCESS",
                        evidence={"late": True}, actor="worker:wA")
        job = self.gate.get_job(j)
        self.assertEqual((job["status"], job["commit_outcome"]),
                         ("COMPLETE", "SUCCESS"))

    def test_FENCE_alive_does_not_mean_authorized(self):
        """A stays alive and heartbeating; after B takes a newer token, A
        is fenced: its heartbeats are rejected and it cannot act on the
        job. Liveness != authority.

        R2: the supervisor's fence sweep enforces this by SIGTERMing A's
        process group (exit 143); if A's own heartbeat check notices the
        fencing first it self-fences (exit 4). Both are valid fencing
        outcomes — what must hold is that A loses authority and durable
        fence evidence is recorded.
        """
        j = self.make_job("jWL")
        self.sup.start_worker("wA", j, {"kind": "hang"}, ttl_s=2.0,
                              hb_interval_s=0.2, renew=False)
        self.assertTrue(self.wait_for(
            lambda: len(self.gate.heartbeats_for("wA")) >= 2))
        n_hb_before = len(self.gate.heartbeats_for("wA"))
        self.wait_for(lambda: any(e["job_id"] == j
                                  for e in self.gate.expired_leases()),
                      timeout=10.0)
        self.gate.transition_job(j, "PENDING", "test")
        self.sup.start_worker("wB", j, {"kind": "success_immediate"},
                              ttl_s=60.0, hb_interval_s=0.2)
        # Wait for A's process to die WITHOUT pumping observe(): the
        # worker.fence_enforced assertion below requires the fence sweep
        # -- not observe()'s reap path, which records no fence evidence --
        # to be the first to notice the death. Letting the background
        # sweep thread race observe() for that observation is timing,
        # not a property under test.
        def _a_dead_or_reaped():
            with self.sup._procs_lock:
                info = self.sup._procs.get("wA")
                if info is None:
                    # Reaped without observe(): only the sweep reaps on
                    # its own, and its already-dead branch records the
                    # fence evidence.
                    return True
                return info.popen.poll() is not None
        self.assertTrue(self.wait_for(_a_dead_or_reaped, timeout=20.0))
        # Deterministic: the sweep records the already-dead fence
        # evidence for A (a no-op if the background sweep already did;
        # the sweep lock serializes the two passes).
        self.sup.fence_sweep()
        rec_a = self.wait_reaped(self.sup, "wA", timeout=20.0)
        # A was alive when fenced: either it exited itself on the
        # LeaseError (4, cooperative) or the R2 sweep SIGTERMed its
        # process group (143, enforced). Both mean fenced.
        self.assertIn(rec_a["returncode"], (4, 143))
        # Durable R2 fence evidence was recorded for A.
        n_fenced = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type="
            "'worker.fence_enforced' AND json_extract(payload,"
            "'$.worker_id')='wA'").fetchone()[0]
        self.assertGreaterEqual(n_fenced, 1)
        # No further heartbeats from A were accepted after fencing.
        self.assertTrue(self.wait_for(
            lambda: self.gate.get_job(j)["status"] == "COMPLETE",
            timeout=20.0))
        n_hb_after = len(self.gate.heartbeats_for("wA"))
        time.sleep(1.0)
        self.assertEqual(len(self.gate.heartbeats_for("wA")), n_hb_after)
        self.assertGreaterEqual(n_hb_after, n_hb_before)

    def test_RENEW_lease_renewal_keeps_long_job_authorized(self):
        """A live worker renews its lease through the sanctioned
        gate.renew_lease path: a job running 3x longer than its TTL is
        never spuriously fenced, and renewal is durable ledger evidence."""
        j = self.make_job("jWR")
        t_start = time.time()
        self.sup.start_worker(
            "wR", j, {"kind": "heartbeat_loop", "duration_s": 6.0},
            ttl_s=2.0, hb_interval_s=0.3, renew=True)
        rec = self.wait_reaped(self.sup, "wR", timeout=25.0)
        # Ran the full 6s (3x the TTL) without fencing; exited on its own.
        self.assertEqual(rec["returncode"], 0)
        hbs = self.gate.heartbeats_for("wR")
        self.assertGreaterEqual(len(hbs), 12)
        # The lease was actually extended past the original TTL (store time).
        job = self.gate.get_job(j)
        self.assertGreater(job["lease_expires_at"], t_start + 4.0)
        # Renewal is durable evidence, not silent.
        renewed = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type='lease.renewed'"
            " AND json_extract(payload,'$.job_id')=?", (j,)).fetchone()[0]
        self.assertGreaterEqual(renewed, 1)
        # Heartbeat sequence stayed strictly increasing across renewals.
        seqs = sorted(h["hb_seq"] for h in hbs)
        self.assertEqual(seqs, list(range(len(seqs))))


# =========================================================== process lifecycle
class ProcessLifecycleTest(ExecBase):
    def test_PROC_normal_exit_zero(self):
        j = self.make_job("jP1")
        self.sup.start_worker("wP1", j, {"kind": "success_immediate"},
                              ttl_s=60.0)
        rec = self.wait_reaped(self.sup, "wP1")
        self.assertEqual(rec["returncode"], 0)
        self.assertIsNone(rec["signal"])
        self.assertEqual(self.gate.get_job(j)["status"], "COMPLETE")
        self.assertEqual(self.sup._read_worker("wP1")["status"], "IDLE")

    def test_PROC_crash_nonzero_exit_no_commit(self):
        """CRASH-1: worker dies before completion — job not terminal, worker
        SUSPECT, lease still live by store time."""
        j = self.make_job("jP2")
        self.sup.start_worker("wP2", j, {"kind": "crash"}, ttl_s=60.0)
        rec = self.wait_reaped(self.sup, "wP2")
        self.assertEqual(rec["returncode"], 3)
        job = self.gate.get_job(j)
        self.assertNotIn(job["status"], ("COMPLETE", "FAILED"))
        self.assertIsNone(job["commit_outcome"])
        self.assertEqual(self.sup._read_worker("wP2")["status"], "SUSPECT")

    def test_PROC_sigkill_self_after_progress(self):
        """CRASH-2: dies after progress but before commit — progress is
        durable evidence, the job is not complete."""
        j = self.make_job("jP3")
        self.sup.start_worker("wP3", j, {"kind": "sigkill_self"},
                              ttl_s=60.0)
        rec = self.wait_reaped(self.sup, "wP3")
        self.assertEqual(rec["signal"], 9)
        job = self.gate.get_job(j)
        self.assertGreater(job["progress_done"], 0)
        self.assertNotIn(job["status"], ("COMPLETE", "FAILED"))

    def test_PROC_sigterm_graceful_no_commit(self):
        """SIGTERM: the process stops promptly and commits NOTHING. Exit 143
        is process evidence, not a job outcome."""
        j = self.make_job("jP4")
        self.sup.start_worker("wP4", j, {"kind": "hang"}, ttl_s=60.0,
                              hb_interval_s=0.2)
        self.assertTrue(self.wait_for(
            lambda: len(self.gate.heartbeats_for("wP4")) >= 1))
        rec = self.sup.stop_worker("wP4", timeout=5.0)
        self.assertEqual(rec["returncode"], 143)
        job = self.gate.get_job(j)
        self.assertNotIn(job["status"], ("COMPLETE", "FAILED"))
        self.assertIsNone(job["commit_outcome"])
        self.assertEqual(self.sup._read_worker("wP4")["status"], "RETIRED")

    def test_PROC_supervisor_sigkill(self):
        """CRASH-3: SIGKILL during execution — worker DEAD, signal recorded."""
        j = self.make_job("jP5")
        self.sup.start_worker("wP5", j, {"kind": "hang"}, ttl_s=60.0,
                              hb_interval_s=0.2)
        self.assertTrue(self.wait_for(
            lambda: len(self.gate.heartbeats_for("wP5")) >= 1))
        rec = self.sup.kill_worker("wP5")
        self.assertEqual(rec["signal"], 9)
        self.assertEqual(self.sup._read_worker("wP5")["status"], "DEAD")
        self.assertNotIn(self.gate.get_job(j)["status"],
                         ("COMPLETE", "FAILED"))

    def test_PROC_restart_same_worker_new_proc(self):
        """CRASH-7: same worker_id restarts with a NEW proc_id and must
        re-claim — the old fencing token is dead."""
        j = self.make_job("jP6")
        p1 = self.sup.start_worker("wP6", j, {"kind": "hang"}, ttl_s=2.0,
                                   hb_interval_s=0.2)
        self.assertTrue(self.wait_for(
            lambda: len(self.gate.heartbeats_for("wP6")) >= 1))
        tok1 = self.gate.get_job(j)["fencing_token"]
        self.sup._procs["wP6"].popen.kill()  # sudden death
        self.wait_reaped(self.sup, "wP6")
        self.assertEqual(self.sup._read_worker("wP6")["status"], "SUSPECT")
        self.wait_for(lambda: any(e["job_id"] == j
                                  for e in self.gate.expired_leases()),
                      timeout=10.0)
        self.gate.transition_job(j, "PENDING", "test")
        p2 = self.sup.restart_worker("wP6", j,
                                     {"kind": "success_immediate"},
                                     ttl_s=60.0, hb_interval_s=0.2)
        self.assertNotEqual(p1, p2)
        rec = self.wait_reaped(self.sup, "wP6", timeout=20.0)
        self.assertEqual(rec["returncode"], 0)
        job = self.gate.get_job(j)
        self.assertEqual(job["status"], "COMPLETE")
        self.assertGreater(job["fencing_token"], tok1)
        # The old token is dead even though the worker_id is the same.
        with self.assertRaises(LeaseError):
            r5_complete(self.gate, job_id=j, worker_id="wP6",
                        fencing_token=tok1, task_id="t", outcome="SUCCESS",
                        evidence={}, actor="worker:wP6")

    def test_PROC_race_many_workers_one_job(self):
        """CRASH-10: four workers race for one job — exactly one commits."""
        j = self.make_job("jP7")
        for i in range(4):
            self.sup.start_worker(f"wR{i}", j,
                                  {"kind": "success_immediate"},
                                  ttl_s=60.0, hb_interval_s=0.2)
        recs = [self.wait_reaped(self.sup, f"wR{i}", timeout=20.0)
                for i in range(4)]
        codes = sorted(r["returncode"] for r in recs)
        # Exactly one winner commits (exit 0). Each loser either exits 3
        # on the lost claim, or -15 when the R2 fence sweep — whose
        # contract judges "another worker claimed after I spawned" as
        # revocation — SIGTERMs a still-starting loser. Both are
        # non-commit outcomes; the safety property is that exactly one
        # worker commits and the job completes once.
        self.assertEqual(codes.count(0), 1)
        for c in codes:
            self.assertIn(c, (0, 3, -15), f"unexpected worker exit {c}")
        self.assertEqual(_event_count(self.gate, "job.committed"), 1)
        self.assertEqual(self.gate.get_job(j)["status"], "COMPLETE")


# ============================================================ crash injection
class CrashTest(ExecBase):
    def test_CRASH_5_progress_after_lease_loss_rejected(self):
        """CRASH-5: worker loses the lease, then attempts a progress update:
        rejected, row byte-identical."""
        j = self.make_job("jC5")
        self.gate.claim_job(j, "wC5", 1.0, "scheduler")
        tok = self.gate.get_job(j)["fencing_token"]
        self.gate.update_job_progress(j, "wC5", tok, 10.0, 100.0,
                                      "worker:wC5")
        before = dict(self.gate.get_job(j))
        time.sleep(1.3)
        with self.assertRaises(LeaseError):
            self.gate.update_job_progress(j, "wC5", tok, 50.0, 100.0,
                                          "worker:wC5")
        after = self.gate.get_job(j)
        self.assertEqual(after["progress_done"], before["progress_done"])
        self.assertEqual(after["updated_at"], before["updated_at"])

    def test_CRASH_8_supervisor_dies_workers_continue(self):
        """CRASH-8: the supervisor is SIGKILLed; the worker keeps running.
        A fresh supervisor reconstructs everything from the store — worker
        identity, lease triple, heartbeat evidence — and fabricates nothing."""
        drv, _, _ = self.start_driver("sup_crash")
        n0 = len(self.gate.heartbeats_for("wdrv"))
        self.assertGreaterEqual(n0, 1)
        drv.kill()  # SIGKILL the supervisor
        self.assertIsNotNone(drv.wait(timeout=10))
        time.sleep(1.5)  # orphan keeps heartbeating into the store
        n1 = len(self.gate.heartbeats_for("wdrv"))
        self.assertGreater(n1, n0, "orphan worker kept heartbeating")

        sup2 = self.new_supervisor()
        r = sup2.reconstruct()
        self.assertFalse(r["supervisor_memory_used"])
        self.assertEqual(sup2._procs, {})
        w = next(x for x in r["workers"] if x["worker_id"] == "wdrv")
        self.assertEqual(w["os_reports_alive"], "yes")
        self.assertIsNotNone(w["latest_heartbeat"])
        self.assertIn(w["job"]["status"], ("CLAIMED", "RUNNING"))
        self.assertEqual(w["job"]["owner_worker_id"], "wdrv")
        self.assertEqual(w["job"]["fencing_token"], 1)
        # Leases still governed by store time, not by anyone's memory.
        self.assertEqual(self.gate.expired_leases(), [])
        # Cleanup the orphan via its recorded pid, then heartbeats stop.
        os.kill(w["pid"], signal.SIGKILL)
        time.sleep(1.0)
        n2 = len(self.gate.heartbeats_for("wdrv"))
        time.sleep(1.0)
        self.assertEqual(len(self.gate.heartbeats_for("wdrv")), n2)

    def test_CRASH_9a_supervisor_dies_during_kill(self):
        """CRASH-9: supervisor SIGKILLed while killing a worker. The fresh
        supervisor finds consistent state: no duplicate reaps, lease intact,
        nothing fabricated."""
        drv, _, _ = self.start_driver("kill_midway")
        time.sleep(0.5)  # kill lands during or just after kill_worker
        drv.kill()
        self.assertIsNotNone(drv.wait(timeout=10))
        sup2 = self.new_supervisor()
        r = sup2.reconstruct()
        w = next(x for x in r["workers"] if x["worker_id"] == "wdrv")
        # Either the kill completed (DEAD + reap milestone) or the worker is
        # still tracked as non-terminal — both are consistent, none is
        # fabricated: the row must match the milestones.
        reaps = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type='worker.proc_reaped'"
            " AND json_extract(payload,'$.worker_id')='wdrv'").fetchone()[0]
        self.assertLessEqual(reaps, 1)
        self.assertIn(w["worker_status"],
                      ("DEAD", "RETIRED", "SUSPECT", "RUNNING", "ASSIGNED"))
        job = self.gate.get_job("jdrv")
        self.assertEqual(job["owner_worker_id"], "wdrv")
        self.assertNotIn(job["status"], ("COMPLETE", "FAILED"))

    def test_CRASH_9b_supervisor_dies_during_restart(self):
        """CRASH-9: supervisor SIGKILLed mid-restart. State stays
        consistent: at most one reap per proc, lease triple intact, and a
        fresh supervisor can still operate the store."""
        drv, _, ready2 = self.start_driver("restart_midway")
        # Kill during the restart window (SIGTERM wait or just after spawn).
        time.sleep(0.6)
        drv.kill()
        self.assertIsNotNone(drv.wait(timeout=10))
        sup2 = self.new_supervisor()
        r = sup2.reconstruct()
        w = next(x for x in r["workers"] if x["worker_id"] == "wdrv")
        # No proc may be reaped twice; every spawn has <= 1 reap.
        n_spawn = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type='worker.proc_spawned'"
            " AND json_extract(payload,'$.worker_id')='wdrv'").fetchone()[0]
        n_reap = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type='worker.proc_reaped'"
            " AND json_extract(payload,'$.worker_id')='wdrv'").fetchone()[0]
        self.assertLessEqual(n_reap, n_spawn)
        self.assertIn(w["worker_status"],
                      ("DEAD", "RETIRED", "SUSPECT", "RUNNING", "ASSIGNED",
                       "IDLE"))
        job = self.gate.get_job("jdrv")
        self.assertEqual(job["owner_worker_id"], "wdrv")
        # The store is fully operable by the new supervisor.
        self.assertEqual(sup2.gate.verify_ledger_chain()[0], True)
        # Cleanup any orphaned worker procs from the killed driver.
        for pid in {w["pid"]}:
            if pid:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


class SupervisorCrashTest(ExecBase):
    """Section 11: the supervisor must not be an authoritative single point
    of truth. Kill it; authoritative state must remain intact."""

    def test_SUP_CRASH_state_survives_and_reconstructs(self):
        drv, _, _ = self.start_driver("sup_crash")
        self.assertTrue(self.wait_for(
            lambda: len(self.gate.heartbeats_for("wdrv")) >= 2,
            timeout=15.0))
        hb_before = self.gate.heartbeats_for("wdrv")[0]
        drv.kill()
        self.assertIsNotNone(drv.wait(timeout=10))

        sup2 = self.new_supervisor(actor="post-crash-supervisor")
        # Nothing in the new supervisor's memory: pure store reconstruction.
        self.assertEqual(sup2._procs, {})
        self.assertEqual(sup2._reaped, {})
        r = sup2.reconstruct()
        self.assertFalse(r["supervisor_memory_used"])
        w = next(x for x in r["workers"] if x["worker_id"] == "wdrv")
        self.assertEqual(w["latest_heartbeat"]["hb_seq"],
                         hb_before["hb_seq"])
        # Authoritative state intact: lease triple exactly as the driver left.
        job = sup2.gate.get_job("jdrv")
        self.assertIn(job["status"], ("CLAIMED", "RUNNING"))
        self.assertEqual(job["owner_worker_id"], "wdrv")
        self.assertEqual(job["fencing_token"], 1)
        self.assertGreater(job["lease_expires_at"],
                           sup2.store.current_time())
        # The ledger chain still verifies end to end.
        ok, detail = sup2.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)
        # The store remains fully operable: a new worker can be started on
        # a different job through the fresh supervisor.
        sup2.gate.create_job("jnew", "t", "s", "scheduler")
        sup2.start_worker("wnew", "jnew", {"kind": "success_immediate"},
                          ttl_s=60.0)
        rec = self.wait_reaped(sup2, "wnew", timeout=20.0)
        self.assertEqual(rec["returncode"], 0)
        self.assertEqual(sup2.gate.get_job("jnew")["status"], "COMPLETE")
        # Cleanup the orphan from the killed driver.
        try:
            os.kill(w["pid"], signal.SIGKILL)
        except ProcessLookupError:
            pass


# ================================================================ observability
class ObservabilityTest(ExecBase):
    def test_OBS_full_story_reconstructible_from_store(self):
        """Every required evidence item is durable in the store: worker
        instance, job, lease, fencing token, lifecycle transitions,
        heartbeat sequence, process termination, execution result,
        commit reason."""
        j = self.make_job("jO")
        proc_id = self.sup.start_worker(
            "wO", j, {"kind": "success_delayed", "duration_s": 1.0,
                      "steps": 2},
            ttl_s=60.0, hb_interval_s=0.2)
        rec = self.wait_reaped(self.sup, "wO", timeout=20.0)
        self.assertEqual(rec["returncode"], 0)
        conn = self.gate.store.conn

        # worker instance + lifecycle transitions (ledger worker.transition)
        wtrans = [json.loads(r["payload"]) for r in conn.execute(
            "SELECT payload FROM ledger WHERE event_type='worker.transition'"
            " AND json_extract(payload,'$.worker_id')='wO'"
            " ORDER BY seq").fetchall()]
        states = [t["to"] for t in wtrans]
        for s in ("IDLE", "ASSIGNED", "RUNNING"):
            self.assertIn(s, states)

        # process instance evidence: spawn + reap milestones
        spawned = conn.execute(
            "SELECT payload FROM ledger WHERE event_type='worker.proc_spawned'"
            " AND json_extract(payload,'$.proc_id')=?",
            (proc_id,)).fetchone()
        reaped = conn.execute(
            "SELECT payload FROM ledger WHERE event_type='worker.proc_reaped'"
            " AND json_extract(payload,'$.proc_id')=?",
            (proc_id,)).fetchone()
        self.assertIsNotNone(spawned)
        self.assertIsNotNone(reaped)
        self.assertEqual(json.loads(reaped["payload"])["returncode"], 0)

        # heartbeat sequence is dense and ordered per process instance
        hbs = self.gate.heartbeats_for("wO", proc_id, limit=1000)
        seqs = sorted(h["hb_seq"] for h in hbs)
        self.assertEqual(seqs, list(range(len(seqs))))
        self.assertTrue(all(h["job_id"] == j for h in hbs))
        tok = self.gate.get_job(j)["fencing_token"]
        self.assertTrue(all(h["fencing_token"] == tok for h in hbs))

        # execution result + commit reason on the job row and in the ledger
        job = self.gate.get_job(j)
        self.assertEqual(job["commit_outcome"], "SUCCESS")
        ev = json.loads(job["commit_evidence"])
        self.assertEqual(ev["proc_id"], proc_id)
        committed = conn.execute(
            "SELECT payload FROM ledger WHERE event_type='job.committed'"
        ).fetchall()
        self.assertEqual(len(committed), 1)
        cp = json.loads(committed[0]["payload"])
        self.assertEqual(cp["fencing_token"], tok)
        self.assertEqual(cp["outcome"], "SUCCESS")


class IdentityTest(ExecBase):
    def test_IDENT_five_identities_never_collapsed(self):
        p1, p2 = new_proc_id(), new_proc_id()
        self.assertNotEqual(p1, p2)
        lease = LeaseRef(job_id="j", worker_id="w", fencing_token=3)
        self.assertEqual(lease.lease_id, "j#3")
        # Same worker name, new process instance, new lease epoch: the old
        # authority is gone and the identities are all distinct.
        self.assertNotEqual(lease.lease_id,
                            LeaseRef("j", "w", 4).lease_id)


if __name__ == "__main__":
    unittest.main()
