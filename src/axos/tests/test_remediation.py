"""Phase 1A remediation regression tests — F1, F2, F3.

F1: the application-level authority boundary. Non-gate components receive a
    ReadOnlyStore; Store.conn is read-only; only the gate's write_txn can
    mutate authoritative state.
F2: lease TTL validation (positive durations only).
F3: no progress mutations once a job leaves the active execution window.

Uses real SQLite files, real threads where concurrency matters, and real
subprocess/SIGKILL for the crash-path re-verification of F1.
"""
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace

from axos.store import (
    open_store, open_readonly_store, migrate, TransitionGate,
    TransitionRejected, LeaseError, ReadOnlyStore,
)


class RemediationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="remediation-")
        self.db = os.path.join(self.tmp, "t.db")
        self.store = open_store(self.db)
        migrate(self.store)
        self.g = TransitionGate(self.store)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------ F1: boundary
    def test_f1_store_conn_is_read_only(self):
        for sql in (
            "INSERT INTO tasks(task_id,status,objective,budgets,created_at,updated_at)"
            " VALUES('x','PROPOSED','{}','{}',1,1)",
            "UPDATE tasks SET status='EXECUTING'",
            "DELETE FROM tasks",
            "CREATE TABLE evil(x)",
            "INSERT INTO ledger(event_type,payload,actor,ts,prev_hash,hash)"
            " VALUES('e','{}','a',1,'h','h')",
        ):
            with self.assertRaises(sqlite3.OperationalError, msg=sql[:40]):
                self.store.conn.execute(sql)
        # reads still work through the same handle
        row = self.store.conn.execute("SELECT COUNT(*) FROM tasks").fetchone()
        self.assertEqual(row[0], 0)
        self.assertEqual(
            self.store.conn.execute("PRAGMA query_only").fetchone()[0], 1)

    def test_f1_readonly_store_rejects_every_mutation_class(self):
        self.g.create_task("t1", {}, {}, "scheduler")
        ro = self.store.read_only()
        self.assertIsInstance(ro, ReadOnlyStore)
        attacks = [
            # INSERT authoritative entities
            "INSERT INTO tasks(task_id,status,objective,budgets,created_at,updated_at)"
            " VALUES('evil','EXECUTING','{}','{}',1,1)",
            "INSERT INTO approvals(approval_id,task_id,reason,requested_action,"
            "status,created_at,updated_at) VALUES('fa','t1','r','resume','APPROVED',1,1)",
            # UPDATE authoritative state / leases / approvals / evidence /
            # checkpoints / validators
            "UPDATE tasks SET status='FINALIZED' WHERE task_id='t1'",
            "UPDATE jobs SET owner_worker_id='mallory'",
            "UPDATE approvals SET status='APPROVED'",
            "UPDATE recovery_attempts SET decision='success',progress_delta=9",
            "UPDATE checkpoints SET verification_status='VERIFIED'",
            "UPDATE validators SET status='ACTIVE'",
            # DELETE authoritative state
            "DELETE FROM tasks WHERE task_id='t1'",
            # arbitrary ledger append
            "INSERT INTO ledger(event_type,payload,actor,ts,prev_hash,hash)"
            " VALUES('forged','{}','mallory',1,'x','y')",
            # DDL
            "CREATE TABLE pwned(x)",
        ]
        for sql in attacks:
            with self.assertRaises(sqlite3.OperationalError, msg=sql[:50]):
                ro.execute(sql)
        # reads work
        self.assertEqual(
            ro.execute("SELECT task_id FROM tasks WHERE task_id='t1'"
                       ).fetchone()[0], "t1")
        # the read-only handle cannot be escalated: no write capability exists
        self.assertFalse(hasattr(ro, "write_txn"))
        self.assertFalse(hasattr(ro, "conn"))
        ro.close()

    def test_f1_open_readonly_store_is_independent_and_unescalatable(self):
        self.g.create_task("t1", {}, {}, "scheduler")
        ro = open_readonly_store(self.db)
        with self.assertRaises(sqlite3.OperationalError):
            ro.execute("UPDATE tasks SET status='X' WHERE task_id='t1'")
        self.assertEqual(ro.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 1)
        ro.close()

    def test_f1_gate_still_mutates_through_write_txn(self):
        # The gate's sanctioned path is unaffected by the boundary.
        self.g.create_task("t1", {"o": 1}, {"usd": 5}, "scheduler")
        self.g.create_job("j1", "t1", "s", "scheduler")
        self.assertTrue(self.g.claim_job("j1", "w1", 60.0, "scheduler"))
        ok, _ = self.g.verify_ledger_chain()
        self.assertTrue(ok)

    def test_f1_concurrent_readers_while_gate_writes(self):
        # Read-only handles stay usable and consistent under write load.
        stop = False
        seen = []

        def reader():
            ro = open_readonly_store(self.db)
            try:
                while not stop:
                    seen.append(ro.execute(
                        "SELECT COUNT(*) FROM tasks").fetchone()[0])
            finally:
                ro.close()

        ts = [threading.Thread(target=reader) for _ in range(3)]
        for t in ts:
            t.start()
        try:
            for i in range(50):
                self.g.create_task(f"w-{i}", {}, {}, "scheduler")
        finally:
            stop = True
            for t in ts:
                t.join()
        self.assertTrue(seen)
        # every observed count was a real prefix of the final state
        self.assertTrue(all(0 <= n <= 50 for n in seen))

    def test_f1_i17_forged_approval_impossible_via_public_api(self):
        # Mandatory re-test of the audit's critical I-17 attack: the forged
        # APPROVED approval must now be uncreatable through every publicly
        # reachable application API.
        self.g.create_task("t1", {}, {}, "scheduler")
        for s in ("AUTHORIZED", "PLANNED", "EXECUTING"):
            self.g.transition_task("t1", s, "scheduler")
        self.g.transition_task("t1", "PAUSED_FOR_HUMAN", "scheduler",
                               pause_reason="audit")

        # Attack 1: forge via Store.conn (now read-only)
        with self.assertRaises(sqlite3.OperationalError):
            self.store.conn.execute(
                "INSERT INTO approvals(approval_id,task_id,reason,"
                "requested_action,status,created_at,updated_at) VALUES"
                "('forged','t1','r','resume','APPROVED',1,1)")
        # Attack 2: forge via a read-only handle
        ro = self.store.read_only()
        with self.assertRaises(sqlite3.OperationalError):
            ro.execute(
                "UPDATE approvals SET status='APPROVED'")
        ro.close()
        # Attack 3: flip via read-only open
        ro2 = open_readonly_store(self.db)
        with self.assertRaises(sqlite3.OperationalError):
            ro2.execute(
                "INSERT INTO approvals(approval_id,task_id,reason,"
                "requested_action,status,created_at,updated_at) VALUES"
                "('forged2','t1','r','resume','APPROVED',1,1)")
        ro2.close()

        # The human gate is still sticky, and only the legitimate pathway
        # releases it — including across a close/reopen.
        self.assertEqual(self.g.get_task("t1")["status"], "PAUSED_FOR_HUMAN")
        with self.assertRaises(TransitionRejected):
            self.g.transition_task("t1", "EXECUTING", "mallory")
        self.store.close()
        self.store = open_store(self.db)
        self.g = TransitionGate(self.store)
        self.assertEqual(self.g.get_task("t1")["status"], "PAUSED_FOR_HUMAN")
        ap = self.g.create_approval("ap-real", "t1", "r", "resume", "scheduler")
        self.g.decide_approval("ap-real", "APPROVED", "human", "scheduler")
        self.g.transition_task("t1", "EXECUTING", "mallory",
                               approval_ref="ap-real")
        self.assertEqual(self.g.get_task("t1")["status"], "EXECUTING")

    def test_f1_crash_mid_gate_txn_still_atomic(self):
        # Real SIGKILL during a gate write transaction (via the sanctioned
        # write_txn capability): nothing partial survives.
        workspace = os.path.dirname(os.path.dirname(HERE))
        killer = os.path.join(self.tmp, "k.py")
        with open(killer, "w") as f:
            f.write(
                "import os, signal, sys\n"
                f"sys.path.insert(0, {workspace!r})\n"
                "from axos.store import open_store, migrate\n"
                f"st = open_store({self.db!r}); migrate(st)\n"
                "with st.write_txn() as (conn, now):\n"
                "    conn.execute(\"INSERT INTO tasks(task_id,status,objective,budgets,created_at,updated_at)\"\n"
                "                 \" VALUES('k-task','PROPOSED','{}','{}',?,?)\", (now, now))\n"
                "    os.kill(os.getpid(), signal.SIGKILL)\n")
        r = subprocess.run([sys.executable, killer], capture_output=True,
                           timeout=60)
        self.assertEqual(r.returncode, -signal.SIGKILL)
        st2 = open_store(self.db)
        try:
            self.assertIsNone(st2.conn.execute(
                "SELECT task_id FROM tasks WHERE task_id='k-task'").fetchone())
            self.assertTrue(st2.integrity_check()[0])
        finally:
            st2.close()

    # ------------------------------------------------------- F2: TTL validation
    def test_f2_negative_ttl_rejected(self):
        self.g.create_task("t1", {}, {}, "scheduler")
        self.g.create_job("j1", "t1", "s", "scheduler")
        with self.assertRaises(TransitionRejected):
            self.g.claim_job("j1", "w1", -30.0, "scheduler")
        j = self.g.get_job("j1")
        self.assertEqual(j["status"], "PENDING")
        self.assertIsNone(j["owner_worker_id"])
        self.assertIsNone(j["lease_expires_at"])

    def test_f2_zero_ttl_rejected(self):
        self.g.create_task("t1", {}, {}, "scheduler")
        self.g.create_job("j1", "t1", "s", "scheduler")
        with self.assertRaises(TransitionRejected):
            self.g.claim_job("j1", "w1", 0.0, "scheduler")
        self.assertEqual(self.g.get_job("j1")["status"], "PENDING")

    def test_f2_nan_ttl_rejected(self):
        self.g.create_task("t1", {}, {}, "scheduler")
        self.g.create_job("j1", "t1", "s", "scheduler")
        with self.assertRaises(TransitionRejected):
            self.g.claim_job("j1", "w1", float("nan"), "scheduler")
        self.assertEqual(self.g.get_job("j1")["status"], "PENDING")

    def test_f2_malformed_ttl_rejected_before_mutation(self):
        self.g.create_task("t1", {}, {}, "scheduler")
        self.g.create_job("j1", "t1", "s", "scheduler")
        with self.assertRaises(TransitionRejected):
            self.g.claim_job("j1", "w1", "forever", "scheduler")
        with self.assertRaises(TransitionRejected):
            self.g.claim_job("j1", "w1", None, "scheduler")
        j = self.g.get_job("j1")
        self.assertEqual(j["status"], "PENDING")
        self.assertIsNone(j["lease_expires_at"])

    def test_f2_positive_ttl_works(self):
        self.g.create_task("t1", {}, {}, "scheduler")
        self.g.create_job("j1", "t1", "s", "scheduler")
        self.assertTrue(self.g.claim_job("j1", "w1", 120.0, "scheduler"))
        j = self.g.get_job("j1")
        self.assertAlmostEqual(
            j["lease_expires_at"] - j["lease_acquired_at"], 120.0, places=3)

    def test_f2_huge_ttl_accepted_no_phase0_bound(self):
        # Phase 0 sets no upper bound; float arithmetic carries it exactly.
        self.g.create_task("t1", {}, {}, "scheduler")
        self.g.create_job("j1", "t1", "s", "scheduler")
        self.assertTrue(self.g.claim_job("j1", "w1", 1e15, "scheduler"))
        j = self.g.get_job("j1")
        self.assertAlmostEqual(
            j["lease_expires_at"] - j["lease_acquired_at"], 1e15, places=0)

    def test_f2_renew_validates_ttl_and_leaves_lease_untouched(self):
        self.g.create_task("t1", {}, {}, "scheduler")
        self.g.create_job("j1", "t1", "s", "scheduler")
        self.assertTrue(self.g.claim_job("j1", "w1", 600.0, "scheduler"))
        tok = self.g.get_job("j1")["fencing_token"]
        before = dict(self.g.get_job("j1"))
        with self.assertRaises(TransitionRejected):
            self.g.renew_lease("j1", "w1", tok, -5.0, "scheduler")
        after = dict(self.g.get_job("j1"))
        self.assertEqual(before, after)
        self.assertTrue(self.g.renew_lease("j1", "w1", tok, 600.0, "scheduler"))

    # ------------------------------------------- F3: terminal progress guard
    def _run_to_complete(self, jid="j1"):
        self.g.create_task("t1", {}, {}, "scheduler")
        self.g.create_job(jid, "t1", "s", "scheduler")
        self.assertTrue(self.g.claim_job(jid, "w1", 6000.0, "scheduler"))
        tok = self.g.get_job(jid)["fencing_token"]
        self.g.transition_job(jid, "RUNNING", "scheduler")
        return tok

    def test_f3_progress_before_completion_works(self):
        tok = self._run_to_complete()
        r = self.g.update_job_progress("j1", "w1", tok, 3, 10, "scheduler")
        self.assertEqual(r["progress_done"], 3)
        self.assertEqual(r["progress_total"], 10)

    def test_f3_progress_after_complete_rejected_state_identical(self):
        tok = self._run_to_complete()
        self.g.update_job_progress("j1", "w1", tok, 3, 10, "scheduler")
        # Phase 1C R5 (I-4): COMPLETE is reachable only through the
        # artifact-backed commit protocol.
        art = self.g.stage_artifact(job_id="j1", worker_id="w1",
                                    fencing_token=tok, task_id="t1",
                                    kind="result", data=b"f3",
                                    actor="worker:w1")
        self.g.begin_commit("j1", "w1", tok,
                            artifact_id=art["artifact_id"],
                            actor="worker:w1")
        self.g.verify_artifact(art["artifact_id"], actor="worker:w1",
                               worker_id="w1", fencing_token=tok)
        self.g.commit_artifact("j1", "w1", tok,
                               artifact_id=art["artifact_id"],
                               actor="worker:w1", evidence={})
        before = dict(self.g.get_job("j1"))
        with self.assertRaises(TransitionRejected):
            self.g.update_job_progress("j1", "w1", tok, 99, 100, "scheduler")
        after = dict(self.g.get_job("j1"))
        self.assertEqual(before, after)
        self.assertEqual(after["progress_done"], 3)

    def test_f3_progress_after_failed_rejected(self):
        tok = self._run_to_complete()
        self.g.transition_job("j1", "FAILED", "scheduler")
        with self.assertRaises(TransitionRejected):
            self.g.update_job_progress("j1", "w1", tok, 5, 10, "scheduler")

    def test_f3_fencing_still_enforced_on_active_job(self):
        tok = self._run_to_complete()
        with self.assertRaises(LeaseError):
            self.g.update_job_progress("j1", "w2", tok, 1, 10, "scheduler")
        with self.assertRaises(LeaseError):
            self.g.update_job_progress("j1", "w1", tok + 1, 1, 10, "scheduler")


if __name__ == "__main__":
    unittest.main()
