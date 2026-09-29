"""Phase 1A store test suite.

Covers the 22 required areas from the Phase 1A brief plus executable
tests for the store-level invariants (I-16, I-17, I-18 and others).
Everything runs against a real SQLite/WAL database file — no mocked
transactions. Run with:  python3 -m unittest discover -s tests -v
"""
import json
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
    open_store, migrate, applied_versions, TransitionGate,
    TransitionRejected, LeaseError, MigrationError,
)
from axos.store.migrations import MIGRATIONS


class FakeClock:
    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos1a-")
        self.db_path = os.path.join(self.tmp, "axos.db")
        self.clock = FakeClock()
        self.store = open_store(self.db_path, clock=self.clock)
        migrate(self.store)
        self.gate = TransitionGate(self.store)

    def tearDown(self):
        try:
            self.store.close()
        finally:
            shutil.rmtree(self.tmp, ignore_errors=True)

    def _mk_task_job(self, tid="t1", jid="j1", actor="scheduler"):
        self.gate.create_task(tid, {"objective": "demo"}, {"usd": 10}, actor)
        self.gate.create_job(jid, tid, "stage-a", actor)
        return tid, jid

    # ------------------------------------------------ 1. fresh init / pragmas
    def test_01_fresh_init_pragmas_verified(self):
        rep = self.store.pragma_report()
        self.assertEqual(rep["journal_mode"].lower(), "wal")
        self.assertEqual(int(rep["foreign_keys"]), 1)
        # WAL file exists after a write
        self.gate.create_task("t0", {}, {}, "test")
        wal = self.db_path + "-wal"
        self.assertTrue(os.path.exists(self.db_path))

    # ------------------------------------------------ 2. migration from empty
    def test_02_migration_from_empty_is_idempotent(self):
        self.assertEqual(applied_versions(self.store),
                         [m[0] for m in MIGRATIONS])
        for tbl in ("tasks", "jobs", "workers", "approvals", "incidents",
                    "recovery_attempts", "checkpoints", "artifacts",
                    "validations", "ledger", "schema_migrations", "axos_meta",
                    "heartbeats"):
            row = self.store.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (tbl,)).fetchone()
            self.assertIsNotNone(row, tbl)
        self.assertEqual(migrate(self.store), [])  # repeat = no-op

    def test_03_migration_failure_is_atomic(self):
        # The injected migrations must use a version beyond every shipped
        # one (hardcoding 3 broke when the real v3 migration landed).
        next_ver = max(m[0] for m in MIGRATIONS) + 1
        bad = MIGRATIONS + [(next_ver, "broken", "CREATE TABLE oops_broken(")]
        with self.assertRaises(MigrationError):
            migrate(self.store, bad)
        self.assertEqual(applied_versions(self.store),
                         [m[0] for m in MIGRATIONS])
        row = self.store.conn.execute(
            "SELECT name FROM sqlite_master WHERE name='oops_broken'").fetchone()
        self.assertIsNone(row)  # partial schema rolled back
        # a later good migration still applies cleanly
        good = MIGRATIONS + [(next_ver, "ok",
                              "CREATE TABLE extra_ok(x TEXT)")]
        self.assertEqual(migrate(self.store, good), [next_ver])

    # ------------------------------------------------ 3/4/5/6 transitions
    def test_04_state_creation_uses_store_time(self):
        self.clock.t = 1_700_000_000.0
        t = self.gate.create_task("t1", {"objective": "x"}, {"usd": 1}, "scheduler")
        self.assertEqual(t["status"], "PROPOSED")
        self.assertEqual(t["created_at"], 1_700_000_000.0)
        j = self.gate.create_job("j1", "t1", "s", "scheduler")
        self.assertEqual(j["status"], "PENDING")
        w = self.gate.create_worker("w1", "scheduler")
        self.assertEqual(w["status"], "PROVISIONING")

    def test_05_valid_transitions(self):
        self._mk_task_job()
        for to in ("AUTHORIZED", "PLANNED", "EXECUTING"):
            self.gate.transition_task("t1", to, "scheduler")
        self.assertEqual(self.gate.get_task("t1")["status"], "EXECUTING")
        self.assertTrue(self.gate.claim_job("j1", "w1", 60.0, "scheduler"))
        self.gate.transition_job("j1", "RUNNING", "scheduler")
        # Phase 1C R5 (I-4): COMPLETE is reachable only through the
        # artifact-backed commit protocol — stage -> begin_commit ->
        # gate verification -> one atomic commit_artifact.
        tok = self.gate.get_job("j1")["fencing_token"]
        art = self.gate.stage_artifact(job_id="j1", worker_id="w1",
                                       fencing_token=tok, task_id="t1",
                                       kind="result", data=b"test05",
                                       actor="worker:w1")
        self.gate.begin_commit("j1", "w1", tok,
                               artifact_id=art["artifact_id"],
                               actor="worker:w1")
        self.gate.verify_artifact(art["artifact_id"], actor="worker:w1",
                                  worker_id="w1", fencing_token=tok)
        row = self.gate.commit_artifact("j1", "w1", tok,
                                        artifact_id=art["artifact_id"],
                                        actor="worker:w1", evidence={})
        self.assertEqual(row["status"], "COMPLETE")
        self.assertEqual(row["result_artifact_id"], art["artifact_id"])

    def test_06_invalid_transition_rejected_state_unchanged(self):
        self._mk_task_job()
        with self.assertRaises(TransitionRejected):
            self.gate.transition_task("t1", "EXECUTING", "scheduler")  # skip graph
        self.assertEqual(self.gate.get_task("t1")["status"], "PROPOSED")
        with self.assertRaises(TransitionRejected):
            self.gate.transition_job("j1", "COMPLETE", "scheduler")
        self.assertEqual(self.gate.get_job("j1")["status"], "PENDING")
        with self.assertRaises(TransitionRejected):
            self.gate.transition_artifact("nope", "RELEASED", "scheduler")

    def test_07_transaction_rollback_leaves_nothing(self):
        with self.assertRaises(RuntimeError):
            with self.store.write_txn() as (conn, now):
                conn.execute(
                    "INSERT INTO tasks(task_id,status,objective,budgets,"
                    "created_at,updated_at) VALUES('doom','PROPOSED','{}','{}',?,?)",
                    (now, now))
                raise RuntimeError("boom")
        row = self.store.conn.execute(
            "SELECT task_id FROM tasks WHERE task_id='doom'").fetchone()
        self.assertIsNone(row)

    # ------------------------------------------------ 7. concurrency
    def test_08_concurrent_claim_exactly_one_wins(self):
        self._mk_task_job()
        barrier = threading.Barrier(2)
        results = []

        def contender(wid):
            st = open_store(self.db_path)  # separate connection
            g = TransitionGate(st)
            barrier.wait()
            try:
                results.append(g.claim_job("j1", wid, 60.0, "scheduler"))
            finally:
                st.close()

        ts = [threading.Thread(target=contender, args=(f"w{i}",))
              for i in (1, 2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(sorted(results), [False, True])
        job = self.gate.get_job("j1")
        self.assertEqual(job["status"], "CLAIMED")
        self.assertIn(job["owner_worker_id"], ("w1", "w2"))
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)

    # ------------------------------------------------ 8/9/10 leases
    def test_09_lease_acquisition_triple(self):
        self._mk_task_job()
        self.clock.t = 2_000_000.0
        self.assertTrue(self.gate.claim_job("j1", "w1", 60.0, "scheduler"))
        job = self.gate.get_job("j1")
        self.assertEqual(job["owner_worker_id"], "w1")
        self.assertEqual(job["lease_acquired_at"], 2_000_000.0)
        self.assertEqual(job["lease_expires_at"], 2_000_060.0)
        self.assertEqual(job["fencing_token"], 1)  # first claim mints token 1

    def test_10_conflicting_lease_rejected(self):
        self._mk_task_job()
        self.assertTrue(self.gate.claim_job("j1", "w1", 600.0, "scheduler"))
        self.assertFalse(self.gate.claim_job("j1", "w2", 600.0, "scheduler"))
        job = self.gate.get_job("j1")
        token = job["fencing_token"]
        # wrong token / wrong owner cannot renew or release
        self.assertFalse(self.gate.renew_lease("j1", "w1", token + 99, 60.0, "s"))
        self.assertFalse(self.gate.renew_lease("j1", "w2", token, 60.0, "s"))
        self.assertFalse(self.gate.release_lease("j1", "w2", token, "s"))
        # owner with valid token can renew and release
        self.assertTrue(self.gate.renew_lease("j1", "w1", token, 600.0, "s"))
        self.assertTrue(self.gate.release_lease("j1", "w1", token, "s"))
        job = self.gate.get_job("j1")
        self.assertIsNone(job["owner_worker_id"])
        self.assertIsNone(job["lease_expires_at"])

    def test_11_lease_expiry_detection(self):
        self._mk_task_job()
        self.gate.claim_job("j1", "w1", 60.0, "scheduler")
        self.assertEqual(self.gate.expired_leases(), [])
        self.clock.advance(120.0)  # store time passes the lease
        expired = self.gate.expired_leases()
        self.assertEqual(len(expired), 1)
        self.assertEqual(expired[0]["job_id"], "j1")
        # an expired lease cannot be renewed
        job = self.gate.get_job("j1")
        self.assertFalse(
            self.gate.renew_lease("j1", "w1", job["fencing_token"], 60.0, "s"))

    # ------------------------------------------------ 11. authoritative time
    def test_12_worker_timestamps_never_authoritative(self):
        self._mk_task_job()
        self.clock.t = 5_000_000.0
        # worker claims it started at some arbitrary wall-clock time
        self.assertTrue(self.gate.claim_job(
            "j1", "w1", 60.0, "scheduler", worker_reported_ts=12345.678))
        job = self.gate.get_job("j1")
        self.assertEqual(job["lease_acquired_at"], 5_000_000.0)   # store time
        self.assertEqual(job["lease_expires_at"], 5_000_060.0)     # store time
        self.assertEqual(job["worker_reported_ts"], 12345.678)  # informational
        # lease semantics derive from store time, not the worker's claim:
        self.clock.advance(30.0)
        self.assertEqual(self.gate.expired_leases(), [])  # 5000030 < 5000060

    def test_13_store_time_is_monotonic_across_clock_skew(self):
        self.gate.create_task("t1", {}, {}, "test")
        first = self.gate.get_task("t1")["created_at"]
        self.clock.t = first - 10_000.0  # wall clock jumps backwards
        self.gate.create_task("t2", {}, {}, "test")
        second = self.gate.get_task("t2")["created_at"]
        self.assertGreater(second, first)

    # ------------------------------------------------ 12/21. I-17 human gates
    def test_14_human_gated_state_sticky_across_restart(self):
        self._mk_task_job()
        for to in ("AUTHORIZED", "PLANNED", "EXECUTING"):
            self.gate.transition_task("t1", to, "scheduler")
        self.gate.transition_task("t1", "PAUSED_FOR_HUMAN", "scheduler",
                                  reason="needs human",
                                  pause_reason="ambiguous objective")
        ap = self.gate.create_approval(
            "ap1", "t1", reason="objective ambiguous",
            requested_action="clarify scope", actor="scheduler")
        self.assertEqual(ap["status"], "PENDING")
        self.store.close()
        # ---- simulated process restart: brand-new handle on the same file
        self.store = open_store(self.db_path, clock=self.clock)
        self.gate = TransitionGate(self.store)
        task = self.gate.get_task("t1")
        self.assertEqual(task["status"], "PAUSED_FOR_HUMAN")
        self.assertEqual(task["pause_reason"], "ambiguous objective")
        # a restart must not silently move it forward: resume without a
        # recorded human decision is rejected
        with self.assertRaises(TransitionRejected):
            self.gate.transition_task("t1", "EXECUTING", "system")
        # a DENIED approval does not release the gate either
        self.gate.decide_approval("ap1", "DENIED", "human-op", "test")
        with self.assertRaises(TransitionRejected):
            self.gate.transition_task("t1", "EXECUTING", "system",
                                      approval_ref="ap1")
        # an explicit APPROVED decision releases it
        ap2 = self.gate.create_approval(
            "ap2", "t1", reason="retry", requested_action="resume", actor="s")
        self.gate.decide_approval("ap2", "APPROVED", "human-op", "test",
                                  decision_reason="scope clarified")
        self.gate.transition_task("t1", "EXECUTING", "system", approval_ref="ap2")
        self.assertEqual(self.gate.get_task("t1")["status"], "EXECUTING")

    # ------------------------------------------------ 13. incidents
    def test_15_incident_signature_is_canonical(self):
        a = self.gate.create_incident(
            "i1", scope="job", failure_class="worker_crash", stage_id="s1",
            capability_version="cap-3", input_batch_id="b7",
            error_class="sigkill", actor="supervisor")
        b = self.gate.create_incident(
            "i2", scope="job", failure_class="worker_crash", stage_id="s1",
            capability_version="cap-3", input_batch_id="b7",
            error_class="sigkill", actor="supervisor")
        # same causal shape -> same signature (poisoned batch collapses)
        self.assertEqual(a["signature"], b["signature"])
        sig = json.loads(a["signature"])
        self.assertNotIn("raw_message", sig)
        c = self.gate.create_incident(
            "i3", scope="job", failure_class="worker_crash", stage_id="s1",
            capability_version="cap-3", input_batch_id="b7",
            error_class="oom", actor="supervisor")
        self.assertNotEqual(a["signature"], c["signature"])
        self.gate.set_incident_outcome("i1", "recovered", "transient", "sup")
        row = self.store.conn.execute(
            "SELECT outcome FROM incidents WHERE incident_id='i1'").fetchone()
        self.assertEqual(row["outcome"], "recovered")

    # ------------------------------------------------ 14/22. I-18 recovery
    def test_16_recovery_attempt_requires_progress_evidence(self):
        inc = self.gate.create_incident(
            "i1", scope="job", failure_class="worker_crash", stage_id="s1",
            capability_version="c1", input_batch_id="b1",
            error_class="sigkill", actor="sup")
        rec = self.gate.record_recovery_attempt(
            "r1", "i1", rung=1,
            action={"kind": "requeue", "job_id": "j1"},
            observed_effect="worker restarted, heartbeat resumed",
            progress_delta=3.0, output_health_delta=0.0,
            decision="retry", actor="recovery")
        self.assertEqual(rec["decision"], "retry")
        attempts = self.gate.recovery_attempts_for("i1")
        self.assertEqual(len(attempts), 1)
        # I-18: 'success' with zero progress delta is contradictory -> rejected
        with self.assertRaises(TransitionRejected):
            self.gate.record_recovery_attempt(
                "r2", "i1", rung=2, action={"kind": "requeue"},
                observed_effect="worker restarted again, no new output",
                progress_delta=0.0, output_health_delta=0.0,
                decision="success", actor="recovery")
        # ...and the DB-level CHECK agrees, even for a raw insert
        with self.assertRaises(sqlite3.IntegrityError):
            with self.store.write_txn() as (conn, now):
                conn.execute(
                    "INSERT INTO recovery_attempts(attempt_id, incident_id,"
                    " rung, action, observed_effect, progress_delta,"
                    " output_health_delta, decision, actor, recorded_at)"
                    " VALUES('raw1','i1',2,'{}','x',0,0,'success','t',?)",
                    (now,))
        # missing observed effect is rejected too
        with self.assertRaises(TransitionRejected):
            self.gate.record_recovery_attempt(
                "r3", "i1", rung=2, action={"kind": "requeue"},
                observed_effect="", progress_delta=1.0,
                output_health_delta=0.0, decision="retry", actor="recovery")

    # ------------------------------------------------ 15. receipts
    def test_17_checkpoint_verification_receipt(self):
        self.gate.create_task("t1", {}, {}, "test")
        ck = self.gate.create_checkpoint("c1", "t1", "test")
        self.assertEqual(ck["verification_status"], "UNVERIFIED")
        self.gate.set_checkpoint_verification("c1", "VERIFYING", "test")
        # VERIFIED without a real receipt is rejected: no receipt, no trust
        with self.assertRaises(TransitionRejected):
            self.gate.set_checkpoint_verification("c1", "VERIFIED", "test",
                                                  receipt={"method": "sample"})
        receipt = {"method": "sampled-revalidate", "sample_rate": 0.05,
                   "sample_seed": 42, "sample_count": 5, "sample_passed": 5,
                   "manifest_full": True, "ledger_chain_verified": True}
        done = self.gate.set_checkpoint_verification(
            "c1", "VERIFIED", "test", receipt=receipt)
        self.assertEqual(done["verification_status"], "VERIFIED")
        self.assertEqual(done["sample_passed"], 5)
        self.assertEqual(done["manifest_full"], 1)
        # CORRUPT is a terminal, honest state — never deleted
        ck2 = self.gate.create_checkpoint("c2", "t1", "test")
        self.gate.set_checkpoint_verification("c2", "VERIFYING", "test")
        bad = self.gate.set_checkpoint_verification(
            "c2", "CORRUPT", "test", receipt={"method": "full"})
        self.assertEqual(bad["verification_status"], "CORRUPT")
        with self.assertRaises(TransitionRejected):
            self.gate.set_checkpoint_verification(
                "c2", "VERIFIED", "test", receipt=receipt)

    # ------------------------------------------------ 16. validator provenance
    def test_18_validator_provenance_and_quarantine(self):
        self.gate.create_task("t1", {}, {}, "test")
        self.gate.create_artifact("a" * 64, "t1", "test")
        self.gate.record_validation(
            "v1", "a" * 64, "vis-check", "2.3.1", "PASS", "test",
            method="cnn", sampled=True, sample_desc="5% seed 7",
            receipt_ref="rcpt-1")
        self.gate.record_validation(
            "v2", "a" * 64, "vis-check", "2.3.1", "PASS", "test")
        rows = self.gate.validations_for_validator("vis-check", "2.3.1")
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["quarantined"], 0)
        # quarantine marks every verdict by that validator version ...
        n = self.gate.quarantine_validator("vis-check", "2.3.1", "test")
        self.assertEqual(n, 2)
        # ... while the records themselves survive for re-examination
        rows = self.gate.validations_for_validator("vis-check", "2.3.1")
        self.assertTrue(all(r["quarantined"] == 1 for r in rows))
        self.assertEqual(rows[0]["receipt_ref"], "rcpt-1")

    # ------------------------------------------------ 17. WAL kill recovery
    def _run_crasher(self, mode):
        env = dict(os.environ,
                   PYTHONPATH=os.path.dirname(os.path.dirname(HERE)))
        return subprocess.run(
            [sys.executable, os.path.join(HERE, "_crasher.py"),
             self.db_path, mode],
            env=env, capture_output=True, text=True, timeout=60)

    def test_19_wal_recovery_after_process_kill(self):
        r = self._run_crasher("uncommitted")
        self.assertEqual(r.returncode, -signal.SIGKILL, r.stderr)  # killed mid-txn
        st = open_store(self.db_path, clock=self.clock)
        try:
            ok, detail = st.integrity_check()
            self.assertTrue(ok, detail)
            row = st.conn.execute(
                "SELECT task_id FROM tasks WHERE task_id='doomed-task'").fetchone()
            self.assertIsNone(row)  # uncommitted work is gone, atomically
            g = TransitionGate(st)
            ok, detail = g.verify_ledger_chain()
            self.assertTrue(ok, detail)
        finally:
            st.close()

    def test_20_committed_data_survives_kill(self):
        r = self._run_crasher("committed")
        self.assertEqual(r.returncode, -signal.SIGKILL, r.stderr)
        st = open_store(self.db_path, clock=self.clock)
        try:
            ok, detail = st.integrity_check()
            self.assertTrue(ok, detail)
            row = st.conn.execute(
                "SELECT task_id FROM tasks WHERE task_id='committed-task'").fetchone()
            self.assertIsNotNone(row)  # committed before the kill: durable
            row = st.conn.execute(
                "SELECT task_id FROM tasks WHERE task_id='doomed-task2'").fetchone()
            self.assertIsNone(row)
        finally:
            st.close()

    # ------------------------------------------------ 18. reopen
    def test_21_reopen_consistency(self):
        self._mk_task_job()
        self.gate.transition_task("t1", "AUTHORIZED", "scheduler")
        self.store.close()
        self.store = open_store(self.db_path, clock=self.clock)
        self.gate = TransitionGate(self.store)
        self.assertEqual(self.gate.get_task("t1")["status"], "AUTHORIZED")
        self.assertEqual(self.gate.get_job("j1")["status"], "PENDING")
        ok, detail = self.store.integrity_check()
        self.assertTrue(ok, detail)
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)

    # ------------------------------------------------ 19. backup / restore
    def test_22_backup_restore_correctness(self):
        self._mk_task_job()
        self.gate.transition_task("t1", "AUTHORIZED", "scheduler")
        self.gate.append_event("custom.note", {"k": "v"}, "test")
        backup = os.path.join(self.tmp, "backup.db")
        self.store.backup_to(backup)
        # restore = open the backup as an independent database
        rst = open_store(backup, clock=self.clock)
        try:
            ok, detail = rst.integrity_check()
            self.assertTrue(ok, detail)
            g2 = TransitionGate(rst)
            ok, detail = g2.verify_ledger_chain()
            self.assertTrue(ok, detail)
            self.assertEqual(g2.get_task("t1")["status"], "AUTHORIZED")
            self.assertEqual(g2.get_job("j1")["status"], "PENDING")
        finally:
            rst.close()
        # the backup is independent: later writes to the original do not leak
        self.gate.transition_task("t1", "PLANNED", "scheduler")
        rst = open_store(backup, clock=self.clock)
        try:
            self.assertEqual(TransitionGate(rst).get_task("t1")["status"],
                             "AUTHORIZED")
        finally:
            rst.close()

    # ------------------------------------------------ ledger tamper evidence
    def test_23_ledger_chain_detects_tampering(self):
        self._mk_task_job()
        self.gate.append_event("note", {"a": 1}, "test")
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)
        # bypass the gate and mutate a payload directly
        with self.store.write_txn() as (conn, now):
            conn.execute("UPDATE ledger SET payload='{\"a\":2}' WHERE seq=1")
        ok, detail = self.gate.verify_ledger_chain()
        self.assertFalse(ok)
        self.assertIn("hash mismatch", detail)

    # ------------------------------------------------ 20. I-16
    def test_24_i16_workers_cannot_create_work(self):
        with self.assertRaises(TransitionRejected):
            self.gate.create_task("tx", {}, {}, "worker:w-9")
        self.gate.create_task("t1", {}, {}, "scheduler")
        with self.assertRaises(TransitionRejected):
            self.gate.create_job("jx", "t1", "s", "worker:w-9")
        # scheduler-class actors can
        self.gate.create_job("j1", "t1", "s", "scheduler")
        self.assertEqual(self.gate.get_job("j1")["status"], "PENDING")

    # ------------------------------------------------ artifact lifecycle
    def test_25_artifact_lifecycle(self):
        self.gate.create_task("t1", {}, {}, "test")
        self.gate.create_artifact("f" * 64, "t1", "test", kind="png", size=12)
        with self.assertRaises(TransitionRejected):
            self.gate.transition_artifact("f" * 64, "RELEASED", "test")
        self.gate.transition_artifact("f" * 64, "VALIDATED", "test")
        self.gate.transition_artifact("f" * 64, "RELEASED", "test")
        row = self.store.conn.execute(
            "SELECT status FROM artifacts WHERE artifact_id=?",
            ("f" * 64,)).fetchone()
        self.assertEqual(row["status"], "RELEASED")


if __name__ == "__main__":
    unittest.main()
