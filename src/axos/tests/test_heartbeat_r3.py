"""Phase 1C R3 — durable heartbeat ingestion & progress evidence.

STANDARD: real SQLite/WAL files, real transactions, real concurrency
(threads with one Store per thread, exactly like production components),
and real worker subprocesses. No mocks except the injected-failure
rollback tests, which must raise *inside* the gate's transaction to prove
atomicity — the single sanctioned exception.

Test IDs:
  R3-01  heartbeat accepted: durable row, store-time ts, informational worker ts
  R3-02  sequence monotonicity: N -> N+1 ok; N+1 -> N rejected, zero mutation
  R3-03  duplicate heartbeat: same seq rejected, no duplicate durable state
  R3-04  stale fencing token: post-reclaim old-token heartbeat rejected + journaled
  R3-05  owner-cleared heartbeat: cannot restore ownership or protected state
  R3-06  replacement worker: new token accepted, old rejected, no overwrite
  R3-07  concurrent heartbeats: highest valid seq wins, never regresses
  R3-08  progress timestamp: progress writes stamp progress_updated_at (store time)
  R3-09  heartbeat alone never advances progress_updated_at
  R3-10  rollback: injected failure leaves no partial heartbeat/progress state
  R3-11  stale heartbeat vs reclaim race: stale never wins
  R3-12  replacement race: old evidence never overwrites replacement
  R3-13  real worker subprocess: heartbeats + progress flow end-to-end
  R3-14  supervisor compatibility: R2 fence sweep composes with R3 ingestion
  R3-15  heartbeat is not reclaim authority
  R3-16  audit enforcement: audit 09 R3 checks present and green
Schema / migration (contract D9):
  R3-M1  v3 migration: progress_updated_at added, no backfill, values preserved
  R3-M2  migrate() idempotent; versions recorded [1,2,3]
  R3-S1  heartbeat rows carry no progress columns (D9 liveness-only)
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace
AXOS_DIR = os.path.dirname(HERE)

from axos.store import (
    open_store, migrate, TransitionGate,
    TransitionRejected, LeaseError,
)


class FakeClock:
    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class HeartbeatR3Test(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r3-")
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

    def _reclaim(self, jid, tok, owner, incident="i-r3"):
        return self.gate.reclaim_lease(
            jid, actor="recovery-controller", reason="r3 test",
            expected_owner=owner, expected_token=tok,
            force=True, verdict="DEAD", incident_id=incident)

    def _hb(self, wid, proc, jid, tok, seq, **kw):
        return self.gate.ingest_heartbeat(
            wid, proc, jid, tok, seq, "RUNNING", "op", f"worker:{wid}", **kw)

    def _fenced_events(self):
        return self.store.conn.execute(
            "SELECT * FROM ledger WHERE event_type='worker.heartbeat_fenced'"
            " ORDER BY seq").fetchall()

    def _wait_for(self, pred, timeout=20.0, interval=0.1):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("wait_for timed out")

    # ------------------------------------------------------- R3-01 accepted
    def test_R3_01_heartbeat_accepted_durable(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        self.clock.advance(5.0)
        t_store = self.clock.t
        row = self._hb("w1", "p1", "j1", tok, 0, worker_reported_ts=123.0)
        # store time is authoritative ...
        self.assertEqual(row["ts"], t_store)
        # ... the worker-supplied timestamp is informational only
        self.assertEqual(row["worker_reported_ts"], 123.0)
        self.assertEqual(row["hb_seq"], 0)
        self.assertEqual(row["fencing_token"], tok)
        self.assertEqual(row["job_id"], "j1")
        # durable: visible through the read path
        rows = self.gate.heartbeats_for("w1", "p1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["seq"], row["seq"])
        # liveness evidence on the worker row
        self.assertEqual(self.gate.get_worker("w1")["last_seen_at"], t_store)
        # a lying worker clock (far future) changes nothing authoritative:
        # ts still comes from the store clock (monotonic w.r.t. the last
        # committed transaction), while the claim is stored informationally
        row2 = self._hb("w1", "p1", "j1", tok, 1, worker_reported_ts=9e18)
        self.assertGreaterEqual(row2["ts"], t_store)
        self.assertNotEqual(row2["ts"], 9e18)
        self.assertEqual(row2["worker_reported_ts"], 9e18)
        self.assertEqual(row2["hb_seq"], 1)
        # no ledger event per accepted heartbeat (summaries/milestones only)
        self.assertEqual(len(self._fenced_events()), 0)
        n_ledger = self.store.conn.execute(
            "SELECT COUNT(*) FROM ledger").fetchone()[0]
        # only worker.created / job lifecycle events from setup, none heartbeat
        hb_ev = self.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type LIKE '%heartbeat%'"
        ).fetchone()[0]
        self.assertEqual(hb_ev, 0)
        self.assertGreater(n_ledger, 0)  # sanity: ledger itself works

    # ------------------------------------------------- R3-02 sequence order
    def test_R3_02_sequence_monotonicity(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        self._hb("w1", "p1", "j1", tok, 0)
        self._hb("w1", "p1", "j1", tok, 1)
        seen_before = self.gate.get_worker("w1")["last_seen_at"]
        self.clock.advance(1.0)
        # older sequence: rejected
        with self.assertRaises(TransitionRejected):
            self._hb("w1", "p1", "j1", tok, 0)
        # duplicate sequence: rejected
        with self.assertRaises(TransitionRejected):
            self._hb("w1", "p1", "j1", tok, 1)
        # zero mutation: no new row, last_seen_at untouched
        rows = self.gate.heartbeats_for("w1", "p1")
        self.assertEqual([r["hb_seq"] for r in rows], [1, 0])
        self.assertEqual(self.gate.get_worker("w1")["last_seen_at"],
                         seen_before)
        # malformed sequences never reach the store
        for bad in (-1, True, "3", 1.5, None):
            with self.assertRaises(TransitionRejected):
                self.gate.ingest_heartbeat("w1", "p1", "j1", tok, bad,
                                           "RUNNING", "op", "worker:w1")
        self.assertEqual(len(self.gate.heartbeats_for("w1", "p1")), 2)

    # ------------------------------------------------- R3-03 duplicate
    def test_R3_03_duplicate_cannot_duplicate_progress(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        self._hb("w1", "p1", "j1", tok, 0,
                 progress_done=10.0, progress_total=100.0)
        self.assertEqual(self.gate.get_job("j1")["progress_done"], 10.0)
        with self.assertRaises(TransitionRejected):
            self._hb("w1", "p1", "j1", tok, 0,
                     progress_done=99.0, progress_total=100.0)
        jj = self.gate.get_job("j1")
        self.assertEqual(jj["progress_done"], 10.0)  # not overwritten
        self.assertEqual(len(self.gate.heartbeats_for("w1", "p1")), 1)

    # ------------------------------------------------- R3-04 stale token
    def test_R3_04_stale_token_rejected_and_journaled(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        self._hb("w1", "p1", "j1", tok, 0)  # legitimate, pre-reclaim
        self._reclaim("j1", tok, "w1")
        newtok = self.gate.get_job("j1")["fencing_token"]
        self.assertEqual(newtok, tok + 1)
        with self.assertRaises(LeaseError):
            self._hb("w1", "p1", "j1", tok, 1)
        # no new heartbeat row from the stale worker
        rows = self.gate.heartbeats_for("w1", "p1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["hb_seq"], 0)
        # durable state stays authoritative for N+1
        jj = self.gate.get_job("j1")
        self.assertEqual(jj["fencing_token"], newtok)
        self.assertIsNone(jj["owner_worker_id"])
        # the rejection is journaled with observed vs authoritative evidence
        evs = self._fenced_events()
        self.assertEqual(len(evs), 1)
        p = json.loads(evs[0]["payload"])
        self.assertEqual(p["worker_id"], "w1")
        self.assertEqual(p["proc_id"], "p1")
        self.assertEqual(p["job_id"], "j1")
        self.assertEqual(p["observed_token"], tok)
        self.assertEqual(p["current_token"], newtok)
        self.assertIsNone(p["current_owner"])

    # ------------------------------------------------- R3-05 owner cleared
    def test_R3_05_owner_cleared_heartbeat_cannot_restore(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        self._reclaim("j1", tok, "w1")
        newtok = self.gate.get_job("j1")["fencing_token"]
        # stale token: rejected
        with self.assertRaises(LeaseError):
            self._hb("w1", "p1", "j1", tok, 0)
        # even the CURRENT token from the old (non-owner) worker: rejected,
        # and it must not restore ownership
        with self.assertRaises(LeaseError):
            self._hb("w1", "p1", "j1", newtok, 0)
        # fencing_token=None: rejected, never treated as a match
        with self.assertRaises(LeaseError):
            self.gate.ingest_heartbeat("w1", "p1", "j1", None, 0,
                                       "RUNNING", "op", "worker:w1")
        jj = self.gate.get_job("j1")
        self.assertIsNone(jj["owner_worker_id"])
        self.assertEqual(jj["fencing_token"], newtok)
        self.assertEqual(len(self.gate.heartbeats_for("w1", "p1")), 0)
        # three fencing rejections journaled, zero state restored
        self.assertEqual(len(self._fenced_events()), 3)

    # ------------------------------------------------- R3-06 replacement
    def test_R3_06_replacement_worker(self):
        j = self._mk("j1", "w1")
        tok1 = j["fencing_token"]
        self._hb("w1", "p1", "j1", tok1, 0)
        self._reclaim("j1", tok1, "w1", incident="i-r3-06")
        # reclaim moved RUNNING -> UNCERTAIN; requeue before the replacement
        self.gate.transition_job("j1", "PENDING", "recovery-controller")
        self.gate.create_worker("w2", "test")
        assert self.gate.claim_job("j1", "w2", 60.0, "scheduler")
        tok2 = self.gate.get_job("j1")["fencing_token"]
        self.assertGreater(tok2, tok1)
        self.gate.transition_job("j1", "RUNNING", "worker:w2")
        # replacement heartbeat accepted on its own proc identity (seq restarts)
        r = self._hb("w2", "p2", "j1", tok2, 0)
        self.assertEqual(r["hb_seq"], 0)
        self.assertEqual(r["fencing_token"], tok2)
        # old worker heartbeat rejected; replacement evidence untouched
        with self.assertRaises(LeaseError):
            self._hb("w1", "p1", "j1", tok1, 1)
        rows2 = self.gate.heartbeats_for("w2", "p2")
        self.assertEqual(len(rows2), 1)
        self.assertEqual(rows2[0]["hb_seq"], 0)
        self.assertEqual(len(self.gate.heartbeats_for("w1", "p1")), 1)

    # ------------------------------------------------- R3-07 concurrency
    def test_R3_07_concurrent_heartbeats_preserve_max_seq(self):
        # Each execution (worker, proc) owns its own sequence space and emits
        # serially; concurrency happens ACROSS executions. Preassigning
        # sequences to one (worker, proc) across threads would be out-of-order
        # by construction (commit order is nondeterministic) — correctly
        # rejected. Here: 8 executions hammer the store concurrently; every
        # execution's sequence must arrive intact, none regressing.
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        n_threads, n_each = 8, 25
        barrier = threading.Barrier(n_threads)
        errors = []

        def sender(idx):
            proc = f"p{idx}"
            st = open_store(self.db_path, clock=self.clock)
            g = TransitionGate(st)
            try:
                barrier.wait()
                for s in range(n_each):
                    g.ingest_heartbeat("w1", proc, "j1", tok, s,
                                       "RUNNING", "op", "worker:w1")
            except Exception as e:  # noqa: BLE001 - collected, asserted below
                errors.append(e)
            finally:
                st.close()

        threads = [threading.Thread(target=sender, args=(i,))
                   for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        for i in range(n_threads):
            rows = self.gate.heartbeats_for("w1", f"p{i}", limit=n_each + 10)
            seqs = sorted(r["hb_seq"] for r in rows)
            # each execution's sequence arrived exactly once, in order,
            # highest valid last — never regressed, never corrupted
            self.assertEqual(seqs, list(range(n_each)))
            self.assertEqual(max(seqs), n_each - 1)
            self.assertTrue(all(r["fencing_token"] == tok for r in rows))

    def test_R3_07b_concurrent_duplicate_seq_exactly_one_wins(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        self._hb("w1", "p1", "j1", tok, 4)
        n = 10
        barrier = threading.Barrier(n)
        outcomes = []
        out_lock = threading.Lock()

        def sender():
            st = open_store(self.db_path, clock=self.clock)
            g = TransitionGate(st)
            try:
                barrier.wait()
                try:
                    g.ingest_heartbeat("w1", "p1", "j1", tok, 5,
                                       "RUNNING", "op", "worker:w1")
                    res = "accepted"
                except TransitionRejected:
                    res = "rejected"
                with out_lock:
                    outcomes.append(res)
            finally:
                st.close()

        threads = [threading.Thread(target=sender) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(outcomes.count("accepted"), 1)
        self.assertEqual(outcomes.count("rejected"), n - 1)
        n5 = self.store.conn.execute(
            "SELECT COUNT(*) FROM heartbeats WHERE worker_id='w1'"
            " AND proc_id='p1' AND hb_seq=5").fetchone()[0]
        self.assertEqual(n5, 1)

    # ------------------------------------------------- R3-08 progress ts
    def test_R3_08_progress_write_stamps_progress_updated_at(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        self.assertIsNone(self.gate.get_job("j1")["progress_updated_at"])
        self.clock.advance(3.0)
        t1 = self.clock.t
        out = self.gate.update_job_progress("j1", "w1", tok, 25.0, 100.0,
                                            "worker:w1")
        self.assertEqual(out["progress_updated_at"], t1)
        self.assertEqual(self.gate.get_job("j1")["progress_updated_at"], t1)
        # heartbeat-carried progress: stamped atomically with the heartbeat
        self.clock.advance(2.0)
        t2 = self.clock.t
        hb = self._hb("w1", "p1", "j1", tok, 0,
                      progress_done=50.0, progress_total=100.0)
        jj = self.gate.get_job("j1")
        self.assertEqual(jj["progress_done"], 50.0)
        self.assertEqual(jj["progress_updated_at"], t2)
        self.assertEqual(jj["progress_updated_at"], hb["ts"])
        # progress without a job is a protocol error, not silent loss
        with self.assertRaises(TransitionRejected):
            self.gate.ingest_heartbeat("w1", "p1", None, tok, 1,
                                       "RUNNING", "op", "worker:w1",
                                       progress_done=10.0)

    # ------------------------------------------------- R3-09 hb != progress
    def test_R3_09_heartbeat_alone_does_not_advance_progress(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        for s in range(5):
            self.clock.advance(0.5)
            self._hb("w1", "p1", "j1", tok, s)
        self.assertEqual(len(self.gate.heartbeats_for("w1", "p1")), 5)
        self.assertIsNone(self.gate.get_job("j1")["progress_updated_at"])
        # ... and after a real progress write, bare heartbeats leave it alone
        self.gate.update_job_progress("j1", "w1", tok, 10.0, 100.0,
                                      "worker:w1")
        stamped = self.gate.get_job("j1")["progress_updated_at"]
        self.assertIsNotNone(stamped)
        self.clock.advance(5.0)
        self._hb("w1", "p1", "j1", tok, 5)
        self.assertEqual(self.gate.get_job("j1")["progress_updated_at"],
                         stamped)

    # ------------------------------------------------- R3-10 rollback
    def test_R3_10a_heartbeat_progress_rollback_atomic(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        seen_before = self.gate.get_worker("w1")["last_seen_at"]
        with mock.patch.object(self.gate, "_apply_progress",
                               side_effect=RuntimeError("injected")):
            with self.assertRaises(RuntimeError):
                self._hb("w1", "p1", "j1", tok, 0,
                         progress_done=10.0, progress_total=100.0)
        # the heartbeat row rolled back with the failed progress write
        self.assertEqual(len(self.gate.heartbeats_for("w1", "p1")), 0)
        jj = self.gate.get_job("j1")
        self.assertEqual(jj["progress_done"], 0)
        self.assertIsNone(jj["progress_updated_at"])
        self.assertEqual(self.gate.get_worker("w1")["last_seen_at"],
                         seen_before)

    def test_R3_10b_progress_commit_failure_rolls_back(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        real = self.store.write_txn

        @contextmanager
        def boom():
            with real() as (conn, now):
                yield conn, now
                raise RuntimeError("injected commit failure")

        with mock.patch.object(self.store, "write_txn", boom):
            with self.assertRaises(RuntimeError):
                self.gate.update_job_progress("j1", "w1", tok, 10.0, 100.0,
                                              "worker:w1")
        jj = self.gate.get_job("j1")
        self.assertEqual(jj["progress_done"], 0)
        self.assertIsNone(jj["progress_total"])
        self.assertIsNone(jj["progress_updated_at"])

    # ------------------------------------------------- R3-11 stale race
    def test_R3_11_stale_heartbeat_never_wins_against_reclaim(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        stop = threading.Event()
        errors = []
        barrier = threading.Barrier(4)

        def hammer(proc):
            st = open_store(self.db_path, clock=self.clock)
            g = TransitionGate(st)
            try:
                barrier.wait()
                s = 0
                while not stop.is_set():
                    try:
                        g.ingest_heartbeat("w1", proc, "j1", tok, s,
                                           "RUNNING", "op", "worker:w1")
                        s += 1
                    except (LeaseError, TransitionRejected):
                        pass  # expected: fenced, or seq contention
            except Exception as e:  # noqa: BLE001 - collected below
                errors.append(e)
            finally:
                st.close()

        threads = [threading.Thread(target=hammer, args=(f"p{i}",))
                   for i in range(4)]
        for t in threads:
            t.start()
        time.sleep(0.3)  # let stale-token heartbeats flow (legitimate, pre-reclaim)
        self._reclaim("j1", tok, "w1", incident="i-r3-11")
        reclaim_ts = self.store.conn.execute(
            "SELECT ts FROM ledger WHERE event_type='job.lease_reclaimed'"
            " ORDER BY seq DESC LIMIT 1").fetchone()["ts"]
        time.sleep(0.3)  # stale heartbeats keep racing the bumped token
        stop.set()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        newtok = self.gate.get_job("j1")["fencing_token"]
        self.assertEqual(newtok, tok + 1)
        # THE invariant: no heartbeat accepted with the stale token after
        # the fencing transaction's store timestamp. The fencing check and
        # the row insert live in one write_txn, so a stale heartbeat either
        # commits fully before the reclaim or is rejected after it.
        bad = self.store.conn.execute(
            "SELECT COUNT(*) FROM heartbeats WHERE job_id='j1'"
            " AND fencing_token=? AND ts > ?",
            (tok, reclaim_ts)).fetchone()[0]
        self.assertEqual(bad, 0)
        # every accepted stale-token row predates the reclaim
        n_stale = self.store.conn.execute(
            "SELECT COUNT(*) FROM heartbeats WHERE job_id='j1'"
            " AND fencing_token=?", (tok,)).fetchone()[0]
        self.assertGreater(n_stale, 0)  # the race actually ran pre-reclaim
        # rejections were journaled, not silent
        self.assertGreaterEqual(len(self._fenced_events()), 1)

    # ------------------------------------------------- R3-12 replacement race
    def test_R3_12_old_heartbeat_cannot_overwrite_replacement(self):
        j = self._mk("j1", "w1")
        tok1 = j["fencing_token"]
        self._reclaim("j1", tok1, "w1", incident="i-r3-12a")
        # reclaim moved RUNNING -> UNCERTAIN; requeue before the replacement
        self.gate.transition_job("j1", "PENDING", "recovery-controller")
        self.gate.create_worker("w2", "test")
        assert self.gate.claim_job("j1", "w2", 60.0, "scheduler")
        tok2 = self.gate.get_job("j1")["fencing_token"]
        self.gate.transition_job("j1", "RUNNING", "worker:w2")
        self._hb("w2", "p2", "j1", tok2, 0,
                 progress_done=40.0, progress_total=100.0)
        w1_seen_before = self.gate.get_worker("w1")["last_seen_at"]
        w2_seen_before = self.gate.get_worker("w2")["last_seen_at"]
        stop = threading.Event()
        errors = []
        barrier = threading.Barrier(6)

        def hammer(wid, proc, tk):
            st = open_store(self.db_path, clock=self.clock)
            g = TransitionGate(st)
            try:
                barrier.wait()
                s = 1
                while not stop.is_set():
                    try:
                        g.ingest_heartbeat(wid, proc, "j1", tk, s,
                                           "RUNNING", "op", f"worker:{wid}")
                        s += 1
                    except (LeaseError, TransitionRejected):
                        pass
            except Exception as e:  # noqa: BLE001 - collected below
                errors.append(e)
            finally:
                st.close()

        threads = ([threading.Thread(target=hammer, args=("w1", "p1", tok1))
                    for _ in range(3)]
                   + [threading.Thread(target=hammer, args=("w2", "p2", tok2))
                      for _ in range(3)])
        for t in threads:
            t.start()
        time.sleep(0.5)
        stop.set()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        # replacement evidence advanced monotonically, all with the new token
        rows2 = self.gate.heartbeats_for("w2", "p2", limit=100000)
        self.assertGreater(len(rows2), 1)
        self.assertEqual(sorted(r["hb_seq"] for r in rows2),
                         list(range(len(rows2))))
        self.assertTrue(all(r["fencing_token"] == tok2 for r in rows2))
        self.assertGreater(self.gate.get_worker("w2")["last_seen_at"],
                           w2_seen_before)
        # the old worker contributed nothing: zero new rows, last_seen_at
        # untouched by its rejected heartbeats, progress intact
        self.assertEqual(len(self.gate.heartbeats_for("w1", "p1")), 0)
        self.assertEqual(self.gate.get_worker("w1")["last_seen_at"],
                         w1_seen_before)
        jj = self.gate.get_job("j1")
        self.assertEqual(jj["progress_done"], 40.0)
        self.assertEqual(jj["fencing_token"], tok2)
        self.assertEqual(jj["owner_worker_id"], "w2")

    # ------------------------------------------------- R3-13 real worker
    def test_R3_13a_real_worker_heartbeat_and_progress(self):
        from axos.exec.supervisor import Supervisor
        self._task()
        self.gate.create_job("j13", "t", "s", "scheduler")
        sup = Supervisor(self.db_path, actor="test-r3", heartbeat_interval_s=0.2)
        self._sups.append(sup)
        sup.start_worker("w13", "j13", {"kind": "success_immediate"},
                         ttl_s=60.0, hb_interval_s=0.1)
        self._wait_for(
            lambda: self.gate.get_job("j13")["status"] == "COMPLETE",
            timeout=30.0)
        rows = self.gate.heartbeats_for("w13", limit=100)
        self.assertGreaterEqual(len(rows), 1)
        # monotonic per-proc sequences through the real subprocess path
        by_proc = {}
        for r in rows:
            by_proc.setdefault(r["proc_id"], []).append(r["hb_seq"])
        for seqs in by_proc.values():
            self.assertEqual(sorted(seqs), list(range(len(seqs))))
        # progress flowed through the gate and was stamped (store time)
        jj = self.gate.get_job("j13")
        self.assertEqual(jj["progress_done"], 100.0)
        self.assertIsNotNone(jj["progress_updated_at"])
        # heartbeat ts values are sane store times, nondecreasing per proc
        for seqs_proc in by_proc:
            tss = [r["ts"] for r in rows if r["proc_id"] == seqs_proc]
            tss_sorted = sorted(tss)
            self.assertEqual(tss, tss_sorted)
            self.assertTrue(all(t > 1_000_000 for t in tss))

    def test_R3_13b_real_worker_heartbeat_only_no_progress_signal(self):
        from axos.exec.supervisor import Supervisor
        self._task()
        self.gate.create_job("j13b", "t", "s", "scheduler")
        sup = Supervisor(self.db_path, actor="test-r3", heartbeat_interval_s=0.2)
        self._sups.append(sup)
        sup.start_worker("w13b", "j13b",
                         {"kind": "heartbeat_loop", "duration_s": 1.0},
                         ttl_s=60.0, hb_interval_s=0.1)
        self._wait_for(lambda: len(self.gate.heartbeats_for("w13b")) >= 3,
                       timeout=20.0)

        def reaped():
            sup.observe()
            return "w13b" in sup._reaped

        self._wait_for(reaped, timeout=20.0)
        rows = self.gate.heartbeats_for("w13b", limit=100)
        self.assertGreaterEqual(len(rows), 3)
        # heartbeats alone never advance the progress signal
        self.assertIsNone(self.gate.get_job("j13b")["progress_updated_at"])
        self.assertEqual(self.gate.get_job("j13b")["progress_done"], 0)

    # ------------------------------------------------- R3-14 supervisor
    def test_R3_14_supervisor_fence_sweep_composes_with_r3(self):
        from axos.exec.supervisor import Supervisor
        self._task()
        self.gate.create_job("j14", "t", "s", "scheduler")
        sup = Supervisor(self.db_path, actor="test-r3", heartbeat_interval_s=0.2)
        self._sups.append(sup)
        sup.start_worker("w14", "j14",
                         {"kind": "heartbeat_loop", "duration_s": 30.0},
                         ttl_s=60.0, hb_interval_s=0.1)
        self._wait_for(lambda: len(self.gate.heartbeats_for("w14")) >= 2,
                       timeout=20.0)
        # reconstruct() still surfaces the latest ingested heartbeat
        # (read-only; R3 changed nothing about the read path)
        view = sup.reconstruct()
        wv = [w for w in view["workers"] if w["worker_id"] == "w14"]
        self.assertEqual(len(wv), 1)
        self.assertIsNotNone(wv[0]["latest_heartbeat"])
        self.assertEqual(wv[0]["latest_heartbeat"]["job_id"], "j14")
        # fencing transaction while heartbeats flow: R2 sweep still enforces
        tok = self.gate.get_job("j14")["fencing_token"]
        self._reclaim("j14", tok, "w14", incident="i-r3-14")
        ftx = self.store.conn.execute(
            "SELECT ts FROM ledger WHERE event_type='job.lease_reclaimed'"
            " ORDER BY seq DESC LIMIT 1").fetchone()["ts"]

        def enforced():
            return self.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type="
                "'worker.fence_enforced' AND json_extract(payload,"
                "'$.worker_id')='w14'").fetchone()[0] > 0

        self._wait_for(enforced, timeout=20.0)
        # ... and no stale-token heartbeat was accepted after the fencing tx
        bad = self.store.conn.execute(
            "SELECT COUNT(*) FROM heartbeats WHERE job_id='j14'"
            " AND fencing_token=? AND ts > ?", (tok, ftx)).fetchone()[0]
        self.assertEqual(bad, 0)

    # ------------------------------------------------- R3-15 no authority
    def test_R3_15_heartbeat_is_not_reclaim_authority(self):
        j = self._mk("j1", "w1")
        tok = j["fencing_token"]
        before = dict(self.gate.get_job("j1"))
        for s in range(20):
            self._hb("w1", "p1", "j1", tok, s)
        after = self.gate.get_job("j1")
        for col in ("owner_worker_id", "fencing_token", "lease_acquired_at",
                    "lease_expires_at", "status", "attempt"):
            self.assertEqual(after[col], before[col], col)
        n = self.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type='job.lease_reclaimed'"
        ).fetchone()[0]
        self.assertEqual(n, 0)
        # heartbeat on an unclaimed PENDING job cannot claim it
        self.gate.create_job("j2", "t", "s", "scheduler")
        self.gate.create_worker("w9", "test")
        with self.assertRaises(LeaseError):
            self.gate.ingest_heartbeat("w9", "p9", "j2", 0, 0,
                                       "IDLE", "op", "worker:w9")
        jj = self.gate.get_job("j2")
        self.assertIsNone(jj["owner_worker_id"])
        self.assertEqual(jj["status"], "PENDING")
        self.assertEqual(jj["fencing_token"], 0)
        # source-level: the ingestion path never calls reclaim_lease
        import inspect
        src = inspect.getsource(TransitionGate.ingest_heartbeat)
        self.assertNotIn("reclaim_lease", src)

    # ------------------------------------------------- R3-16 audit
    def test_R3_16_authority_audit_covers_r3(self):
        p = subprocess.run(
            [sys.executable, "audit/09_authority_audit.py"],
            cwd=AXOS_DIR, capture_output=True, text=True, timeout=180)
        self.assertEqual(p.returncode, 0, p.stdout + "\n" + p.stderr)
        for marker in ("A9a", "A9b", "A9c", "A9d"):
            self.assertIn(marker, p.stdout)


class HeartbeatR3SchemaTest(unittest.TestCase):
    """Migration + D9 schema assertions for R3."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r3m-")
        self.db_path = os.path.join(self.tmp, "axos.db")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_R3_M1_v3_migration_no_backfill(self):
        from axos.store.migrations import MIGRATIONS
        st2 = open_store(self.db_path, clock=FakeClock())
        migrate(st2, MIGRATIONS[:2])  # v1+v2 only
        # seed a v2-schema job with progress via the privileged setup path
        c = st2._conn
        c.execute("INSERT INTO tasks(task_id,status,objective,budgets,"
                  "created_at,updated_at)"
                  " VALUES('t','PROPOSED','{}','{}',1.0,1.0)")
        c.execute("INSERT INTO jobs(job_id,task_id,status,attempt,"
                  "owner_worker_id,fencing_token,progress_done,"
                  "created_at,updated_at)"
                  " VALUES('j','t','RUNNING',1,'w',1,30.0,1.0,1.0)")
        c.execute("INSERT INTO workers(worker_id,status,created_at,updated_at)"
                  " VALUES('w','ACTIVE',1.0,1.0)")
        c.commit()
        st2.close()
        # upgrade to v3+ (the R5 artifact/checkpoint schema and the R7
        # watchdog schema ride along): applied versions derive from the
        # migration registry, not a hardcoded list.
        st3 = open_store(self.db_path, clock=FakeClock())
        applied = migrate(st3)
        self.assertEqual(applied, [v for (v, _, _) in MIGRATIONS[2:]])
        row = st3.conn.execute(
            "SELECT progress_done, progress_updated_at FROM jobs"
            " WHERE job_id='j'").fetchone()
        self.assertEqual(row["progress_done"], 30.0)
        # honest NULL: no informational progress recorded since the column
        # existed; values are preserved, history is not invented
        self.assertIsNone(row["progress_updated_at"])
        st3.close()

    def test_R3_M2_migrate_idempotent_and_versions(self):
        from axos.store.migrations import MIGRATIONS, applied_versions
        expected = [v for (v, _, _) in MIGRATIONS]
        st = open_store(self.db_path, clock=FakeClock())
        try:
            self.assertEqual(migrate(st), expected)
            self.assertEqual(applied_versions(st), expected)
            self.assertEqual(migrate(st), [])
        finally:
            st.close()

    def test_R3_S1_heartbeat_rows_carry_no_progress_columns(self):
        st = open_store(self.db_path, clock=FakeClock())
        try:
            migrate(st)
            cols = {r["name"] for r in st.conn.execute(
                "PRAGMA table_info(heartbeats)").fetchall()}
            for banned in ("progress_done", "progress_total",
                           "last_progress_ts", "progress_rate", "resources",
                           "checkpoint_ref", "task_id"):
                self.assertNotIn(banned, cols,
                                 f"heartbeats must stay liveness-only (D9)")
            # ... and the fencing/identity fields the I-3 model needs stay
            for needed in ("worker_id", "proc_id", "job_id", "fencing_token",
                           "hb_seq", "worker_state", "current_operation",
                           "worker_reported_ts", "ts"):
                self.assertIn(needed, cols)
        finally:
            st.close()


if __name__ == "__main__":
    unittest.main()
