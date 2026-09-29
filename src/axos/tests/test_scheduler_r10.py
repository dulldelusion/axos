"""Phase 1C R10 — scheduler & admission control gate tests.

STANDARD: real SQLite/WAL; real subprocesses where the action needs them
(dispatch, worker lifecycle, token verification); gate-level fixtures
elsewhere. No fake process objects, no in-memory model of the kernel.
Deterministic polling instead of blind sleeps; every spawned process is
killed and reaped in tearDown.

Test IDs:
  R10-00  SchedulerConfig validation (bonus: frozen, positive timings,
          ints >= 1; Scheduler requires a SchedulerConfig)
  R10-01  PENDING discovered -> evaluate_once admits it (CLAIMED, proc
          spawned, proc_spawned evidence present)
  R10-02  non-PENDING (CLAIMED, RUNNING, BLOCKED, FAILED, COMPLETE,
          UNCERTAIN) never scheduled
  R10-03  claim atomic: threads racing claim_job_bounded on one job ->
          exactly one True; row has full triple + event
  R10-04  two Scheduler instances, one PENDING job -> one claim total
  R10-05  claim establishes owner/lease triple, fencing_token bumped,
          job.claimed ledger event with the scheduler actor
  R10-06  worker receives the exact token: durable LeaseRef triple
          matches the claim; worker reaches RUNNING (claim_verified +
          CLAIMED->RUNNING transition evidence)
  R10-07  max_concurrent_jobs=N, >N PENDING -> admitted <= N (durable
          active count <= N)
  R10-08  two schedulers racing, capacity N -> combined admitted <= N
  R10-09  capacity full -> remaining jobs stay PENDING (not claimed,
          not failed)
  R10-10  job completes/frees slot -> next evaluate admits a deferred
          PENDING job
  R10-11  dispatch path calls supervisor.start_worker (proc spawned,
          worker.proc_spawned evidence with job_id; call recorded)
  R10-12  scheduler never kills: a RUNNING worker proc stays alive
          across evaluations; no kill verbs in scheduler source (AST)
  R10-13  scheduler never calls reclaim_lease/release_lease (AST +
          behavioral: expired-lease PENDING row with stale owner is
          claimed normally, never "reclaimed")
  R10-14  all mutations flow through TransitionGate: claim/dispatch
          state visible via a fresh gate (durable, not in-memory)
  R10-15  claimed-but-undispatched -> re-dispatched under the SAME
          worker_id/token, no second claim (fencing_token unchanged)
  R10-16  new Scheduler over the same db reconstructs purely from
          durable state (no local state needed)
  R10-17  evaluate_once twice -> second is a no-op for
          already-dispatched (no second proc)
  R10-18  proc_spawned evidence present for (worker_id, job_id) -> no
          respawn even if the job row looks dispatchable
  R10-19  worker launched with wrong --expect-token -> exit 7, job
          remains CLAIMED by original owner/token, never RUNNING
  R10-20  expired-lease sched-owned CLAIMED job -> scheduler does NOT
          spawn; R1 reclaim_lease still works on it afterwards
  R10-21  R2 authority intact: scheduler has no fence/kill path (AST);
          supervisor.kill_worker remains the killer (exercised directly)
  R10-22  STALLED/UNCERTAIN job (or job with open recovery incident)
          -> scheduler skips; no recovery_attempts rows created
  R10-23  BLOCKED and PAUSED_FOR_HUMAN jobs never scheduled; remain so
          across scheduler restart
  R10-24  full dispatch -> worker success -> COMPLETE only via the
          worker's R5 contract (ledger actor is worker:*, never
          scheduler:*)
  R10-25  boot_ready()=False -> evaluate_once admits nothing, zero
          claims; then True -> admits
  R10-26  background thread: start() the loop, job admitted from the
          loop thread; loop thread's gate differs from main's gate
  R10-27  stop() returns within a hard bound (timed)
  R10-28  crash before claim: scheduler dropped before evaluate -> job
          PENDING, unclaimed
  R10-29  crash during claim: racing claims -> row either fully PENDING
          or fully CLAIMED-with-triple; never partial
  R10-30  crash between claim and dispatch (start_worker raises) ->
          claim left standing; new scheduler re-dispatches under the
          identical owner/token
  R10-31  crash after spawn: proc_spawned evidence exists -> restarted
          scheduler does NOT spawn a second worker for the identity
  R10-32  after dispatch, worker failure is governed by kernel paths:
          scheduler takes no recovery action (no incident/attempt/claim/
          spawn)
  R10-33  admission order matches ORDER BY created_at, job_id
  R10-34  two fresh schedulers over identical db states -> identical
          admission order/decisions
  R10-35  Scheduler has no create_job/delete variants (hasattr)
  R10-36  scheduler source has no job-row INSERT/DELETE (AST/keyword)
  R10-37  no reconciliation/circuit-breaker/load-shedding (AST)
  R10-38  no recovery/policy machinery beyond the read-only incident
          scan (AST; extends A16i/A16j coverage)
  R10-39  three schedulers x one PENDING job -> exactly one claim;
          x K jobs with capacity N -> admitted <= N, no duplicate
          dispatch identities
  R10-40  full R1-R9 composition: PENDING -> dispatch -> RUNNING ->
          COMPLETE via R5; failure leg: expiring lease -> R4 expiry ->
          R1 reclaim -> PENDING -> scheduler re-admits -> success
Race extras:
  R10-RACE-1  16 threads x claim_job_bounded on one job -> exactly one
              True; losers get False (no exceptions)
  R10-RACE-2  two threads x one Scheduler.evaluate_once -> one claim,
              one spawn (same-instance duplicate-dispatch race)
  R10-RACE-3  two schedulers x one orphaned CLAIMED dispatch ->
              exactly-once execution (documented residual: two procs may
              spawn, one wins CLAIMED->RUNNING, loser exits before
              executing)
  R10-RACE-4  capacity race, gate level: 6 threads x distinct jobs,
              max_concurrent_jobs=2 -> exactly 2 claims won
  R10-RACE-5  scheduler evaluate racing an external direct claim on the
              same PENDING job -> exactly one claimant, no duplicate
  R10-RACE-6  claim-liveness beats: a live scheduler's fresh claim is
              never stolen by a racing scheduler's orphan path; a dead
              scheduler's orphan (stale beat) is re-dispatched under the
              identical owner/token with no second claim
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

from axos.exec.scheduler import Scheduler, SchedulerConfig  # noqa: E402
from axos.exec.supervisor import Supervisor  # noqa: E402
from axos.store import (TransitionGate, TransitionRejected,  # noqa: E402
                        open_store, migrate)

SCHED_SRC = os.path.join(os.path.dirname(HERE), "exec",
                         "scheduler.py")
SCHED_SRC = os.path.abspath(SCHED_SRC)
assert os.path.isfile(SCHED_SRC), SCHED_SRC


class SchedulerBase(unittest.TestCase):
    H = 0.4

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r10-")
        self.db = os.path.join(self.tmp, "t.db")
        self._sups: list = []
        self._scheds: list = []
        self._temp_stores: list = []
        self.sup = self._new_sup(actor="test-r10")
        self.gate = self.sup.gate
        self.gate.create_task("t", {"objective": "x"}, {"usd": 1}, "test")

    def tearDown(self):
        for sc in self._scheds:
            try:
                sc.close()
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
        s = Supervisor(self.db, actor=actor or f"test-r10-{len(self._sups)}",
                       heartbeat_interval_s=H or self.H)
        self._sups.append(s)
        return s

    def _new_sched(self, sup=None, scheduler_id=None, boot_ready=None,
                   actor=None, poll_interval_s=0.2, lease_ttl_s=60.0,
                   max_concurrent_jobs=4, batch_size=10,
                   scheduler_stale_after_s=2.0) -> Scheduler:
        cfg = SchedulerConfig(poll_interval_s=poll_interval_s,
                              lease_ttl_s=lease_ttl_s,
                              max_concurrent_jobs=max_concurrent_jobs,
                              batch_size=batch_size,
                              scheduler_stale_after_s=
                              scheduler_stale_after_s)
        sc = Scheduler(
            self.db, sup or self.sup, cfg,
            scheduler_id=scheduler_id or f"r10-{len(self._scheds)}",
            boot_ready=(lambda: True) if boot_ready is None else boot_ready,
            actor=actor)
        self._scheds.append(sc)
        return sc

    def _mk_job(self, jid, task_id="t", gate=None):
        g = gate or self.gate
        return g.create_job(jid, task_id, None, "test")

    def _temp_gate(self) -> TransitionGate:
        store = open_store(self.db)
        migrate(store)
        self._temp_stores.append(store)
        return TransitionGate(store)

    def _wait(self, pred, timeout=20.0, interval=0.02):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("timed out waiting for condition")

    def _wait_status(self, gate, jid, states, timeout=25.0):
        if isinstance(states, str):
            states = (states,)
        self._wait(lambda: gate.get_job(jid)["status"] in states, timeout)
        return gate.get_job(jid)

    def _run_barrier(self, fns, timeout=90):
        """Run fns concurrently, aligned on a barrier; return {i: result}.

        Exceptions are captured per thread (never kill the thread); the
        caller asserts on them."""
        barrier = threading.Barrier(len(fns))
        results: dict = {}

        def run(i, fn):
            try:
                barrier.wait(timeout=15)
                results[i] = fn()
            except Exception as exc:  # noqa: BLE001 - recorded, asserted later
                results[i] = exc

        ts = [threading.Thread(target=run, args=(i, fn))
              for i, fn in enumerate(fns)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=timeout)
        self.assertFalse(any(t.is_alive() for t in ts), "worker thread hung")
        return results

    def _claimed_actors(self, gate, jid):
        rows = gate.store.conn.execute(
            "SELECT actor FROM ledger WHERE event_type='job.claimed'"
            " AND json_extract(payload,'$.job_id')=?"
            " ORDER BY seq", (jid,)).fetchall()
        return [r["actor"] for r in rows]

    def _job_transitioned(self, gate, jid, frm, to):
        rows = gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='job.transition'"
        ).fetchall()
        for r in rows:
            p = json.loads(r["payload"])
            if (p.get("job_id") == jid and p.get("from") == frm
                    and p.get("to") == to):
                return True
        return False

    def _unreaped_for(self, gate, wid, jid):
        return [s for s in gate.unreaped_proc_spawns()
                if s.get("worker_id") == wid and s.get("job_id") == jid]

    def _active_count(self, gate):
        return gate.store.conn.execute(
            "SELECT COUNT(*) FROM jobs"
            " WHERE status IN ('CLAIMED','RUNNING','COMMITTING')"
        ).fetchone()[0]

    def _attempt_count(self, gate):
        return gate.store.conn.execute(
            "SELECT COUNT(*) FROM recovery_attempts").fetchone()[0]

    @staticmethod
    def _sch_calls_defs():
        """(attribute-call names, defined names) in exec/scheduler.py."""
        with open(SCHED_SRC) as f:
            tree = ast.parse(f.read())
        calls, defs = set(), set()
        for n in ast.walk(tree):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                defs.add(n.name)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
                calls.add(n.func.attr)
        return calls, defs


# ------------------------------------------------------------------ R10-00
class TestR1000(SchedulerBase):
    def test_R10_00_config_validation(self):
        """R10-00: SchedulerConfig is frozen and validated: timings must
        be positive numbers, max_concurrent_jobs/batch_size ints >= 1;
        Scheduler requires a SchedulerConfig."""
        good = dict(poll_interval_s=0.2, lease_ttl_s=60.0,
                    max_concurrent_jobs=2, batch_size=5)
        cfg = SchedulerConfig(**good)
        self.assertEqual(cfg.max_concurrent_jobs, 2)
        self.assertEqual(cfg.scheduler_stale_after_s, 2.0)  # sane default
        with self.assertRaises(Exception):
            cfg.poll_interval_s = 99.0  # frozen dataclass
        bad_cases = [
            dict(poll_interval_s=0), dict(poll_interval_s=-1.0),
            dict(lease_ttl_s=0), dict(lease_ttl_s=-5.0),
            dict(lease_ttl_s=float("nan")),
            dict(max_concurrent_jobs=0), dict(max_concurrent_jobs=-3),
            dict(max_concurrent_jobs=2.5), dict(max_concurrent_jobs=True),
            dict(batch_size=0), dict(batch_size=-1),
            dict(poll_interval_s=True), dict(poll_interval_s="0.2"),
            dict(scheduler_stale_after_s=0),
            dict(scheduler_stale_after_s=-0.5),
            dict(scheduler_stale_after_s=float("nan")),
            dict(scheduler_stale_after_s=True),
        ]
        for bad in bad_cases:
            kw = dict(good)
            kw.update(bad)
            with self.assertRaises(ValueError, msg=f"{bad}"):
                SchedulerConfig(**kw)
        with self.assertRaises(TypeError):
            Scheduler(self.db, self.sup,
                      {"poll_interval_s": 0.2})  # not a SchedulerConfig


# ------------------------------------------------------------------ R10-01
class TestR1001(SchedulerBase):
    def test_R10_01_pending_discovered_and_admitted(self):
        """R10-01: a PENDING job is discovered by evaluate_once and
        admitted: job CLAIMED, worker proc spawned, worker.proc_spawned
        evidence present for (worker_id, job_id)."""
        self._mk_job("j-101")
        sched = self._new_sched()
        rep = sched.evaluate_once()
        self.assertEqual(rep["detail"], "ok")
        self.assertEqual(rep["admitted"], 1)
        self.assertEqual(rep["errors"], [])
        job = self.gate.get_job("j-101")
        self.assertEqual(job["status"], "CLAIMED")
        wid = job["owner_worker_id"]
        self.assertTrue(wid.startswith("sched-"))
        self.assertIn(wid, self.sup._procs)
        self.assertIsNone(self.sup._procs[wid].popen.poll())  # alive
        self.assertEqual(len(self._unreaped_for(self.gate, wid, "j-101")), 1)


# ------------------------------------------------------------------ R10-02
class TestR1002(SchedulerBase):
    def test_R10_02_non_pending_never_scheduled(self):
        """R10-02: jobs in CLAIMED, RUNNING, BLOCKED, FAILED, COMPLETE,
        UNCERTAIN are never scheduled: evaluate_once admits/redispatches
        nothing and leaves every row byte-identical."""
        g = self.gate
        # CLAIMED by a non-scheduler owner (orphan path only matches sched-%)
        self._mk_job("j-cl")
        g.claim_job("j-cl", "w-other", 60.0, "test")
        # RUNNING
        self._mk_job("j-run")
        g.claim_job("j-run", "w-run", 60.0, "test")
        self.sup._ensure_worker_row("w-run")
        g.transition_job("j-run", "RUNNING", "worker:w-run")
        # FAILED
        self._mk_job("j-fail")
        g.claim_job("j-fail", "w-fail", 60.0, "test")
        tok = g.get_job("j-fail")["fencing_token"]
        g.fail_job_execution("j-fail", "w-fail", tok, actor="test",
                             reason="r10-02")
        # BLOCKED
        self._mk_job("j-block")
        g.claim_job("j-block", "w-block", 60.0, "test")
        tok = g.get_job("j-block")["fencing_token"]
        g.fail_job_execution("j-block", "w-block", tok, actor="test",
                             reason="r10-02")
        g.transition_job("j-block", "BLOCKED", "test")
        # COMPLETE via a real worker running the R5 contract
        self._mk_job("j-done")
        self.sup.start_worker("w-done", "j-done",
                              {"kind": "success_immediate"})
        self._wait_status(g, "j-done", "COMPLETE", timeout=30.0)
        # UNCERTAIN
        self._mk_job("j-unc")
        g.claim_job("j-unc", "w-unc", 60.0, "test")
        self.sup._ensure_worker_row("w-unc")
        g.transition_job("j-unc", "RUNNING", "worker:w-unc")
        g.transition_job("j-unc", "UNCERTAIN", "test")

        before = {jid: dict(g.get_job(jid)) for jid in
                  ("j-cl", "j-run", "j-fail", "j-block", "j-done", "j-unc")}
        procs_before = dict(self.sup._procs)
        sched = self._new_sched()
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["redispatched"], 0)
        for jid, row0 in before.items():
            row1 = g.get_job(jid)
            self.assertEqual(row1["status"], row0["status"], jid)
            self.assertEqual(row1["owner_worker_id"],
                             row0["owner_worker_id"], jid)
            self.assertEqual(row1["fencing_token"], row0["fencing_token"],
                             jid)
        self.assertEqual(set(self.sup._procs), set(procs_before))


# ------------------------------------------------------------------ R10-03
class TestR1003(SchedulerBase):
    def test_R10_03_claim_atomic_under_threads(self):
        """R10-03: threads racing claim_job_bounded on one job -> exactly
        one True; the row carries the full triple + one job.claimed
        event; losers get False, never an exception."""
        self._mk_job("j-103")

        def make_fn(i):
            def fn():
                store = open_store(self.db)
                migrate(store)
                try:
                    gg = TransitionGate(store)
                    return gg.claim_job_bounded(
                        "j-103", f"w-103-{i}", 60.0, "test", 4)
                finally:
                    store.close()
            return fn

        res = self._run_barrier([make_fn(i) for i in range(4)])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        self.assertEqual(sum(1 for v in res.values() if v is True), 1)
        job = self.gate.get_job("j-103")
        self.assertEqual(job["status"], "CLAIMED")
        self.assertIsNotNone(job["owner_worker_id"])
        self.assertIsNotNone(job["lease_acquired_at"])
        self.assertIsNotNone(job["lease_expires_at"])
        self.assertIsNotNone(job["fencing_token"])
        self.assertEqual(len(self._claimed_actors(self.gate, "j-103")), 1)


# ------------------------------------------------------------------ R10-04
class TestR1004(SchedulerBase):
    def test_R10_04_two_schedulers_one_job_one_claim(self):
        """R10-04: two Scheduler instances racing one PENDING job ->
        exactly one claim total, one dispatch, one job.claimed event."""
        self._mk_job("j-104")
        s1 = self._new_sched(scheduler_id="a")
        s2 = self._new_sched(scheduler_id="b")
        res = self._run_barrier([s1.evaluate_once, s2.evaluate_once])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        total = sum(v["admitted"] for v in res.values())
        self.assertEqual(total, 1)
        self.assertEqual(len(self._claimed_actors(self.gate, "j-104")), 1)
        job = self.gate.get_job("j-104")
        self.assertEqual(job["status"], "CLAIMED")
        wid = job["owner_worker_id"]
        self.assertIn(wid, self.sup._procs)
        self.assertEqual(len(self._unreaped_for(self.gate, wid, "j-104")), 1)


# ------------------------------------------------------------------ R10-05
class TestR1005(SchedulerBase):
    def test_R10_05_claim_establishes_triple_and_event(self):
        """R10-05: the claim establishes owner_worker_id,
        lease_acquired_at/lease_expires_at (~now+ttl), bumps
        fencing_token 0->1, and appends one job.claimed ledger event
        with the scheduler actor."""
        self._mk_job("j-105")
        sched = self._new_sched(scheduler_id="s5")
        before = sched.store.current_time()
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        job = self.gate.get_job("j-105")
        self.assertTrue(job["owner_worker_id"].startswith("sched-s5-"))
        acq, exp = job["lease_acquired_at"], job["lease_expires_at"]
        self.assertIsNotNone(acq)
        self.assertIsNotNone(exp)
        self.assertAlmostEqual(exp - acq, 60.0, places=2)
        self.assertLessEqual(acq, sched.store.current_time())
        self.assertGreater(exp, before)
        self.assertEqual(job["fencing_token"], 1)  # bumped from 0
        actors = self._claimed_actors(self.gate, "j-105")
        self.assertEqual(actors, ["scheduler:s5"])


# ------------------------------------------------------------------ R10-06
class TestR1006(SchedulerBase):
    def test_R10_06_worker_receives_exact_token(self):
        """R10-06: after dispatch the worker's durable LeaseRef triple
        matches the claim; the worker verifies it (claim_verified in its
        log) and reaches RUNNING (durable CLAIMED->RUNNING transition)."""
        self._mk_job("j-106")
        sched = self._new_sched()
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        job = self.gate.get_job("j-106")
        wid, tok = job["owner_worker_id"], job["fencing_token"]
        # The durable triple is exactly the dispatched identity.
        self.assertTrue(wid.startswith("sched-"))
        self.assertEqual(tok, 1)
        # The worker proves the match itself, then runs.
        self._wait_status(self.gate, "j-106", ("RUNNING", "COMPLETE"))
        with open(self.sup._procs[wid].log_path, errors="replace") as f:
            log = f.read()
        self.assertIn('"claim_verified"', log)
        self.assertTrue(
            self._job_transitioned(self.gate, "j-106", "CLAIMED", "RUNNING"))


# ------------------------------------------------------------------ R10-07
class TestR1007(SchedulerBase):
    def test_R10_07_capacity_bound(self):
        """R10-07: max_concurrent_jobs=2 with 5 PENDING -> admitted <= 2
        and the durable active count (CLAIMED/RUNNING/COMMITTING) <= 2."""
        for i in range(5):
            self._mk_job(f"j-107-{i}")
        sched = self._new_sched(max_concurrent_jobs=2)
        rep = sched.evaluate_once()
        self.assertLessEqual(rep["admitted"], 2)
        self.assertEqual(rep["admitted"], 2)
        self.assertLessEqual(self._active_count(self.gate), 2)
        claimed = [f"j-107-{i}" for i in range(5)
                   if self.gate.get_job(f"j-107-{i}")["status"] == "CLAIMED"]
        self.assertEqual(len(claimed), 2)


# ------------------------------------------------------------------ R10-08
class TestR1008(SchedulerBase):
    def test_R10_08_two_schedulers_capacity_race(self):
        """R10-08: two schedulers racing with capacity 2 over 4 PENDING
        jobs -> combined admitted <= 2, no job claimed twice."""
        for i in range(4):
            self._mk_job(f"j-108-{i}")
        s1 = self._new_sched(scheduler_id="a", max_concurrent_jobs=2)
        s2 = self._new_sched(scheduler_id="b", max_concurrent_jobs=2)
        res = self._run_barrier([s1.evaluate_once, s2.evaluate_once])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        total = sum(v["admitted"] for v in res.values())
        self.assertLessEqual(total, 2)
        self.assertLessEqual(self._active_count(self.gate), 2)
        for i in range(4):
            self.assertLessEqual(
                len(self._claimed_actors(self.gate, f"j-108-{i}")), 1)


# ------------------------------------------------------------------ R10-09
class TestR1009(SchedulerBase):
    def test_R10_09_capacity_full_leaves_pending(self):
        """R10-09: capacity full -> remaining jobs stay PENDING: not
        claimed, not failed, ownerless."""
        for i in range(3):
            self._mk_job(f"j-109-{i}")
        sched = self._new_sched(max_concurrent_jobs=1)
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        states = [self.gate.get_job(f"j-109-{i}")["status"] for i in range(3)]
        self.assertEqual(states.count("CLAIMED"), 1)
        self.assertEqual(states.count("PENDING"), 2)
        for i in range(3):
            job = self.gate.get_job(f"j-109-{i}")
            if job["status"] == "PENDING":
                self.assertIsNone(job["owner_worker_id"])
                self.assertEqual(
                    len(self._claimed_actors(self.gate, f"j-109-{i}")), 0)


# ------------------------------------------------------------------ R10-10
class TestR1010(SchedulerBase):
    def test_R10_10_slot_freed_by_completion_admits_next(self):
        """R10-10: with capacity 1, the first job's natural completion
        frees the slot; the next evaluate admits the deferred PENDING
        job."""
        self._mk_job("j-110-a")
        self._mk_job("j-110-b")
        sched = self._new_sched(max_concurrent_jobs=1)
        rep1 = sched.evaluate_once()
        self.assertEqual(rep1["admitted"], 1)
        self.assertEqual(self.gate.get_job("j-110-a")["status"], "CLAIMED")
        # The dispatched worker runs success_immediate -> COMPLETE via R5.
        self._wait_status(self.gate, "j-110-a", "COMPLETE", timeout=30.0)
        rep2 = sched.evaluate_once()
        self.assertEqual(rep2["admitted"], 1)
        self.assertEqual(self.gate.get_job("j-110-b")["status"], "CLAIMED")


# ------------------------------------------------------------------ R10-11
class TestR1011(SchedulerBase):
    def test_R10_11_dispatch_calls_supervisor_start_worker(self):
        """R10-11: the dispatch path calls supervisor.start_worker with
        the claimed identity and the re-read durable token; the proc is
        spawned and worker.proc_spawned evidence names the job."""
        self._mk_job("j-111")
        sched = self._new_sched()
        calls = []
        orig = self.sup.start_worker

        def rec(worker_id, job_id, **kw):
            calls.append((worker_id, job_id, kw))
            return orig(worker_id, job_id, **kw)

        self.sup.start_worker = rec
        try:
            rep = sched.evaluate_once()
        finally:
            self.sup.start_worker = orig
        self.assertEqual(rep["admitted"], 1)
        self.assertEqual(len(calls), 1)
        worker_id, job_id, kw = calls[0]
        self.assertEqual(job_id, "j-111")
        job = self.gate.get_job("j-111")
        self.assertEqual(worker_id, job["owner_worker_id"])
        self.assertEqual(kw.get("expect_token"), job["fencing_token"])
        self.assertIn(worker_id, self.sup._procs)
        self.assertEqual(len(self._unreaped_for(self.gate, worker_id,
                                               "j-111")), 1)


# ------------------------------------------------------------------ R10-12
class TestR1012(SchedulerBase):
    def test_R10_12_scheduler_never_kills(self):
        """R10-12: a dispatched worker proc stays alive across repeated
        evaluations (same pid, never signalled); the scheduler source
        contains no kill/terminate verbs (AST, mirrors audit A16e)."""
        self._mk_job("j-112")
        sched = self._new_sched()
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        wid = self.gate.get_job("j-112")["owner_worker_id"]
        proc = self.sup._procs[wid].popen
        pid0 = proc.pid
        for _ in range(5):
            r = sched.evaluate_once()
            self.assertEqual(r["admitted"], 0)
            self.assertEqual(r["redispatched"], 0)
            self.assertIsNone(proc.poll())  # still alive, never signalled
            self.assertEqual(proc.pid, pid0)
        calls, _defs = self._sch_calls_defs()
        kill_verbs = {"killpg", "Popen", "kill", "terminate", "kill_worker",
                      "restart_worker", "send_signal"}
        self.assertFalse(calls & kill_verbs, calls & kill_verbs)
        with open(SCHED_SRC) as f:
            src = f.read()
        for tok in ("import signal", "import subprocess", "os.kill"):
            self.assertNotIn(tok, src)


# ------------------------------------------------------------------ R10-13
class TestR1013(SchedulerBase):
    def test_R10_13_no_reclaim_or_release(self):
        """R10-13: the scheduler never calls reclaim_lease/release_lease
        (AST, mirrors audit A16f). Behaviorally: an expired-lease PENDING
        row carrying a stale owner is claimed through the normal bounded
        claim path — never "reclaimed" (no job.lease_reclaimed event)."""
        calls, _defs = self._sch_calls_defs()
        self.assertNotIn("reclaim_lease", calls)
        self.assertNotIn("release_lease", calls)
        # Seed: CLAIMED with a short lease, let it expire, then force the
        # row back to PENDING with the stale owner still attached.
        self._mk_job("j-113")
        self.gate.claim_job("j-113", "w-stale", 0.5, "test")
        self._wait(lambda: any(
            e["job_id"] == "j-113"
            for e in self.gate.observe_expired_leases()), timeout=10.0)
        with self.gate.store.write_txn() as (conn, _now):
            conn.execute("UPDATE jobs SET status='PENDING'"
                         " WHERE job_id='j-113'")
        reclaimed_before = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type="
            "'job.lease_reclaimed' AND json_extract(payload,'$.job_id')"
            "='j-113'").fetchone()[0]
        claimed_before = self._claimed_actors(self.gate, "j-113")
        sched = self._new_sched()
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        job = self.gate.get_job("j-113")
        self.assertEqual(job["status"], "CLAIMED")
        self.assertTrue(job["owner_worker_id"].startswith("sched-"))
        # The scheduler's admission mints exactly one NEW job.claimed
        # event (its own); the earlier setup claim is untouched history.
        claimed_after = self._claimed_actors(self.gate, "j-113")
        self.assertEqual(len(claimed_after), len(claimed_before) + 1)
        self.assertTrue(claimed_after[-1].startswith("scheduler:"))
        reclaimed_after = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type="
            "'job.lease_reclaimed' AND json_extract(payload,'$.job_id')"
            "='j-113'").fetchone()[0]
        self.assertEqual(reclaimed_after, reclaimed_before)


# ------------------------------------------------------------------ R10-14
class TestR1014(SchedulerBase):
    def test_R10_14_mutations_visible_through_fresh_gate(self):
        """R10-14: every scheduler mutation flows through TransitionGate:
        the claim triple and the spawn evidence are visible through a
        brand-new gate on a new store handle (durable, not in-memory)."""
        self._mk_job("j-114")
        sched = self._new_sched()
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        fresh = self._temp_gate()
        job = fresh.get_job("j-114")
        self.assertEqual(job["status"], "CLAIMED")
        wid = job["owner_worker_id"]
        self.assertTrue(wid.startswith("sched-"))
        self.assertIsNotNone(job["lease_expires_at"])
        self.assertEqual(job["fencing_token"], 1)
        self.assertEqual(len(self._claimed_actors(fresh, "j-114")), 1)
        self.assertEqual(len(self._unreaped_for(fresh, wid, "j-114")), 1)


# ------------------------------------------------------------------ R10-15
class TestR1015(SchedulerBase):
    def test_R10_15_claimed_undispatched_redispatched_same_identity(self):
        """R10-15: claimed-but-undispatched (crash between claim and
        dispatch) -> a new scheduler re-dispatches under the SAME
        worker_id and fencing_token; no second claim is minted."""
        self._mk_job("j-115")
        # Simulate the crash: gate-level claim by a scheduler identity,
        # no worker ever spawned.
        self.assertTrue(
            self.gate.claim_job("j-115", "sched-s0-crash15", 60.0, "test"))
        tok0 = self.gate.get_job("j-115")["fencing_token"]
        claimed_before = len(self._claimed_actors(self.gate, "j-115"))
        sched = self._new_sched(scheduler_id="s0")
        rep = sched.evaluate_once()
        self.assertEqual(rep["redispatched"], 1)
        self.assertEqual(rep["admitted"], 0)
        job = self.gate.get_job("j-115")
        self.assertEqual(job["owner_worker_id"], "sched-s0-crash15")
        self.assertEqual(job["fencing_token"], tok0)
        self.assertEqual(len(self._claimed_actors(self.gate, "j-115")),
                         claimed_before)
        # The re-dispatched worker verifies the same token and runs.
        self._wait_status(self.gate, "j-115", ("RUNNING", "COMPLETE"))
        log = open(self.sup._procs["sched-s0-crash15"].log_path,
                   errors="replace").read()
        self.assertIn('"claim_verified"', log)


# ------------------------------------------------------------------ R10-16
class TestR1016(SchedulerBase):
    def test_R10_16_new_scheduler_reconstructs_from_durable_state(self):
        """R10-16: a fresh Scheduler over the same db needs no local
        state: it leaves the already-dispatched job alone (spawn
        evidence) and admits a new PENDING job."""
        self._mk_job("j-116-a")
        sched_a = self._new_sched(scheduler_id="a")
        rep = sched_a.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        wid_a = self.gate.get_job("j-116-a")["owner_worker_id"]
        sched_a.close()
        self._mk_job("j-116-b")
        sched_b = self._new_sched(scheduler_id="b")
        rep2 = sched_b.evaluate_once()
        self.assertEqual(rep2["admitted"], 1)
        self.assertEqual(rep2["redispatched"], 0)
        # j-116-a untouched: same owner, one spawn, original proc.
        job_a = self.gate.get_job("j-116-a")
        self.assertEqual(job_a["owner_worker_id"], wid_a)
        self.assertEqual(len(self._unreaped_for(self.gate, wid_a,
                                               "j-116-a")), 1)
        # j-116-b admitted by the new scheduler from durable state.
        job_b = self.gate.get_job("j-116-b")
        self.assertEqual(job_b["status"], "CLAIMED")
        self.assertTrue(job_b["owner_worker_id"].startswith("sched-b-"))


# ------------------------------------------------------------------ R10-17
class TestR1017(SchedulerBase):
    def test_R10_17_second_evaluate_is_noop(self):
        """R10-17: evaluating twice -> the second pass is a no-op for the
        already-dispatched job: no second proc, same Popen object."""
        self._mk_job("j-117")
        sched = self._new_sched()
        rep1 = sched.evaluate_once()
        self.assertEqual(rep1["admitted"], 1)
        wid = self.gate.get_job("j-117")["owner_worker_id"]
        proc_before = self.sup._procs[wid].popen
        rep2 = sched.evaluate_once()
        self.assertEqual(rep2["admitted"], 0)
        self.assertEqual(rep2["redispatched"], 0)
        self.assertIs(self.sup._procs[wid].popen, proc_before)
        self.assertEqual(len(self._unreaped_for(self.gate, wid, "j-117")), 1)


# ------------------------------------------------------------------ R10-18
class TestR1018(SchedulerBase):
    def test_R10_18_spawn_evidence_blocks_respawn(self):
        """R10-18: unreaped worker.proc_spawned evidence for
        (worker_id, job_id) blocks any respawn, even though the job row
        (CLAIMED, sched-owned, live lease) looks dispatchable."""
        self._mk_job("j-118")
        sched = self._new_sched()
        self.assertEqual(sched.evaluate_once()["admitted"], 1)
        wid = self.gate.get_job("j-118")["owner_worker_id"]
        # The row looks dispatchable...
        job = self.gate.get_job("j-118")
        self.assertEqual(job["status"], "CLAIMED")
        self.assertTrue(job["owner_worker_id"].startswith("sched-"))
        self.assertGreater(job["lease_expires_at"],
                           self.gate.store.current_time())
        # ...but spawn evidence exists, so no respawn across restarts.
        for _ in range(3):
            r = sched.evaluate_once()
            self.assertEqual(r["redispatched"], 0)
            self.assertEqual(r["admitted"], 0)
        self.assertEqual(len(self._unreaped_for(self.gate, wid, "j-118")), 1)
        self.assertEqual(
            sum(1 for w in self.sup._procs
                if self.sup._procs[w].job_id == "j-118"), 1)


# ------------------------------------------------------------------ R10-19
class TestR1019(SchedulerBase):
    def test_R10_19_wrong_expect_token_exits_7(self):
        """R10-19: a worker launched with the wrong --expect-token exits
        7 (EXIT_STALE_TOKEN) with zero side effects: the job remains
        CLAIMED by the original owner/token and never reaches RUNNING."""
        self._mk_job("j-119")
        self.assertTrue(
            self.gate.claim_job("j-119", "sched-s0-j19", 60.0, "test"))
        tok = self.gate.get_job("j-119")["fencing_token"]
        proc_id = self.sup.start_worker("sched-s0-j19", "j-119",
                                        expect_token=int(tok) + 999)
        info = self.sup._procs["sched-s0-j19"]
        self.assertEqual(info.proc_id, proc_id)
        rc = info.popen.wait(timeout=30)
        self.assertEqual(rc, 7)
        job = self.gate.get_job("j-119")
        self.assertEqual(job["status"], "CLAIMED")
        self.assertEqual(job["owner_worker_id"], "sched-s0-j19")
        self.assertEqual(job["fencing_token"], tok)
        self.assertFalse(
            self._job_transitioned(self.gate, "j-119", "CLAIMED", "RUNNING"))
        with open(info.log_path, errors="replace") as f:
            log = f.read()
        self.assertIn('"stale_token"', log)


# ------------------------------------------------------------------ R10-20
class TestR1020(SchedulerBase):
    def test_R10_20_expired_lease_orphan_not_spawned_r1_recycles(self):
        """R10-20: an expired-lease sched-owned CLAIMED job is NOT
        spawned by the scheduler (R4/R1's domain); R1 reclaim_lease
        still recycles it afterwards."""
        self._mk_job("j-120")
        self.assertTrue(
            self.gate.claim_job("j-120", "sched-s0-j20", 0.5, "test"))
        tok = self.gate.get_job("j-120")["fencing_token"]
        # Wait for the R4 expiry observation (lease actually expired).
        self._wait(lambda: any(
            e["job_id"] == "j-120"
            for e in self.gate.observe_expired_leases()), timeout=10.0)
        sched = self._new_sched()
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["redispatched"], 0)
        self.assertNotIn("sched-s0-j20", self.sup._procs)
        job = self.gate.get_job("j-120")
        self.assertEqual(job["status"], "CLAIMED")
        self.assertEqual(job["owner_worker_id"], "sched-s0-j20")
        # R1 still recycles it: CLAIMED -> PENDING, owner cleared.
        out = self.gate.reclaim_lease(
            "j-120", actor="test", reason="r10-20",
            expected_owner="sched-s0-j20", expected_token=tok)
        self.assertEqual(out["status"], "PENDING")
        self.assertIsNone(out["owner_worker_id"])
        job2 = self.gate.get_job("j-120")
        self.assertEqual(job2["status"], "PENDING")
        self.assertIsNone(job2["owner_worker_id"])


# ------------------------------------------------------------------ R10-21
class TestR1021(SchedulerBase):
    def test_R10_21_kill_authority_lives_outside_scheduler(self):
        """R10-21: the scheduler has no fence/kill path (AST, mirrors
        A16e); supervisor.kill_worker remains the only killer and is
        exercised directly on a scheduler-dispatched worker."""
        calls, _defs = self._sch_calls_defs()
        self.assertFalse(
            calls & {"kill", "terminate", "kill_worker", "restart_worker",
                     "send_signal", "killpg", "Popen"})
        self._mk_job("j-121")
        sched = self._new_sched()
        self.assertEqual(sched.evaluate_once()["admitted"], 1)
        wid = self.gate.get_job("j-121")["owner_worker_id"]
        proc = self.sup._procs[wid].popen
        self.assertIsNone(proc.poll())
        # The kill path exists outside the scheduler: exercise it.
        rec = self.sup.kill_worker(wid)
        self.assertIsNotNone(rec)
        self.assertIsNotNone(proc.poll())
        wrow = self.gate.store.conn.execute(
            "SELECT status FROM workers WHERE worker_id=?", (wid,)).fetchone()
        self.assertEqual(wrow["status"], "DEAD")


# ------------------------------------------------------------------ R10-22
class TestR1022(SchedulerBase):
    def test_R10_22_recovery_incident_and_uncertain_skipped(self):
        """R10-22: a PENDING job with an open recovery incident (STALLED
        verdict => R8's scope) and an UNCERTAIN job are skipped; the
        scheduler creates no recovery_attempts rows and mutates nothing."""
        g = self.gate
        # PENDING job carrying an open scope='recovery' incident.
        self._mk_job("j-122-inc")
        inc = g.find_or_create_recovery_incident(
            "j-122-inc", "STALLED", {"job_id": "j-122-inc"}, "test")
        self.assertIsNone(inc["outcome"])
        # UNCERTAIN job.
        self._mk_job("j-122-unc")
        g.claim_job("j-122-unc", "w-122", 60.0, "test")
        self.sup._ensure_worker_row("w-122")
        g.transition_job("j-122-unc", "RUNNING", "worker:w-122")
        g.transition_job("j-122-unc", "UNCERTAIN", "test")

        attempts_before = self._attempt_count(g)
        sched = self._new_sched()
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["redispatched"], 0)
        self.assertGreaterEqual(rep["skipped"], 1)
        self.assertEqual(g.get_job("j-122-inc")["status"], "PENDING")
        self.assertIsNone(g.get_job("j-122-inc")["owner_worker_id"])
        self.assertEqual(g.get_job("j-122-unc")["status"], "UNCERTAIN")
        self.assertEqual(self._attempt_count(g), attempts_before)
        # The incident is still open: the scheduler did not resolve it.
        self.assertEqual(len(g.open_recovery_incidents(scope="recovery")), 1)


# ------------------------------------------------------------------ R10-23
class TestR1023(SchedulerBase):
    def test_R10_23_blocked_and_paused_never_scheduled(self):
        """R10-23: BLOCKED jobs and PENDING jobs under a PAUSED_FOR_HUMAN
        task are never scheduled; both stay that way across a scheduler
        restart (I-17 stickiness)."""
        g = self.gate
        self._mk_job("j-123-block")
        g.claim_job("j-123-block", "w-123", 60.0, "test")
        tok = g.get_job("j-123-block")["fencing_token"]
        g.fail_job_execution("j-123-block", "w-123", tok, actor="test",
                             reason="r10-23")
        g.transition_job("j-123-block", "BLOCKED", "test")
        g.create_task("tp", {"objective": "x"}, {"usd": 1}, "test")
        g.transition_task("tp", "AUTHORIZED", "test")
        g.transition_task("tp", "PAUSED_FOR_HUMAN", "test",
                          pause_reason="r10-23")
        self._mk_job("j-123-paused", task_id="tp")

        sched = self._new_sched()
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(g.get_job("j-123-block")["status"], "BLOCKED")
        paused = g.get_job("j-123-paused")
        self.assertEqual(paused["status"], "PENDING")
        self.assertIsNone(paused["owner_worker_id"])
        # Restart the scheduler: still never scheduled, states sticky.
        sched.close()
        sched2 = self._new_sched(scheduler_id="after-restart")
        rep2 = sched2.evaluate_once()
        self.assertEqual(rep2["admitted"], 0)
        self.assertEqual(rep2["redispatched"], 0)
        self.assertEqual(g.get_job("j-123-block")["status"], "BLOCKED")
        paused2 = g.get_job("j-123-paused")
        self.assertEqual(paused2["status"], "PENDING")
        self.assertIsNone(paused2["owner_worker_id"])
        self.assertEqual(
            g.store.conn.execute(
                "SELECT status FROM tasks WHERE task_id='tp'").fetchone()
            ["status"], "PAUSED_FOR_HUMAN")


# ------------------------------------------------------------------ R10-24
class TestR1024(SchedulerBase):
    def test_R10_24_completion_only_via_worker_r5_contract(self):
        """R10-24: dispatch -> worker success -> COMPLETE happens only
        through the worker's R5 contract: the job.committed ledger actor
        is worker:*, never scheduler:*."""
        self._mk_job("j-124")
        sched = self._new_sched(scheduler_id="s24")
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        self._wait_status(self.gate, "j-124", "COMPLETE", timeout=30.0)
        rows = self.gate.store.conn.execute(
            "SELECT actor FROM ledger WHERE event_type='job.committed'"
            " AND json_extract(payload,'$.job_id')='j-124'"
            " ORDER BY seq").fetchall()
        self.assertTrue(rows, "no job.committed event")
        for r in rows:
            self.assertTrue(r["actor"].startswith("worker:"),
                            f"completion actor {r['actor']!r}")
            self.assertFalse(r["actor"].startswith("scheduler:"))
        # The claim itself is the scheduler's; the completion is not.
        self.assertEqual(self._claimed_actors(self.gate, "j-124"),
                         ["scheduler:s24"])


# ------------------------------------------------------------------ R10-25
class TestR1025(SchedulerBase):
    def test_R10_25_boot_not_ready_admits_nothing(self):
        """R10-25: boot_ready()=False -> evaluate_once admits nothing
        and claims nothing (detail boot-not-ready); flipping to True ->
        the same scheduler admits."""
        self._mk_job("j-125")
        flag = [False]
        sched = self._new_sched(boot_ready=lambda: flag[0])
        rep = sched.evaluate_once()
        self.assertEqual(rep["detail"], "boot-not-ready")
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["redispatched"], 0)
        job = self.gate.get_job("j-125")
        self.assertEqual(job["status"], "PENDING")
        self.assertIsNone(job["owner_worker_id"])
        self.assertEqual(len(self._claimed_actors(self.gate, "j-125")), 0)
        flag[0] = True
        rep2 = sched.evaluate_once()
        self.assertEqual(rep2["detail"], "ok")
        self.assertEqual(rep2["admitted"], 1)
        self.assertEqual(self.gate.get_job("j-125")["status"], "CLAIMED")


# ------------------------------------------------------------------ R10-26
class TestR1026(SchedulerBase):
    def test_R10_26_background_loop_thread_admits(self):
        """R10-26: start() runs the loop in a background thread; the job
        is admitted from that thread, whose thread-local gate differs
        from the constructing thread's gate."""
        self._mk_job("j-126")
        sched = self._new_sched(poll_interval_s=0.1)
        sched.start()
        try:
            self.assertTrue(sched._thread.is_alive())
            # No evaluate_once on this thread: admission must come from
            # the loop thread.
            self._wait(lambda: self.gate.get_job("j-126")["status"]
                       == "CLAIMED", timeout=20.0)
            tid = sched._thread.ident
            loop_gate = sched._thread_gates.get(tid)
            self.assertIsNotNone(loop_gate)
            self.assertIsNot(loop_gate, sched.gate)
            self.assertIsInstance(loop_gate, TransitionGate)
        finally:
            sched.stop()
        self.assertFalse(sched._thread and sched._thread.is_alive())


# ------------------------------------------------------------------ R10-27
class TestR1027(SchedulerBase):
    def test_R10_27_stop_bounded(self):
        """R10-27: stop() returns within a hard bound (timed):
        strictly less than poll_interval_s + 5.0s."""
        sched = self._new_sched(poll_interval_s=0.5)
        sched.start()
        self.assertTrue(sched._thread.is_alive())
        t0 = time.monotonic()
        sched.stop()
        dt = time.monotonic() - t0
        self.assertLess(dt, 0.5 + 5.0, f"stop() took {dt:.2f}s")
        self.assertFalse(sched._thread and sched._thread.is_alive())


# ------------------------------------------------------------------ R10-28
class TestR1028(SchedulerBase):
    def test_R10_28_crash_before_claim(self):
        """R10-28: the scheduler is dropped before evaluate_once -> the
        job stays PENDING and unclaimed (zero durable effect)."""
        self._mk_job("j-128")
        sched = self._new_sched()
        sched.close()  # crashed before any evaluation
        job = self.gate.get_job("j-128")
        self.assertEqual(job["status"], "PENDING")
        self.assertIsNone(job["owner_worker_id"])
        self.assertEqual(len(self._claimed_actors(self.gate, "j-128")), 0)


# ------------------------------------------------------------------ R10-29
class TestR1029(SchedulerBase):
    def test_R10_29_crash_during_claim_row_never_partial(self):
        """R10-29: racing claims under threads -> the row is either fully
        PENDING or fully CLAIMED-with-triple; never partial (owner set
        without lease, token without owner, ...)."""
        self._mk_job("j-129")

        def make_fn(i):
            def fn():
                store = open_store(self.db)
                migrate(store)
                try:
                    gg = TransitionGate(store)
                    return gg.claim_job_bounded(
                        "j-129", f"w-129-{i}", 60.0, "test", 4)
                finally:
                    store.close()
            return fn

        res = self._run_barrier([make_fn(i) for i in range(8)])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        self.assertEqual(sum(1 for v in res.values() if v is True), 1)
        job = self.gate.get_job("j-129")
        if job["status"] == "PENDING":
            self.assertIsNone(job["owner_worker_id"])
        else:
            self.assertEqual(job["status"], "CLAIMED")
            triple = (job["owner_worker_id"], job["lease_acquired_at"],
                      job["lease_expires_at"], job["fencing_token"])
            self.assertTrue(all(v is not None for v in triple),
                            f"partial claim triple: {triple}")
            self.assertGreater(job["lease_expires_at"],
                               job["lease_acquired_at"])


# ------------------------------------------------------------------ R10-30
class TestR1030(SchedulerBase):
    def test_R10_30_crash_between_claim_and_dispatch(self):
        """R10-30: start_worker raises mid-evaluate -> the claim is LEFT
        STANDING (error recorded, no rollback); a new scheduler
        re-dispatches under the identical owner/token with no second
        claim."""
        self._mk_job("j-130")
        sched_a = self._new_sched(scheduler_id="s0")
        orig = self.sup.start_worker

        def boom(worker_id, job_id, **kw):
            raise RuntimeError("injected dispatch crash")

        self.sup.start_worker = boom
        try:
            rep = sched_a.evaluate_once()
        finally:
            self.sup.start_worker = orig
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(len(rep["errors"]), 1)
        self.assertEqual(rep["errors"][0]["phase"], "dispatch")
        self.assertEqual(rep["errors"][0]["job_id"], "j-130")
        job = self.gate.get_job("j-130")
        self.assertEqual(job["status"], "CLAIMED")
        wid, tok = job["owner_worker_id"], job["fencing_token"]
        self.assertTrue(wid.startswith("sched-s0-"))
        claimed_before = len(self._claimed_actors(self.gate, "j-130"))
        # A fresh scheduler re-dispatches the standing claim identically.
        sched_b = self._new_sched(scheduler_id="s1")
        rep2 = sched_b.evaluate_once()
        self.assertEqual(rep2["redispatched"], 1)
        job2 = self.gate.get_job("j-130")
        self.assertEqual(job2["owner_worker_id"], wid)
        self.assertEqual(job2["fencing_token"], tok)
        self.assertEqual(len(self._claimed_actors(self.gate, "j-130")),
                         claimed_before)
        self._wait_status(self.gate, "j-130", ("RUNNING", "COMPLETE"))


# ------------------------------------------------------------------ R10-31
class TestR1031(SchedulerBase):
    def test_R10_31_crash_after_spawn_no_second_worker(self):
        """R10-31: proc_spawned evidence exists (crash after spawn) ->
        the restarted scheduler does NOT spawn a second worker for the
        same claim identity."""
        self._mk_job("j-131")
        sched_a = self._new_sched(scheduler_id="a")
        self.assertEqual(sched_a.evaluate_once()["admitted"], 1)
        wid = self.gate.get_job("j-131")["owner_worker_id"]
        proc_before = self.sup._procs[wid].popen
        sched_a.close()  # crash after spawn
        sched_b = self._new_sched(scheduler_id="b")
        rep = sched_b.evaluate_once()
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["redispatched"], 0)
        self.assertIs(self.sup._procs[wid].popen, proc_before)
        self.assertEqual(len(self._unreaped_for(self.gate, wid, "j-131")), 1)


# ------------------------------------------------------------------ R10-32
class TestR1032(SchedulerBase):
    def test_R10_32_worker_failure_no_scheduler_intervention(self):
        """R10-32: after dispatch, a worker failure driven through the
        kernel failure path leaves the scheduler inert: the job stays
        FAILED across evaluations; no incident, no attempt, no new
        claim, no new spawn is created by the scheduler."""
        self._mk_job("j-132")
        sched = self._new_sched()
        self.assertEqual(sched.evaluate_once()["admitted"], 1)
        job = self.gate.get_job("j-132")
        wid, tok = job["owner_worker_id"], job["fencing_token"]
        # Failure governed by the kernel path (not the scheduler).
        self.gate.fail_job_execution("j-132", wid, tok, actor="test",
                                     reason="injected-failure")
        claimed_before = len(self._claimed_actors(self.gate, "j-132"))
        attempts_before = self._attempt_count(self.gate)
        proc_before = self.sup._procs[wid].popen
        for _ in range(3):
            r = sched.evaluate_once()
            self.assertEqual(r["admitted"], 0)
            self.assertEqual(r["redispatched"], 0)
        job2 = self.gate.get_job("j-132")
        self.assertEqual(job2["status"], "FAILED")
        self.assertEqual(len(self._claimed_actors(self.gate, "j-132")),
                         claimed_before)
        self.assertEqual(self._attempt_count(self.gate), attempts_before)
        self.assertIs(self.sup._procs[wid].popen, proc_before)
        # The failure is recorded under the injector's actor, not the
        # scheduler's.
        rows = self.gate.store.conn.execute(
            "SELECT actor FROM ledger WHERE event_type='job.failed'"
            " AND json_extract(payload,'$.job_id')='j-132'").fetchall()
        if rows:
            for r_ in rows:
                self.assertFalse(r_["actor"].startswith("scheduler:"))


# ------------------------------------------------------------------ R10-33
class TestR1033(SchedulerBase):
    def test_R10_33_admission_order_created_at_job_id(self):
        """R10-33: admission order matches ORDER BY created_at, job_id:
        with capacity 1, five sequential PENDING jobs are admitted in
        creation order, one per evaluate."""
        jids = [f"j-133-{i:02d}" for i in range(5)]
        for jid in jids:
            self._mk_job(jid)
        expected = [r["job_id"] for r in
                    self.gate.pending_jobs(10)]
        self.assertEqual(expected, jids)
        sched = self._new_sched(max_concurrent_jobs=1)
        admitted = []
        for jid in jids:
            rep = sched.evaluate_once()
            self.assertEqual(rep["admitted"], 1, jid)
            job = self.gate.get_job(jid)
            self.assertEqual(job["status"], "CLAIMED", jid)
            admitted.append(jid)
            # Free the slot through the kernel failure path (not the
            # scheduler).
            self.gate.fail_job_execution(
                jid, job["owner_worker_id"], job["fencing_token"],
                actor="test", reason="order-test")
        self.assertEqual(admitted, expected)


# ------------------------------------------------------------------ R10-34
class TestR1034(SchedulerBase):
    def test_R10_34_deterministic_across_identical_dbs(self):
        """R10-34: two fresh schedulers over identically-built db states
        make identical admission decisions in identical order."""
        db2 = os.path.join(self.tmp, "t2.db")
        sup2 = Supervisor(db2, actor="test-r10-34b",
                          heartbeat_interval_s=self.H)
        self._sups.append(sup2)
        gate2 = sup2.gate
        gate2.create_task("t", {"objective": "x"}, {"usd": 1}, "test")
        jids = [f"j-134-{i:02d}" for i in range(4)]
        for jid in jids:
            self._mk_job(jid)
            gate2.create_job(jid, "t", None, "test")
        sched1 = self._new_sched(scheduler_id="d", max_concurrent_jobs=2)
        cfg2 = SchedulerConfig(poll_interval_s=0.2, lease_ttl_s=60.0,
                               max_concurrent_jobs=2, batch_size=10)
        sched2 = Scheduler(db2, sup2, cfg2, scheduler_id="d",
                           boot_ready=lambda: True)
        self._scheds.append(sched2)
        r1 = sched1.evaluate_once()
        r2 = sched2.evaluate_once()
        self.assertEqual(r1, r2)
        self.assertEqual(r1["admitted"], 2)

        def claim_order(g):
            rows = g.store.conn.execute(
                "SELECT json_extract(payload,'$.job_id') AS jid FROM ledger"
                " WHERE event_type='job.claimed' ORDER BY seq").fetchall()
            return [r["jid"] for r in rows]

        self.assertEqual(claim_order(self.gate), claim_order(gate2))
        self.assertEqual(claim_order(self.gate), jids[:2])


# ------------------------------------------------------------------ R10-35
class TestR1035(SchedulerBase):
    def test_R10_35_no_job_creation(self):
        """R10-35: the Scheduler exposes no job/task creation API; its
        source contains no job-row INSERT; and its own actor is refused
        by the I-16 work-creator check."""
        for name in ("create_job", "create_task"):
            self.assertFalse(hasattr(Scheduler, name), name)
        with open(SCHED_SRC) as f:
            src = f.read().upper()
        self.assertNotIn("INSERT INTO JOBS", src)
        # Defense in depth: the scheduler's actor cannot create work.
        sched = self._new_sched(scheduler_id="s35")
        with self.assertRaises(TransitionRejected):
            self.gate.create_job("j-135-x", "t", None, sched.actor)


# ------------------------------------------------------------------ R10-36
class TestR1036(SchedulerBase):
    def test_R10_36_no_job_deletion(self):
        """R10-36: the Scheduler exposes no job/task deletion API and
        its source contains no job-row DELETE."""
        for name in ("delete_job", "remove_job", "destroy_job",
                     "delete_task"):
            self.assertFalse(hasattr(Scheduler, name), name)
        with open(SCHED_SRC) as f:
            src = f.read().upper()
        self.assertNotIn("DELETE FROM JOBS", src)


# ------------------------------------------------------------------ R10-37
class TestR1037(SchedulerBase):
    def test_R10_37_no_reconciliation_machinery(self):
        """R10-37: no reconciliation/circuit-breaker/load-shedding
        behavior in the scheduler (AST over calls+defs, mirroring
        audit A16i)."""
        calls, defs = self._sch_calls_defs()
        banned = {"reconcile", "reconcile_desired_state",
                  "trip_circuit_breaker", "circuit_breaker", "load_shed",
                  "load_shedding"}
        self.assertFalse((calls | defs) & banned, (calls | defs) & banned)
        with open(SCHED_SRC) as f:
            src = f.read()
        for tok in ("circuit_breaker", "load_shed"):
            self.assertNotIn(tok, src)


# ------------------------------------------------------------------ R10-38
class TestR1038(SchedulerBase):
    def test_R10_38_no_policy_machinery(self):
        """R10-38: no recovery/policy machinery in the scheduler (AST
        over calls+defs+imports, mirroring audit A16j)."""
        calls, defs = self._sch_calls_defs()
        banned = {"finalize", "finalize_execution", "schedule_pending_jobs",
                  "retry_job", "recover_incident", "recovery_rung",
                  "recovery_budget", "select_rung", "escalate"}
        self.assertFalse((calls | defs) & banned, (calls | defs) & banned)
        with open(SCHED_SRC) as f:
            src = f.read()
        tree = ast.parse(src)
        imports = set()
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                imports.update(a.name for a in n.names)
            elif isinstance(n, ast.ImportFrom) and n.module:
                imports.add(n.module)
        self.assertFalse(
            any("exec.recovery" in m or "exec.policy" in m for m in imports),
            imports)


# ------------------------------------------------------------------ R10-39
class TestR1039(SchedulerBase):
    def test_R10_39_three_schedulers_race(self):
        """R10-39: three schedulers x one PENDING job -> exactly one
        claim; x 6 jobs with capacity 2 -> combined admitted <= 2 with
        no duplicate dispatch identities."""
        self._mk_job("j-139-one")
        scheds = [self._new_sched(scheduler_id=c) for c in ("a", "b", "c")]
        res = self._run_barrier([s.evaluate_once for s in scheds])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        self.assertEqual(sum(v["admitted"] for v in res.values()), 1)
        self.assertEqual(len(self._claimed_actors(self.gate, "j-139-one")),
                         1)
        # Let the winner's worker finish so capacity is clean for phase 2.
        self._wait_status(self.gate, "j-139-one", "COMPLETE", timeout=30.0)

        jids = [f"j-139-{i:02d}" for i in range(6)]
        for jid in jids:
            self._mk_job(jid)
        scheds2 = [self._new_sched(scheduler_id=c, max_concurrent_jobs=2)
                   for c in ("d", "e", "f")]
        res2 = self._run_barrier([s.evaluate_once for s in scheds2])
        for v in res2.values():
            self.assertNotIsInstance(v, Exception)
        total = sum(v["admitted"] for v in res2.values())
        self.assertLessEqual(total, 2)
        self.assertLessEqual(self._active_count(self.gate), 2)
        owners = []
        for jid in jids:
            actors = self._claimed_actors(self.gate, jid)
            self.assertLessEqual(len(actors), 1, jid)
            if actors:
                owners.append(self.gate.get_job(jid)["owner_worker_id"])
        self.assertEqual(len(owners), total)
        self.assertEqual(len(set(owners)), len(owners))  # no dup identities


# ------------------------------------------------------------------ R10-40
class TestR1040(SchedulerBase):
    def test_R10_40_full_r1_r9_composition(self):
        """R10-40: PENDING -> scheduler dispatch -> RUNNING -> COMPLETE
        via the R5 contract; then the failure leg: expiring lease -> R4
        expiry observation -> R1 reclaim -> PENDING -> scheduler
        re-admits -> success."""
        g = self.gate
        # Leg 1: the happy path through the scheduler.
        self._mk_job("j-140-a")
        sched = self._new_sched(scheduler_id="s40")
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        self._wait_status(g, "j-140-a", "COMPLETE", timeout=30.0)
        rows = g.store.conn.execute(
            "SELECT actor FROM ledger WHERE event_type='job.committed'"
            " AND json_extract(payload,'$.job_id')='j-140-a'").fetchall()
        self.assertTrue(rows)
        self.assertTrue(all(r["actor"].startswith("worker:") for r in rows))

        # Leg 2: failure. A CLAIMED job whose worker never renews: the
        # lease expires (R4 observes), R1 reclaims -> PENDING, the
        # scheduler re-admits -> worker success.
        self._mk_job("j-140-b")
        self.assertTrue(g.claim_job("j-140-b", "w-140-b", 0.5, "test"))
        tok = g.get_job("j-140-b")["fencing_token"]
        self._wait(lambda: any(
            e["job_id"] == "j-140-b" for e in g.observe_expired_leases()),
            timeout=10.0)
        out = g.reclaim_lease("j-140-b", actor="test", reason="r10-40",
                              expected_owner="w-140-b",
                              expected_token=tok)
        self.assertEqual(out["status"], "PENDING")
        rep2 = sched.evaluate_once()
        self.assertEqual(rep2["admitted"], 1)
        job = g.get_job("j-140-b")
        self.assertEqual(job["status"], "CLAIMED")
        self.assertTrue(job["owner_worker_id"].startswith("sched-s40-"))
        self._wait_status(g, "j-140-b", "COMPLETE", timeout=30.0)
        rows2 = g.store.conn.execute(
            "SELECT actor FROM ledger WHERE event_type='job.committed'"
            " AND json_extract(payload,'$.job_id')='j-140-b'").fetchall()
        self.assertTrue(rows2)
        self.assertTrue(all(r["actor"].startswith("worker:") for r in rows2))


# ------------------------------------------------------------ race extras
class TestR10Race1(SchedulerBase):
    def test_R10_RACE_1_sixteen_threads_one_claim(self):
        """R10-RACE-1: 16 threads racing claim_job_bounded on one job ->
        exactly one True; every loser observes False (never an
        exception, never a partial row)."""
        self._mk_job("j-r1")

        def make_fn(i):
            def fn():
                store = open_store(self.db)
                migrate(store)
                try:
                    gg = TransitionGate(store)
                    return gg.claim_job_bounded(
                        "j-r1", f"w-r1-{i}", 60.0, "test", 8)
                finally:
                    store.close()
            return fn

        res = self._run_barrier([make_fn(i) for i in range(16)])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        self.assertEqual(sum(1 for v in res.values() if v is True), 1)
        self.assertEqual(sum(1 for v in res.values() if v is False), 15)
        self.assertEqual(len(self._claimed_actors(self.gate, "j-r1")), 1)


class TestR10Race2(SchedulerBase):
    def test_R10_RACE_2_same_instance_double_evaluate(self):
        """R10-RACE-2: two threads driving evaluate_once on the SAME
        scheduler instance -> one claim, one spawn (the thread-local
        gate pattern holds; no duplicate dispatch)."""
        self._mk_job("j-r2")
        sched = self._new_sched(scheduler_id="race2")
        res = self._run_barrier([sched.evaluate_once, sched.evaluate_once])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        total = sum(v["admitted"] for v in res.values())
        self.assertEqual(total, 1)
        self.assertEqual(len(self._claimed_actors(self.gate, "j-r2")), 1)
        wid = self.gate.get_job("j-r2")["owner_worker_id"]
        self.assertEqual(len(self._unreaped_for(self.gate, wid, "j-r2")), 1)


class TestR10Race3(SchedulerBase):
    def test_R10_RACE_3_orphan_double_dispatch_exactly_once(self):
        """R10-RACE-3: two schedulers (separate supervisors) racing one
        orphaned CLAIMED dispatch -> the documented residual: two worker
        processes may spawn under the same identity, but exactly one
        wins the atomic CLAIMED->RUNNING transition and executes; the
        loser exits before executing. No second claim, token unchanged,
        exactly one job.committed. The dispatch idempotency guard
        (_unreaped_spawn_keys) may legitimately deduplicate when one
        scheduler's spawn event lands first, so total_redisp is 1 or 2;
        the exactly-once invariants hold in both cases."""
        supB = self._new_sup(actor="test-r10-race3b")
        wid = "sched-s0-orphan3"
        # Pre-drive the shared worker row to ASSIGNED so neither
        # supervisor races the worker-state machine.
        self.sup._ensure_worker_row(wid)
        self.sup._drive_worker(wid, "ASSIGNED")
        self._mk_job("j-r3")
        self.assertTrue(self.gate.claim_job("j-r3", wid, 60.0, "test"))
        schedA = self._new_sched(sup=self.sup, scheduler_id="ra")
        schedB = self._new_sched(sup=supB, scheduler_id="rb")
        res = self._run_barrier([schedA.evaluate_once, schedB.evaluate_once])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        total_redisp = sum(v["redispatched"] for v in res.values())
        # 1 = the idempotency guard deduplicated (legitimate); 2 = true
        # simultaneity, both attempted (the documented residual). 0
        # would mean the orphan was never dispatched: a real defect.
        self.assertIn(total_redisp, (1, 2))
        # Exactly-once authority: one claim ever, token never re-minted.
        self.assertEqual(len(self._claimed_actors(self.gate, "j-r3")), 1)
        self.assertEqual(self.gate.get_job("j-r3")["fencing_token"], 1)
        # Exactly-once execution. Capture log paths NOW (the fence sweep
        # reaps dead procs, popping them from _procs; fall back to the
        # reap record), then poll every spawned proc to exit before
        # reading: a loser may still be booting when the winner commits,
        # and its log is only complete after exit.
        tracked = []  # (log_path, popen-or-None-if-already-reaped)
        for sup in (self.sup, supB):
            info = sup._procs.get(wid)
            if info is not None:
                tracked.append((info.log_path, info.popen))
                continue
            rec = sup._reaped.get(wid)
            if rec is not None:
                tracked.append((rec["log_path"], None))
        self.assertEqual(len(tracked), total_redisp)
        self._wait_status(self.gate, "j-r3", "COMPLETE", timeout=30.0)
        committed = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type='job.committed'"
            " AND json_extract(payload,'$.job_id')='j-r3'").fetchone()[0]
        self.assertEqual(committed, 1)
        for _lp, popen in tracked:
            if popen is not None:
                self._wait(lambda: popen.poll() is not None, timeout=30.0)
        logs = []
        for lp, _ in tracked:
            with open(lp, errors="replace") as f:
                logs.append(f.read())
        self.assertEqual(sum('"status": "committed"' in lg for lg in logs),
                         1)
        self.assertEqual(
            sum('"status": "run_transition_failed"' in lg for lg in logs),
            total_redisp - 1)


class TestR10Race4(SchedulerBase):
    def test_R10_RACE_4_capacity_race_exactly_n_winners(self):
        """R10-RACE-4: 6 threads racing claim_job_bounded on distinct
        jobs with max_concurrent_jobs=2 -> exactly 2 claims won; the
        capacity predicate and the claim UPDATE share one gate
        transaction, so over-admission is impossible."""
        for i in range(6):
            self._mk_job(f"j-r4-{i}")

        def make_fn(i):
            def fn():
                store = open_store(self.db)
                migrate(store)
                try:
                    gg = TransitionGate(store)
                    return (f"j-r4-{i}", gg.claim_job_bounded(
                        f"j-r4-{i}", f"w-r4-{i}", 60.0, "test", 2))
                finally:
                    store.close()
            return fn

        res = self._run_barrier([make_fn(i) for i in range(6)])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        winners = [jid for jid, won in res.values() if won is True]
        self.assertEqual(len(winners), 2)
        self.assertLessEqual(self._active_count(self.gate), 2)
        # A further claim on an actual loser now fails on capacity, with
        # zero mutation (the winners are nondeterministic, so select a
        # job that is still PENDING).
        losers = [f"j-r4-{i}" for i in range(6)
                  if self.gate.get_job(f"j-r4-{i}")["status"] == "PENDING"]
        self.assertTrue(losers)
        self.assertFalse(self.gate.claim_job_bounded(
            losers[0], "w-late", 60.0, "test", 2))
        self.assertEqual(self.gate.get_job(losers[0])["status"], "PENDING")


class TestR10Race5(SchedulerBase):
    def test_R10_RACE_5_scheduler_vs_external_claimer(self):
        """R10-RACE-5: evaluate_once racing an external direct claim on
        the same PENDING job -> exactly one claimant; if the external
        claimer wins, the scheduler admits nothing and spawns nothing
        for the job."""
        self._mk_job("j-r5")
        sched = self._new_sched(scheduler_id="race5")
        barrier = threading.Barrier(2)

        def external():
            store = open_store(self.db)
            migrate(store)
            try:
                gg = TransitionGate(store)
                barrier.wait(timeout=15)
                return gg.claim_job("j-r5", "w-external", 60.0, "test")
            finally:
                store.close()

        def scheduled():
            barrier.wait(timeout=15)
            return sched.evaluate_once()

        res = self._run_barrier([scheduled, external])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        sched_rep = res[0]
        ext_won = res[1]
        self.assertEqual(len(self._claimed_actors(self.gate, "j-r5")), 1)
        job = self.gate.get_job("j-r5")
        if ext_won:
            self.assertEqual(job["owner_worker_id"], "w-external")
            self.assertEqual(sched_rep["admitted"], 0)
            self.assertFalse(any(
                info.job_id == "j-r5" for info in self.sup._procs.values()))
        else:
            self.assertTrue(job["owner_worker_id"].startswith("sched-"))
            self.assertEqual(sched_rep["admitted"], 1)


class TestR10Race6(SchedulerBase):
    def test_R10_RACE_6_claim_liveness_gates_orphan_redispatch(self):
        """R10-RACE-6: claim-liveness beats. A scheduler that claimed
        (beat planted atomically) but died before dispatch leaves an
        orphan with a FRESH beat -> a racing scheduler's orphan path
        must NOT steal it (redispatched=0). Once the beat goes stale
        (owner dead), the same scheduler re-dispatches under the
        IDENTICAL owner/token with no second claim minted."""
        self._mk_job("j-r6")
        # A "dead scheduler": gate-level claim with scheduler_id plants
        # the beat atomically; no dispatch ever happens, no refresh.
        self.assertTrue(self.gate.claim_job_bounded(
            "j-r6", "sched-dead-r6", 60.0, "test", 4, scheduler_id="dead"))
        tok = self.gate.get_job("j-r6")["fencing_token"]
        claimed_before = len(self._claimed_actors(self.gate, "j-r6"))
        sched = self._new_sched(scheduler_id="live",
                               scheduler_stale_after_s=0.05)
        # Fresh beat -> not stolen.
        rep1 = sched.evaluate_once()
        self.assertEqual(rep1["redispatched"], 0)
        self.assertEqual(rep1["admitted"], 0)
        self.assertNotIn("sched-dead-r6", self.sup._procs)
        job = self.gate.get_job("j-r6")
        self.assertEqual(job["status"], "CLAIMED")
        self.assertEqual(job["owner_worker_id"], "sched-dead-r6")
        # Deterministic wait: poll the liveness predicate itself until
        # the beat ages out (no blind sleep).
        self._wait(lambda: not self.gate.scheduler_claim_live(
            "sched-dead-r6", 0.05, self.gate.store.current_time()),
            timeout=10.0)
        # Stale beat -> re-dispatched under the identical identity, no
        # second claim.
        rep2 = sched.evaluate_once()
        self.assertEqual(rep2["redispatched"], 1)
        job2 = self.gate.get_job("j-r6")
        self.assertEqual(job2["owner_worker_id"], "sched-dead-r6")
        self.assertEqual(job2["fencing_token"], tok)
        self.assertEqual(len(self._claimed_actors(self.gate, "j-r6")),
                         claimed_before)
        self.assertIn("sched-dead-r6", self.sup._procs)
        # The re-dispatched worker verifies the original token and runs.
        self._wait_status(self.gate, "j-r6", ("RUNNING", "COMPLETE"))
