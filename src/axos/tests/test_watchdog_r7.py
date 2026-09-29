"""Phase 1C R7 — watchdog: deterministic STALLED/DEAD detection tests.

STANDARD: real SQLite/WAL, real subprocesses, real process groups, real
signals, real supervisor boot/restart on the same db. No fake process
objects, no in-memory model of the kernel. Deterministic polling instead
of blind sleeps; every spawned process is killed and reaped in tearDown.

Test IDs:
  R7-01  healthy worker with recent progress -> HEALTHY
  R7-02  progress stale while heartbeat fresh -> STALLED
  R7-03  heartbeat alone never counts as progress
  R7-04  fresh progress prevents STALLED (even with zero heartbeats)
  R7-05  worker/process definitively dead -> DEAD
  R7-06  missing heartbeat alone does not imply DEAD
  R7-07  stale progress alone does not imply DEAD
  R7-08  expired lease composes with R4 (read-only); no reclaim by watchdog
  R7-09  watchdog cannot reclaim through R1 bypass (behavioral + static)
  R7-10  watchdog cannot physically fence through R2 bypass (behavioral)
  R7-11  stale fencing token cannot produce a healthy authoritative verdict
  R7-12  verdict evaluation is idempotent
  R7-13  repeated identical evaluations: no duplicate milestone noise
  R7-14  HEALTHY -> STALLED transition
  R7-15  STALLED -> HEALTHY when genuine durable progress resumes
  R7-16  STALLED -> DEAD with definitive death evidence
  R7-17  DEAD does not resurrect execution authority
  R7-18  watchdog refuses before READY
  R7-19  watchdog after R6 restart reconstruction
  R7-20  two concurrent watchdog instances -> one authoritative transition
  R7-21  authoritative store time used, never worker-reported time
  R7-22  configuration thresholds are respected (+ validation)
  R7-23  threshold boundary conditions are deterministic (strict >)
  R7-24  fresh watchdog instance reconstructs the identical verdict
  R7-25  database/read failure fails closed
  R7-26  real heartbeat_loop with zero progress eventually -> STALLED
  R7-27  real process death -> DEAD only with definitive death evidence
  R7-28  full R1-R6 composition: R3 -> R4 -> R7 -> R1 (only path that mutates)
  R7-29  background evaluation loop records verdicts; errors never kill it
  R7-30  WatchdogConfig validation rejects non-positive/non-finite values
"""
import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace
AXOS_DIR = os.path.join(os.path.dirname(os.path.dirname(HERE)), "axos")

from axos.exec.supervisor import Supervisor  # noqa: E402
from axos.exec.watchdog import (Watchdog, WatchdogConfig, WatchdogError,  # noqa: E402
                                WatchdogNotReady)
from axos.store import (TransitionGate, TransitionRejected,  # noqa: E402
                        open_store, migrate)


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


class WatchBase(unittest.TestCase):
    H = 0.4

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r7-")
        self.db = os.path.join(self.tmp, "t.db")
        self._sups: list[Supervisor] = []
        self._watchdogs: list[Watchdog] = []
        self._temp_stores: list = []
        self._extra_popen: list = []
        self.sup = self._new_sup(H=self.H, actor="test-r7")
        self.gate = self.sup.gate
        self.gate.create_task("t", {"objective": "x"}, {"usd": 1},
                              "scheduler")

    def tearDown(self):
        for wd in self._watchdogs:
            try:
                wd.close()
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
        for p in self._extra_popen:
            try:
                if p.poll() is None:
                    os.killpg(p.pid, signal.SIGKILL)
            except Exception:
                pass
            try:
                p.wait(timeout=10)
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
        s = Supervisor(self.db, actor=actor or f"test-r7-{len(self._sups)}",
                       heartbeat_interval_s=H or self.H)
        self._sups.append(s)
        return s

    def _new_wd(self, supervisor=None, readiness=None, **cfg_kw) -> Watchdog:
        cfg = WatchdogConfig(
            heartbeat_stale_s=cfg_kw.get("heartbeat_stale_s", 1.0),
            progress_stale_s=cfg_kw.get("progress_stale_s", 1.5),
            evaluation_interval_s=cfg_kw.get("evaluation_interval_s", 0.2),
        )
        kw = {}
        if supervisor is not None:
            kw["supervisor"] = supervisor
        elif readiness is not None:
            kw["readiness"] = readiness
        else:
            kw["readiness"] = lambda: True
        wd = Watchdog(self.db, cfg, actor=f"test-wd-{len(self._watchdogs)}",
                      **kw)
        self._watchdogs.append(wd)
        return wd

    def _temp_gate(self) -> TransitionGate:
        store = open_store(self.db)
        migrate(store)
        self._temp_stores.append(store)
        return TransitionGate(store)

    def _spawn(self, sup, wid, jid, kind, fresh_job=True, ttl_s=60.0,
               hb=0.3, renew=True, **behavior_kw):
        if fresh_job:
            sup.gate.create_job(jid, "t", "s", "scheduler")
        behavior = {"kind": kind, **behavior_kw}
        return sup.start_worker(wid, jid, behavior, ttl_s=ttl_s,
                                hb_interval_s=hb, renew=renew)

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

    def _progress(self, gate, jid, wid, tok, done=1.0, total=10.0):
        return gate.update_job_progress(jid, wid, tok, done, total,
                                        "test-progress")

    def _verdict_rows(self, gate, jid):
        return gate.watchdog_verdicts_for(jid)

    def _verdict_events(self, gate, jid=None):
        q = ("SELECT payload FROM ledger WHERE event_type='watchdog.verdict'"
             " ORDER BY seq")
        rows = gate.store.conn.execute(q).fetchall()
        evs = [json.loads(r[0]) for r in rows]
        if jid is not None:
            evs = [e for e in evs if e.get("job_id") == jid]
        return evs

    def _reclaim_events(self, gate, jid=None):
        rows = gate.store.conn.execute(
            "SELECT actor, payload FROM ledger WHERE event_type="
            "'job.lease_reclaimed' ORDER BY seq").fetchall()
        evs = []
        for r in rows:
            p = json.loads(r[1])
            p["actor"] = r[0]
            evs.append(p)
        if jid is not None:
            evs = [e for e in evs if e.get("job_id") == jid]
        return evs

    def _fence_events(self, gate, wid=None):
        q = ("SELECT payload FROM ledger WHERE event_type="
             "'worker.fence_enforced' ORDER BY seq")
        rows = gate.store.conn.execute(q).fetchall()
        evs = [json.loads(r[0]) for r in rows]
        if wid is not None:
            evs = [e for e in evs if e.get("worker_id") == wid]
        return evs

    def _wait_verdict(self, wd, jid, verdict, timeout=12.0):
        """Poll evaluate() until the job's latest verdict equals `verdict`."""
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            recs = wd.evaluate()
            last = next((r for r in recs if r["job_id"] == jid), None)
            if last is not None and last["verdict"] == verdict:
                return last
            time.sleep(0.05)
        raise AssertionError(
            f"timed out waiting for verdict {verdict} on {jid}; last={last}")

    def _gate_claim_running(self, gate, jid, wid, ttl=60.0):
        """Gate-level RUNNING job with a worker row but no process."""
        gate.create_job(jid, "t", "s", "scheduler")
        self.sup._ensure_worker_row(wid)
        self.assertTrue(gate.claim_job(jid, wid, ttl, "scheduler"))
        gate.transition_job(jid, "RUNNING", f"worker:{wid}")
        return gate.get_job(jid)


# ------------------------------------------------------------------ R7-01
class TestR701(WatchBase):
    def test_R7_01_healthy_worker_with_recent_progress(self):
        """Recent durable progress + valid ownership + live lease ->
        HEALTHY, recorded exactly once."""
        self._spawn(self.sup, "w-h", "j-h", "heartbeat_loop",
                    ttl_s=60, hb=0.3, renew=True, duration_s=120)
        j = self._wait_claimed(self.gate, "j-h", "w-h")
        tok = j["fencing_token"]
        self._wait(lambda: len(self.gate.heartbeats_for("w-h")) >= 2,
                   timeout=15)
        self._progress(self.gate, "j-h", "w-h", tok, done=3.0)
        wd = self._new_wd()
        recs = wd.evaluate()
        rec = next(r for r in recs if r["job_id"] == "j-h")
        self.assertEqual(rec["verdict"], "HEALTHY")
        self.assertTrue(rec["transition"])
        self.assertEqual(rec["previous_verdict"], None)
        self.assertEqual(rec["fencing_token"], tok)
        rows = self._verdict_rows(self.gate, "j-h")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["verdict"], "HEALTHY")
        self.assertEqual(self._verdict_events(self.gate, "j-h").__len__(), 1)


# ------------------------------------------------------------------ R7-02
class TestR702(WatchBase):
    def test_R7_02_stale_progress_with_fresh_heartbeat_is_stalled(self):
        """Progress goes stale while the heartbeat loop keeps heartbeating:
        STALLED. Heartbeat freshness must not mask stale progress."""
        self._spawn(self.sup, "w-s", "j-s", "heartbeat_loop",
                    ttl_s=60, hb=0.2, renew=True, duration_s=120)
        j = self._wait_claimed(self.gate, "j-s", "w-s")
        tok = j["fencing_token"]
        self._progress(self.gate, "j-s", "w-s", tok, done=1.0)
        wd = self._new_wd(progress_stale_s=1.0)
        rec = self._wait_verdict(wd, "j-s", "STALLED", timeout=12)
        ev = rec["evidence"]
        # Heartbeats kept flowing the whole time: liveness is fresh.
        self.assertFalse(ev["heartbeat_stale"],
                         f"heartbeat should be fresh: {ev}")
        self.assertIsNotNone(ev["last_heartbeat_ts"])
        self.assertLess(ev["heartbeat_age_s"], 1.0)
        # ...but progress is stale: that alone drives STALLED.
        self.assertTrue(ev["progress_stale"])
        self.assertGreater(ev["progress_age_s"], 1.0)
        self.assertIsNone(ev["death_evidence"])
        self.assertFalse(ev["lease_expired"])


# ------------------------------------------------------------------ R7-03
class TestR703(WatchBase):
    def test_R7_03_heartbeat_alone_never_counts_as_progress(self):
        """A heartbeat_loop that never records progress becomes STALLED
        even though heartbeats are continuous: the verdict's progress
        baseline stays at claim time, never advanced by heartbeats."""
        self._spawn(self.sup, "w-hb", "j-hb", "heartbeat_loop",
                    ttl_s=60, hb=0.2, renew=True, duration_s=120)
        j = self._wait_claimed(self.gate, "j-hb", "w-hb")
        self._wait(lambda: len(self.gate.heartbeats_for("w-hb")) >= 5,
                   timeout=15)
        # No progress ever recorded.
        self.assertIsNone(self.gate.get_job("j-hb")["progress_updated_at"])
        wd = self._new_wd(progress_stale_s=1.0)
        rec = self._wait_verdict(wd, "j-hb", "STALLED", timeout=12)
        ev = rec["evidence"]
        self.assertIsNone(ev["progress_updated_at"])
        # Baseline is the claim-time lease acquisition, not a heartbeat.
        self.assertAlmostEqual(ev["progress_baseline_ts"],
                               j["lease_acquired_at"], delta=2.0)
        self.assertFalse(ev["heartbeat_stale"])


# ------------------------------------------------------------------ R7-04
class TestR704(WatchBase):
    def test_R7_04_fresh_progress_prevents_stalled(self):
        """Fresh durable progress -> HEALTHY even with zero heartbeats on
        record: progress is the authoritative signal, heartbeat absence
        alone changes nothing."""
        j = self._gate_claim_running(self.gate, "j-fp", "w-fp", ttl=600)
        tok = j["fencing_token"]
        self.assertEqual(self.gate.heartbeats_for("w-fp"), [])
        self._progress(self.gate, "j-fp", "w-fp", tok, done=2.0)
        wd = self._new_wd(progress_stale_s=30.0)
        recs = wd.evaluate()
        rec = next(r for r in recs if r["job_id"] == "j-fp")
        self.assertEqual(rec["verdict"], "HEALTHY")
        self.assertIsNone(rec["evidence"]["last_heartbeat_ts"])


# ------------------------------------------------------------------ R7-05
class TestR705(WatchBase):
    def test_R7_05_definitively_dead_process_is_dead(self):
        """SIGKILL + reap: the process is definitively absent (ESRCH) ->
        DEAD with process-identity death evidence."""
        self._spawn(self.sup, "w-d", "j-d", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-d", "w-d")
        pid = self.sup._procs["w-d"].popen.pid
        popen = self.sup._procs["w-d"].popen
        os.killpg(pid, signal.SIGKILL)
        popen.wait(timeout=10)  # reap: ESRCH afterwards, no zombie
        self.assertTrue(_proc_gone(pid))
        wd = self._new_wd()
        rec = self._wait_verdict(wd, "j-d", "DEAD", timeout=12)
        ev = rec["evidence"]
        self.assertIsNotNone(ev["death_evidence"])
        self.assertEqual(ev["death_evidence"]["kind"],
                         "process_identity_dead")
        self.assertEqual(ev["death_evidence"]["classification"], "dead")


# ------------------------------------------------------------------ R7-06
class TestR706(WatchBase):
    def test_R7_06_missing_heartbeat_alone_is_not_dead(self):
        """SIGSTOP freezes the worker: heartbeats stop, but the process is
        provably alive. The verdict must be STALLED (progress stale), never
        DEAD — a late heartbeat is not death evidence."""
        self._spawn(self.sup, "w-q", "j-q", "heartbeat_loop",
                    ttl_s=120, hb=0.2, renew=True, duration_s=120)
        self._wait_claimed(self.gate, "j-q", "w-q")
        self._wait(lambda: len(self.gate.heartbeats_for("w-q")) >= 3,
                   timeout=15)
        pid = self.sup._procs["w-q"].popen.pid
        t_stop = time.monotonic()
        os.kill(pid, signal.SIGSTOP)  # freeze: no more heartbeats, alive
        try:
            wd = self._new_wd(heartbeat_stale_s=1.0, progress_stale_s=1.0)
            rec = self._wait_verdict(wd, "j-q", "STALLED", timeout=12)
            self.assertIsNone(rec["evidence"]["death_evidence"],
                              "a stopped-but-alive process is not dead")
            # Wait until the heartbeat itself is stale by the store clock,
            # then re-evaluate: still STALLED, never DEAD.
            self._wait(lambda: time.monotonic() - t_stop > 1.3, timeout=10)
            rec2 = next(r for r in wd.evaluate()
                        if r["job_id"] == "j-q")
            ev = rec2["evidence"]
            self.assertTrue(ev["heartbeat_stale"])
            self.assertIsNone(ev["death_evidence"])
            self.assertEqual(rec2["verdict"], "STALLED")
            # And it never becomes DEAD on later evaluations either.
            for _ in range(3):
                rec3 = wd.evaluate()
                r = next(x for x in rec3 if x["job_id"] == "j-q")
                self.assertNotEqual(r["verdict"], "DEAD")
        finally:
            os.kill(pid, signal.SIGCONT)


# ------------------------------------------------------------------ R7-07
class TestR707(WatchBase):
    def test_R7_07_stale_progress_alone_is_not_dead(self):
        """Stale progress with a provably live worker -> STALLED, never
        DEAD: 'progress is old' is weaker evidence than death."""
        self._spawn(self.sup, "w-sp", "j-sp", "wedged",
                    ttl_s=120, renew=False)
        j = self._wait_claimed(self.gate, "j-sp", "w-sp")
        pid = self.sup._procs["w-sp"].popen.pid
        wd = self._new_wd(progress_stale_s=1.0)
        rec = self._wait_verdict(wd, "j-sp", "STALLED", timeout=12)
        self.assertFalse(_proc_gone(pid), "worker must still be alive")
        self.assertIsNone(rec["evidence"]["death_evidence"])
        self.assertNotEqual(rec["verdict"], "DEAD")
        # Still not DEAD after more evaluations.
        rec2 = wd.evaluate()
        r = next(x for x in rec2 if x["job_id"] == "j-sp")
        self.assertEqual(r["verdict"], "STALLED")


# ------------------------------------------------------------------ R7-08
class TestR708(WatchBase):
    def test_R7_08_expired_lease_composes_with_r4(self):
        """Expired lease: R4 observes (read-only), the watchdog reports
        STALLED with lease_expired evidence, and nothing is reclaimed by
        the watchdog — R1 remains the only reclaim path."""
        self._spawn(self.sup, "w-e", "j-e", "heartbeat_loop",
                    ttl_s=2, hb=0.2, renew=False, duration_s=120)
        j = self._wait_claimed(self.gate, "j-e", "w-e")
        tok = j["fencing_token"]
        exp = j["lease_expires_at"]
        g = self._temp_gate()
        self._wait(lambda: g.get_job("j-e")["lease_expires_at"]
                   <= g.store.current_time(), timeout=20)
        # R4 observes the expiry, read-only.
        observed = [ev for ev in g.observe_expired_leases()
                    if ev["job_id"] == "j-e"]
        self.assertEqual(len(observed), 1)
        wd = self._new_wd()
        rec = self._wait_verdict(wd, "j-e", "STALLED", timeout=12)
        self.assertTrue(rec["evidence"]["lease_expired"])
        # The watchdog changed nothing: owner, token, and lease intact.
        j2 = self.gate.get_job("j-e")
        self.assertEqual(j2["owner_worker_id"], "w-e")
        self.assertEqual(j2["fencing_token"], tok)
        self.assertEqual(j2["lease_expires_at"], exp)
        self.assertEqual(self._reclaim_events(self.gate, "j-e"), [])
        # R1 alone performs the reclaim.
        g.reclaim_lease("j-e", actor="system",
                        reason="R7-08: R1 still the only reclaim path",
                        expected_owner="w-e", expected_token=tok)
        j3 = self.gate.get_job("j-e")
        self.assertIsNone(j3["owner_worker_id"])
        self.assertEqual(j3["fencing_token"], tok + 1)
        self.assertEqual(len(self._reclaim_events(self.gate, "j-e")), 1)


# ------------------------------------------------------------------ R7-09
class TestR709(WatchBase):
    def test_R7_09_watchdog_cannot_reclaim_through_r1_bypass(self):
        """Behavioral: repeated watchdog evaluations against a STALLED job
        never move owner/token/lease and never emit job.lease_reclaimed.
        Static: exec/watchdog.py contains no reclaim_lease reference."""
        self._spawn(self.sup, "w-nr", "j-nr", "wedged",
                    ttl_s=120, renew=False)
        j = self._wait_claimed(self.gate, "j-nr", "w-nr")
        tok, exp = j["fencing_token"], j["lease_expires_at"]
        wd = self._new_wd(progress_stale_s=0.5)
        self._wait_verdict(wd, "j-nr", "STALLED", timeout=12)
        for _ in range(3):
            wd.evaluate()
        j2 = self.gate.get_job("j-nr")
        self.assertEqual(j2["owner_worker_id"], "w-nr")
        self.assertEqual(j2["fencing_token"], tok)
        self.assertEqual(j2["lease_expires_at"], exp)
        self.assertEqual(self._reclaim_events(self.gate, "j-nr"), [])
        # Static: no reclaim_lease *call* anywhere in exec/watchdog.py
        # (AST, not substring — the docstring names the forbidden call).
        import ast as _ast
        tree = _ast.parse(open(
            os.path.join(AXOS_DIR, "exec", "watchdog.py")).read())
        calls = {_n.func.attr for _n in _ast.walk(tree)
                 if isinstance(_n, _ast.Call)
                 and isinstance(_n.func, _ast.Attribute)}
        self.assertNotIn("reclaim_lease", calls)


# ------------------------------------------------------------------ R7-10
class TestR710(WatchBase):
    def test_R7_10_watchdog_cannot_fence_through_r2_bypass(self):
        """Behavioral: the watchdog never kills the process group and
        never emits worker.fence_enforced. Static: no kill/subprocess
        capability in exec/watchdog.py."""
        self._spawn(self.sup, "w-nf", "j-nf", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-nf", "w-nf")
        pid = self.sup._procs["w-nf"].popen.pid
        wd = self._new_wd(progress_stale_s=0.5)
        self._wait_verdict(wd, "j-nf", "STALLED", timeout=12)
        for _ in range(3):
            wd.evaluate()
            time.sleep(0.1)
        self.assertFalse(_proc_gone(pid), "watchdog must never kill")
        self.assertEqual(self._fence_events(self.gate, "w-nf"), [])
        # Static: no process-signalling/spawning capability (AST — the
        # docstring describes the boundary in prose).
        import ast as _ast
        tree = _ast.parse(open(
            os.path.join(AXOS_DIR, "exec", "watchdog.py")).read())
        calls = {_n.func.attr for _n in _ast.walk(tree)
                 if isinstance(_n, _ast.Call)
                 and isinstance(_n.func, _ast.Attribute)}
        imports = {_n.names[0].name for _n in _ast.walk(tree)
                   if isinstance(_n, _ast.Import)}
        imports |= {_n.module for _n in _ast.walk(tree)
                    if isinstance(_n, _ast.ImportFrom) and _n.module}
        for banned in ("killpg", "Popen", "run", "call", "kill",
                       "terminate", "kill_worker", "restart_worker"):
            self.assertNotIn(banned, calls, f"banned call {banned!r}")
        for banned_mod in ("signal", "subprocess"):
            self.assertNotIn(banned_mod, imports,
                             f"banned import {banned_mod!r}")


# ------------------------------------------------------------------ R7-11
class TestR711(WatchBase):
    def test_R7_11_stale_fencing_token_cannot_yield_healthy_verdict(self):
        """A HEALTHY verdict recorded for epoch N is history, not
        authority: after R1 reclaims (token N+1, owner cleared) the new
        identity has no verdict, and the watchdog emits nothing for it."""
        self._spawn(self.sup, "w-t", "j-t", "wedged",
                    ttl_s=120, renew=False)
        j = self._wait_claimed(self.gate, "j-t", "w-t")
        tok = j["fencing_token"]
        self._progress(self.gate, "j-t", "w-t", tok, done=1.0)
        wd = self._new_wd()
        rec = next(r for r in wd.evaluate() if r["job_id"] == "j-t")
        self.assertEqual(rec["verdict"], "HEALTHY")
        self.gate.reclaim_lease("j-t", actor="test",
                                reason="R7-11: force reclaim",
                                expected_owner="w-t", expected_token=tok,
                                force=True, verdict="STALLED",
                                incident_id="inc-r7-11")
        j2 = self.gate.get_job("j-t")
        self.assertEqual(j2["fencing_token"], tok + 1)
        self.assertIsNone(j2["owner_worker_id"])
        recs = wd.evaluate()
        self.assertFalse([r for r in recs if r["job_id"] == "j-t"],
                         "ownerless job must not be evaluated")
        self.assertIsNone(
            self.gate.latest_watchdog_verdict("j-t", tok + 1),
            "no verdict may exist for the new fencing epoch")
        old = self.gate.latest_watchdog_verdict("j-t", tok)
        self.assertIsNotNone(old)
        self.assertEqual(old["verdict"], "HEALTHY")


# ------------------------------------------------------------------ R7-12
class TestR712(WatchBase):
    def test_R7_12_verdict_evaluation_is_idempotent(self):
        """Evaluating twice against unchanged evidence: same verdict, zero
        new mutations on the second pass."""
        self._spawn(self.sup, "w-i", "j-i", "wedged",
                    ttl_s=120, renew=False)
        j = self._wait_claimed(self.gate, "j-i", "w-i")
        tok = j["fencing_token"]
        self._progress(self.gate, "j-i", "w-i", tok, done=1.0)
        wd = self._new_wd()
        first = next(r for r in wd.evaluate() if r["job_id"] == "j-i")
        self.assertTrue(first["transition"])
        self.assertIsNotNone(first["verdict_id"])
        second = next(r for r in wd.evaluate() if r["job_id"] == "j-i")
        self.assertEqual(second["verdict"], first["verdict"])
        self.assertFalse(second["transition"])
        self.assertIsNone(second["verdict_id"])
        self.assertEqual(len(self._verdict_rows(self.gate, "j-i")), 1)
        self.assertEqual(len(self._verdict_events(self.gate, "j-i")), 1)


# ------------------------------------------------------------------ R7-13
class TestR713(WatchBase):
    def test_R7_13_no_duplicate_milestone_noise(self):
        """Five identical evaluations -> exactly one verdict row and one
        ledger event: no unbounded duplicate milestone stream."""
        self._spawn(self.sup, "w-m", "j-m", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-m", "w-m")
        wd = self._new_wd(progress_stale_s=0.5)
        self._wait_verdict(wd, "j-m", "STALLED", timeout=12)
        # The genuine transitions are HEALTHY (fresh claim) -> STALLED.
        # Everything after that must be silent: four more identical
        # evaluations add no rows and no ledger events.
        for _ in range(4):
            recs = wd.evaluate()
            r = next(x for x in recs if x["job_id"] == "j-m")
            self.assertEqual(r["verdict"], "STALLED")
            self.assertFalse(r["transition"])
        self.assertEqual([r["verdict"]
                          for r in self._verdict_rows(self.gate, "j-m")],
                         ["HEALTHY", "STALLED"])
        self.assertEqual(len(self._verdict_events(self.gate, "j-m")), 2)


# ------------------------------------------------------------------ R7-14
class TestR714(WatchBase):
    def test_R7_14_healthy_to_stalled_transition(self):
        """HEALTHY -> STALLED: two rows, two ledger events, the second
        names HEALTHY as its previous verdict."""
        self._spawn(self.sup, "w-tr", "j-tr", "heartbeat_loop",
                    ttl_s=120, hb=0.2, renew=True, duration_s=120)
        j = self._wait_claimed(self.gate, "j-tr", "w-tr")
        tok = j["fencing_token"]
        self._progress(self.gate, "j-tr", "w-tr", tok, done=1.0)
        wd = self._new_wd(progress_stale_s=1.0)
        first = next(r for r in wd.evaluate() if r["job_id"] == "j-tr")
        self.assertEqual(first["verdict"], "HEALTHY")
        second = self._wait_verdict(wd, "j-tr", "STALLED", timeout=12)
        self.assertEqual(second["previous_verdict"], "HEALTHY")
        self.assertTrue(second["transition"])
        rows = self._verdict_rows(self.gate, "j-tr")
        self.assertEqual([r["verdict"] for r in rows],
                         ["HEALTHY", "STALLED"])
        evs = self._verdict_events(self.gate, "j-tr")
        self.assertEqual(len(evs), 2)
        self.assertEqual(evs[1]["previous_verdict"], "HEALTHY")


# ------------------------------------------------------------------ R7-15
class TestR715(WatchBase):
    def test_R7_15_stalled_to_healthy_on_genuine_progress(self):
        """STALLED -> HEALTHY only when fresh durable progress evidence
        exists; the transition names STALLED as previous."""
        self._spawn(self.sup, "w-sh", "j-sh", "wedged",
                    ttl_s=120, renew=False)
        j = self._wait_claimed(self.gate, "j-sh", "w-sh")
        tok = j["fencing_token"]
        wd = self._new_wd(progress_stale_s=1.0)
        self._wait_verdict(wd, "j-sh", "STALLED", timeout=12)
        self._progress(self.gate, "j-sh", "w-sh", tok, done=5.0)
        rec = next(r for r in wd.evaluate() if r["job_id"] == "j-sh")
        self.assertEqual(rec["verdict"], "HEALTHY")
        self.assertEqual(rec["previous_verdict"], "STALLED")
        self.assertTrue(rec["transition"])
        rows = self._verdict_rows(self.gate, "j-sh")
        # HEALTHY (fresh claim) -> STALLED -> HEALTHY: every step is a
        # genuine transition, each recorded exactly once.
        self.assertEqual([r["verdict"] for r in rows],
                         ["HEALTHY", "STALLED", "HEALTHY"])


# ------------------------------------------------------------------ R7-16
class TestR716(WatchBase):
    def test_R7_16_stalled_to_dead_on_definitive_death(self):
        """STALLED -> DEAD when the process is then definitively killed
        and reaped."""
        self._spawn(self.sup, "w-sd", "j-sd", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-sd", "w-sd")
        pid = self.sup._procs["w-sd"].popen.pid
        popen = self.sup._procs["w-sd"].popen
        wd = self._new_wd(progress_stale_s=1.0)
        self._wait_verdict(wd, "j-sd", "STALLED", timeout=12)
        os.killpg(pid, signal.SIGKILL)
        popen.wait(timeout=10)
        rec = self._wait_verdict(wd, "j-sd", "DEAD", timeout=12)
        self.assertEqual(rec["previous_verdict"], "STALLED")
        self.assertTrue(rec["transition"])
        self.assertEqual(
            rec["evidence"]["death_evidence"]["classification"], "dead")


# ------------------------------------------------------------------ R7-17
class TestR717(WatchBase):
    def test_R7_17_dead_does_not_resurrect_authority(self):
        """After DEAD, even fresh progress evidence cannot flip the
        verdict: DEAD is terminal for the execution identity, and nothing
        restores leases, resets tokens, or changes ownership."""
        self._spawn(self.sup, "w-nr2", "j-nr2", "wedged",
                    ttl_s=120, renew=False)
        j = self._wait_claimed(self.gate, "j-nr2", "w-nr2")
        tok = j["fencing_token"]
        pid = self.sup._procs["w-nr2"].popen.pid
        popen = self.sup._procs["w-nr2"].popen
        wd = self._new_wd()
        os.killpg(pid, signal.SIGKILL)
        popen.wait(timeout=10)
        self._wait_verdict(wd, "j-nr2", "DEAD", timeout=12)
        # The gate still accepts progress from the (dead) owner: durable
        # evidence changed, but the verdict must stay DEAD.
        self._progress(self.gate, "j-nr2", "w-nr2", tok, done=9.0)
        rec = next(r for r in wd.evaluate() if r["job_id"] == "j-nr2")
        self.assertEqual(rec["verdict"], "DEAD")
        self.assertFalse(rec["transition"])
        j2 = self.gate.get_job("j-nr2")
        self.assertEqual(j2["owner_worker_id"], "w-nr2")
        self.assertEqual(j2["fencing_token"], tok)


# ------------------------------------------------------------------ R7-18
class TestR718(WatchBase):
    def test_R7_18_watchdog_refuses_before_ready(self):
        """Evaluation before READY raises WatchdogNotReady and writes
        nothing. Constructing without any readiness source is refused."""
        self._spawn(self.sup, "w-br", "j-br", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-br", "w-br")
        wd = self._new_wd(readiness=lambda: False)
        with self.assertRaises(WatchdogNotReady):
            wd.evaluate()
        self.assertEqual(self._verdict_rows(self.gate, "j-br"), [])
        self.assertEqual(self._verdict_events(self.gate, "j-br"), [])
        # Supervisor-backed readiness follows the boot phase.
        sup2 = self._new_sup(actor="test-r7-18b")
        self.assertEqual(sup2._boot_report["phase"], "READY")
        wd2 = self._new_wd(supervisor=sup2)
        wd2.evaluate()  # READY: allowed
        sup2._boot_report = {"phase": "RECOVERING"}
        with self.assertRaises(WatchdogNotReady):
            wd2.evaluate()
        # No readiness source at all: constructor refuses.
        with self.assertRaises(ValueError):
            Watchdog(self.db, WatchdogConfig(1.0, 1.0, 1.0))


# ------------------------------------------------------------------ R7-19
class TestR719(WatchBase):
    def test_R7_19_watchdog_after_r6_restart_reconstruction(self):
        """Supervisor restart -> R6 boot reconstructs (ADOPT) -> READY ->
        the watchdog evaluates the adopted execution from durable state."""
        self._spawn(self.sup, "w-rs", "j-rs", "heartbeat_loop",
                    ttl_s=120, hb=0.2, renew=True, duration_s=300)
        j = self._wait_claimed(self.gate, "j-rs", "w-rs")
        tok = j["fencing_token"]
        self._progress(self.gate, "j-rs", "w-rs", tok, done=1.0)
        self._wait(lambda: len(self.gate.heartbeats_for("w-rs")) >= 2,
                   timeout=15)
        # Restart: the old supervisor dies, the worker process survives.
        self.sup.close()
        sup2 = self._new_sup(actor="test-r7-19-restart")
        self.assertEqual(sup2._boot_report["phase"], "READY")
        disps = {(d["worker_id"], d["proc_id"]): d["disposition"]
                 for d in sup2._boot_report["dispositions"]}
        self.assertIn("w-rs", [w for w, _ in disps])
        self.assertEqual(disps[("w-rs", next(
            k[1] for k in disps if k[0] == "w-rs"))], "ADOPT")
        wd = self._new_wd(supervisor=sup2)
        recs = wd.evaluate()
        rec = next(r for r in recs if r["job_id"] == "j-rs")
        self.assertEqual(rec["verdict"], "HEALTHY")
        self.assertEqual(rec["fencing_token"], tok)
        self.assertEqual(rec["evidence"]["owner_worker_id"], "w-rs")


# ------------------------------------------------------------------ R7-20
class TestR720(WatchBase):
    def test_R7_20_concurrent_instances_yield_one_transition(self):
        """Two watchdog instances evaluating concurrently produce exactly
        one authoritative verdict transition (CAS inside one write_txn)."""
        self._spawn(self.sup, "w-cc", "j-cc", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-cc", "w-cc")
        self.assertEqual(self._verdict_rows(self.gate, "j-cc"), [])
        # Wait until progress is stale by the STORE clock before racing:
        # both instances must race on the same STALLED transition.
        acquired = self.gate.get_job("j-cc")["lease_acquired_at"]
        deadline = acquired + 0.7
        while self.gate.store.current_time() < deadline:
            time.sleep(0.02)
        # sqlite3 connections are thread-bound: each watchdog instance is
        # constructed, evaluated, and closed on its own thread — the same
        # deployment shape as two independent watchdog services.
        barrier = threading.Barrier(2)
        results = {}
        errors = {}

        def _run(name):
            import traceback as _tb
            try:
                wd = self._new_wd(progress_stale_s=0.5)
                barrier.wait(timeout=10)
                results[name] = wd.evaluate()
            except Exception:  # noqa: BLE001
                errors[name] = _tb.format_exc()

        t1 = threading.Thread(target=_run, args=("a",))
        t2 = threading.Thread(target=_run, args=("b",))
        t1.start()
        t2.start()
        t1.join(timeout=20)
        t2.join(timeout=20)
        self.assertFalse(t1.is_alive() or t2.is_alive())
        self.assertEqual(errors, {}, f"watchdog threads raised: {errors}")
        for name in ("a", "b"):
            r = next(x for x in results[name] if x["job_id"] == "j-cc")
            self.assertEqual(r["verdict"], "STALLED")
        transitions = [next(x for x in results[n] if x["job_id"] == "j-cc")
                       ["transition"] for n in ("a", "b")]
        self.assertEqual(sorted(transitions), [False, True],
                         "exactly one instance may win the transition")
        self.assertEqual(len(self._verdict_rows(self.gate, "j-cc")), 1)
        self.assertEqual(len(self._verdict_events(self.gate, "j-cc")), 1)


# ------------------------------------------------------------------ R7-21
class TestR721(WatchBase):
    def test_R7_21_store_time_beats_worker_reported_time(self):
        """A heartbeat carrying a far-future worker_reported_ts does not
        make the heartbeat fresh: staleness uses the store-stamped ts.
        Static: exec/watchdog.py never references worker_reported_ts."""
        j = self._gate_claim_running(self.gate, "j-wt", "w-wt", ttl=600)
        self._wait_claimed(self.gate, "j-wt", "w-wt")
        g = self._temp_gate()
        future = g.store.current_time() + 10000.0
        g.ingest_heartbeat("w-wt", "p-wt", "j-wt", j["fencing_token"], 1,
                           "RUNNING", "op", "test",
                           worker_reported_ts=future)
        hb = g.heartbeats_for("w-wt")[0]
        self.assertEqual(hb["worker_reported_ts"], future)
        wd = self._new_wd(heartbeat_stale_s=1.0, progress_stale_s=3600.0)
        time.sleep(1.3)  # store-stamped ts ages; reported ts stays "fresh"
        recs = wd.evaluate()
        rec = next(r for r in recs if r["job_id"] == "j-wt")
        ev = rec["evidence"]
        self.assertTrue(ev["heartbeat_stale"],
                        "verdict must follow the store clock, not the"
                        " worker's claimed timestamp")
        self.assertLess(ev["last_heartbeat_ts"], future - 1000.0)
        with open(os.path.join(AXOS_DIR, "exec", "watchdog.py")) as f:
            self.assertNotIn("worker_reported_ts", f.read())


# ------------------------------------------------------------------ R7-22
class TestR722(WatchBase):
    def test_R7_22_configuration_thresholds_are_respected(self):
        """The same durable evidence yields different verdicts under
        different injected configs: thresholds drive the verdict."""
        self._spawn(self.sup, "w-cfg", "j-cfg", "wedged",
                    ttl_s=600, renew=False)
        j = self._wait_claimed(self.gate, "j-cfg", "w-cfg")
        tok = j["fencing_token"]
        self._progress(self.gate, "j-cfg", "w-cfg", tok, done=1.0)
        time.sleep(0.7)
        lenient = self._new_wd(progress_stale_s=3600.0)
        strict = self._new_wd(progress_stale_s=0.5)
        r_lenient = next(r for r in lenient.evaluate()
                         if r["job_id"] == "j-cfg")
        self.assertEqual(r_lenient["verdict"], "HEALTHY")
        r_strict = next(r for r in strict.evaluate()
                        if r["job_id"] == "j-cfg")
        self.assertEqual(r_strict["verdict"], "STALLED")
        self.assertEqual(
            r_strict["evidence"]["thresholds"]["progress_stale_s"], 0.5)

    def test_R7_22b_config_validation(self):
        """Non-positive / non-finite thresholds are refused at
        construction (Supervisor heartbeat_interval_s convention)."""
        for bad in (0, -1.0, float("nan"), float("inf")):
            with self.assertRaises(ValueError, msg=f"{bad!r}"):
                WatchdogConfig(heartbeat_stale_s=bad, progress_stale_s=1.0,
                               evaluation_interval_s=1.0)
            with self.assertRaises(ValueError, msg=f"{bad!r}"):
                WatchdogConfig(heartbeat_stale_s=1.0, progress_stale_s=bad,
                               evaluation_interval_s=1.0)
        with self.assertRaises(ValueError):
            WatchdogConfig(1.0, 1.0, 0.0)


# ------------------------------------------------------------------ R7-23
class TestR723(WatchBase):
    def test_R7_23_threshold_boundary_is_deterministic(self):
        """Stale is strictly (now - ts) > threshold: age exactly equal to
        the threshold is NOT stale. The store clock is pinned so the
        boundary is exact, not racy."""
        j = self._gate_claim_running(self.gate, "j-b", "w-b", ttl=3600)
        wd = self._new_wd(progress_stale_s=10.0)
        t0 = wd.store.current_time()
        conn = wd.store._conn
        with mock.patch.object(wd.store, "current_time", return_value=t0):
            conn.execute("UPDATE jobs SET progress_updated_at=? WHERE"
                         " job_id='j-b'", (t0 - 10.0,))
            conn.commit()
            rec = next(r for r in wd.evaluate() if r["job_id"] == "j-b")
            self.assertEqual(rec["verdict"], "HEALTHY",
                             "age == threshold is not stale (strict >)")
            conn.execute("UPDATE jobs SET progress_updated_at=? WHERE"
                         " job_id='j-b'", (t0 - 10.0 - 0.01,))
            conn.commit()
            # New execution identity is unnecessary here: same verdict
            # key, but force re-evaluation by clearing the recorded row.
            conn.execute("DELETE FROM watchdog_verdicts WHERE job_id='j-b'")
            conn.commit()
            rec2 = next(r for r in wd.evaluate() if r["job_id"] == "j-b")
            self.assertEqual(rec2["verdict"], "STALLED",
                             "age > threshold by any epsilon is stale")


# ------------------------------------------------------------------ R7-24
class TestR724(WatchBase):
    def test_R7_24_fresh_instance_reconstructs_identical_verdict(self):
        """No watchdog-local memory is required: a brand-new instance
        reconstructs the same verdict from durable evidence alone, with
        zero new mutations."""
        self._spawn(self.sup, "w-fr", "j-fr", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-fr", "w-fr")
        wd1 = self._new_wd(progress_stale_s=0.5)
        first = self._wait_verdict(wd1, "j-fr", "STALLED", timeout=12)
        self.assertTrue(first["transition"])
        wd1.close()
        self._watchdogs.remove(wd1)
        wd2 = self._new_wd(progress_stale_s=0.5)
        recs = wd2.evaluate()
        rec = next(r for r in recs if r["job_id"] == "j-fr")
        self.assertEqual(rec["verdict"], "STALLED")
        self.assertFalse(rec["transition"])
        self.assertIsNone(rec["verdict_id"])
        self.assertEqual(rec["evidence"]["reason"], first["evidence"]["reason"])
        # The genuine history is HEALTHY (fresh claim) -> STALLED; the
        # fresh instance added nothing.
        self.assertEqual([r["verdict"]
                          for r in self._verdict_rows(self.gate, "j-fr")],
                         ["HEALTHY", "STALLED"])
        self.assertEqual(len(self._verdict_events(self.gate, "j-fr")), 2)


# ------------------------------------------------------------------ R7-25
class TestR725(WatchBase):
    def test_R7_25_read_failure_fails_closed(self):
        """An unreadable store fails closed: evaluate() raises
        WatchdogError and records nothing — no verdict invented from
        incomplete state, no partial writes."""
        self._spawn(self.sup, "w-fc", "j-fc", "wedged",
                    ttl_s=120, renew=False)
        j = self._wait_claimed(self.gate, "j-fc", "w-fc")
        tok = j["fencing_token"]
        self._progress(self.gate, "j-fc", "w-fc", tok, done=1.0)
        wd = self._new_wd()
        rec = next(r for r in wd.evaluate() if r["job_id"] == "j-fc")
        self.assertEqual(rec["verdict"], "HEALTHY")
        before_rows = len(self._verdict_rows(self.gate, "j-fc"))
        before_evs = len(self._verdict_events(self.gate, "j-fc"))
        wd.store.close()  # genuine read failure from here on
        with self.assertRaises(WatchdogError):
            wd.evaluate()
        with self.assertRaises(WatchdogError):
            wd.evaluate()
        g = self._temp_gate()
        self.assertEqual(len(self._verdict_rows(g, "j-fc")), before_rows)
        self.assertEqual(len(self._verdict_events(g, "j-fc")), before_evs)


# ------------------------------------------------------------------ R7-26
class TestR726(WatchBase):
    def test_R7_26_real_heartbeat_loop_with_zero_progress_goes_stalled(self):
        """End-to-end on a real renewing heartbeat_loop worker: heartbeats
        flow forever, progress never arrives -> STALLED."""
        self._spawn(self.sup, "w-zp", "j-zp", "heartbeat_loop",
                    ttl_s=120, hb=0.2, renew=True, duration_s=120)
        self._wait_claimed(self.gate, "j-zp", "w-zp")
        self._wait(lambda: len(self.gate.heartbeats_for("w-zp")) >= 5,
                   timeout=15)
        wd = self._new_wd(progress_stale_s=2.0)
        rec = self._wait_verdict(wd, "j-zp", "STALLED", timeout=15)
        ev = rec["evidence"]
        self.assertFalse(ev["heartbeat_stale"])
        self.assertGreater(len(self.gate.heartbeats_for("w-zp")), 5)
        self.assertIsNone(ev["progress_updated_at"])
        self.assertNotEqual(rec["verdict"], "DEAD")


# ------------------------------------------------------------------ R7-27
class TestR727(WatchBase):
    def test_R7_27_real_death_yields_dead_only_with_definitive_evidence(self):
        """Real SIGKILL + reap -> DEAD grounded in process-identity
        evidence (ESRCH), not in heartbeat timing."""
        self._spawn(self.sup, "w-rd", "j-rd", "heartbeat_loop",
                    ttl_s=120, hb=0.2, renew=True, duration_s=120)
        self._wait_claimed(self.gate, "j-rd", "w-rd")
        self._wait(lambda: len(self.gate.heartbeats_for("w-rd")) >= 2,
                   timeout=15)
        pid = self.sup._procs["w-rd"].popen.pid
        popen = self.sup._procs["w-rd"].popen
        os.killpg(pid, signal.SIGKILL)
        popen.wait(timeout=10)
        wd = self._new_wd()
        rec = self._wait_verdict(wd, "j-rd", "DEAD", timeout=12)
        de = rec["evidence"]["death_evidence"]
        self.assertIsNotNone(de)
        self.assertEqual(de["kind"], "process_identity_dead")
        self.assertIn("ESRCH", de["detail"])


# ------------------------------------------------------------------ R7-28
class TestR728(WatchBase):
    def test_R7_28_full_r1_to_r6_composition(self):
        """R3 heartbeat evidence -> R4 expiry observation -> R7 STALLED
        verdict -> R1 reclaim: the watchdog observes every step and
        mutates nothing; R1 is the only path that changes authority."""
        self._spawn(self.sup, "w-full", "j-full", "heartbeat_loop",
                    ttl_s=2, hb=0.2, renew=False, duration_s=120)
        j = self._wait_claimed(self.gate, "j-full", "w-full")
        tok = j["fencing_token"]
        self._wait(lambda: len(self.gate.heartbeats_for("w-full")) >= 2,
                   timeout=15)
        g = self._temp_gate()
        self._wait(lambda: g.get_job("j-full")["lease_expires_at"]
                   <= g.store.current_time(), timeout=20)
        # R4: read-only expiry observation.
        self.assertTrue(any(ev["job_id"] == "j-full"
                            for ev in g.observe_expired_leases()))
        # R7: STALLED with lease_expired evidence; authority untouched.
        wd = self._new_wd()
        rec = self._wait_verdict(wd, "j-full", "STALLED", timeout=12)
        self.assertTrue(rec["evidence"]["lease_expired"])
        j_mid = self.gate.get_job("j-full")
        self.assertEqual(j_mid["owner_worker_id"], "w-full")
        self.assertEqual(j_mid["fencing_token"], tok)
        self.assertEqual(self._reclaim_events(self.gate, "j-full"), [])
        # R1: the sole reclaim authority acts on the evidence.
        g.reclaim_lease("j-full", actor="system",
                        reason="R7-28: composition, R1 acts",
                        expected_owner="w-full", expected_token=tok)
        j_after = self.gate.get_job("j-full")
        self.assertEqual(j_after["status"], "UNCERTAIN")
        self.assertIsNone(j_after["owner_worker_id"])
        self.assertEqual(j_after["fencing_token"], tok + 1)
        reclaimed = self._reclaim_events(self.gate, "j-full")
        self.assertEqual(len(reclaimed), 1)
        self.assertEqual(reclaimed[0]["actor"], "system")
        # Post-reclaim the job leaves the watchdog's evaluation set; the
        # STALLED verdict remains as history for the old identity only.
        recs = wd.evaluate()
        self.assertFalse([r for r in recs if r["job_id"] == "j-full"])
        old = self.gate.latest_watchdog_verdict("j-full", tok)
        self.assertEqual(old["verdict"], "STALLED")
        self.assertIsNone(self.gate.latest_watchdog_verdict("j-full",
                                                             tok + 1))


# ------------------------------------------------------------------ R7-29
class TestR729(WatchBase):
    def test_R7_29_background_loop_records_verdicts(self):
        """start()/stop(): the bounded loop evaluates on cadence and
        records the verdict without a manual evaluate() call."""
        self._spawn(self.sup, "w-bg", "j-bg", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-bg", "w-bg")
        wd = self._new_wd(progress_stale_s=0.5, evaluation_interval_s=0.2)
        wd.start()
        try:
            self._wait(lambda: any(
                r["verdict"] == "STALLED"
                for r in self._verdict_rows(self.gate, "j-bg")),
                timeout=12)
        finally:
            wd.stop()
        self.assertEqual(wd._errors, [])
        rows = self._verdict_rows(self.gate, "j-bg")
        # The genuine history is HEALTHY (fresh claim) -> STALLED; the
        # background loop must not multiply them.
        self.assertEqual([r["verdict"] for r in rows],
                         ["HEALTHY", "STALLED"])
        self.assertEqual(len(self._verdict_events(self.gate, "j-bg")), 2)


# ------------------------------------------------------------------ R7-30
class TestR730(WatchBase):
    def test_R7_30_config_is_frozen_and_complete(self):
        """WatchdogConfig carries every threshold; it is immutable and
        the watchdog refuses a non-config object."""
        cfg = WatchdogConfig(heartbeat_stale_s=2.0, progress_stale_s=3.0,
                             evaluation_interval_s=0.5)
        with self.assertRaises(Exception):
            cfg.progress_stale_s = 99.0  # frozen dataclass
        wd = Watchdog(self.db, cfg, readiness=lambda: True)
        self._watchdogs.append(wd)
        self.assertEqual(wd.config.progress_stale_s, 3.0)
        with self.assertRaises(TypeError):
            Watchdog(self.db, {"progress_stale_s": 1.0},
                     readiness=lambda: True)


# ------------------------------------------------------------------ R7-31
class TestR731(WatchBase):
    def test_R7_31_same_verdict_write_is_true_zero_mutation(self):
        """record_watchdog_verdict on a same-verdict or DEAD-terminal
        identity opens no write_txn: last_commit_ts, the verdict table,
        and the ledger are all byte-identical afterwards. Deterministic:
        no live worker, no background writes."""
        self.gate.create_task("t-zm", {"objective": "x"}, {"usd": 1},
                             "scheduler")
        self.gate.create_job("j-zm", "t-zm", "s", "scheduler")
        ev = {"reason": "zero-mutation probe"}

        def _commit_ts():
            return self.gate.store.conn.execute(
                "SELECT v FROM axos_meta"
                " WHERE k='last_commit_ts'").fetchone()[0]

        def _counts():
            v = self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM watchdog_verdicts").fetchone()[0]
            e = self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM ledger").fetchone()[0]
            return (v, e)

        first = self.gate.record_watchdog_verdict(
            "j-zm", 1, "STALLED", ev, "test")
        self.assertTrue(first["transition"])
        ts_before, counts_before = _commit_ts(), _counts()
        for _ in range(3):
            r = self.gate.record_watchdog_verdict(
                "j-zm", 1, "STALLED", ev, "test")
            self.assertFalse(r["transition"])
            self.assertIsNone(r["verdict_id"])
            self.assertEqual(r["verdict"], "STALLED")
        self.assertEqual(_commit_ts(), ts_before,
                         "same-verdict no-op moved the store clock")
        self.assertEqual(_counts(), counts_before,
                         "same-verdict no-op mutated durable state")
        # DEAD-terminal path is equally mutation-free.
        d = self.gate.record_watchdog_verdict(
            "j-zm", 1, "DEAD", ev, "test")
        self.assertTrue(d["transition"])
        ts_dead, counts_dead = _commit_ts(), _counts()
        for _ in range(2):
            r = self.gate.record_watchdog_verdict(
                "j-zm", 1, "STALLED", ev, "test")
            self.assertFalse(r["transition"])
            self.assertIsNone(r["verdict_id"])
            self.assertEqual(r["verdict"], "DEAD",
                             "DEAD stays terminal for the identity")
        self.assertEqual(_commit_ts(), ts_dead,
                         "DEAD-terminal no-op moved the store clock")
        self.assertEqual(_counts(), counts_dead,
                         "DEAD-terminal no-op mutated durable state")


# ------------------------------------------------------------------ R7-32
class TestR732(WatchBase):
    def test_R7_32_crash_mid_scan_loses_nothing(self):
        """A watchdog subprocess that os._exit()s mid-scan (no close, no
        cleanup) loses nothing: every transition committed atomically, so
        the restarted watchdog sees the full history and stays silent."""
        self._spawn(self.sup, "w-crash", "j-crash", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-crash", "w-crash")
        script = (
            "import sys, os, time\n"
            f"sys.path.insert(0, '{os.path.dirname(AXOS_DIR)}')\n"
            "from axos.exec.watchdog import Watchdog, WatchdogConfig\n"
            f"wd = Watchdog({self.db!r}, WatchdogConfig(1.0, 0.5, 0.2),"
            " actor='crasher', readiness=lambda: True)\n"
            "deadline = time.monotonic() + 20\n"
            "while time.monotonic() < deadline:\n"
            "    recs = wd.evaluate()\n"
            "    r = next(x for x in recs if x['job_id'] == 'j-crash')\n"
            "    if r['verdict'] == 'STALLED' and r['transition']:\n"
            "        sys.stdout.write('STALLED_RECORDED\\n')\n"
            "        sys.stdout.flush()\n"
            "        os._exit(42)\n"
            "    time.sleep(0.05)\n"
            "os._exit(99)\n"
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            out, _ = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            raise AssertionError("crashed watchdog child never recorded")
        self.assertEqual(proc.returncode, 42,
                         "child must die by os._exit (simulated crash)")
        self.assertIn("STALLED_RECORDED", out)
        # Restart on the same database: history intact, no duplicate
        # transition, no partial state.
        wd2 = self._new_wd(progress_stale_s=0.5)
        recs = wd2.evaluate()
        rec = next(r for r in recs if r["job_id"] == "j-crash")
        self.assertEqual(rec["verdict"], "STALLED")
        self.assertFalse(rec["transition"])
        self.assertIsNone(rec["verdict_id"])
        rows = self._verdict_rows(self.gate, "j-crash")
        self.assertEqual(rows[-1]["verdict"], "STALLED")
        self.assertEqual(len(self._verdict_events(self.gate, "j-crash")),
                         len(rows),
                         "one ledger event per recorded transition")


# ------------------------------------------------------------------ R7-33
class TestR733(WatchBase):
    def test_R7_33_scan_fails_closed_before_any_write(self):
        """Two-phase scan: when one job's authoritative evidence is
        unreadable, evaluate() raises WatchdogError having written
        nothing — not even the other job's perfectly good verdict."""
        self._spawn(self.sup, "w-s1", "j-s1", "wedged",
                    ttl_s=120, renew=False)
        self._spawn(self.sup, "w-s2", "j-s2", "wedged",
                    ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-s1", "w-s1")
        self._wait_claimed(self.gate, "j-s2", "w-s2")
        wd = self._new_wd(progress_stale_s=0.5)
        real_get_worker = wd.gate.get_worker

        def _flaky(worker_id):
            if worker_id == "w-s2":
                raise RuntimeError("simulated unreadable worker evidence")
            return real_get_worker(worker_id)

        with mock.patch.object(wd.gate, "get_worker",
                               side_effect=_flaky):
            with self.assertRaises(WatchdogError):
                wd.evaluate()
        # Nothing was written for either job: no partial scan results.
        self.assertEqual(
            self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM watchdog_verdicts").fetchone()[0], 0)
        self.assertEqual(
            self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type="
                "'watchdog.verdict'").fetchone()[0], 0)
        # And with the evidence readable again, the scan completes.
        recs = wd.evaluate()
        self.assertEqual(len(recs), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
