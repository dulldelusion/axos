"""Phase 1C R6 — durable boot recovery & runtime reconstruction tests.

STANDARD: real SQLite/WAL, real subprocesses, real process groups, real
signals, real supervisor boot/restart on the same db, real artifact bytes
and hashes, real crash injection (SIGKILL of supervisor/driver processes).
No fake process objects. The single controlled exception is R6-05's
PID-reuse observation: genuine OS-level PID reuse cannot be forced safely
in a test, so the B!=A condition is injected at the observation layer
(axos.exec.boot.proc_start_jiffies) — documented in that test.

Test IDs:
  R6-01  clean boot -> READY, fully recovered, boot journal milestones
  R6-02  authoritative worker adoption (live worker, current authority)
  R6-03  stale worker fenced through the existing R2 mechanism
  R6-04  already-dead worker -> ALREADY_DEAD, boot not blocked
  R6-05  PID reuse -> PID_REUSE; unrelated process survives, never adopted
  R6-06  process-group identity on adoption (session leader, pgid == pid)
  R6-07  expired lease: R4 observes -> R1 reclaims (exactly one token bump)
  R6-08  live lease + missing process -> UNCERTAIN, never COMPLETE
  R6-09  COMMITTING inspected via R5 machinery, never auto-completed
  R6-10  consistent COMPLETE + verified artifact survives restart
  R6-11  COMPLETE contradiction surfaced, row not repaired
  R6-12  checkpoint pointer preserved (never advanced to unverified)
  R6-13  stale heartbeat cannot regain authority (R3 path rejects)
  R6-14  restart during fencing converges (real supervisor SIGKILL)
  R6-15  boot idempotency: repeated passes, no duplicated mutations
  R6-16  duplicate reclaim protection
  R6-17  no unbounded duplicate authoritative mutation events
  R6-18  orphan process evidence -> ORPHAN, never adopted/killed
  R6-19  boot interruption: supervisor crash + mid-boot kill converge
  R6-20  mixed workers: five dispositions, no cross-contamination
  R6-21  no scheduling before READY; BLOCKED boot permits nothing
  R6-22  static: R6 implements no second reclaim authority
  R6-23  static: R6 implements no process-kill authority
  R6-24  authority audit green
  R6-25  boot synchronous sweep + background sweep do not double-fence
"""
import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace
sys.path.insert(0, HERE)  # tests/r5_helpers.py
AXOS_DIR = os.path.join(os.path.dirname(os.path.dirname(HERE)), "axos")

from axos.exec.supervisor import Supervisor  # noqa: E402
from axos.exec import boot as boot_mod  # noqa: E402
from axos.exec.boot import boot_recover  # noqa: E402
from axos.store import (TransitionGate, TransitionRejected, LeaseError,  # noqa: E402
                        open_store, migrate)
from r5_helpers import r5_complete  # noqa: E402


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


# Driver for R6-19b: blocks *inside* boot's observe_expired_leases (after
# integrity + ledger checks, before any R4/R1 mutation) so the test can
# SIGKILL the process mid-recovery. Written to tmpdir by the test; never
# committed to the repo.
_R6_19B_DRIVER = """\
import argparse, os, sys, time
sys.path.insert(0, os.path.expanduser("~/workspace"))
from axos.store.gate import TransitionGate
from axos.exec.supervisor import Supervisor

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--sentinel", required=True)
    a = ap.parse_args()
    orig = TransitionGate.observe_expired_leases
    def patched(self):
        with open(a.sentinel, "w") as f:
            f.write("in-boot\\n")
        time.sleep(30)  # killed here: mid-recovery, before any reclaim
        return orig(self)
    TransitionGate.observe_expired_leases = patched
    Supervisor(a.db, actor="r6-19b-driver")
    with open(a.sentinel + ".done", "w") as f:
        f.write("survived\\n")
    time.sleep(30)

main()
"""


class BootBase(unittest.TestCase):
    H = 0.6

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r6-")
        self.db = os.path.join(self.tmp, "t.db")
        self._sups: list[Supervisor] = []
        self._temp_stores: list = []
        self._extra_popen: list = []  # our children to kill+reap
        self._extra_pids: list[int] = []  # not our children: killpg only
        self.sup = self._new_sup(H=self.H, actor="test-boot")
        self.gate = self.sup.gate
        self.gate.create_task("t", {"objective": "x"}, {"usd": 1},
                              "scheduler")

    def _new_sup(self, H=None, actor=None) -> Supervisor:
        s = Supervisor(self.db, actor=actor or f"test-boot-{len(self._sups)}",
                       heartbeat_interval_s=H or self.H)
        self._sups.append(s)
        return s

    def _temp_gate(self) -> TransitionGate:
        """A fresh gate on the db for use after a supervisor was closed."""
        store = open_store(self.db)
        migrate(store)
        self._temp_stores.append(store)
        return TransitionGate(store)

    def tearDown(self):
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
        for pid in self._extra_pids:
            try:
                os.killpg(pid, signal.SIGKILL)
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
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- helpers
    def _spawn(self, sup, wid, jid, kind, fresh_job=True, ttl_s=60.0,
               hb=0.5, renew=True, **behavior_kw):
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

    def _running_job(self, gate, jid, wid, task="t", ttl=60.0):
        """Gate-level RUNNING job (no worker process): claim + RUNNING."""
        gate.create_job(jid, task, "s", "scheduler")
        self.assertTrue(gate.claim_job(jid, wid, ttl, "scheduler"))
        gate.transition_job(jid, "RUNNING", f"worker:{wid}")
        return gate.get_job(jid)["fencing_token"]

    def _fence_events(self, gate, wid=None, proc_id=None):
        q = ("SELECT payload FROM ledger "
             "WHERE event_type='worker.fence_enforced'")
        params: tuple = ()
        if wid is not None:
            q += " AND json_extract(payload,'$.worker_id')=?"
            params = (wid,)
        q += " ORDER BY seq"
        rows = gate.store.conn.execute(q, params).fetchall()
        evs = [json.loads(r[0]) for r in rows]
        if proc_id is not None:
            evs = [e for e in evs if e.get("proc_id") == proc_id]
        return evs

    def _wait_fence(self, gate, wid, proc_id=None, timeout=15.0):
        self._wait(lambda: len(self._fence_events(gate, wid, proc_id)) > 0,
                   timeout)
        return self._fence_events(gate, wid, proc_id)[0]

    def _lease_reclaimed_events(self, gate):
        rows = gate.store.conn.execute(
            "SELECT seq, ts, actor, payload FROM ledger "
            "WHERE event_type='job.lease_reclaimed' ORDER BY seq").fetchall()
        return [{"seq": r[0], "ts": r[1], "actor": r[2],
                 "payload": json.loads(r[3])} for r in rows]

    def _event_count(self, gate, event_type):
        return gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?",
            (event_type,)).fetchone()[0]

    def _boot_events(self, gate, boot_id):
        """{event_type: [payloads]} for one boot pass."""
        rows = gate.store.conn.execute(
            "SELECT event_type, payload FROM ledger "
            "WHERE json_extract(payload,'$.boot_id')=? ORDER BY seq",
            (boot_id,)).fetchall()
        out: dict = {}
        for et, p in rows:
            out.setdefault(et, []).append(json.loads(p))
        return out

    @staticmethod
    def _dispositions(rep):
        return {(d["worker_id"], d["proc_id"]): d
                for d in rep["dispositions"]}

    def _durable_snapshot(self, gate):
        """Durable state that must converge across boot passes. The live
        renewing worker's lease_expires_at legitimately moves (renewal), so
        it is captured separately and compared only where it must not."""
        jobs = {}
        for r in gate.store.conn.execute(
                "SELECT job_id, status, owner_worker_id, fencing_token,"
                " lease_expires_at FROM jobs"):
            jobs[r[0]] = {"status": r[1], "owner": r[2], "token": r[3],
                           "lease_expires_at": r[4]}
        workers = {r[0]: r[1] for r in gate.store.conn.execute(
            "SELECT worker_id, status FROM workers")}
        lkgs = {p["task_id"]: p["checkpoint_id"]
                for p in gate.all_latest_known_good()}
        evcounts = {}
        for (et,) in gate.store.conn.execute(
                "SELECT DISTINCT event_type FROM ledger"):
            evcounts[et] = self._event_count(gate, et)
        return {"jobs": jobs, "workers": workers, "lkgs": lkgs,
                "evcounts": evcounts}

    # ----------------------------------------------- shared static audits
    def _boot_source(self):
        with open(os.path.join(AXOS_DIR, "exec", "boot.py")) as f:
            return f.read()

    def _assert_no_direct_reclaim(self):
        """R6-22 core: boot.py performs no mutation of its own; the only
        reclaim path is the gate's."""
        src = self._boot_source()
        self.assertIsNone(
            re.search(r"(?i)\b(UPDATE|INSERT\s+INTO|DELETE\s+FROM)\b", src),
            "boot.py contains SQL writes")
        self.assertNotIn("conn.execute", src)
        self.assertNotIn("write_txn", src)
        self.assertNotIn("def reclaim", src)
        self.assertIn("gate.reclaim_lease", src)
        for m in re.finditer(r"reclaim_lease\s*\(", src):
            ls = src.rfind("\n", 0, m.start()) + 1
            line = src[ls:src.find("\n", m.start())]
            self.assertIn("gate.reclaim_lease", line,
                          f"non-gate reclaim call: {line.strip()}")

    def _assert_no_direct_kill(self):
        """R6-23 core: boot.py never signals a process itself; os.kill
        appears only as the signal-0 existence probe."""
        src = self._boot_source()
        self.assertNotIn("os.killpg", src)
        self.assertNotIn("subprocess", src)
        self.assertNotIn("SIGTERM", src)
        self.assertNotIn("SIGKILL", src)
        kills = [m.start() for m in re.finditer(r"os\.kill\s*\(", src)]
        self.assertTrue(kills, "expected the signal-0 existence probe")
        for pos in kills:
            snippet = src[pos:pos + 20]
            self.assertTrue(snippet.startswith("os.kill(pid, 0)"),
                            f"non-probe os.kill: {snippet!r}")

    # ------------------------------------------------------- shared setups
    def _adopt_live_worker(self, wid, jid):
        """Spawn a renewing worker on sup1, then abandon sup1 (close stops
        the sweep thread but never touches OS processes; the worker keeps
        running and renewing) and boot sup2 on the same db."""
        proc_id = self._spawn(self.sup, wid, jid, "heartbeat_loop",
                              ttl_s=120, hb=0.3, renew=True,
                              duration_s=600)
        self._wait_claimed(self.gate, jid, wid)
        self._wait(lambda: len(self.gate.heartbeats_for(wid)) >= 2,
                   timeout=15)
        self._wait(lambda: self.gate.get_job(jid)["status"] == "RUNNING",
                   timeout=15)
        pid = self.sup._procs[wid].popen.pid
        self.sup.close()
        sup2 = self._new_sup(actor="test-boot-2")
        return proc_id, pid, sup2

    def _idempotency_setup(self):
        """One live renewing worker plus one expired-lease job; returns
        (sup2_after_boot_pass_1, live_pid)."""
        self._spawn(self.sup, "w-live", "j-live", "heartbeat_loop",
                    ttl_s=120, hb=0.3, renew=True, duration_s=600)
        self._wait_claimed(self.gate, "j-live", "w-live")
        self._wait(lambda: len(self.gate.heartbeats_for("w-live")) >= 2,
                   timeout=15)
        pid_live = self.sup._procs["w-live"].popen.pid
        proc_exp = self._spawn(self.sup, "w-exp", "j-exp", "heartbeat_loop",
                               ttl_s=2, hb=0.2, renew=False)
        self._wait_claimed(self.gate, "j-exp", "w-exp")
        pid_exp = self.sup._procs["w-exp"].popen.pid
        popen_exp = self.sup._procs["w-exp"].popen
        self.sup.close()
        os.killpg(pid_exp, signal.SIGKILL)
        popen_exp.wait(timeout=10)  # reap our zombie; ledger untouched
        g = self._temp_gate()
        self._wait(lambda: g.get_job("j-exp")["lease_expires_at"]
                   <= g.store.current_time(), timeout=20)
        sup2 = self._new_sup(actor="test-boot-idem")
        return sup2, pid_live, proc_exp

# ------------------------------------------------------------------ R6-01
class TestR601(BootBase):
    def test_R6_01_clean_boot(self):
        """No active runtime: boot reaches READY, fully recovered, with the
        boot.started / boot.reconstructed / boot.completed milestones."""
        rep = self.sup._boot_report
        self.assertEqual(rep["phase"], "READY")
        self.assertTrue(rep["fully_recovered"])
        self.assertEqual(rep["dispositions"], [])
        self.assertEqual(rep["leases_reclaimed"], 0)
        self.assertEqual(rep["integrity_contradictions"], [])
        self.assertEqual(rep["errors"], [])
        evs = self._boot_events(self.gate, rep["boot_id"])
        for et in ("boot.started", "boot.reconstructed", "boot.completed"):
            self.assertIn(et, evs, f"missing {et}")
        self.assertNotIn("boot.recovery_blocked", evs)


# ------------------------------------------------------------------ R6-02
class TestR602(BootBase):
    def test_R6_02_authoritative_worker_adoption(self):
        """A genuinely live worker — PID matches, kernel start identity
        matches, PGID matches, durable owner matches, fencing token is
        current, lease live, job RUNNING — is adopted, not fenced."""
        proc_id, pid, sup2 = self._adopt_live_worker("w-adopt", "j-adopt")
        rep = sup2._boot_report
        self.assertEqual(rep["phase"], "READY")
        d = self._dispositions(rep).get(("w-adopt", proc_id))
        self.assertIsNotNone(d, f"no disposition: {rep['dispositions']}")
        self.assertEqual(d["disposition"], "ADOPT")
        self.assertIn(("w-adopt", proc_id), sup2._adopted)
        self.assertFalse(_proc_gone(pid), "adopted worker was killed")
        # Durable authority untouched by the adoption.
        j2 = sup2.gate.get_job("j-adopt")
        self.assertEqual(j2["owner_worker_id"], "w-adopt")
        self.assertEqual(j2["status"], "RUNNING")
        self.assertEqual(self._fence_events(sup2.gate, "w-adopt"), [])


# ------------------------------------------------------------------ R6-03
class TestR603(BootBase):
    def test_R6_03_stale_worker_fenced_through_r2(self):
        """A live worker whose durable authority is stale is detected at
        boot and fenced through the EXISTING R2 sweep — R6 signals
        nothing itself."""
        proc_id = self._spawn(self.sup, "w-stale", "j-stale", "wedged",
                              ttl_s=120, renew=False)
        j = self._wait_claimed(self.gate, "j-stale", "w-stale")
        tok = j["fencing_token"]
        pid = self.sup._procs["w-stale"].popen.pid
        self.sup.close()  # worker keeps running; it never heartbeats, so
        # it cannot cooperatively self-fence: the R2 sweep must do it.
        g = self._temp_gate()
        g.reclaim_lease("j-stale", actor="reconciler",
                        reason="STALLED test",
                        expected_owner="w-stale", expected_token=tok,
                        force=True, verdict="STALLED",
                        incident_id="inc-r6-03")
        sup2 = self._new_sup(actor="test-r6-03")
        rep = sup2._boot_report
        d = self._dispositions(rep).get(("w-stale", proc_id))
        self.assertIsNotNone(d, f"no disposition: {rep['dispositions']}")
        self.assertEqual(d["disposition"], "FENCE")
        # Boot's synchronous R2 sweep enforces during construction.
        ev = self._wait_fence(sup2.gate, "w-stale", proc_id)
        self.assertEqual(ev["outcome"], "killed")
        self.assertTrue(ev["adopted"])
        self.assertEqual(ev["old_token"], tok)
        self.assertEqual(ev["new_token"], tok + 1)
        self._wait(lambda: _proc_gone(pid), timeout=15)
        self._assert_no_direct_kill()


# ------------------------------------------------------------------ R6-04
class TestR604(BootBase):
    def test_R6_04_already_dead_worker(self):
        """Persisted spawn evidence whose process is actually dead does not
        block boot: ALREADY_DEAD, READY, fully recovered."""
        proc_id = self._spawn(self.sup, "w-dead", "j-dead", "heartbeat_loop",
                              ttl_s=120, hb=0.3, renew=True)
        j = self._wait_claimed(self.gate, "j-dead", "w-dead")
        tok = j["fencing_token"]
        pid = self.sup._procs["w-dead"].popen.pid
        popen = self.sup._procs["w-dead"].popen
        self.sup.close()  # stop the sweep BEFORE the kill: no reap
        # milestone can land, so the spawn record stays unreaped.
        os.killpg(pid, signal.SIGKILL)
        popen.wait(timeout=10)  # reap our zombie; ledger untouched
        # Put the orphaned job somewhere terminal so this test is purely
        # about the spawn disposition (live-lease + dead-owner is R6-08).
        g = self._temp_gate()
        g.fail_job_execution("j-dead", "w-dead", tok, actor="test",
                             reason="test", evidence={})
        sup2 = self._new_sup(actor="test-r6-04")
        rep = sup2._boot_report
        self.assertEqual(rep["phase"], "READY")
        d = self._dispositions(rep).get(("w-dead", proc_id))
        self.assertIsNotNone(d, f"no disposition: {rep['dispositions']}")
        self.assertEqual(d["disposition"], "ALREADY_DEAD")
        self.assertTrue(rep["fully_recovered"])
        self.assertNotIn(("w-dead", proc_id), sup2._adopted)


# ------------------------------------------------------------------ R6-05
class TestR605(BootBase):
    def test_R6_05_pid_reuse_never_kills_unrelated_process(self):
        """PID matches but kernel start identity differs (B != A): the
        current process is not adopted and is never killed.

        Genuine OS-level PID reuse cannot be forced safely in a test, so
        the B!=A condition is injected at the observation layer
        (axos.exec.boot.proc_start_jiffies). The disposition logic under
        test — classify -> PID_REUSE -> never adopt, never signal — is the
        real production path."""
        proc_id = self._spawn(self.sup, "w-reuse", "j-reuse", "wedged",
                              ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-reuse", "w-reuse")
        pid = self.sup._procs["w-reuse"].popen.pid
        durable_start = boot_mod.proc_start_jiffies(pid)
        self.assertIsNotNone(durable_start)
        real_fn = boot_mod.proc_start_jiffies

        def fake(pid2):
            if pid2 == pid:
                return durable_start + 99999  # B != A
            return real_fn(pid2)

        self.sup.close()
        with mock.patch.object(boot_mod, "proc_start_jiffies",
                               side_effect=fake):
            sup2 = self._new_sup(actor="test-r6-05")
        rep = sup2._boot_report
        d = self._dispositions(rep).get(("w-reuse", proc_id))
        self.assertIsNotNone(d, f"no disposition: {rep['dispositions']}")
        self.assertEqual(d["disposition"], "PID_REUSE")
        self.assertNotIn(("w-reuse", proc_id), sup2._adopted)
        # The unrelated current process survives: boot never signalled it,
        # and the sweep cannot see it (popped from the adoption registry).
        time.sleep(self.H * 2)
        self.assertFalse(_proc_gone(pid),
                         "unrelated process was killed on PID match alone")
        self.assertEqual(self._fence_events(sup2.gate, "w-reuse"), [])


# ------------------------------------------------------------------ R6-06
class TestR606(BootBase):
    def test_R6_06_process_group_identity(self):
        """Adopted runtime evidence corresponds to the expected process
        group: the worker is a session leader (pgid == pid), recorded in
        the durable ADOPT journal payload."""
        proc_id, pid, sup2 = self._adopt_live_worker("w-adopt", "j-adopt")
        evs = self._boot_events(sup2.gate,
                                sup2._boot_report["boot_id"]
                                ).get("boot.worker_adopted", [])
        match = [e for e in evs
                 if e.get("worker_id") == "w-adopt"
                 and e.get("proc_id") == proc_id]
        self.assertEqual(len(match), 1)
        self.assertEqual(match[0]["observed_pgid"], pid)
        self.assertEqual(os.getpgid(pid), pid,
                         "adopted worker is not a session leader")


# ------------------------------------------------------------------ R6-07
class TestR607(BootBase):
    def test_R6_07_expired_lease_r4_to_r1(self):
        """Boot detects an expired lease through R4 and routes the
        authoritative mutation through R1: exactly one token bump, owner
        cleared, correct state transition, actor 'system', reason naming
        the R4 observation. R6 performs no mutation of its own."""
        proc_id = self._spawn(self.sup, "w-exp", "j-exp", "heartbeat_loop",
                              ttl_s=3, hb=0.2, renew=False)
        j = self._wait_claimed(self.gate, "j-exp", "w-exp")
        tok = j["fencing_token"]
        self._wait(lambda: self.gate.get_job("j-exp")["status"] == "RUNNING",
                   timeout=15)
        pid = self.sup._procs["w-exp"].popen.pid
        popen = self.sup._procs["w-exp"].popen
        self.sup.close()
        os.killpg(pid, signal.SIGKILL)
        popen.wait(timeout=10)
        g = self._temp_gate()
        self._wait(lambda: g.get_job("j-exp")["lease_expires_at"]
                   <= g.store.current_time(), timeout=20)
        sup2 = self._new_sup(actor="test-r6-07")
        rep = sup2._boot_report
        self.assertIn("j-exp", rep["leases_reclaimed_jobs"])
        j2 = sup2.gate.get_job("j-exp")
        self.assertIsNone(j2["owner_worker_id"])
        self.assertEqual(j2["fencing_token"], tok + 1)  # exactly one bump
        # RUNNING -> UNCERTAIN is the D1 outcome for a reclaimed live state.
        self.assertEqual(j2["status"], "UNCERTAIN")
        evs = [e for e in self._lease_reclaimed_events(sup2.gate)
               if e["payload"]["job_id"] == "j-exp"]
        self.assertEqual(len(evs), 1, "reclaim must happen exactly once")
        self.assertEqual(evs[0]["actor"], "system")
        self.assertIn("R4", evs[0]["payload"]["reason"])
        self.assertFalse(evs[0]["payload"]["forced"])
        self._assert_no_direct_reclaim()


# ------------------------------------------------------------------ R6-08
class TestR608(BootBase):
    def test_R6_08_live_lease_missing_process_not_complete(self):
        """A durable owned job (live lease) whose runtime process is
        missing must not become COMPLETE: ALREADY_DEAD disposition plus
        the D1 forced reclaim to UNCERTAIN — uncertainty, not completion."""
        proc_id = self._spawn(self.sup, "w-miss", "j-miss", "heartbeat_loop",
                              ttl_s=120, hb=0.3, renew=True)
        self._wait_claimed(self.gate, "j-miss", "w-miss")
        self._wait(lambda: self.gate.get_job("j-miss")["status"] == "RUNNING",
                   timeout=15)
        pid = self.sup._procs["w-miss"].popen.pid
        popen = self.sup._procs["w-miss"].popen
        self.sup.close()
        os.killpg(pid, signal.SIGKILL)
        popen.wait(timeout=10)
        sup2 = self._new_sup(actor="test-r6-08")
        rep = sup2._boot_report
        d = self._dispositions(rep).get(("w-miss", proc_id))
        self.assertIsNotNone(d, f"no disposition: {rep['dispositions']}")
        self.assertEqual(d["disposition"], "ALREADY_DEAD")
        self.assertEqual(rep["forced_reclaims"], 1)
        j2 = sup2.gate.get_job("j-miss")
        self.assertEqual(j2["status"], "UNCERTAIN")
        self.assertNotEqual(j2["status"], "COMPLETE")
        self.assertIsNone(j2["owner_worker_id"])
        evs = [e for e in self._lease_reclaimed_events(sup2.gate)
               if e["payload"]["job_id"] == "j-miss"]
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["payload"]["verdict"], "DEAD")
        self.assertTrue(evs[0]["payload"]["forced"])


# ------------------------------------------------------------------ R6-09
class TestR609(BootBase):
    def test_R6_09_committing_inspected_not_completed(self):
        """A COMMITTING job is inspected through the R5
        uncertain-completion machinery: it stays COMMITTING, the row is
        byte-identical, no contradiction is raised, and the ledger gains
        only boot.* observation events — never a completion."""
        tok = self._running_job(self.gate, "j9", "w9")
        art = self.gate.stage_artifact(
            job_id="j9", worker_id="w9", fencing_token=tok, task_id="t",
            kind="result", data=b"r6-09-bytes", actor="worker:w9")
        self.gate.begin_commit("j9", "w9", tok,
                               artifact_id=art["artifact_id"],
                               actor="worker:w9")
        self.assertEqual(self.gate.get_job("j9")["status"], "COMMITTING")
        row_before = dict(self.gate.store.conn.execute(
            "SELECT * FROM jobs WHERE job_id='j9'").fetchone())
        seq_before = self.gate.store.conn.execute(
            "SELECT MAX(seq) FROM ledger").fetchone()[0]
        sup2 = self._new_sup(actor="test-r6-09")
        rep = sup2._boot_report
        j2 = sup2.gate.get_job("j9")
        self.assertEqual(j2["status"], "COMMITTING")
        row_after = dict(sup2.gate.store.conn.execute(
            "SELECT * FROM jobs WHERE job_id='j9'").fetchone())
        self.assertEqual(row_before, row_after)
        contra = [c for c in rep["integrity_contradictions"]
                  if c.get("job_id") == "j9"]
        self.assertEqual(contra, [])
        new_types = [r[0] for r in sup2.gate.store.conn.execute(
            "SELECT event_type FROM ledger WHERE seq > ?",
            (seq_before,)).fetchall()]
        self.assertTrue(new_types, "boot must journal its pass")
        for et in new_types:
            self.assertTrue(et.startswith("boot."),
                            f"non-observation event during boot: {et}")


# ------------------------------------------------------------------ R6-10
class TestR610(BootBase):
    def test_R6_10_complete_valid_artifact_preserved(self):
        """A consistent COMPLETE job with a verified artifact survives
        restart unchanged: still COMPLETE, consistent, bytes intact."""
        tok = self._running_job(self.gate, "j10", "w10")
        r5_complete(self.gate, job_id="j10", worker_id="w10",
                    fencing_token=tok, task_id="t", outcome="SUCCESS",
                    evidence={}, actor="worker:w10", data=b"r6-10-bytes")
        self.assertEqual(self.gate.get_job("j10")["status"], "COMPLETE")
        aid = hashlib.sha256(b"r6-10-bytes").hexdigest()
        art = self.gate.get_artifact(aid)
        with open(art["uri"], "rb") as f:
            before = f.read()
        sup2 = self._new_sup(actor="test-r6-10")
        rep = sup2._boot_report
        self.assertEqual(rep["complete_jobs_checked"], 1)
        self.assertEqual(rep["complete_jobs_consistent"], 1)
        self.assertEqual(rep["integrity_contradictions"], [])
        self.assertEqual(sup2.gate.get_job("j10")["status"], "COMPLETE")
        with open(art["uri"], "rb") as f:
            after = f.read()
        self.assertEqual(before, after)
        self.assertEqual(hashlib.sha256(after).hexdigest(), aid)


# ------------------------------------------------------------------ R6-11
class TestR611(BootBase):
    def test_R6_11_complete_contradiction_surfaced(self):
        """A COMPLETE job whose artifact bytes were deleted behind its
        back is surfaced as an integrity contradiction: the COMPLETE row
        is NOT rewritten or repaired, and the boot is not fully recovered."""
        tok = self._running_job(self.gate, "j11", "w11")
        r5_complete(self.gate, job_id="j11", worker_id="w11",
                    fencing_token=tok, task_id="t", outcome="SUCCESS",
                    evidence={}, actor="worker:w11", data=b"r6-11-bytes")
        aid = hashlib.sha256(b"r6-11-bytes").hexdigest()
        art = self.gate.get_artifact(aid)
        os.remove(art["uri"])  # corrupt behind the COMPLETE row's back
        sup2 = self._new_sup(actor="test-r6-11")
        rep = sup2._boot_report
        contra = [c for c in rep["integrity_contradictions"]
                  if c.get("job_id") == "j11"]
        self.assertTrue(contra, "contradiction not surfaced")
        self.assertEqual(contra[0]["kind"], "complete_artifact_contradiction")
        self.assertTrue(contra[0]["problems"])
        # The row is surfaced, never silently repaired.
        self.assertEqual(sup2.gate.get_job("j11")["status"], "COMPLETE")
        self.assertEqual(rep["phase"], "READY")
        self.assertFalse(rep["fully_recovered"])
        evs = self._boot_events(sup2.gate, rep["boot_id"])
        self.assertIn("boot.recovery_blocked", evs)
        self.assertIn("boot.integrity_contradiction", evs)

# ------------------------------------------------------------------ R6-12
class TestR612(BootBase):
    def _verified_artifact(self, gate, jid, wid, data):
        tok = self._running_job(gate, jid, wid)
        art = gate.stage_artifact(job_id=jid, worker_id=wid,
                                  fencing_token=tok, task_id="t",
                                  kind="result", data=data,
                                  actor=f"worker:{wid}")
        gate.verify_artifact(art["artifact_id"], actor="test")
        return gate.get_artifact(art["artifact_id"])

    def test_R6_12_checkpoint_pointer_preserved(self):
        """Boot cannot move latest_known_good to an unverified checkpoint:
        the pointer still names the VERIFIED checkpoint after the pass."""
        a1 = self._verified_artifact(self.gate, "jc1", "wc1", b"r6-12-a")
        m1 = [{"artifact_id": a1["artifact_id"],
               "content_hash": a1["content_hash"]}]
        cp1 = self.gate.stage_checkpoint("t", actor="test", manifest=m1,
                                         trigger="policy")
        self.gate.verify_checkpoint(cp1["checkpoint_id"], actor="test")
        a2 = self._verified_artifact(self.gate, "jc2", "wc2", b"r6-12-b")
        m2 = [{"artifact_id": a2["artifact_id"],
               "content_hash": a2["content_hash"]}]
        cp2 = self.gate.stage_checkpoint("t", actor="test", manifest=m2,
                                         trigger="policy")
        self.assertNotEqual(cp1["checkpoint_id"], cp2["checkpoint_id"])
        lkg_before = {p["task_id"]: p["checkpoint_id"]
                      for p in self.gate.all_latest_known_good()}
        self.assertEqual(lkg_before.get("t"), cp1["checkpoint_id"])
        sup2 = self._new_sup(actor="test-r6-12")
        rep = sup2._boot_report
        self.assertGreaterEqual(rep["checkpoints_checked"], 1)
        lkg_after = {p["task_id"]: p["checkpoint_id"]
                     for p in sup2.gate.all_latest_known_good()}
        self.assertEqual(lkg_after, lkg_before)
        self.assertEqual(lkg_after.get("t"), cp1["checkpoint_id"])
        # The pointed checkpoint is still VERIFIED; the unverified one was
        # never adopted as latest-known-good.
        ptr = [p for p in sup2.gate.all_latest_known_good()
               if p["task_id"] == "t"][0]
        self.assertEqual(ptr["checkpoint"]["verification_status"],
                         "VERIFIED")


# ------------------------------------------------------------------ R6-13
class TestR613(BootBase):
    def test_R6_13_stale_heartbeat_cannot_regain_authority(self):
        """A reconstructed stale worker cannot regain authority through
        the R3 heartbeat path: the revoked token is rejected and the
        rejection is journaled as evidence."""
        proc_id = self._spawn(self.sup, "w-hb", "j-hb", "heartbeat_loop",
                              ttl_s=120, hb=0.3, renew=True)
        j = self._wait_claimed(self.gate, "j-hb", "w-hb")
        tok = j["fencing_token"]
        self._wait(lambda: len(self.gate.heartbeats_for("w-hb")) >= 2,
                   timeout=15)
        self.gate.reclaim_lease("j-hb", actor="reconciler",
                                reason="STALLED test",
                                expected_owner="w-hb", expected_token=tok,
                                force=True, verdict="STALLED",
                                incident_id="inc-r6-13")
        n_before = self._event_count(self.gate, "worker.heartbeat_fenced")
        with self.assertRaises(LeaseError):
            self.gate.ingest_heartbeat("w-hb", proc_id, "j-hb", tok, 10 ** 9,
                                       "RUNNING", "op", "test")
        n_after = self._event_count(self.gate, "worker.heartbeat_fenced")
        self.assertGreater(n_after, n_before)
        # The stale worker gained nothing: owner cleared, token moved on.
        j2 = self.gate.get_job("j-hb")
        self.assertIsNone(j2["owner_worker_id"])
        self.assertEqual(j2["fencing_token"], tok + 1)


# ------------------------------------------------------------------ R6-14
class TestR614(BootBase):
    def test_R6_14_restart_during_fencing_converges(self):
        """Supervisor alive -> worker alive -> supervisor SIGKILLed right
        after authority is revoked -> new supervisor boots -> no stale
        process authority remains. Convergence from durable evidence, not
        graceful shutdown.

        Timing rationale: the driver is SIGSTOPped before the reclaim so
        its own sweep thread deterministically cannot enforce first; the
        reclaim then lands while the driver is frozen, and SIGKILL follows
        within milliseconds. The worker (a separate process) is unaffected
        by the driver's SIGSTOP. At the new boot the worker has either
        already exited cooperatively (its own heartbeat noticed the fence)
        or is still alive — both converge; the test accepts both
        dispositions and asserts the converged end state."""
        drv_path = os.path.join(HERE, "_supdrv.py")
        ready = os.path.join(self.tmp, "ready14")
        drv = subprocess.Popen([sys.executable, drv_path, "--db", self.db,
                                "--scenario", "sup_crash", "--ready", ready])
        pid = None
        try:
            self._wait(lambda: os.path.exists(ready), timeout=30)
            g = self._temp_gate()
            j = g.get_job("jdrv")
            tok = j["fencing_token"]
            self._wait(lambda: len(g.heartbeats_for("wdrv")) >= 2,
                       timeout=15)
            sp = [s for s in g.unreaped_proc_spawns()
                  if s["worker_id"] == "wdrv"][0]
            proc_id, pid = sp["proc_id"], sp["pid"]
            os.kill(drv.pid, signal.SIGSTOP)  # freeze the driver's sweep
            g.reclaim_lease("jdrv", actor="reconciler",
                            reason="STALLED test",
                            expected_owner="wdrv", expected_token=tok,
                            force=True, verdict="STALLED",
                            incident_id="inc-r6-14")
            os.kill(drv.pid, signal.SIGKILL)
            drv.wait(timeout=10)
            sup2 = self._new_sup(actor="test-r6-14")
            rep = sup2._boot_report
            self.assertEqual(rep["phase"], "READY")
            d = self._dispositions(rep).get(("wdrv", proc_id))
            # Either the worker already exited cooperatively (its heartbeat
            # noticed the fence) or boot fenced it: both are convergence.
            self.assertIn(
                d["disposition"] if d else None,
                ("FENCE", "ALREADY_DEAD"),
                f"unexpected disposition: {rep['dispositions']}")
            if d and d["disposition"] == "FENCE":
                self._wait_fence(sup2.gate, "wdrv", proc_id)
            self._wait(lambda: _proc_gone(pid), timeout=15)
            j2 = sup2.gate.get_job("jdrv")
            self.assertIsNone(j2["owner_worker_id"])
            evs = [e for e in self._lease_reclaimed_events(sup2.gate)
                   if e["payload"]["job_id"] == "jdrv"]
            self.assertEqual(len(evs), 1)
        finally:
            try:
                os.kill(drv.pid, signal.SIGKILL)
            except Exception:
                pass
            if pid is not None:
                self._extra_pids.append(pid)


# ------------------------------------------------------------- R6-15..17
class TestR615Idempotency(BootBase):
    def test_R6_15_boot_idempotent(self):
        """Repeated boot passes converge to the same durable state: no new
        fencing-token bumps, no duplicate reclaim, no duplicate completion,
        no corrupted state, no unnecessary process termination. Boot's own
        observation journal may grow; authoritative mutations must not."""
        sup2, pid_live, _ = self._idempotency_setup()
        rep1 = sup2._boot_report
        self.assertEqual(rep1["phase"], "READY")
        self.assertEqual(rep1["leases_reclaimed"], 1)
        self.assertIn("j-exp", rep1["leases_reclaimed_jobs"])
        snap1 = self._durable_snapshot(sup2.gate)
        rep2 = boot_recover(sup2)
        snap2 = self._durable_snapshot(sup2.gate)
        rep3 = boot_recover(sup2)
        snap3 = self._durable_snapshot(sup2.gate)
        self.assertEqual(rep2["leases_reclaimed"], 0)
        self.assertEqual(rep3["leases_reclaimed"], 0)
        self.assertEqual(rep2["phase"], "READY")
        self.assertEqual(rep3["phase"], "READY")

        def _core(snap):
            # The live worker's lease_expires_at legitimately moves under
            # renewal; everything else must be identical.
            return ({jid: (v["status"], v["owner"], v["token"])
                     for jid, v in snap["jobs"].items()},
                    snap["workers"], snap["lkgs"])

        self.assertEqual(_core(snap1), _core(snap2))
        self.assertEqual(_core(snap2), _core(snap3))
        # The expired job's cleared lease stays cleared.
        self.assertIsNone(snap3["jobs"]["j-exp"]["lease_expires_at"])
        # No duplicated authoritative mutations across the passes.
        for et in ("job.lease_reclaimed", "worker.fence_enforced",
                   "job.uncertain_opened"):
            self.assertEqual(snap1["evcounts"].get(et, 0),
                             snap3["evcounts"].get(et, 0), et)
        self.assertEqual(snap1["evcounts"].get("job.lease_reclaimed", 0), 1)
        # The live worker was never fenced and is still alive: no
        # unnecessary termination.
        self.assertEqual(self._event_count(sup2.gate,
                                           "worker.fence_enforced"), 0)
        self.assertFalse(_proc_gone(pid_live))
        d3 = self._dispositions(rep3)
        live_keys = [k for k in d3 if k[0] == "w-live"]
        self.assertTrue(live_keys)
        self.assertEqual(d3[live_keys[0]]["disposition"], "ADOPT")


class TestR616DupReclaim(BootBase):
    def test_R6_16_duplicate_reclaim_protection(self):
        """An expired job reclaimed on the first boot pass cannot be
        reclaimed again: the second pass reports zero reclaims and the
        fencing token does not move."""
        sup2, _, _ = self._idempotency_setup()
        tok_after_p1 = sup2.gate.get_job("j-exp")["fencing_token"]
        rep2 = boot_recover(sup2)
        self.assertEqual(rep2["leases_reclaimed"], 0)
        self.assertEqual(rep2["leases_reclaimed_jobs"], [])
        self.assertEqual(sup2.gate.get_job("j-exp")["fencing_token"],
                         tok_after_p1)
        evs = [e for e in self._lease_reclaimed_events(sup2.gate)
               if e["payload"]["job_id"] == "j-exp"]
        self.assertEqual(len(evs), 1)


class TestR617DupEvents(BootBase):
    def test_R6_17_no_duplicate_mutation_events(self):
        """Repeated boot passes create no unbounded duplicate authoritative
        mutation events."""
        sup2, _, _ = self._idempotency_setup()
        snap1 = self._durable_snapshot(sup2.gate)
        boot_recover(sup2)
        snap2 = self._durable_snapshot(sup2.gate)
        boot_recover(sup2)
        snap3 = self._durable_snapshot(sup2.gate)
        for et in ("job.lease_reclaimed", "worker.fence_enforced",
                   "job.uncertain_opened"):
            self.assertEqual(snap1["evcounts"].get(et, 0),
                             snap2["evcounts"].get(et, 0), et)
            self.assertEqual(snap2["evcounts"].get(et, 0),
                             snap3["evcounts"].get(et, 0), et)


# ------------------------------------------------------------------ R6-18
class TestR618(BootBase):
    def test_R6_18_orphan_process_evidence(self):
        """Persisted spawn evidence with no corresponding authoritative
        worker row reaches the explicit ORPHAN disposition: never silently
        adopted, never killed."""
        sleep = subprocess.Popen(["sleep", "60"], start_new_session=True)
        self._extra_popen.append(sleep)
        pid = sleep.pid
        start = boot_mod.proc_start_jiffies(pid)
        self.assertIsNotNone(start)
        self.gate.append_event(
            "worker.proc_spawned",
            {"worker_id": "w-ghost", "proc_id": "p-ghost", "pid": pid,
             "start_jiffies": start, "pgid": pid, "job_id": None,
             "behavior_kind": "test-orphan"}, "test")
        sup2 = self._new_sup(actor="test-r6-18")
        rep = sup2._boot_report
        d = self._dispositions(rep).get(("w-ghost", "p-ghost"))
        self.assertIsNotNone(d, f"no disposition: {rep['dispositions']}")
        self.assertEqual(d["disposition"], "ORPHAN")
        self.assertNotIn(("w-ghost", "p-ghost"), sup2._adopted)
        # The orphan is never signalled — not at boot, not by later sweeps.
        time.sleep(self.H * 2)
        self.assertFalse(_proc_gone(pid))
        self.assertEqual(self._fence_events(sup2.gate, "w-ghost"), [])


# ------------------------------------------------------------------ R6-19
class TestR619(BootBase):
    def test_R6_19a_supervisor_crash_converges(self):
        """Kill the supervisor process itself (SIGKILL, real crash): the
        next boot adopts the surviving renewing worker; a further restart
        converges identically with no duplicate reclaims."""
        drv_path = os.path.join(HERE, "_supdrv.py")
        ready = os.path.join(self.tmp, "ready19a")
        drv = subprocess.Popen(
            [sys.executable, drv_path, "--db", self.db,
             "--scenario", "boot_crash", "--ready", ready])
        pid = None
        try:
            self._wait(lambda: os.path.exists(ready), timeout=30)
            g = self._temp_gate()
            sp = [s for s in g.unreaped_proc_spawns()
                  if s["worker_id"] == "wdrv"][0]
            proc_id, pid = sp["proc_id"], sp["pid"]
            os.kill(drv.pid, signal.SIGKILL)  # the crash
            drv.wait(timeout=10)
            sup2 = self._new_sup(actor="test-r6-19a")
            rep = sup2._boot_report
            self.assertEqual(rep["phase"], "READY")
            d = self._dispositions(rep).get(("wdrv", proc_id))
            self.assertIsNotNone(d, f"no disposition: {rep['dispositions']}")
            self.assertEqual(d["disposition"], "ADOPT")
            self.assertIn(("wdrv", proc_id), sup2._adopted)
            self.assertFalse(_proc_gone(pid))
            # Interrupt again: a second boot converges identically.
            sup2.close()
            sup3 = self._new_sup(actor="test-r6-19a-2")
            rep3 = sup3._boot_report
            self.assertEqual(rep3["phase"], "READY")
            d3 = self._dispositions(rep3).get(("wdrv", proc_id))
            self.assertEqual(d3["disposition"], "ADOPT")
            self.assertEqual(rep3["leases_reclaimed"], 0)
            self.assertEqual(
                self._event_count(sup3.gate, "job.lease_reclaimed"), 0)
            j3 = sup3.gate.get_job("jdrv")
            self.assertEqual(j3["owner_worker_id"], "wdrv")
        finally:
            try:
                os.kill(drv.pid, signal.SIGKILL)
            except Exception:
                pass
            if pid is not None:
                self._extra_pids.append(pid)

    def test_R6_19b_boot_interrupted_mid_recovery(self):
        """SIGKILL the recovery process while it is blocked inside the R4
        observation step (after integrity/ledger checks, before any
        reclaim): the next boot completes the pass and reclaims the expired
        lease exactly once. Deterministic convergence, no partial state."""
        drv_path = os.path.join(self.tmp, "r6_19b_drv.py")
        with open(drv_path, "w") as f:
            f.write(_R6_19B_DRIVER)
        proc_id = self._spawn(self.sup, "w-int", "j-int", "heartbeat_loop",
                              ttl_s=2, hb=0.2, renew=False)
        j = self._wait_claimed(self.gate, "j-int", "w-int")
        tok = j["fencing_token"]
        pid = self.sup._procs["w-int"].popen.pid
        popen = self.sup._procs["w-int"].popen
        self.sup.close()
        os.killpg(pid, signal.SIGKILL)
        popen.wait(timeout=10)
        g = self._temp_gate()
        self._wait(lambda: g.get_job("j-int")["lease_expires_at"]
                   <= g.store.current_time(), timeout=20)
        sentinel = os.path.join(self.tmp, "inboot")
        drv = subprocess.Popen([sys.executable, drv_path, "--db", self.db,
                                "--sentinel", sentinel])
        try:
            self._wait(lambda: os.path.exists(sentinel), timeout=30)
            # The driver is now blocked inside boot's observe_expired_leases:
            # integrity + ledger checks passed, no reclaim has run.
            os.kill(drv.pid, signal.SIGKILL)
            drv.wait(timeout=10)
            sup2 = self._new_sup(actor="test-r6-19b")
            rep = sup2._boot_report
            self.assertEqual(rep["phase"], "READY")
            self.assertIn("j-int", rep["leases_reclaimed_jobs"])
            evs = [e for e in self._lease_reclaimed_events(sup2.gate)
                   if e["payload"]["job_id"] == "j-int"]
            self.assertEqual(len(evs), 1,
                             "interrupted boot must not double-reclaim")
            self.assertEqual(evs[0]["actor"], "system")
            j2 = sup2.gate.get_job("j-int")
            self.assertEqual(j2["fencing_token"], tok + 1)
            self.assertIsNone(j2["owner_worker_id"])
        finally:
            try:
                os.kill(drv.pid, signal.SIGKILL)
            except Exception:
                pass


# ------------------------------------------------------------------ R6-20
class TestR620(BootBase):
    def test_R6_20_mixed_worker_outcomes(self):
        """One boot with five workers — ADOPT, FENCE, ALREADY_DEAD,
        PID_REUSE, ORPHAN — each disposition lands on the right worker and
        no worker's outcome corrupts another's state."""
        p_adopt = self._spawn(self.sup, "w-adopt", "j-adopt",
                              "heartbeat_loop", ttl_s=120, hb=0.3,
                              renew=True, duration_s=600)
        self._wait_claimed(self.gate, "j-adopt", "w-adopt")
        self._wait(lambda: len(self.gate.heartbeats_for("w-adopt")) >= 2,
                   timeout=15)
        p_fence = self._spawn(self.sup, "w-fence", "j-fence", "wedged",
                              ttl_s=120, renew=False)
        jf = self._wait_claimed(self.gate, "j-fence", "w-fence")
        p_dead = self._spawn(self.sup, "w-dead", "j-dead", "heartbeat_loop",
                             ttl_s=120, hb=0.3, renew=True)
        self._wait_claimed(self.gate, "j-dead", "w-dead")
        p_reuse = self._spawn(self.sup, "w-reuse", "j-reuse", "wedged",
                              ttl_s=120, renew=False)
        self._wait_claimed(self.gate, "j-reuse", "w-reuse")
        sleep = subprocess.Popen(["sleep", "60"], start_new_session=True)
        self._extra_popen.append(sleep)

        pid_adopt = self.sup._procs["w-adopt"].popen.pid
        pid_fence = self.sup._procs["w-fence"].popen.pid
        pid_dead = self.sup._procs["w-dead"].popen.pid
        popen_dead = self.sup._procs["w-dead"].popen
        pid_reuse = self.sup._procs["w-reuse"].popen.pid
        durable_reuse = boot_mod.proc_start_jiffies(pid_reuse)
        self.assertIsNotNone(durable_reuse)
        j_adopt_before = self.gate.get_job("j-adopt")
        tok_adopt = j_adopt_before["fencing_token"]
        status_adopt = j_adopt_before["status"]

        self.sup.close()
        os.killpg(pid_dead, signal.SIGKILL)
        popen_dead.wait(timeout=10)
        g = self._temp_gate()
        g.reclaim_lease("j-fence", actor="reconciler",
                        reason="STALLED test",
                        expected_owner="w-fence",
                        expected_token=jf["fencing_token"],
                        force=True, verdict="STALLED",
                        incident_id="inc-r6-20")
        g.append_event(
            "worker.proc_spawned",
            {"worker_id": "w-ghost", "proc_id": "p-ghost",
             "pid": sleep.pid,
             "start_jiffies": boot_mod.proc_start_jiffies(sleep.pid),
             "pgid": sleep.pid, "job_id": None,
             "behavior_kind": "test-orphan"}, "test")
        real_fn = boot_mod.proc_start_jiffies
        with mock.patch.object(
                boot_mod, "proc_start_jiffies",
                side_effect=lambda p: (durable_reuse + 99999
                                       if p == pid_reuse else real_fn(p))):
            sup2 = self._new_sup(actor="test-r6-20")
        rep = sup2._boot_report
        self.assertEqual(rep["phase"], "READY")
        disps = self._dispositions(rep)
        self.assertEqual(disps.get(("w-adopt", p_adopt), {}).get(
            "disposition"), "ADOPT")
        self.assertEqual(disps.get(("w-fence", p_fence), {}).get(
            "disposition"), "FENCE")
        self.assertEqual(disps.get(("w-dead", p_dead), {}).get(
            "disposition"), "ALREADY_DEAD")
        self.assertEqual(disps.get(("w-reuse", p_reuse), {}).get(
            "disposition"), "PID_REUSE")
        self.assertEqual(disps.get(("w-ghost", "p-ghost"), {}).get(
            "disposition"), "ORPHAN")
        # No cross-contamination.
        j2 = sup2.gate.get_job("j-adopt")
        self.assertEqual(j2["owner_worker_id"], "w-adopt")
        self.assertEqual(j2["fencing_token"], tok_adopt)
        self.assertEqual(j2["status"], status_adopt)
        self._wait(lambda: _proc_gone(pid_fence), timeout=15)
        self.assertFalse(_proc_gone(pid_reuse),
                         "PID_REUSE process must survive")
        self.assertFalse(_proc_gone(sleep.pid), "orphan must survive")
        self.assertFalse(_proc_gone(pid_adopt), "adopted worker must live")

# ------------------------------------------------------------------ R6-21
class TestR621(BootBase):
    def test_R6_21a_no_scheduling_before_ready(self):
        """start_worker and restart_worker refuse while boot has not
        reached READY (existing scheduling boundary, not a new scheduler)."""
        rep = self.sup._boot_report
        self.assertEqual(rep["phase"], "READY")
        self.sup._boot_report = {"phase": "RECOVERING"}
        try:
            with self.assertRaises(TransitionRejected):
                self.sup.start_worker("w-x", "j-x", {"kind": "hang"})
            with self.assertRaises(TransitionRejected):
                self.sup.restart_worker("w-x", "j-x", {"kind": "hang"})
        finally:
            self.sup._boot_report = rep

    def test_R6_21b_blocked_boot_permits_nothing(self):
        """A boot that fails integrity/ledger verification stays BLOCKED:
        no sweep thread, no scheduling, boot.recovery_blocked journaled and
        boot.completed never emitted."""
        with mock.patch.object(TransitionGate, "verify_ledger_chain",
                               return_value=(False, "test-block")):
            sup = self._new_sup(actor="test-r6-21b")
        rep = sup._boot_report
        self.assertEqual(rep["phase"], "BLOCKED")
        self.assertFalse(rep["fully_recovered"])
        self.assertIsNone(sup._sweep_thread)
        with self.assertRaises(TransitionRejected):
            sup.start_worker("w-x", "j-x", {"kind": "hang"})
        with self.assertRaises(TransitionRejected):
            sup.restart_worker("w-x", "j-x", {"kind": "hang"})
        evs = self._boot_events(sup.gate, rep["boot_id"])
        self.assertIn("boot.recovery_blocked", evs)
        self.assertNotIn("boot.completed", evs)
        self.assertNotIn("boot.reconstructed", evs)


# ------------------------------------------------------------- R6-22/23
class TestR622Static(BootBase):
    def test_R6_22_no_direct_reclaim_authority(self):
        """Static: exec/boot.py implements no second reclaim mechanism —
        no SQL writes, no transaction of its own; gate.reclaim_lease is the
        only reclaim path."""
        self._assert_no_direct_reclaim()


class TestR623Static(BootBase):
    def test_R6_23_no_direct_kill_authority(self):
        """Static: exec/boot.py implements no process-group termination —
        no killpg, no subprocess spawning, no SIGTERM/SIGKILL; os.kill
        appears only as the signal-0 existence probe."""
        self._assert_no_direct_kill()


# ------------------------------------------------------------------ R6-24
class TestR624Audit(BootBase):
    def test_R6_24_authority_audit_green(self):
        """The complete authority audit (09, incl. the R6 A12 checks)
        passes: R6 introduces no bypass."""
        p = subprocess.run(
            [sys.executable, "audit/09_authority_audit.py"],
            cwd=AXOS_DIR, capture_output=True, text=True, timeout=180)
        self.assertEqual(p.returncode, 0,
                         f"authority audit failed:\n{p.stdout}\n{p.stderr}")
        self.assertIn("all checks passed", p.stdout)


# ------------------------------------------------------------------ R6-25
class TestR625SweepRace(BootBase):
    def test_R6_25_boot_and_background_sweep_do_not_double_fence(self):
        """The previously fixed race: boot's synchronous sweep and the
        background supervisor sweep must not double-process the same
        runtime evidence — exactly one worker.fence_enforced event and one
        kill for the fenced process."""
        proc_id = self._spawn(self.sup, "w-race", "j-race", "wedged",
                              ttl_s=120, renew=False)
        j = self._wait_claimed(self.gate, "j-race", "w-race")
        tok = j["fencing_token"]
        pid = self.sup._procs["w-race"].popen.pid
        self.sup.close()
        g = self._temp_gate()
        g.reclaim_lease("j-race", actor="reconciler",
                        reason="STALLED test",
                        expected_owner="w-race", expected_token=tok,
                        force=True, verdict="STALLED",
                        incident_id="inc-r6-25")
        # Boot fences synchronously during construction; the background
        # thread then starts and must not re-fence.
        sup2 = self._new_sup(actor="test-r6-25")  # H=0.6 -> sweep / 0.3s
        rep = sup2._boot_report
        d = self._dispositions(rep).get(("w-race", proc_id))
        self.assertIsNotNone(d, f"no disposition: {rep['dispositions']}")
        self.assertEqual(d["disposition"], "FENCE")
        self._wait_fence(sup2.gate, "w-race", proc_id)
        time.sleep(self.H * 4)  # several background sweep intervals
        evs = self._fence_events(sup2.gate, "w-race", proc_id)
        self.assertEqual(len(evs), 1,
                         f"double-fenced: {len(evs)} enforcement events")
        self.assertEqual(evs[0]["outcome"], "killed")
        self.assertTrue(_proc_gone(pid))


if __name__ == "__main__":
    unittest.main()
