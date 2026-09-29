"""Phase 1C R2 — enforced process-group self-fencing tests.

STANDARD: no mocks for the critical guarantees. Real SQLite/WAL files,
real subprocesses (parent/child/grandchild), real SIGTERM/SIGKILL via
killpg, real monotonic timing, real supervisor restart on the same db.
The single exception is the DB-read-failure test, which raises inside the
gate to prove the sweep fails closed instead of killing.

Test IDs:
  R2-01  external authorized R1 reclaim -> automatic sweep enforcement
  R2-02  process-group death: parent, child, grandchild all die
  R2-03  graceful SIGTERM: no SIGKILL, worker exits 143
  R2-04  SIGTERM ignored: SIGKILL escalation
  R2-05  measured fencing-transaction -> group-death latency < H
  R2-06  sweep runs continuously, approximately every H/2
  R2-07  owner divergence detected (adopted path after restart)
  R2-08  token divergence detected (tracked path, owner re-claims)
  R2-09  already-dead process: evidence recorded, no signals sent
  R2-10  multiple workers: unrelated worker survives
  R2-11  fresh replacement (new worker, new authority) survives
  R2-12  repeated sweeps: exactly-once enforcement
  R2-13  supervisor restart: adopted proc fenced from durable evidence
  R2-14  cooperative fencing compatible (worker self-fences first)
"""
import json
import os
import shutil
import signal
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace

from axos.exec.supervisor import Supervisor, _FenceRefused


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
            # pid (comm) state ppid pgrp ... -> [0] is the state
            state = f.read().rsplit(")", 1)[1].split()[0]
        return state in ("Z", "X", "x")
    except Exception:
        return True


def _descendants(root_pid: int) -> list[int]:
    """All live descendant pids of root_pid via /proc parent links."""
    children: dict[int, list[int]] = {}
    for pid_s in os.listdir("/proc"):
        if not pid_s.isdigit():
            continue
        try:
            with open(f"/proc/{pid_s}/stat") as f:
                ppid = int(f.read().rsplit(")", 1)[1].split()[1])
        except Exception:
            continue
        children.setdefault(ppid, []).append(int(pid_s))
    out: list[int] = []
    stack = list(children.get(root_pid, []))
    while stack:
        p = stack.pop()
        out.append(p)
        stack.extend(children.get(p, []))
    return out


class FenceBase(unittest.TestCase):
    H = 0.6  # heartbeat interval for the default supervisor

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r2-")
        self.db = os.path.join(self.tmp, "t.db")
        self._sups: list[Supervisor] = []
        self.sup = self._new_sup(H=self.H, actor="test-supervisor")
        self.gate = self.sup.gate
        self.gate.create_task("t", {"objective": "x"}, {"usd": 1},
                              "scheduler")

    def _new_sup(self, H=None, actor=None) -> Supervisor:
        s = Supervisor(self.db,
                       actor=actor or f"test-sup-{len(self._sups)}",
                       heartbeat_interval_s=H or self.H)
        self._sups.append(s)
        return s

    def tearDown(self):
        for s in self._sups:
            try:
                for wid in list(s._procs):
                    try:
                        if s._procs[wid].popen.poll() is None:
                            s.kill_worker(wid)
                        else:
                            # Reap the zombie through the stale handle so no
                            # duplicate kill is attempted.
                            s.reap(wid)
                    except Exception:
                        pass
                s.close()
            except Exception:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- helpers
    def _spawn(self, sup, wid, jid, kind, fresh_job=True, ttl_s=60.0,
               hb=0.5, renew=False):
        if fresh_job:
            self.gate.create_job(jid, "t", "s", "scheduler")
        return sup.start_worker(wid, jid, {"kind": kind}, ttl_s=ttl_s,
                                hb_interval_s=hb, renew=renew)

    def _wait(self, pred, timeout=15.0, interval=0.02):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("timed out waiting for condition")

    def _wait_claimed(self, jid, wid, timeout=15.0):
        def _p():
            try:
                j = self.gate.get_job(jid)
            except Exception:
                return False
            return (j["owner_worker_id"] == wid
                    and j["status"] in ("CLAIMED", "RUNNING"))
        self._wait(_p, timeout)
        return self.gate.get_job(jid)

    def _wait_epoch_learned(self, sup, wid, token, timeout=15.0):
        """Wait until the sweep has observed this proc's authority epoch."""
        self._wait(lambda: sup._procs[wid].known_token == token, timeout)

    def _force_reclaim(self, jid, wid, token):
        return self.gate.reclaim_lease(
            jid, actor="reconciler", reason="STALLED",
            expected_owner=wid, expected_token=token,
            force=True, verdict="STALLED", incident_id="inc-r2")

    def _fence_events(self, wid=None):
        q = ("SELECT payload FROM ledger "
             "WHERE event_type='worker.fence_enforced'")
        params: tuple = ()
        if wid is not None:
            q += " AND json_extract(payload,'$.worker_id')=?"
            params = (wid,)
        q += " ORDER BY seq"
        rows = self.gate.store.conn.execute(q, params).fetchall()
        return [json.loads(r[0]) for r in rows]

    def _wait_fence(self, wid, proc_id=None, timeout=15.0):
        def _p():
            evs = self._fence_events(wid)
            if proc_id is not None:
                evs = [e for e in evs if e["proc_id"] == proc_id]
            return len(evs) > 0
        self._wait(_p, timeout)
        evs = self._fence_events(wid)
        if proc_id is not None:
            evs = [e for e in evs if e["proc_id"] == proc_id]
        return evs[0]

    def _assert_chain(self):
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)


# ------------------------------------------------------------------ R2-01
class TestR201(FenceBase):
    def test_R2_01_reclaim_triggers_automatic_fence(self):
        """An external authorized R1 reclaim is enforced by the sweep with
        no further prompting: SIGTERM the group, reap, durable evidence."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)
        pid = self.sup._procs["w1"].popen.pid

        self._force_reclaim("j1", "w1", tok)

        ev = self._wait_fence("w1", proc_id)
        self.assertEqual(ev["worker_id"], "w1")
        self.assertEqual(ev["proc_id"], proc_id)
        self.assertEqual(ev["pid"], pid)
        self.assertEqual(ev["pgid"], pid)  # session leader => pgid == pid
        self.assertEqual(ev["old_token"], tok)
        self.assertEqual(ev["new_token"], tok + 1)
        self.assertEqual(ev["signals"], ["SIGTERM"])
        self.assertEqual(ev["outcome"], "killed")
        self.assertTrue(ev["within_bound"])
        self.assertGreaterEqual(ev["fenced_out_worker_seconds"], 0)
        self.assertIsNotNone(ev["fence_tx_ts"])
        self.assertIsNotNone(ev["death_ts"])
        self.assertFalse(ev["adopted"])

        self._wait(lambda: "w1" in self.sup._reaped)
        self.assertTrue(_proc_gone(pid))
        self._assert_chain()


# ------------------------------------------------------------------ R2-02
class TestR202(FenceBase):
    def test_R2_02_process_group_death(self):
        """Fencing kills the whole process group: parent, child and
        grandchild are all gone afterwards."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged_spawn_child")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)
        pid = self.sup._procs["w1"].popen.pid

        self._wait(lambda: len(_descendants(pid)) >= 2, timeout=15.0)
        fam = _descendants(pid)
        self.assertGreaterEqual(len(fam), 2)

        self._force_reclaim("j1", "w1", tok)
        ev = self._wait_fence("w1", proc_id)
        self.assertEqual(ev["outcome"], "killed")

        for p in [pid] + fam:
            self._wait(lambda p=p: _proc_gone(p), timeout=15.0)
        self._assert_chain()


# ------------------------------------------------------------------ R2-03
class TestR203(FenceBase):
    def test_R2_03_graceful_sigterm_no_sigkill(self):
        """A cooperative worker dies on SIGTERM inside the grace period:
        no SIGKILL is sent and the worker reports its SIGTERM exit."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)

        self._force_reclaim("j1", "w1", tok)
        ev = self._wait_fence("w1", proc_id)

        self.assertEqual(ev["signals"], ["SIGTERM"])
        self.assertNotIn("SIGKILL", ev["signals"])
        self._wait(lambda: "w1" in self.sup._reaped)
        # InterruptedExecution on SIGTERM -> worker EXIT_SIGTERM
        self.assertEqual(self.sup._reaped["w1"]["returncode"], 143)


# ------------------------------------------------------------------ R2-04
class TestR204(FenceBase):
    def test_R2_04_sigterm_ignored_escalates_to_sigkill(self):
        """A worker that ignores SIGTERM survives the grace period and is
        then SIGKILLed; both signals are recorded."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged_ignore_sigterm")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)
        pid = self.sup._procs["w1"].popen.pid

        self._force_reclaim("j1", "w1", tok)
        ev = self._wait_fence("w1", proc_id)

        self.assertEqual(ev["signals"], ["SIGTERM", "SIGKILL"])
        self.assertEqual(ev["outcome"], "killed")
        self._wait(lambda: _proc_gone(pid), timeout=15.0)
        self._wait(lambda: "w1" in self.sup._reaped)
        self.assertEqual(self.sup._reaped["w1"]["returncode"], -9)


# ------------------------------------------------------------------ R2-05
class TestR205(FenceBase):
    def test_R2_05_fencing_latency_within_one_heartbeat(self):
        """Real monotonic measurement: group death happens within one
        heartbeat interval H of the fencing transaction (worst-case
        detection alignment: reclaim lands just after a sweep)."""
        H = 1.5
        sup = self._new_sup(H=H, actor="test-sup-timing")
        proc_id = self._spawn(sup, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(sup, "w1", tok)
        pid = sup._procs["w1"].popen.pid

        # Synchronize: reclaim immediately after a sweep completes, so the
        # detection latency is near its H/2 worst case.
        last = sup._last_sweep.get("swept_at")
        self._wait(lambda: sup._last_sweep.get("swept_at") != last,
                   timeout=10.0)
        t_reclaim = time.monotonic()
        self._force_reclaim("j1", "w1", tok)

        while True:
            try:
                os.killpg(pid, 0)
            except ProcessLookupError:
                break
            if time.monotonic() - t_reclaim >= H:
                break
            time.sleep(0.02)
        t_death = time.monotonic()
        try:
            os.killpg(pid, 0)
            still_alive = True
        except ProcessLookupError:
            still_alive = False
        self.assertFalse(still_alive, "process group outlived H")
        self.assertLess(t_death - t_reclaim, H,
                        f"fence latency {t_death - t_reclaim:.3f}s >= H={H}s")

        ev = self._wait_fence("w1", proc_id)
        self.assertTrue(ev["within_bound"])
        self.assertLess(ev["fenced_out_worker_seconds"], H)


# ------------------------------------------------------------------ R2-06
class TestR206(FenceBase):
    def test_R2_06_sweep_runs_continuously(self):
        """The sweep fires on its own approximately every H/2 with no
        prompting — it is not a busy loop and it does not stall."""
        stamps: list[float] = []
        last = None
        deadline = time.monotonic() + 2.5
        while time.monotonic() < deadline:
            s = self.sup._last_sweep.get("swept_at")
            if s is not None and s != last:
                stamps.append(time.monotonic())
                last = s
            time.sleep(0.02)
        self.assertGreaterEqual(len(stamps), 4,
                                f"only {len(stamps)} sweeps in 2.5s")
        for a, b in zip(stamps, stamps[1:]):
            self.assertGreater(b - a, 0.10, "sweep is a busy loop")
            self.assertLess(b - a, 0.90, "sweep stalled")


# ------------------------------------------------------------------ R2-07
class TestR207(FenceBase):
    def test_R2_07_owner_divergence_detected(self):
        """Owner divergence on the adopted path: after a supervisor
        restart, a live worker whose job now belongs to someone else is
        fenced from durable evidence alone."""
        sup1 = self._new_sup(actor="test-sup-a")
        proc_id = self._spawn(sup1, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        pid = sup1._procs["w1"].popen.pid
        sup1.close()  # wedged worker keeps running; no reap milestone

        # A new owner takes the job through the gate.
        self._force_reclaim("j1", "w1", tok)
        self.gate.transition_job("j1", "PENDING", "reconciler")
        self.assertTrue(self.gate.claim_job("j1", "w2", 60.0, "scheduler"))

        sup2 = self._new_sup(actor="test-sup-b")
        # R6: boot recovery fences stale survivors synchronously during
        # construction (deterministic containment, not a background race).
        # The boot report is the durable source of truth for the adopted
        # path — the process may already be gone from _adopted.
        disps = {(d["worker_id"], d["proc_id"]): d["disposition"]
                 for d in sup2._boot_report["dispositions"]}
        self.assertEqual(disps.get(("w1", proc_id)), "FENCE")

        ev = self._wait_fence("w1", proc_id)
        self.assertTrue(ev["adopted"])
        self.assertEqual(ev["outcome"], "killed")
        self.assertEqual(ev["signals"], ["SIGTERM"])
        # reclaim bumped the token, w2's claim bumped it again
        self.assertEqual(ev["new_token"], tok + 2)
        self._wait(lambda: _proc_gone(pid), timeout=15.0)
        self._assert_chain()


# ------------------------------------------------------------------ R2-08
class TestR208(FenceBase):
    def test_R2_08_token_divergence_detected(self):
        """Token divergence with the owner unchanged: the same worker
        re-claims after a reclaim, but this process instance holds the old
        epoch and must still be fenced."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)

        # Reclaim + re-claim atomically w.r.t. the sweep so the fence
        # decision deterministically sees owner w1 with a newer token.
        with self.sup._sweep_lock:
            self._force_reclaim("j1", "w1", tok)
            self.gate.transition_job("j1", "PENDING", "reconciler")
            self.assertTrue(
                self.gate.claim_job("j1", "w1", 60.0, "scheduler"))
        j2 = self.gate.get_job("j1")
        self.assertEqual(j2["owner_worker_id"], "w1")
        self.assertNotEqual(j2["fencing_token"], tok)

        ev = self._wait_fence("w1", proc_id)
        self.assertEqual(ev["old_token"], tok)
        self.assertEqual(ev["new_token"], j2["fencing_token"])
        self.assertEqual(ev["outcome"], "killed")
        self.assertTrue(ev["within_bound"])


# ------------------------------------------------------------------ R2-09
class TestR209(FenceBase):
    def test_R2_09_already_dead_process(self):
        """A process that dies after fencing but before the sweep detects
        it: fencing evidence is still recorded, but no signal is sent to a
        dead group."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)
        pid = self.sup._procs["w1"].popen.pid

        # Kill and reclaim atomically w.r.t. the sweep: the sweep must see
        # a dead-but-tracked, fenced process.
        with self.sup._sweep_lock:
            os.kill(pid, signal.SIGKILL)
            self._force_reclaim("j1", "w1", tok)

        ev = self._wait_fence("w1", proc_id)
        self.assertEqual(ev["outcome"], "already_dead")
        self.assertEqual(ev["signals"], [])
        self.assertEqual(len(self._fence_events("w1")), 1)
        self._assert_chain()


# ------------------------------------------------------------------ R2-10
class TestR210(FenceBase):
    def test_R2_10_unrelated_worker_survives(self):
        """Fencing one worker never touches another worker's live process,
        even on the same supervisor."""
        p1 = self._spawn(self.sup, "w1", "j1", "wedged")
        p2 = self._spawn(self.sup, "w2", "j2", "wedged")
        j1 = self._wait_claimed("j1", "w1")
        j2 = self._wait_claimed("j2", "w2")
        self._wait_epoch_learned(self.sup, "w1", j1["fencing_token"])
        self._wait_epoch_learned(self.sup, "w2", j2["fencing_token"])

        self._force_reclaim("j1", "w1", j1["fencing_token"])
        ev = self._wait_fence("w1", p1)
        self.assertEqual(ev["outcome"], "killed")

        time.sleep(self.H * 3)  # several sweep intervals
        self.assertIsNone(self.sup._procs["w2"].popen.poll())
        self.assertEqual(self._fence_events("w2"), [])
        j2b = self.gate.get_job("j2")
        self.assertEqual(j2b["owner_worker_id"], "w2")
        self.assertEqual(j2b["fencing_token"], j2["fencing_token"])


# ------------------------------------------------------------------ R2-11
class TestR211(FenceBase):
    def test_R2_11_fresh_replacement_survives(self):
        """A fresh worker with fresh authority on the same job is not
        fenced; the old process's enforcement stays exactly-once."""
        p1 = self._spawn(self.sup, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)
        self._force_reclaim("j1", "w1", tok)
        ev = self._wait_fence("w1", p1)
        self.assertEqual(ev["outcome"], "killed")

        self.gate.transition_job("j1", "PENDING", "reconciler")
        p2 = self._spawn(self.sup, "w2", "j1", "wedged", fresh_job=False)
        j2 = self._wait_claimed("j1", "w2")
        self._wait_epoch_learned(self.sup, "w2", j2["fencing_token"])

        time.sleep(self.H * 3)
        self.assertIsNone(self.sup._procs["w2"].popen.poll())
        self.assertEqual(self._fence_events("w2"), [])
        self.assertEqual(len(self._fence_events("w1")), 1)


# ------------------------------------------------------------------ R2-12
class TestR212(FenceBase):
    def test_R2_12_repeated_sweeps_idempotent(self):
        """Enforcement is exactly-once: repeated sweeps after a fence
        append no further events and attempt no further kills."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)
        self._force_reclaim("j1", "w1", tok)
        ev = self._wait_fence("w1", proc_id)

        for _ in range(3):
            self.sup.fence_sweep()
        evs = self._fence_events("w1")
        self.assertEqual(len(evs), 1)
        self.assertEqual(evs[0]["signals"], ["SIGTERM"])
        self.assertEqual(evs[0]["outcome"], "killed")
        self.assertEqual(ev["proc_id"], proc_id)


# ------------------------------------------------------------------ R2-13
class TestR213(FenceBase):
    def test_R2_13_supervisor_restart_enforces(self):
        """A restarted supervisor's first duty is a fence sweep: the
        wedged worker outlives sup1, is adopted from durable spawn
        evidence, and is fenced once the reclaim lands."""
        sup1 = self._new_sup(actor="test-sup-a")
        proc_id = self._spawn(sup1, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        pid = sup1._procs["w1"].popen.pid
        self._wait(lambda: sup1._procs["w1"].known_token == tok)
        sup1.close()  # wedged worker keeps running; no reap milestone

        sup2 = self._new_sup(actor="test-sup-b")
        self._wait(lambda: ("w1", proc_id) in sup2._adopted, timeout=10.0)
        # Still owned: adoption alone must NOT fence.
        time.sleep(self.H * 2)
        self.assertEqual(self._fence_events("w1"), [])
        self.assertFalse(_proc_gone(pid))

        self._force_reclaim("j1", "w1", tok)
        ev = self._wait_fence("w1", proc_id)
        self.assertTrue(ev["adopted"])
        self.assertEqual(ev["outcome"], "killed")
        self.assertEqual(ev["old_token"], tok)
        self.assertEqual(ev["new_token"], tok + 1)
        self.assertEqual(ev["signals"], ["SIGTERM"])
        self._wait(lambda: _proc_gone(pid), timeout=15.0)
        self._assert_chain()


# ------------------------------------------------------------------ R2-14
class TestR214(FenceBase):
    def test_R2_14_cooperative_fencing_compatible(self):
        """A cooperative worker notices the fencing transaction itself via
        heartbeat rejection and exits FENCED(4) before the sweep ever needs
        to signal it: the sweep then only records the evidence."""
        sup = self._new_sup(H=5.0, actor="test-sup-coop")
        proc_id = self._spawn(sup, "w1", "j1", "heartbeat_loop", hb=0.2)
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        # Heartbeats every 0.2s vs sweep every 2.5s: the worker's own
        # fencing check deterministically wins the race.
        self._force_reclaim("j1", "w1", tok)

        self._wait(lambda: "w1" in sup._reaped, timeout=15.0)
        self.assertEqual(sup._reaped["w1"]["returncode"], 4)

        ev = self._wait_fence("w1", proc_id)
        self.assertEqual(ev["outcome"], "already_dead")
        self.assertEqual(ev["signals"], [])


# ------------------------------------------------------- failure semantics
class TestFenceFailures(FenceBase):
    def test_db_read_failure_kills_nothing(self):
        """Fail closed: when authoritative state cannot be read, the sweep
        attempts no kill and records one observation-failure episode."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)

        with mock.patch("axos.store.gate.TransitionGate.get_job",
                        side_effect=sqlite3.OperationalError("disk I/O")):
            for _ in range(3):
                self.sup.fence_sweep()
                time.sleep(0.05)
            self.assertIsNone(self.sup._procs["w1"].popen.poll())
            self.assertEqual(self._fence_events("w1"), [])
            n = self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type="
                "'worker.fence_sweep_observation_failed'").fetchone()[0]
            self.assertEqual(n, 1)

        # Store healthy again and the job still owned: still no fence.
        self.sup.fence_sweep()
        time.sleep(self.H)
        self.assertIsNone(self.sup._procs["w1"].popen.poll())
        self.assertEqual(self._fence_events("w1"), [])

    def test_child_already_dead_group_still_fenced(self):
        """A group with an independently-dead member is still fenced as a
        group; the dead child does not break killpg."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged_spawn_child")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)
        pid = self.sup._procs["w1"].popen.pid
        self._wait(lambda: len(_descendants(pid)) >= 2, timeout=15.0)
        fam = _descendants(pid)

        os.kill(fam[0], signal.SIGKILL)
        self._wait(lambda: _proc_gone(fam[0]), timeout=15.0)

        self._force_reclaim("j1", "w1", tok)
        ev = self._wait_fence("w1", proc_id)
        self.assertEqual(ev["outcome"], "killed")
        for p in [pid] + fam[1:]:
            self._wait(lambda p=p: _proc_gone(p), timeout=15.0)

    def test_invalid_pgid_is_refused(self):
        """The pgid guard refuses the supervisor's own group and unknown
        pids, and resolves a real worker target to its session leader."""
        with self.assertRaises(_FenceRefused):
            self.sup._resolve_target_pgid(os.getpid())
        with self.assertRaises(ProcessLookupError):
            self.sup._resolve_target_pgid(1 << 30)
        self._spawn(self.sup, "w1", "j1", "wedged")
        pid = self.sup._procs["w1"].popen.pid
        self.assertEqual(self.sup._resolve_target_pgid(pid), pid)

    def test_worker_exits_during_grace_counts_as_graceful(self):
        """A worker that exits promptly on SIGTERM (during the grace
        window) is never SIGKILLed — covered by R2-03's signal record,
        asserted here against the grace bound explicitly."""
        proc_id = self._spawn(self.sup, "w1", "j1", "wedged")
        j = self._wait_claimed("j1", "w1")
        tok = j["fencing_token"]
        self._wait_epoch_learned(self.sup, "w1", tok)

        t0 = time.monotonic()
        self._force_reclaim("j1", "w1", tok)
        ev = self._wait_fence("w1", proc_id)
        # SIGTERM-only enforcement must complete well inside H/4 grace + H/2
        # detection; SIGKILL would prove the grace was exceeded.
        self.assertEqual(ev["signals"], ["SIGTERM"])
        self.assertLess(time.monotonic() - t0, self.H)


if __name__ == "__main__":
    unittest.main()
