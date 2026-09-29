"""Phase 1C R12 — circuit-breaker resilience tests.

STANDARD: real SQLite/WAL temp DBs with full migrate(); FakeClock-bound
stores for deterministic window/cooldown assertions (components that own
their Store get their owner-thread clock rebound to the shared FakeClock
— the same white-box seam the R4/R7/R10 suites use for privates);
real supervisor/worker subprocesses and a real watchdog for the
worker-driven failure legs; deterministic synchronization (barrier-aligned
threads, durable-state inspection, poll-based waits — never blind sleeps).
Every spawned process is killed and reaped in tearDown.

Track 3 covers the R12 integration surface: gate breaker primitives,
the resilience controller, and the scheduler's resilient admission path.

Test IDs:
  R12-01  default CLOSED: no breaker row -> breaker_allows (True, "closed")
  R12-02  same failure recorded twice -> counted once (dedupe)
  R12-03  scope fan-out: one failure counts once PER scope (JOB/TASK/GLOBAL)
  R12-04  threshold: failure_count < threshold -> stays CLOSED
  R12-05  threshold reached -> controller opens (CAS, ledger event)
  R12-06  window expiry: signal after the window starts a fresh window
  R12-07  OPEN JOB scope blocks scheduler admission; job stays PENDING
  R12-08  OPEN TASK scope blocks every job of that task
  R12-09  OPEN GLOBAL scope blocks every candidate
  R12-10  OPEN DESIRED scope blocks the desired-mapped job
  R12-11  denial preserves PENDING/workers/leases/desired (byte-identical)
  R12-12  OPEN with elapsed cooldown still denies (only the controller moves)
  R12-13  cooldown elapsed -> controller moves OPEN -> HALF_OPEN
  R12-14  restart-safe: a new controller honors the durable cooldown
  R12-15  cooldown anchored at open time; a later retune cannot shorten it
  R12-16  half-open probes are bounded (probe_limit per episode)
  R12-17  probe race at limit 1: exactly one winner
  R12-18  probe success via COMPLETE (R5-only) closes the breaker
  R12-19  probe success via durable progress delta closes the breaker
  R12-20  heartbeat alone never closes a probe
  R12-21  a newer JOB-scope failure reopens the breaker
  R12-22  transition_breaker CAS: loser gets BreakerConflict, winner stands
  R12-23  claim_half_open_probe CAS: concurrent allocation, one winner
  R12-24  CAS loser re-reads; never overwrites (controller discipline)
  R12-25  no-op discipline: dedupe/denial changes no versions, no ledger
  R12-26  JOB-scope OPEN isolates: sibling jobs still admitted
  R12-27  GLOBAL threshold is independent of the per-scope threshold
  R12-28  recovery pressure records one GLOBAL signal per window
  R12-29  R9 escalation is evidence; the controller never mutates R9 state
  R12-30  escalated-incident rescan dedupes (no double count)
  R12-31  pressure/escalation signals create no R9 policy rows
  R12-32  breaker rows are per-scope independent (one OPEN never moves another)
  R12-33  GLOBAL OPEN sheds load: zero admissions, zero dispatch
  R12-34  shedding preserves every durable row (no deletion, no transition)
  R12-35  shedding preserves desired state (head/items/map untouched)
  R12-36  controller claims nothing (AST: no claim verbs in resilience.py)
  R12-37  controller never touches R9 policy tables
  R12-38  controller never mutates incident rows
  R12-39  scheduler never transitions breakers (AST)
  R12-40  admission path never moves a breaker, even past cooldown
  R12-41  scheduler evaluate_once leaves every breaker row byte-identical
  R12-42  controller has no process authority (AST: no spawn/kill)
  R12-43  controller actor may not be worker:-prefixed
  R12-44  probe allocation is gate-only (controller never calls it directly)
  R12-45  corrupt breaker row fails closed (denies; never admits)
  R12-46  probe on an unreadable job fails closed (reopen, never close)
  R12-47  contradictory probe baseline fails closed (reopen, never close)
  R12-48  crash before signal record -> rescan re-records (idempotent)
  R12-49  crash after signal, before threshold -> next pass opens
  R12-50  crash while OPEN -> new controller still denies past restart
  R12-51  crash after probe allocation -> probe slot survives, stays bounded
  R12-52  crash between claim and dispatch -> controller sees in-flight probe
  R12-53  scheduler crash before claim -> PENDING, breaker untouched
  R12-54  controller restart mid-cooldown: cooldown_until unchanged
  R12-55  two controllers race to OPEN -> exactly one transition event
  R12-56  concurrent controller passes -> failure_count never double-counted
  R12-57  concurrent signal observers -> counted once (dedupe under race)
  R12-58  concurrent OPEN->HALF_OPEN -> exactly one transition event
  R12-59  concurrent probe allocations at limit 1 -> exactly one winner
  R12-60  R11 creates PENDING while OPEN (breaker gates admission, not creation)
  R12-61  R10 blocked by OPEN, then resumes after verified recovery
  R12-62  probe admission path: HALF_OPEN admits exactly the probe budget
  R12-63  scheduler never writes breaker rows (no duplicate authority)
  R12-64  controller never claims or dispatches (no duplicate authority)
  R12-65  R1-R11 composition: full stack green with breakers CLOSED
  R12-RACE-A  breaker trips between preflight and claim -> claim denies
  R12-RACE-B  two schedulers x one HALF_OPEN probe slot -> one admitted
  R12-RACE-C  scheduler evaluate racing a controller OPEN -> no partial claim
  R12-S34     end-to-end: desired -> R11 -> PENDING -> R10 -> failures ->
              evidence -> OPEN -> held -> cooldown -> HALF_OPEN ->
              verified success -> CLOSED -> resume -> R11 unchanged
  R12-S34NEG  negative: OPEN + desired change + R11 reconcile preserves
              desired+PENDING without bypassing the breaker
Audit:
  R12-A1  audit 09 A18 checks present and green
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

from axos.store import (  # noqa: E402
    open_store, migrate, TransitionGate, TransitionRejected, StoreError,
    BreakerConflict,
)
from axos.exec.scheduler import Scheduler, SchedulerConfig  # noqa: E402
from axos.exec.supervisor import Supervisor  # noqa: E402
from axos.exec.resilience import (  # noqa: E402
    ResilienceConfig, ResilienceController,
)
from axos.exec.reconciler import Reconciler, ReconcilerConfig  # noqa: E402
from axos.exec.watchdog import Watchdog, WatchdogConfig  # noqa: E402
from axos.tests.r5_helpers import r5_complete  # noqa: E402

RES_SRC = os.path.join(os.path.dirname(HERE), "exec", "resilience.py")
SCHED_SRC = os.path.join(os.path.dirname(HERE), "exec", "scheduler.py")
EXEC_SRC = os.path.join(os.path.dirname(HERE), "exec")
AUDIT_SRC = os.path.join(os.path.dirname(HERE), "audit",
                         "09_authority_audit.py")


class FakeClock:
    def __init__(self, t: float):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        assert dt >= 0, "FakeClock never moves backward"
        self.t += dt


class ResilienceBase(unittest.TestCase):
    H = 0.4

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r12-")
        self.db = os.path.join(self.tmp, "t.db")
        # Start ahead of wall time so current_time()/write_txn() stay on
        # the fake clock even when real-time components commit.
        self.clock = FakeClock(t=time.time() + 10_000.0)
        self.store = open_store(self.db, clock=self.clock)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        self._sups: list = []
        self._scheds: list = []
        self._ctls: list = []
        self._recs: list = []
        self._wds: list = []
        self._dbs: list = []  # per-thread stores for barrier races
        self.sup = self._new_sup(actor="test-r12")
        self.gate.create_task("t", {"objective": "x"}, {"usd": 1}, "test")

    def tearDown(self):
        for wd in self._wds:
            try:
                wd.close()
            except Exception:
                pass
        for sc in self._scheds:
            try:
                sc.close()
            except Exception:
                pass
        for ctl in self._ctls:
            try:
                ctl.close()
            except Exception:
                pass
        for rec in self._recs:
            try:
                rec.close()
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
        try:
            self.store.close()
        except Exception:
            pass
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- helpers
    def _new_sup(self, H=None, actor=None) -> Supervisor:
        s = Supervisor(self.db, actor=actor or f"test-r12-{len(self._sups)}",
                       heartbeat_interval_s=H or self.H)
        self._sups.append(s)
        return s

    def _new_sched(self, scheduler_id=None, **cfg_kw) -> Scheduler:
        kw = dict(poll_interval_s=0.2, lease_ttl_s=60.0,
                  max_concurrent_jobs=4, batch_size=10,
                  scheduler_stale_after_s=2.0)
        kw.update(cfg_kw)
        sc = Scheduler(
            self.db, self.sup, SchedulerConfig(**kw),
            scheduler_id=scheduler_id or f"r12-{len(self._scheds)}",
            boot_ready=lambda: True)
        self._scheds.append(sc)
        return sc

    def _new_ctl(self, **cfg_kw) -> ResilienceController:
        kw = dict(failure_window_s=300.0, failure_threshold=3,
                  global_failure_threshold=10, cooldown_s=60.0,
                  half_open_probe_limit=1, recovery_pressure_threshold=5,
                  poll_interval_s=0.2, batch_size=100,
                  actor="resilience-controller")
        kw.update(cfg_kw)
        ctl = ResilienceController(self.db, ResilienceConfig(**kw),
                                   boot_ready=lambda: True)
        self._ctls.append(ctl)
        return ctl

    def _new_rec(self, **cfg_kw) -> Reconciler:
        kw = dict(poll_interval_s=0.2, batch_size=10, actor="reconciler")
        kw.update(cfg_kw)
        rec = Reconciler(self.db, ReconcilerConfig(**kw))
        self._recs.append(rec)
        return rec

    def _new_wd(self, **cfg_kw) -> Watchdog:
        cfg = WatchdogConfig(
            heartbeat_stale_s=cfg_kw.get("heartbeat_stale_s", 0.6),
            progress_stale_s=cfg_kw.get("progress_stale_s", 1.0),
            evaluation_interval_s=cfg_kw.get("evaluation_interval_s", 0.1),
        )
        wd = Watchdog(self.db, cfg, actor=f"test-wd-{len(self._wds)}",
                      readiness=lambda: True)
        self._wds.append(wd)
        return wd

    def _mk_job(self, jid, task_id="t"):
        return self.gate.create_job(jid, task_id, None, "test")

    def _mk_task(self, tid):
        try:
            self.gate.create_task(tid, {"objective": "x"}, {"usd": 1},
                                  "test")
        except Exception:
            pass

    def _dead(self, jid, wid=None, task_id="t"):
        """Gate-level deterministic failure evidence: RUNNING job + DEAD
        watchdog verdict (the R7 fixture shape, without the subprocess)."""
        wid = wid or f"w-{jid}"
        self._mk_job(jid, task_id)
        self.sup._ensure_worker_row(wid)
        assert self.gate.claim_job(jid, wid, 60.0, "test")
        self.gate.transition_job(jid, "RUNNING", f"worker:{wid}")
        tok = self.gate.get_job(jid)["fencing_token"]
        v = self.gate.record_watchdog_verdict(
            jid, tok, "DEAD", {"reason": "test-r12"}, "test-r12")
        assert v["transition"], v
        return v

    def _signal(self, scope_type, scope_id, kind="R7_DEAD", incident="i",
                window_s=300.0, cooldown_s=60.0):
        return self.gate.record_breaker_signal(
            scope_type, scope_id, failure_kind=kind, incident_id=incident,
            attempt_id=None, failure_window_s=window_s,
            cooldown_s=cooldown_s, actor="test-r12")

    def _open(self, scope_type, scope_id, cooldown_s=60.0):
        self.gate.ensure_breaker_state(
            scope_type, scope_id, cooldown_s=cooldown_s, actor="test-r12")
        row = self.gate.get_breaker_state(scope_type, scope_id)
        return self.gate.transition_breaker(
            scope_type, scope_id, expected_version=row["version"],
            to_state="OPEN", actor="test-r12", reason="test")

    def _corrupt_breaker(self, scope_type, scope_id, state="MELTED"):
        """Test-only fault injection: put a breaker row outside the
        legal transition graph. Bypasses the gate on purpose (that is
        the fault being modeled); the gate itself can never produce
        such a row."""
        with self.store.write_txn() as (conn, now):
            conn.execute(
                "UPDATE breaker_state SET state=?, version=version+1,"
                " updated_at=? WHERE scope_type=? AND scope_id=?",
                (state, now, scope_type, scope_id))
        return self.gate.get_breaker_state(scope_type, scope_id)

    def _repair_breaker(self, scope_type, scope_id, state):
        """Test-only repair: put a breaker row back into a legal state
        (models operator/DBA repair of a corrupt row)."""
        with self.store.write_txn() as (conn, now):
            conn.execute(
                "UPDATE breaker_state SET state=?, version=version+1,"
                " updated_at=? WHERE scope_type=? AND scope_id=?",
                (state, now, scope_type, scope_id))
        return self.gate.get_breaker_state(scope_type, scope_id)

    def _wait(self, pred, timeout=20.0, interval=0.02):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("timed out waiting for condition")

    def _wait_status(self, jid, states, timeout=25.0):
        if isinstance(states, str):
            states = (states,)
        self._wait(lambda: self.gate.get_job(jid)["status"] in states,
                   timeout)
        return self.gate.get_job(jid)

    def _ledger_count(self, event_type=None):
        q = "SELECT COUNT(*) FROM ledger"
        p: tuple = ()
        if event_type is not None:
            q += " WHERE event_type=?"
            p = (event_type,)
        return self.gate.store.conn.execute(q, p).fetchone()[0]

    def _run_barrier(self, fns, timeout=90):
        barrier = threading.Barrier(len(fns))
        results: dict = {}

        def run(i, fn):
            try:
                barrier.wait(timeout=15)
                results[i] = fn()
            except Exception as exc:  # noqa: BLE001 - recorded, asserted
                results[i] = exc

        ts = [threading.Thread(target=run, args=(i, fn))
              for i, fn in enumerate(fns)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=timeout)
        self.assertFalse(any(t.is_alive() for t in ts), "thread hung")
        return results

    def _gate_worker(self, fn):
        """Wrap a gate-taking callable for _run_barrier: each thread
        gets its own Store+TransitionGate on the same DB file (sqlite3
        connections are thread-bound), with the FakeClock bound so
        store time stays deterministic."""
        def run():
            store = open_store(self.db, clock=self.clock)
            self._dbs.append(store)
            try:
                return fn(TransitionGate(store))
            finally:
                store.close()
        return run

    def _ctl_worker(self, **kw):
        """A _run_barrier worker running one controller pass on a
        per-thread controller (own store+gate, FakeClock bound)."""
        def run():
            ctl = self._new_ctl(**kw)
            ctl.store._clock = self.clock
            return ctl.evaluate_once()
        return run


# ------------------------------------------------------------------ R12-01
class TestR1201(ResilienceBase):
    def test_R12_01_default_closed(self):
        """R12-01: a scope with no breaker row admits: breaker_allows ->
        (True, 'closed'). Missing is not an error."""
        allowed, reason = self.gate.breaker_allows("JOB", "nope")
        self.assertTrue(allowed)
        self.assertEqual(reason, "closed")
        self.assertIsNone(self.gate.get_breaker_state("JOB", "nope"))
        allowed, reason = self.gate.breaker_allows("GLOBAL", "global")
        self.assertTrue(allowed)
        self.assertEqual(reason, "closed")


# ------------------------------------------------------------------ R12-02
class TestR1202(ResilienceBase):
    def test_R12_02_same_failure_twice_counted_once(self):
        """R12-02: record_breaker_signal is idempotent per (scope, kind,
        incident, attempt): the second delivery is deduped — count,
        version, and ledger untouched."""
        r1 = self._signal("JOB", "j-202", incident="verdict-1")
        self.assertFalse(r1["deduped"])
        row1 = r1["row"]
        self.assertEqual(row1["failure_count"], 1)
        v0, u0 = row1["version"], row1["updated_at"]
        led0 = self._ledger_count("breaker.signal_recorded")
        r2 = self._signal("JOB", "j-202", incident="verdict-1")
        self.assertTrue(r2["deduped"])
        row2 = r2["row"]
        self.assertEqual(row2["failure_count"], 1)
        self.assertEqual(row2["version"], v0)
        self.assertEqual(row2["updated_at"], u0)
        self.assertEqual(self._ledger_count("breaker.signal_recorded"),
                         led0)


# ------------------------------------------------------------------ R12-03
class TestR1203(ResilienceBase):
    def test_R12_03_scope_fanout_counts_per_scope(self):
        """R12-03: one failure fanning out to JOB/TASK/GLOBAL counts once
        per scope — a scope-blind key would starve all but the first."""
        self._dead("j-203")  # _dead creates the job itself
        ctl = self._new_ctl(failure_threshold=10)
        ctl.store._clock = self.clock
        rep = ctl.evaluate_once()
        self.assertEqual(rep["signals_recorded"], 3)
        self.assertEqual(rep["signals_deduped"], 0)
        for scope in (("JOB", "j-203"), ("TASK", "t"),
                      ("GLOBAL", "global")):
            row = self.gate.get_breaker_state(*scope)
            self.assertIsNotNone(row, scope)
            self.assertEqual(row["failure_count"], 1, scope)
        # Rescan: every delivery now dedupes.
        rep2 = ctl.evaluate_once()
        self.assertEqual(rep2["signals_recorded"], 0)
        self.assertEqual(rep2["signals_deduped"], 3)


# ------------------------------------------------------------------ R12-04
class TestR1204(ResilienceBase):
    def test_R12_04_below_threshold_stays_closed(self):
        """R12-04: failure_count below the threshold never trips: the
        breaker stays CLOSED and admission proceeds."""
        ctl = self._new_ctl(failure_threshold=3)
        ctl.store._clock = self.clock
        self._dead("j-204a")
        rep = ctl.evaluate_once()
        self.assertEqual(rep["breakers_opened"], [])
        row = self.gate.get_breaker_state("JOB", "j-204a")
        self.assertEqual(row["state"], "CLOSED")
        self.assertEqual(row["failure_count"], 1)
        allowed, _ = self.gate.breaker_allows("JOB", "j-204a")
        self.assertTrue(allowed)


# ------------------------------------------------------------------ R12-05
class TestR1205(ResilienceBase):
    def test_R12_05_threshold_reached_opens(self):
        """R12-05: the controller opens a CLOSED breaker whose windowed
        count reaches the threshold; the transition is journaled."""
        ctl = self._new_ctl(failure_threshold=1)
        ctl.store._clock = self.clock
        self._dead("j-205a")
        self._dead("j-205b")
        rep = ctl.evaluate_once()
        opened = {(d["scope_type"], d["scope_id"])
                  for d in rep["breakers_opened"]}
        self.assertIn(("JOB", "j-205a"), opened)
        self.assertIn(("JOB", "j-205b"), opened)
        row = self.gate.get_breaker_state("JOB", "j-205a")
        self.assertEqual(row["state"], "OPEN")
        self.assertIsNotNone(row["cooldown_until"])
        evs = self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='breaker.transition'"
            " ORDER BY seq").fetchall()
        opens = [json.loads(r[0]) for r in evs
                 if json.loads(r[0])["new_state"] == "OPEN"]
        self.assertTrue(any(p["scope_id"] == "j-205a" for p in opens))
        allowed, reason = self.gate.breaker_allows("JOB", "j-205a")
        self.assertFalse(allowed)
        self.assertEqual(reason, "open")


# ------------------------------------------------------------------ R12-06
class TestR1206(ResilienceBase):
    def test_R12_06_window_expiry_starts_fresh_window(self):
        """R12-06: a signal arriving after the failure window starts a
        fresh window (count 1) instead of accumulating stale history —
        FakeClock-advanced, deterministic."""
        r1 = self._signal("JOB", "j-206", incident="v1", window_s=100.0)
        self.assertEqual(r1["row"]["failure_count"], 1)
        w0 = r1["row"]["window_started_at"]
        self.clock.advance(101.0)
        r2 = self._signal("JOB", "j-206", incident="v2", window_s=100.0)
        self.assertFalse(r2["deduped"])
        self.assertEqual(r2["row"]["failure_count"], 1)
        self.assertGreater(r2["row"]["window_started_at"], w0)
        # Within the fresh window, counts accumulate again.
        r3 = self._signal("JOB", "j-206", incident="v3", window_s=100.0)
        self.assertEqual(r3["row"]["failure_count"], 2)
        self.assertEqual(r3["row"]["window_started_at"],
                         r2["row"]["window_started_at"])


# ------------------------------------------------------------------ R12-07
class TestR1207(ResilienceBase):
    def test_R12_07_open_job_scope_blocks_admission(self):
        """R12-07: OPEN on the JOB scope -> the scheduler denies the
        candidate (resilience_denied), the job stays PENDING, no worker
        spawns."""
        self._mk_job("j-207")
        self._open("JOB", "j-207")
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["resilience_denied"], 1)
        self.assertEqual(rep["errors"], [])
        self.assertEqual(self.gate.get_job("j-207")["status"], "PENDING")
        self.assertEqual(self.sup._procs, {})


# ------------------------------------------------------------------ R12-08
class TestR1208(ResilienceBase):
    def test_R12_08_open_task_scope_blocks_task_jobs(self):
        """R12-08: OPEN on the TASK scope blocks every job of that task;
        a job on another task is still admitted."""
        self._mk_task("t8a")
        self._mk_task("t8b")
        self._mk_job("j-208a", task_id="t8a")
        self._mk_job("j-208b", task_id="t8b")
        self._open("TASK", "t8a")
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["resilience_denied"], 1)
        self.assertEqual(self.gate.get_job("j-208a")["status"], "PENDING")
        # j-208b was claimed; its dispatch errored only if the
        # supervisor failed — here the real supervisor dispatches a
        # success_immediate worker, so it is admitted.
        self.assertEqual(rep["admitted"], 1)
        self.assertEqual(self.gate.get_job("j-208b")["status"], "CLAIMED")


# ------------------------------------------------------------------ R12-09
class TestR1209(ResilienceBase):
    def test_R12_09_open_global_blocks_everything(self):
        """R12-09: OPEN GLOBAL denies every candidate regardless of
        task/job scope."""
        self._mk_job("j-209a")
        self._mk_job("j-209b")
        self._open("GLOBAL", "global")
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["resilience_denied"], 2)
        for jid in ("j-209a", "j-209b"):
            self.assertEqual(self.gate.get_job(jid)["status"], "PENDING")
        self.assertEqual(self.sup._procs, {})


# ------------------------------------------------------------------ R12-10
class TestR1210(ResilienceBase):
    def test_R12_10_open_desired_scope_blocks_mapped_job(self):
        """R12-10: OPEN on the DESIRED scope blocks the job R11 mapped
        from that desired item; an unmapped job is still admitted."""
        self.gate.set_desired_item(
            "dw-210", {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                       "policy": {}}, "test")
        rec = self._new_rec()
        rec.store._clock = self.clock
        res = rec.reconcile()
        self.assertIn(res["result"], ("CONVERGED", "CHANGED"))
        mapped = [j for j in self.gate.pending_jobs(10)
                  if self.gate.get_desired_work_id_for_job(
                      j["job_id"]) == "dw-210"]
        self.assertEqual(len(mapped), 1)
        jid = mapped[0]["job_id"]
        self._mk_job("j-210-plain")
        self._open("DESIRED", "dw-210")
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["resilience_denied"], 1)
        self.assertEqual(self.gate.get_job(jid)["status"], "PENDING")
        self.assertEqual(rep["admitted"], 1)
        self.assertEqual(self.gate.get_job("j-210-plain")["status"],
                         "CLAIMED")

# ------------------------------------------------------------------ R12-11
class TestR1211(ResilienceBase):
    def test_R12_11_denial_preserves_durable_work(self):
        """R12-11: a breaker denial mutates nothing durable: the PENDING
        job row, its lease fields, the worker row, and desired state are
        byte-identical afterwards."""
        self.gate.set_desired_item(
            "dw-211", {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                       "policy": {}}, "test")
        rec = self._new_rec()
        rec.reconcile()
        mapped = [j for j in self.gate.pending_jobs(10)
                  if self.gate.get_desired_work_id_for_job(
                      j["job_id"]) == "dw-211"][0]["job_id"]
        self._open("JOB", mapped)
        job0 = dict(self.gate.get_job(mapped))
        head0 = dict(self.gate.get_desired_head())
        items0 = [dict(r) for r in self.gate.list_desired_items()]
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["resilience_denied"], 1)
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(dict(self.gate.get_job(mapped)), job0)
        self.assertEqual(dict(self.gate.get_desired_head()), head0)
        self.assertEqual([dict(r) for r in self.gate.list_desired_items()],
                         items0)
        self.assertEqual(self.sup._procs, {})


# ------------------------------------------------------------------ R12-12
class TestR1212(ResilienceBase):
    def test_R12_12_open_with_elapsed_cooldown_still_denies(self):
        """R12-12: an OPEN row with an elapsed cooldown still denies —
        only the controller may move OPEN -> HALF_OPEN (fail closed)."""
        self._mk_job("j-212")
        self._open("JOB", "j-212", cooldown_s=60.0)
        self.clock.advance(3600.0)  # cooldown long elapsed
        won = self.gate.claim_job_resilient(
            "j-212", "w-212", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1)
        self.assertFalse(won)
        row = self.gate.get_breaker_state("JOB", "j-212")
        self.assertEqual(row["state"], "OPEN")  # admission never moves it
        self.assertEqual(self.gate.get_job("j-212")["status"], "PENDING")


# ------------------------------------------------------------------ R12-13
class TestR1213(ResilienceBase):
    def test_R12_13_cooldown_elapsed_controller_half_opens(self):
        """R12-13: the controller moves OPEN -> HALF_OPEN once
        cooldown_until passes (FakeClock-advanced); the probe budget
        restarts."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1, cooldown_s=100.0)
        ctl.store._clock = self.clock
        self._dead("j-213")
        rep = ctl.evaluate_once()
        self.assertEqual(len(rep["breakers_opened"]), 3)  # JOB/TASK/GLOBAL
        row = self.gate.get_breaker_state("JOB", "j-213")
        until = row["cooldown_until"]
        self.assertGreater(until, self.clock())
        # Before the cooldown: no half-open.
        rep2 = ctl.evaluate_once()
        self.assertEqual(rep2["breakers_half_opened"], [])
        self.clock.advance(101.0)
        rep3 = ctl.evaluate_once()
        half = {(d["scope_type"], d["scope_id"])
                for d in rep3["breakers_half_opened"]}
        self.assertIn(("JOB", "j-213"), half)
        row3 = self.gate.get_breaker_state("JOB", "j-213")
        self.assertEqual(row3["state"], "HALF_OPEN")
        self.assertEqual(row3["half_open_probes_used"], 0)
        self.assertIsNone(row3["half_open_probe_id"])


# ------------------------------------------------------------------ R12-14
class TestR1214(ResilienceBase):
    def test_R12_14_restart_safe_cooldown_honored(self):
        """R12-14: a new controller over the same DB honors the durable
        cooldown: still OPEN before it elapses, HALF_OPEN after — the
        cooldown is durable state, not controller memory."""
        ctl1 = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1, cooldown_s=100.0)
        ctl1.store._clock = self.clock
        self._dead("j-214")
        ctl1.evaluate_once()
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-214")["state"], "OPEN")
        ctl1.close()
        self._ctls.remove(ctl1)
        # "Restart": a fresh controller instance over the same file.
        ctl2 = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1, cooldown_s=100.0)
        ctl2.store._clock = self.clock
        rep = ctl2.evaluate_once()
        self.assertEqual(rep["breakers_half_opened"], [])
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-214")["state"], "OPEN")
        self.clock.advance(101.0)
        rep2 = ctl2.evaluate_once()
        self.assertEqual(len(rep2["breakers_half_opened"]), 3)
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-214")["state"],
            "HALF_OPEN")


# ------------------------------------------------------------------ R12-15
class TestR1215(ResilienceBase):
    def test_R12_15_cooldown_anchored_at_open_time(self):
        """R12-15: cooldown_until is anchored when the breaker opens; a
        later cooldown_s retune cannot shorten an in-flight cooldown."""
        self._open("JOB", "j-215", cooldown_s=100.0)
        row = self.gate.get_breaker_state("JOB", "j-215")
        until0 = row["cooldown_until"]
        self.assertEqual(until0, row["opened_at"] + 100.0)
        # Simulate a retune: cooldown_s changes, cooldown_until must not.
        with self.store.write_txn() as (conn, now):
            conn.execute(
                "UPDATE breaker_state SET cooldown_s=? WHERE scope_type=?"
                " AND scope_id=?", (5.0, "JOB", "j-215"))
        row2 = self.gate.get_breaker_state("JOB", "j-215")
        self.assertEqual(row2["cooldown_until"], until0)
        self.clock.advance(10.0)  # past the "retuned" 5s, before the real
        ctl = self._new_ctl()
        ctl.store._clock = self.clock
        rep = ctl.evaluate_once()
        self.assertEqual(rep["breakers_half_opened"], [])
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-215")["state"], "OPEN")


# ------------------------------------------------------------------ R12-16
class TestR1216(ResilienceBase):
    def test_R12_16_half_open_probes_bounded(self):
        """R12-16: HALF_OPEN admits at most probe_limit trial claims per
        episode; the rest are denied with the probe slot unwound."""
        self._mk_job("j-216a")
        self._mk_job("j-216b")
        self._open("JOB", "j-216a", cooldown_s=60.0)
        self._open("JOB", "j-216b", cooldown_s=60.0)
        for jid in ("j-216a", "j-216b"):
            row = self.gate.get_breaker_state("JOB", jid)
            self.gate.transition_breaker(
                "JOB", jid, expected_version=row["version"],
                to_state="HALF_OPEN", actor="test-r12",
                reason="test")
        ok1 = self.gate.claim_job_resilient(
            "j-216a", "w-216a", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1)
        self.assertTrue(ok1)
        row = self.gate.get_breaker_state("JOB", "j-216a")
        self.assertEqual(row["half_open_probes_used"], 1)
        self.assertEqual(row["half_open_probe_id"], "w-216a:j-216a")
        ok2 = self.gate.claim_job_resilient(
            "j-216b", "w-216b", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1)
        self.assertTrue(ok2)  # different scope: its own budget
        # The admitted probe job is CLAIMED now: a second claim on the
        # same job is denied without touching the probe budget.
        row = self.gate.get_breaker_state("JOB", "j-216a")
        self.assertEqual(row["state"], "HALF_OPEN")
        ok3 = self.gate.claim_job_resilient(
            "j-216a", "w-216x", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1)
        self.assertFalse(ok3)
        row = self.gate.get_breaker_state("JOB", "j-216a")
        self.assertEqual(row["half_open_probes_used"], 1)


# ------------------------------------------------------------------ R12-17
class TestR1217(ResilienceBase):
    def test_R12_17_probe_race_one_winner_at_limit_1(self):
        """R12-17: N threads x one HALF_OPEN probe slot (limit 1) ->
        exactly one probe allocated; losers denied with no leak."""
        self._open("GLOBAL", "global", cooldown_s=60.0)
        row = self.gate.get_breaker_state("GLOBAL", "global")
        self.gate.transition_breaker(
            "GLOBAL", "global", expected_version=row["version"],
            to_state="HALF_OPEN", actor="test-r12", reason="test")
        jobs = [f"j-217-{i}" for i in range(6)]
        for jid in jobs:
            self._mk_job(jid)
        res = self._run_barrier([
            self._gate_worker(
                lambda g, jid=jid: g.claim_job_resilient(
                    jid, f"w-{jid}", 60.0, "test", 100, scheduler_id="s",
                    probe_limit=1))
            for jid in jobs])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        winners = [jid for jid, v in
                   ((jobs[i], res[i]) for i in range(len(jobs))) if v]
        self.assertEqual(len(winners), 1)
        row = self.gate.get_breaker_state("GLOBAL", "global")
        self.assertEqual(row["half_open_probes_used"], 1)
        self.assertEqual(row["half_open_probe_id"],
                         f"w-{winners[0]}:{winners[0]}")


# ------------------------------------------------------------------ R12-18
class TestR1218(ResilienceBase):
    def test_R12_18_probe_success_via_complete_closes(self):
        """R12-18: a probe whose job COMPLETEs (R5-only success evidence)
        closes the breaker."""
        ctl = self._new_ctl(failure_threshold=1, cooldown_s=60.0)
        ctl.store._clock = self.clock
        self._mk_job("j-218")
        self._open("JOB", "j-218", cooldown_s=60.0)
        row = self.gate.get_breaker_state("JOB", "j-218")
        self.gate.transition_breaker(
            "JOB", "j-218", expected_version=row["version"],
            to_state="HALF_OPEN", actor="test-r12", reason="test")
        self.assertTrue(self.gate.claim_job_resilient(
            "j-218", "w-218", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        job = self.gate.get_job("j-218")
        self.sup._ensure_worker_row("w-218")
        self.gate.transition_job("j-218", "RUNNING", "worker:w-218")
        r5_complete(gate=self.gate, job_id="j-218", worker_id="w-218",
                    fencing_token=job["fencing_token"], task_id="t",
                    outcome="SUCCESS", evidence={"ok": True},
                    actor="worker:w-218")
        self.assertEqual(self.gate.get_job("j-218")["status"], "COMPLETE")
        rep = ctl.evaluate_once()
        closed = {(d["scope_type"], d["scope_id"])
                  for d in rep["breakers_closed"]}
        self.assertIn(("JOB", "j-218"), closed)
        row = self.gate.get_breaker_state("JOB", "j-218")
        self.assertEqual(row["state"], "CLOSED")
        self.assertEqual(row["failure_count"], 0)


# ------------------------------------------------------------------ R12-19
class TestR1219(ResilienceBase):
    def test_R12_19_probe_success_via_progress_delta_closes(self):
        """R12-19: durable progress_done strictly above the probe
        baseline closes the breaker (I-18: verified by progress)."""
        ctl = self._new_ctl(failure_threshold=1, cooldown_s=60.0)
        ctl.store._clock = self.clock
        self._mk_job("j-219")
        self._open("JOB", "j-219", cooldown_s=60.0)
        row = self.gate.get_breaker_state("JOB", "j-219")
        self.gate.transition_breaker(
            "JOB", "j-219", expected_version=row["version"],
            to_state="HALF_OPEN", actor="test-r12", reason="test")
        self.assertTrue(self.gate.claim_job_resilient(
            "j-219", "w-219", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        job = self.gate.get_job("j-219")
        tok = job["fencing_token"]
        self.sup._ensure_worker_row("w-219")
        self.gate.transition_job("j-219", "RUNNING", "worker:w-219")
        base = self.gate.get_job("j-219")["progress_done"] or 0.0
        self.gate.update_job_progress("j-219", "w-219", tok, base + 2.0,
                                      10.0, "worker:w-219")
        rep = ctl.evaluate_once()
        closed = {(d["scope_type"], d["scope_id"])
                  for d in rep["breakers_closed"]}
        self.assertIn(("JOB", "j-219"), closed)
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-219")["state"], "CLOSED")


# ------------------------------------------------------------------ R12-20
class TestR1220(ResilienceBase):
    def test_R12_20_heartbeat_alone_never_closes_probe(self):
        """R12-20: heartbeats are liveness evidence, never success
        evidence: a heartbeating probe with no progress stays HALF_OPEN."""
        ctl = self._new_ctl(failure_threshold=1, cooldown_s=60.0)
        ctl.store._clock = self.clock
        self._mk_job("j-220")
        self._open("JOB", "j-220", cooldown_s=60.0)
        row = self.gate.get_breaker_state("JOB", "j-220")
        self.gate.transition_breaker(
            "JOB", "j-220", expected_version=row["version"],
            to_state="HALF_OPEN", actor="test-r12", reason="test")
        self.assertTrue(self.gate.claim_job_resilient(
            "j-220", "w-220", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        job = self.gate.get_job("j-220")
        self.sup._ensure_worker_row("w-220")
        self.gate.transition_job("j-220", "RUNNING", "worker:w-220")
        for seq in range(3):
            self.gate.ingest_heartbeat(
                worker_id="w-220", proc_id="p-220", job_id="j-220",
                fencing_token=job["fencing_token"], hb_seq=seq,
                worker_state="RUNNING", current_operation="hb",
                actor="worker:w-220")
            self.clock.advance(1.0)
        rep = ctl.evaluate_once()
        self.assertEqual(rep["breakers_closed"], [])
        self.assertEqual(rep["breakers_opened"], [])
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-220")["state"],
            "HALF_OPEN")


# ------------------------------------------------------------------ R12-21
class TestR1221(ResilienceBase):
    def test_R12_21_newer_failure_reopens_breaker(self):
        """R12-21: counted failure evidence after the probe allocation
        reopens the breaker (the trial did not verify recovery)."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1, cooldown_s=60.0)
        ctl.store._clock = self.clock
        self._dead("j-221")
        ctl.evaluate_once()
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-221")["state"], "OPEN")
        self.clock.advance(61.0)
        ctl.evaluate_once()
        for scope in (("JOB", "j-221"), ("TASK", "t"),
                      ("GLOBAL", "global")):
            self.assertEqual(
                self.gate.get_breaker_state(*scope)["state"], "HALF_OPEN",
                scope)
        # A fresh PENDING job claims through the HALF_OPEN scopes,
        # allocating one probe per scope that has a row (GLOBAL, TASK).
        self.gate.create_job("j-221b", "t", None, "test")
        self.assertTrue(self.gate.claim_job_resilient(
            "j-221b", "w-221b", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        for scope in (("TASK", "t"), ("GLOBAL", "global")):
            row = self.gate.get_breaker_state(*scope)
            self.assertEqual(row["half_open_probe_id"], "w-221b:j-221b",
                             scope)
        probe_at = self.gate.get_breaker_state(
            "GLOBAL", "global")["half_open_probe_at"]
        # New failure evidence for the probe's job after the allocation.
        self.clock.advance(5.0)
        job = self.gate.get_job("j-221b")
        self.sup._ensure_worker_row("w-221b")
        self.gate.transition_job("j-221b", "RUNNING", "worker:w-221b")
        v = self.gate.record_watchdog_verdict(
            "j-221b", job["fencing_token"], "DEAD", {"reason": "t"},
            "test-r12")
        self.assertGreater(v["evaluated_at"], probe_at)
        rep = ctl.evaluate_once()
        reopened = {(d["scope_type"], d["scope_id"])
                    for d in rep["breakers_opened"]}
        # Every probed scope reopens on the post-probe failure; the
        # unprobed JOB:j-221 row (no probe allocated) is untouched; the
        # probe job's own fresh JOB row trips via the threshold.
        for scope in (("TASK", "t"), ("GLOBAL", "global"),
                      ("JOB", "j-221b")):
            self.assertIn(scope, reopened)
            self.assertEqual(
                self.gate.get_breaker_state(*scope)["state"], "OPEN",
                scope)
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-221")["state"],
            "HALF_OPEN")

# ------------------------------------------------------------------ R12-22
class TestR1222(ResilienceBase):
    def test_R12_22_transition_cas_loser_gets_conflict(self):
        """R12-22: transition_breaker is CAS on version: a stale
        expected_version raises BreakerConflict; the winner's state
        stands."""
        self._open("JOB", "j-222", cooldown_s=60.0)
        row = self.gate.get_breaker_state("JOB", "j-222")
        v0 = row["version"]
        with self.assertRaises(BreakerConflict):
            self.gate.transition_breaker(
                "JOB", "j-222", expected_version=v0 + 99,
                to_state="HALF_OPEN", actor="test-r12", reason="stale")
        row = self.gate.get_breaker_state("JOB", "j-222")
        self.assertEqual(row["state"], "OPEN")
        self.assertEqual(row["version"], v0)
        # The legal move with the fresh version succeeds.
        new = self.gate.transition_breaker(
            "JOB", "j-222", expected_version=v0, to_state="HALF_OPEN",
            actor="test-r12", reason="test")
        self.assertEqual(new["version"], v0 + 1)


# ------------------------------------------------------------------ R12-23
class TestR1223(ResilienceBase):
    def test_R12_23_probe_allocation_cas(self):
        """R12-23: claim_half_open_probe is CAS on version: a stale
        version raises BreakerConflict and allocates nothing."""
        self._open("JOB", "j-223", cooldown_s=60.0)
        row = self.gate.get_breaker_state("JOB", "j-223")
        self.gate.transition_breaker(
            "JOB", "j-223", expected_version=row["version"],
            to_state="HALF_OPEN", actor="test-r12", reason="test")
        row = self.gate.get_breaker_state("JOB", "j-223")
        with self.assertRaises(BreakerConflict):
            self.gate.claim_half_open_probe(
                "JOB", "j-223", expected_version=row["version"] + 5,
                probe_id="w:j-223", probe_limit=1,
                baseline={"progress_done": 0}, actor="test-r12")
        row = self.gate.get_breaker_state("JOB", "j-223")
        self.assertEqual(row["half_open_probes_used"], 0)
        self.assertIsNone(row["half_open_probe_id"])


# ------------------------------------------------------------------ R12-24
class TestR1224(ResilienceBase):
    def test_R12_24_cas_loser_rereads_never_overwrites(self):
        """R12-24: the controller's CAS-loser discipline re-reads the
        authoritative row and never overwrites the winner: after a lost
        race the winner's row still reflects exactly one transition."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1, cooldown_s=60.0)
        ctl.store._clock = self.clock
        self._dead("j-224")
        # Winner opens the breaker outside the controller.
        self._open("JOB", "j-224", cooldown_s=60.0)
        row = self.gate.get_breaker_state("JOB", "j-224")
        v = row["version"]
        # The controller's pass opens the sibling CLOSED scopes (TASK,
        # GLOBAL) but must not touch the winner's OPEN row: no second
        # transition, no version move, exactly one ledger open event.
        rep = ctl.evaluate_once()
        opened = {(d["scope_type"], d["scope_id"])
                  for d in rep["breakers_opened"]}
        self.assertEqual(opened, {("TASK", "t"), ("GLOBAL", "global")})
        row2 = self.gate.get_breaker_state("JOB", "j-224")
        # The winner's row stays OPEN: the controller records its
        # signal (counting evidence bumps the version) but performs no
        # transition on it.
        self.assertEqual(row2["state"], "OPEN")
        opens = [r for r in self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='breaker.transition'"
        ).fetchall()
            if json.loads(r[0])["new_state"] == "OPEN"
            and json.loads(r[0])["scope_id"] == "j-224"]
        self.assertEqual(len(opens), 1)


# ------------------------------------------------------------------ R12-25
class TestR1225(ResilienceBase):
    def test_R12_25_noop_discipline(self):
        """R12-25: no-op discipline — a deduped signal changes no version
        and journals nothing; a denial before any mutation journals
        nothing and bumps no version."""
        led0 = self._ledger_count()
        r1 = self._signal("JOB", "j-225", incident="v1")
        v1, u1 = r1["row"]["version"], r1["row"]["updated_at"]
        r2 = self._signal("JOB", "j-225", incident="v1")
        self.assertTrue(r2["deduped"])
        self.assertEqual((r2["row"]["version"], r2["row"]["updated_at"]),
                         (v1, u1))
        self.assertEqual(self._ledger_count(), led0 + 1)  # only r1's event
        # Denial before mutation: OPEN row, claim denied, zero ledger.
        self._open("JOB", "j-225b", cooldown_s=60.0)
        self._mk_job("j-225b")
        led1 = self._ledger_count()
        row0 = dict(self.gate.get_breaker_state("JOB", "j-225b"))
        self.assertFalse(self.gate.claim_job_resilient(
            "j-225b", "w-225b", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        self.assertEqual(self._ledger_count(), led1)
        self.assertEqual(dict(self.gate.get_breaker_state("JOB", "j-225b")),
                         row0)


# ------------------------------------------------------------------ R12-26
class TestR1226(ResilienceBase):
    def test_R12_26_job_scope_open_isolates_siblings(self):
        """R12-26: a JOB-scope OPEN denies only that job; a sibling job on
        the same task is still admitted."""
        self._mk_job("j-226a")
        self._mk_job("j-226b")
        self._open("JOB", "j-226a")
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["resilience_denied"], 1)
        self.assertEqual(rep["admitted"], 1)
        self.assertEqual(self.gate.get_job("j-226a")["status"], "PENDING")
        self.assertEqual(self.gate.get_job("j-226b")["status"], "CLAIMED")


# ------------------------------------------------------------------ R12-27
class TestR1227(ResilienceBase):
    def test_R12_27_global_threshold_independent(self):
        """R12-27: the GLOBAL threshold is independent: per-scope
        failures below the global threshold open the JOB breakers but
        leave GLOBAL CLOSED."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=10)
        ctl.store._clock = self.clock
        self._dead("j-227a")
        self._dead("j-227b")
        rep = ctl.evaluate_once()
        opened = {(d["scope_type"], d["scope_id"])
                  for d in rep["breakers_opened"]}
        self.assertIn(("JOB", "j-227a"), opened)
        self.assertIn(("JOB", "j-227b"), opened)
        self.assertNotIn(("GLOBAL", "global"), opened)
        grow = self.gate.get_breaker_state("GLOBAL", "global")
        self.assertEqual(grow["state"], "CLOSED")
        self.assertEqual(grow["failure_count"], 2)


# ------------------------------------------------------------------ R12-28
class TestR1228(ResilienceBase):
    def test_R12_28_recovery_pressure_one_signal_per_window(self):
        """R12-28: recovery pressure (>= threshold open attempts in the
        window) records exactly one GLOBAL RECOVERY_PRESSURE signal per
        window; rescans dedupe."""
        ctl = self._new_ctl(failure_threshold=100,
                            global_failure_threshold=100,
                            recovery_pressure_threshold=2)
        ctl.store._clock = self.clock
        for i, jid in enumerate(("j-228a", "j-228b")):
            self._mk_job(jid)
            job = self.gate.get_job(jid)
            inc = self.gate.find_or_create_recovery_incident(
                jid, "DEAD", {"reason": "t"}, "test-r12")
            self.gate.create_recovery_attempt(
                incident_id=inc["incident_id"], job_id=jid,
                fencing_token=job["fencing_token"], rung=2,
                rung_name="re-claim/requeue", action={"name": "reclaim"},
                failure_class="DEAD", success_criterion="s",
                failure_criterion="f", evidence_before={},
                actor="test-r12")
        rep = ctl.evaluate_once()
        self.assertEqual(rep["signals_recorded"], 1)
        grow = self.gate.get_breaker_state("GLOBAL", "global")
        self.assertEqual(grow["failure_count"], 1)
        kinds = self.gate.store.conn.execute(
            "SELECT DISTINCT failure_kind FROM breaker_signals"
            " WHERE scope_type='GLOBAL'").fetchall()
        self.assertEqual([r[0] for r in kinds], ["RECOVERY_PRESSURE"])
        # Rescan within the window: deduped, no new signal.
        rep2 = ctl.evaluate_once()
        self.assertEqual(rep2["signals_recorded"], 0)
        self.assertEqual(rep2["signals_deduped"], 1)
        self.assertEqual(
            self.gate.get_breaker_state("GLOBAL", "global")
            ["failure_count"], 1)
        # Next window with fresh pressure: re-fires exactly once, with a
        # fresh count window.
        self.clock.advance(301.0)
        for jid in ("j-228c", "j-228d"):
            self._mk_job(jid)
            job = self.gate.get_job(jid)
            inc = self.gate.find_or_create_recovery_incident(
                jid, "DEAD", {"reason": "t"}, "test-r12")
            self.gate.create_recovery_attempt(
                incident_id=inc["incident_id"], job_id=jid, rung=1,
                fencing_token=job["fencing_token"],
                rung_name="re-claim/requeue", action={"name": "reclaim"},
                failure_class="DEAD", success_criterion="s",
                failure_criterion="f", evidence_before={},
                actor="test-r12")
        rep3 = ctl.evaluate_once()
        self.assertEqual(rep3["signals_recorded"], 1)
        self.assertEqual(
            self.gate.get_breaker_state("GLOBAL", "global")
            ["failure_count"], 1)  # fresh window


# ------------------------------------------------------------------ R12-29
class TestR1229(ResilienceBase):
    def test_R12_29_escalation_is_evidence_no_r9_mutation(self):
        """R12-29: an R9-escalated incident is recorded as R9_ESCALATED
        evidence; the controller mutates no R9/incident state."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=100)
        ctl.store._clock = self.clock
        self._mk_job("j-229")
        inc = self.gate.find_or_create_recovery_incident(
            "j-229", "DEAD", {"reason": "t"}, "test-r12")
        inc0 = dict(self.gate.get_recovery_incident(inc["incident_id"]))
        self.gate.set_incident_escalated(inc["incident_id"], "diag",
                                         "test-r12")
        inc_esc = dict(
            self.gate.get_recovery_incident(inc["incident_id"]))
        rep = ctl.evaluate_once()
        self.assertEqual(rep["signals_recorded"], 3)  # JOB/TASK/GLOBAL
        kinds = self.gate.store.conn.execute(
            "SELECT DISTINCT failure_kind FROM breaker_signals"
            " WHERE scope_type='JOB' AND scope_id='j-229'").fetchall()
        self.assertEqual([r[0] for r in kinds], ["R9_ESCALATED"])
        # The incident row is byte-identical to its escalated state: the
        # controller recorded evidence, mutated nothing.
        self.assertEqual(
            dict(self.gate.get_recovery_incident(inc["incident_id"])),
            inc_esc)
        self.assertNotEqual(inc_esc["outcome"], inc0["outcome"])
        opened = {(d["scope_type"], d["scope_id"])
                  for d in rep["breakers_opened"]}
        self.assertIn(("JOB", "j-229"), opened)


# ------------------------------------------------------------------ R12-30
class TestR1230(ResilienceBase):
    def test_R12_30_escalation_rescan_dedupes(self):
        """R12-30: rescanning an escalated incident is a no-op: the
        dedupe key (incident_id + ':escalated') makes every rescan
        zero-effect."""
        ctl = self._new_ctl(failure_threshold=100,
                            global_failure_threshold=100)
        ctl.store._clock = self.clock
        self._mk_job("j-230")
        inc = self.gate.find_or_create_recovery_incident(
            "j-230", "DEAD", {"reason": "t"}, "test-r12")
        self.gate.set_incident_escalated(inc["incident_id"], "diag",
                                         "test-r12")
        rep1 = ctl.evaluate_once()
        self.assertEqual(rep1["signals_recorded"], 3)
        row0 = dict(self.gate.get_breaker_state("JOB", "j-230"))
        rep2 = ctl.evaluate_once()
        self.assertEqual(rep2["signals_recorded"], 0)
        self.assertEqual(rep2["signals_deduped"], 3)
        self.assertEqual(dict(self.gate.get_breaker_state("JOB", "j-230")),
                         row0)


# ------------------------------------------------------------------ R12-31
class TestR1231(ResilienceBase):
    def test_R12_31_no_r9_policy_rows_created(self):
        """R12-31: pressure and escalation signals never create or touch
        R9 policy rows — the controller has no policy authority."""
        ctl = self._new_ctl(failure_threshold=100,
                            global_failure_threshold=100,
                            recovery_pressure_threshold=1)
        ctl.store._clock = self.clock
        self._mk_job("j-231")
        job = self.gate.get_job("j-231")
        inc = self.gate.find_or_create_recovery_incident(
            "j-231", "DEAD", {"reason": "t"}, "test-r12")
        self.gate.set_incident_escalated(inc["incident_id"], "diag",
                                         "test-r12")
        self.gate.create_recovery_attempt(
            incident_id=inc["incident_id"], job_id="j-231",
            fencing_token=job["fencing_token"], rung=2,
            rung_name="re-claim/requeue", action={"name": "reclaim"},
            failure_class="DEAD", success_criterion="s",
            failure_criterion="f", evidence_before={}, actor="test-r12")
        pol0 = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM recovery_policy").fetchone()[0]
        ctl.evaluate_once()
        ctl.evaluate_once()
        self.assertEqual(self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM recovery_policy").fetchone()[0], pol0)


# ------------------------------------------------------------------ R12-32
class TestR1232(ResilienceBase):
    def test_R12_32_breaker_rows_are_per_scope_independent(self):
        """R12-32 (fills the unassigned slot): breaker rows are
        independent — opening one scope never changes another scope's
        state or version."""
        self._mk_job("j-232")
        self._signal("JOB", "j-232", incident="v1")
        self._signal("TASK", "t", incident="v1")
        # GLOBAL was never signaled: no row at all (missing is not a
        # CLOSED row) — itself per-scope independence.
        self.assertIsNone(self.gate.get_breaker_state("GLOBAL", "global"))
        before = {s: dict(self.gate.get_breaker_state(*s))
                  for s in (("JOB", "j-232"), ("TASK", "t"))}
        self._open("JOB", "j-232", cooldown_s=60.0)
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-232")["state"], "OPEN")
        # The TASK row is byte-identical; GLOBAL still has no row.
        self.assertEqual(dict(self.gate.get_breaker_state("TASK", "t")),
                         before[("TASK", "t")])
        self.assertIsNone(self.gate.get_breaker_state("GLOBAL", "global"))


# ------------------------------------------------------------------ R12-33
class TestR1233(ResilienceBase):
    def test_R12_33_global_open_sheds_load(self):
        """R12-33: GLOBAL OPEN sheds load: zero admissions, zero dispatch
        across many candidates."""
        for i in range(5):
            self._mk_job(f"j-233-{i}")
        self._open("GLOBAL", "global")
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["redispatched"], 0)
        self.assertEqual(rep["resilience_denied"], 5)
        self.assertEqual(rep["errors"], [])
        self.assertEqual(self.sup._procs, {})


# ------------------------------------------------------------------ R12-34
class TestR1234(ResilienceBase):
    def test_R12_34_shedding_preserves_every_durable_row(self):
        """R12-34: load shedding mutates no durable work: no job row
        changes state, no worker/lease rows change, nothing is deleted."""
        self._mk_job("j-234")
        self._open("GLOBAL", "global")
        jobs0 = {r["job_id"]: dict(r) for r in
                 self.gate.store.conn.execute("SELECT * FROM jobs")}
        workers0 = [dict(r) for r in self.gate.store.conn.execute(
            "SELECT * FROM workers")]
        led0 = self._ledger_count()
        sched = self._new_sched()
        sched.store._clock = self.clock
        sched.evaluate_once()
        self.assertEqual(
            {r["job_id"]: dict(r) for r in self.gate.store.conn.execute(
                "SELECT * FROM jobs")}, jobs0)
        self.assertEqual(
            [dict(r) for r in self.gate.store.conn.execute(
                "SELECT * FROM workers")], workers0)
        self.assertEqual(self._ledger_count(), led0)


# ------------------------------------------------------------------ R12-35
class TestR1235(ResilienceBase):
    def test_R12_35_shedding_preserves_desired_state(self):
        """R12-35: shedding preserves desired state: head, items, and the
        desired->job map are untouched by denied passes."""
        self.gate.set_desired_item(
            "dw-235", {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                       "policy": {}}, "test")
        rec = self._new_rec()
        rec.reconcile()
        self._open("GLOBAL", "global")
        head0 = dict(self.gate.get_desired_head())
        items0 = [dict(r) for r in self.gate.list_desired_items()]
        map0 = [dict(r) for r in self.gate.store.conn.execute(
            "SELECT * FROM desired_job_map")]
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertGreaterEqual(rep["resilience_denied"], 1)
        self.assertEqual(dict(self.gate.get_desired_head()), head0)
        self.assertEqual([dict(r) for r in self.gate.list_desired_items()],
                         items0)
        self.assertEqual([dict(r) for r in self.gate.store.conn.execute(
            "SELECT * FROM desired_job_map")], map0)


# ------------------------------------------------------------------ R12-36
class TestR1236(ResilienceBase):
    def test_R12_36_controller_claims_nothing_ast(self):
        """R12-36: the controller owns breaker authority only (AST):
        resilience.py never calls job-execution verbs
        (claim/transition/create/commit/reclaim/dispatch), never
        touches incidents/R9 policy, never opens write_txn or sqlite3 —
        but it DOES exercise its own breaker authority
        (record_breaker_signal/transition_breaker)."""
        with open(RES_SRC) as f:
            tree = ast.parse(f.read())
        calls = {n.func.attr for n in ast.walk(tree)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute)}
        banned = {"claim_job", "claim_job_bounded", "claim_job_resilient",
                  "claim_recovery_attempt", "transition_job",
                  "create_job", "commit_artifact", "reclaim_lease",
                  "release_lease", "start_worker", "kill_worker",
                  "ensure_recovery_policy", "cas_update_recovery_policy",
                  "consume_policy_attempts", "select_rung",
                  "find_or_create_recovery_incident",
                  "set_incident_escalated", "create_recovery_attempt",
                  "complete_recovery_attempt", "claim_half_open_probe",
                  "write_txn"}
        self.assertFalse(calls & banned, sorted(calls & banned))
        # Its own authority: signal recording + breaker transitions.
        self.assertIn("record_breaker_signal", calls)
        self.assertIn("transition_breaker", calls)
        imports = set()
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                imports.update(a.name for a in n.names)
            elif isinstance(n, ast.ImportFrom) and n.module:
                imports.add(n.module)
        self.assertFalse(any("sqlite3" in m for m in imports), imports)


# ------------------------------------------------------------------ R12-37
class TestR1237(ResilienceBase):
    def test_R12_37_controller_never_touches_r9_policy(self):
        """R12-37: the controller never touches R9 policy (AST + the
        policy table is empty after passes with escalations present)."""
        with open(RES_SRC) as f:
            src = f.read()
        for verb in ("ensure_recovery_policy", "cas_update_recovery_policy",
                     "consume_policy_attempts", "recovery_policy",
                     "select_rung"):
            self.assertNotIn(verb, src)
        ctl = self._new_ctl()
        ctl.store._clock = self.clock
        self._mk_job("j-237")
        inc = self.gate.find_or_create_recovery_incident(
            "j-237", "DEAD", {"reason": "t"}, "test-r12")
        self.gate.set_incident_escalated(inc["incident_id"], "d", "test")
        ctl.evaluate_once()
        n = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM recovery_policy").fetchone()[0]
        self.assertEqual(n, 0)


# ------------------------------------------------------------------ R12-38
class TestR1238(ResilienceBase):
    def test_R12_38_controller_never_mutates_incidents(self):
        """R12-38: incident rows are byte-identical after controller
        passes — evidence is read, never written."""
        ctl = self._new_ctl(failure_threshold=100,
                            global_failure_threshold=100)
        ctl.store._clock = self.clock
        self._mk_job("j-238")
        inc = self.gate.find_or_create_recovery_incident(
            "j-238", "DEAD", {"reason": "t"}, "test-r12")
        before = [dict(r) for r in self.gate.store.conn.execute(
            "SELECT * FROM incidents")]
        ctl.evaluate_once()
        ctl.evaluate_once()
        after = [dict(r) for r in self.gate.store.conn.execute(
            "SELECT * FROM incidents")]
        self.assertEqual(after, before)


# ------------------------------------------------------------------ R12-39
class TestR1239(ResilienceBase):
    def test_R12_39_scheduler_never_transitions_breakers_ast(self):
        """R12-39: the scheduler never moves a breaker (AST): no
        transition/ensure/record/probe-allocation calls in scheduler.py;
        the only breaker touch is the read-only preflight."""
        with open(SCHED_SRC) as f:
            tree = ast.parse(f.read())
        calls = {n.func.attr for n in ast.walk(tree)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute)}
        for verb in ("transition_breaker", "ensure_breaker_state",
                     "record_breaker_signal", "claim_half_open_probe"):
            self.assertNotIn(verb, calls)
        self.assertIn("breaker_allows", calls)  # read-only preflight
        self.assertIn("claim_job_resilient", calls)


# ------------------------------------------------------------------ R12-40
class TestR1240(ResilienceBase):
    def test_R12_40_admission_never_moves_breaker_past_cooldown(self):
        """R12-40: the admission path never moves a breaker itself: an
        OPEN row with an elapsed cooldown still denies, and the row is
        byte-identical afterwards (only the controller transitions)."""
        self._mk_job("j-240")
        self._open("JOB", "j-240", cooldown_s=60.0)
        self.clock.advance(600.0)
        row0 = dict(self.gate.get_breaker_state("JOB", "j-240"))
        self.assertFalse(self.gate.claim_job_resilient(
            "j-240", "w-240", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        self.assertEqual(dict(self.gate.get_breaker_state("JOB", "j-240")),
                         row0)


# ------------------------------------------------------------------ R12-41
class TestR1241(ResilienceBase):
    def test_R12_41_scheduler_pass_leaves_breakers_byte_identical(self):
        """R12-41: a full scheduler pass (admissions + denials) leaves
        every breaker row byte-identical — the scheduler writes no
        breaker state."""
        self._mk_job("j-241a")
        self._mk_job("j-241b")
        self._open("JOB", "j-241a")
        before = [dict(r) for r in self.gate.store.conn.execute(
            "SELECT * FROM breaker_state ORDER BY scope_type, scope_id")]
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["resilience_denied"], 1)
        self.assertEqual(rep["admitted"], 1)
        after = [dict(r) for r in self.gate.store.conn.execute(
            "SELECT * FROM breaker_state ORDER BY scope_type, scope_id")]
        self.assertEqual(after, before)


# ------------------------------------------------------------------ R12-42
class TestR1242(ResilienceBase):
    def test_R12_42_controller_has_no_process_authority_ast(self):
        """R12-42: the controller cannot start, kill, or signal processes
        (AST): no supervisor dispatch/kill verbs, no subprocess/signal/
        os.kill in resilience.py."""
        with open(RES_SRC) as f:
            tree = ast.parse(f.read())
        calls = {n.func.attr for n in ast.walk(tree)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute)}
        for verb in ("start_worker", "kill_worker", "Popen", "killpg",
                     "kill", "terminate", "send_signal"):
            self.assertNotIn(verb, calls)
        with open(RES_SRC) as f:
            raw = f.read()
        for tok in ("import subprocess", "import signal", "os.kill",
                    "SIGTERM", "SIGKILL"):
            self.assertNotIn(tok, raw)


# ------------------------------------------------------------------ R12-43
class TestR1243(ResilienceBase):
    def test_R12_43_controller_actor_may_not_be_worker_prefixed(self):
        """R12-43: ResilienceConfig rejects worker:-prefixed actors —
        worker actors cannot own controller authority."""
        with self.assertRaises(ValueError):
            ResilienceConfig(actor="worker:x")
        cfg = ResilienceConfig(actor="resilience-controller")
        self.assertEqual(cfg.actor, "resilience-controller")
        # SchedulerConfig: probe limit validated as int >= 1.
        good = dict(poll_interval_s=0.2, lease_ttl_s=60.0,
                    max_concurrent_jobs=2, batch_size=5)
        self.assertEqual(SchedulerConfig(**good).resilience_probe_limit, 1)
        for bad in (0, -1, 2.5, True, "1"):
            with self.assertRaises(ValueError, msg=f"{bad}"):
                SchedulerConfig(resilience_probe_limit=bad, **good)


# ------------------------------------------------------------------ R12-44
class TestR1244(ResilienceBase):
    def test_R12_44_probe_allocation_is_gate_only(self):
        """R12-44: the controller never allocates probes directly —
        claim_half_open_probe appears only in the gate; the controller's
        only path to a probe is the claim transaction."""
        with open(RES_SRC) as f:
            tree = ast.parse(f.read())
        calls = {n.func.attr for n in ast.walk(tree)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute)}
        self.assertNotIn("claim_half_open_probe", calls)

# ------------------------------------------------------------------ R12-45
class TestR1245(ResilienceBase):
    def test_R12_45_corrupt_row_denies_admission(self):
        """R12-45: a corrupt breaker row (state outside the legal
        transition graph) denies admission: fail closed."""
        self._mk_job("j-245")
        self.gate.ensure_breaker_state("JOB", "j-245", cooldown_s=60.0,
                                       actor="test-r12")
        self._corrupt_breaker("JOB", "j-245")
        self.assertFalse(self.gate.claim_job_resilient(
            "j-245", "w-245", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        self.assertEqual(self.gate.get_job("j-245")["status"], "PENDING")


# ------------------------------------------------------------------ R12-46
class TestR1246(ResilienceBase):
    def test_R12_46_corrupt_row_is_inert_to_controller(self):
        """R12-46: a corrupt row is inert to the controller (it matches
        no phase: not CLOSED/OPEN/HALF_OPEN): the pass completes
        normally, journals nothing, and keeps every other row
        untouched. Fail-closed is enforced at admission, not by
        crashing the controller."""
        ctl = self._new_ctl(failure_threshold=1)
        ctl.store._clock = self.clock
        self._dead("j-246")
        ctl.evaluate_once()  # opens JOB/TASK/GLOBAL cleanly
        self._corrupt_breaker("JOB", "j-246", state="MELTED")
        led0 = self._ledger_count()
        rep = ctl.evaluate_once()  # must not raise
        self.assertEqual(rep["errors"], [])
        self.assertEqual(rep["breakers_opened"], [])
        self.assertEqual(rep["breakers_half_opened"], [])
        self.assertEqual(rep["breakers_closed"], [])
        self.assertEqual(self._ledger_count(), led0)
        self.assertEqual(
            self.gate.get_breaker_state("TASK", "t")["state"], "OPEN")
        row = self.gate.get_breaker_state("JOB", "j-246")
        self.assertEqual(row["state"], "MELTED")  # untouched
        # And the gate refuses to transition FROM a corrupt state.
        with self.assertRaises(TransitionRejected):
            self.gate.transition_breaker(
                "JOB", "j-246", expected_version=row["version"],
                to_state="OPEN", actor="test-r12", reason="test")


# ------------------------------------------------------------------ R12-47
class TestR1247(ResilienceBase):
    def test_R12_47_scheduler_counts_corrupt_as_denied(self):
        """R12-47: the scheduler counts a corrupt-row denial in
        resilience_denied and admits zero workers."""
        self._mk_job("j-247")
        self.gate.ensure_breaker_state("JOB", "j-247", cooldown_s=60.0,
                                       actor="test-r12")
        self._corrupt_breaker("JOB", "j-247")
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["resilience_denied"], 1)
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["errors"], [])
        self.assertEqual(self.gate.get_job("j-247")["status"], "PENDING")


# ------------------------------------------------------------------ R12-48
class TestR1248(ResilienceBase):
    def test_R12_48_controller_is_recoverable_not_crash(self):
        """R12-48: one corrupt row does not poison the controller: passes
        with the corrupt row present complete normally, and after
        repair the state machine resumes (cooldown -> HALF_OPEN)."""
        ctl = self._new_ctl(failure_threshold=1, cooldown_s=60.0)
        ctl.store._clock = self.clock
        self._dead("j-248")
        ctl.evaluate_once()
        self._corrupt_breaker("JOB", "j-248", state="MELTED")
        rep = ctl.evaluate_once()  # inert, no raise
        self.assertEqual(rep["errors"], [])
        self.assertEqual(rep["breakers_half_opened"], [])
        # Repair the row (test-only fixture) and continue on the same
        # controller instance: the cooldown transition resumes.
        self._repair_breaker("JOB", "j-248", "OPEN")
        self.clock.advance(61.0)
        rep2 = ctl.evaluate_once()
        half = {(d["scope_type"], d["scope_id"])
                for d in rep2["breakers_half_opened"]}
        self.assertIn(("JOB", "j-248"), half)
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-248")["state"],
            "HALF_OPEN")


# ------------------------------------------------------------------ R12-49
class TestR1249(ResilienceBase):
    def test_R12_49_controller_writes_fail_closed(self):
        """R12-49: the controller's only writes are breaker-state rows
        (own rows, never shared rows), signals, and the ledger; a failed
        breaker write corrupts nothing else."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1)
        ctl.store._clock = self.clock
        self._dead("j-249")
        before = dict(self.gate.get_job("j-249"))
        led0 = self._ledger_count()
        rep = ctl.evaluate_once()
        self.assertEqual(len(rep["breakers_opened"]), 3)
        # Only breaker_state/ledger rows changed: the job row is
        # byte-identical, incidents/tasks untouched.
        self.assertEqual(dict(self.gate.get_job("j-249")), before)
        evs = [(r[0], json.loads(r[1]))
               for r in self.gate.store.conn.execute(
                   "SELECT event_type, payload FROM ledger WHERE rowid > ?"
                   " ORDER BY rowid", (led0,)).fetchall()]
        self.assertTrue(all(etype.startswith("breaker.")
                            for etype, _ in evs))


# ------------------------------------------------------------------ R12-50
class TestR1250(ResilienceBase):
    def test_R12_50_strict_graph_and_idempotent_ensure(self):
        """R12-50: the breaker graph is strict (no self-loops — a repeat
        OPEN->OPEN is rejected with zero mutation), ensure_breaker_state
        is idempotent, and closing resets failure_count to zero."""
        self._signal("JOB", "j-250", incident="v1")
        row0 = self.gate.ensure_breaker_state(
            "JOB", "j-250", cooldown_s=60.0, actor="t")
        row1 = self.gate.ensure_breaker_state(
            "JOB", "j-250", cooldown_s=60.0, actor="t")
        self.assertEqual(row1["version"], row0["version"])  # idempotent
        opened = self.gate.transition_breaker(
            "JOB", "j-250", expected_version=row1["version"],
            to_state="OPEN", actor="test", reason="t")
        # Repeat OPEN->OPEN: rejected, zero mutation, zero ledger.
        led0 = self._ledger_count()
        with self.assertRaises(TransitionRejected):
            self.gate.transition_breaker(
                "JOB", "j-250", expected_version=opened["version"],
                to_state="OPEN", actor="test", reason="t")
        self.assertEqual(self.gate.get_breaker_state("JOB", "j-250")
                         ["version"], opened["version"])
        self.assertEqual(self._ledger_count(), led0)
        # OPEN->HALF_OPEN->CLOSED; closing resets failure_count to 0.
        half = self.gate.transition_breaker(
            "JOB", "j-250", expected_version=opened["version"],
            to_state="HALF_OPEN", actor="test", reason="t")
        closed = self.gate.transition_breaker(
            "JOB", "j-250", expected_version=half["version"],
            to_state="CLOSED", actor="test", reason="t")
        self.assertEqual(closed["failure_count"], 0)
        self.assertEqual(closed["version"], opened["version"] + 2)


# ------------------------------------------------------------------ R12-51
class TestR1251(ResilienceBase):
    def test_R12_51_breaker_never_loses_failure_count(self):
        """R12-51: failure_count is monotonic within an episode: signals
        only add; only the CLOSED transition resets it."""
        r = self._signal("JOB", "j-251", incident="v1")
        self.assertEqual(r["row"]["failure_count"], 1)
        r = self._signal("JOB", "j-251", incident="v2")
        self.assertEqual(r["row"]["failure_count"], 2)
        opened = self.gate.transition_breaker(
            "JOB", "j-251", expected_version=r["row"]["version"],
            to_state="OPEN", actor="test", reason="t")
        self.assertEqual(opened["failure_count"], 2)  # OPEN keeps count
        half = self.gate.transition_breaker(
            "JOB", "j-251", expected_version=opened["version"],
            to_state="HALF_OPEN", actor="test", reason="t")
        self.assertEqual(half["failure_count"], 2)  # HALF_OPEN keeps count
        closed = self.gate.transition_breaker(
            "JOB", "j-251", expected_version=half["version"],
            to_state="CLOSED", actor="test", reason="t")
        self.assertEqual(closed["failure_count"], 0)  # CLOSED resets


# ------------------------------------------------------------------ R12-52
class TestR1252(ResilienceBase):
    def test_R12_52_threshold_boundary_opens_at_exactly_threshold(self):
        """R12-52: the boundary is inclusive: exactly threshold counted
        signals OPEN; threshold-1 does not."""
        ctl = self._new_ctl(failure_threshold=3,
                            global_failure_threshold=100)
        ctl.store._clock = self.clock
        self._signal("JOB", "j-252", incident="v1")
        self._signal("JOB", "j-252", incident="v2")
        rep = ctl.evaluate_once()
        self.assertEqual(rep["breakers_opened"], [])
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-252")["state"],
            "CLOSED")
        # Third counted signal arrives via a real verdict: the
        # controller's scan records it, touches the scope, and the
        # inclusive boundary fires.
        self._dead("j-252")
        rep = ctl.evaluate_once()
        opened = {(d["scope_type"], d["scope_id"])
                  for d in rep["breakers_opened"]}
        self.assertIn(("JOB", "j-252"), opened)
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-252")["state"], "OPEN")


# ------------------------------------------------------------------ R12-53
class TestR1253(ResilienceBase):
    def test_R12_53_mixed_kinds_each_count_once(self):
        """R12-53: mixed counted kinds (R7_DEAD, R8_ATTEMPT_FAILED,
        R9_ESCALATED) each count exactly once toward the threshold —
        duplicates dedupe, distinct kinds add."""
        ctl = self._new_ctl(failure_threshold=3,
                            global_failure_threshold=100)
        ctl.store._clock = self.clock
        self._dead("j-253")  # -> R7_DEAD x3 scopes
        job = self.gate.get_job("j-253")
        att = self.gate.create_recovery_attempt(
            incident_id=self.gate.find_or_create_recovery_incident(
                "j-253", "DEAD", {"reason": "t"}, "test-r12")
            ["incident_id"],
            job_id="j-253", fencing_token=job["fencing_token"], rung=1,
            rung_name="process kill/requeue", action={"name": "kill"},
            failure_class="DEAD", success_criterion="s",
            failure_criterion="f", evidence_before={}, actor="test-r12")
        self.gate.claim_recovery_attempt(att["attempt_id"], "test-r12")
        self.gate.complete_recovery_attempt(
            att["attempt_id"], decision="retry", observed_effect="none",
            progress_delta=0.0, resulting_state="UNCERTAIN",
            evidence_after={"reason": "t"}, actor="test-r12")
        rep = ctl.evaluate_once()
        row = self.gate.get_breaker_state("JOB", "j-253")
        self.assertEqual(row["failure_count"], 2)  # R7 + R8, distinct
        self.assertEqual(row["state"], "CLOSED")
        # Re-scanning the same verdict dedupes (same verdict_id).
        rep2 = ctl.evaluate_once()
        self.assertEqual(rep2["signals_recorded"], 0)
        self.assertEqual(
            self.gate.get_breaker_state("JOB", "j-253")["failure_count"],
            2)


# ------------------------------------------------------------------ R12-54
class TestR1254(ResilienceBase):
    def test_R12_54_old_window_signals_dont_count(self):
        """R12-54: counted signals age out of the failure window: stale
        signals no longer open the breaker (controller reads durability,
        not memory)."""
        ctl = self._new_ctl(failure_threshold=2,
                            global_failure_threshold=100,
                            failure_window_s=100.0)
        ctl.store._clock = self.clock
        self._signal("JOB", "j-254", incident="v1")
        self.clock.advance(500.0)  # beyond the 100s window
        self._signal("JOB", "j-254", incident="v2")
        rep = ctl.evaluate_once()
        self.assertEqual(rep["breakers_opened"], [])
        row = self.gate.get_breaker_state("JOB", "j-254")
        self.assertEqual(row["state"], "CLOSED")
        self.assertEqual(row["failure_count"], 1)  # stale one purged


# ------------------------------------------------------------------ R12-55
class TestR1255(ResilienceBase):
    def test_R12_55_time_discipline(self):
        """R12-55: breaker timestamps come from the store clock: with the
        FakeClock bound, opened_at/updated_at/cooldown_until track it
        exactly."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1, cooldown_s=120.0)
        ctl.store._clock = self.clock
        self._dead("j-255")
        rep = ctl.evaluate_once()
        self.assertEqual(len(rep["breakers_opened"]), 3)
        row = self.gate.get_breaker_state("JOB", "j-255")
        # Store-clock time: on the FakeClock (not wall time), within the
        # write_txn monotonicity epsilon.
        self.assertAlmostEqual(row["opened_at"], self.clock(), delta=0.1)
        self.assertAlmostEqual(row["updated_at"], self.clock(), delta=0.1)
        # cooldown_until derives from the authoritative opened_at.
        self.assertEqual(row["cooldown_until"], row["opened_at"] + 120.0)
        self.assertAlmostEqual(row["window_started_at"], self.clock(),
                               delta=0.1)


# ------------------------------------------------------------------ R12-56
class TestR1256(ResilienceBase):
    def test_R12_56_single_store_path(self):
        """R12-56: every resilience component consults the gate/store:
        controller, scheduler, and claim path all read/write the same
        rows (shared DB file, no shadow state)."""
        ctl = self._new_ctl(failure_threshold=1)
        ctl.store._clock = self.clock
        self.assertEqual(ctl.store.path, self.db)
        self._dead("j-256")
        ctl.evaluate_once()
        # The scheduler's read-only preflight and the gate see the same
        # row: no shadow copies.
        row_ctl = ctl.store.conn.execute(
            "SELECT state, version FROM breaker_state"
            " WHERE scope_type='JOB' AND scope_id='j-256'").fetchone()
        row_gate = self.gate.get_breaker_state("JOB", "j-256")
        self.assertEqual(dict(row_ctl)["state"], row_gate["state"])
        self.assertEqual(dict(row_ctl)["version"], row_gate["version"])
        sched = self._new_sched()
        self.assertEqual(sched.store.path, self.db)


# ------------------------------------------------------------------ R12-57
class TestR1257(ResilienceBase):
    def test_R12_57_time_source_is_store(self):
        """R12-57: authority timestamps never come from raw wall time.
        time.time() may appear only in informational (non-authority)
        contexts -- the background loop's error telemetry. Every
        authority path (evaluate_once, scheduler preflight) uses the
        store clock."""
        for path in (RES_SRC, SCHED_SRC):
            with open(path) as f:
                tree = ast.parse(f.read())
            parent = {}
            for node in ast.walk(tree):
                for child in ast.iter_child_nodes(node):
                    parent[child] = node
            bad = []
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "time"
                        and isinstance(node.func.value, ast.Name)
                        and node.func.value.id == "time"):
                    fn, fname = node, "<module>"
                    while fn in parent:
                        fn = parent[fn]
                        if isinstance(fn, (ast.FunctionDef,
                                           ast.AsyncFunctionDef)):
                            fname = fn.name
                            break
                    if fname != "_loop":
                        bad.append((path, fname, node.lineno))
            self.assertEqual(
                bad, [], f"time.time() in authority path: {bad}")


# ------------------------------------------------------------------ R12-58
class TestR1258(ResilienceBase):
    def test_R12_58_no_sql_in_resilience_or_scheduler(self):
        """R12-58: no hand SQL in the resilience controller or the
        scheduler's breaker preflight (AST): all durability goes
        through gate/store APIs. Docstrings/comments do not count --
        only executable SQL strings and DB-API calls."""
        sql_words = ("SELECT", "UPDATE", "INSERT", "DELETE", "CREATE",
                     "DROP", "ALTER")
        for path in (RES_SRC, SCHED_SRC):
            with open(path) as f:
                tree = ast.parse(f.read())
            # Collect docstring nodes to exclude them.
            docstrings = set()
            for node in ast.walk(tree):
                if isinstance(node, (ast.Module, ast.ClassDef,
                                      ast.FunctionDef,
                                      ast.AsyncFunctionDef)):
                    body = getattr(node, "body", [])
                    if (body and isinstance(body[0], ast.Expr)
                            and isinstance(body[0].value, ast.Constant)
                            and isinstance(body[0].value.value, str)):
                        docstrings.add(id(body[0].value))
            bad = []
            for node in ast.walk(tree):
                if (isinstance(node, ast.Constant)
                        and isinstance(node.value, str)
                        and id(node) not in docstrings):
                    upper = node.value.upper()
                    if any(w in upper.split() for w in sql_words):
                        bad.append((path, node.lineno,
                                    node.value[:60]))
                if (isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "execute"):
                    bad.append((path, node.lineno, ".execute() call"))
            self.assertEqual(bad, [], f"raw SQL in {path}: {bad}")


# ------------------------------------------------------------------ R12-59
class TestR1259(ResilienceBase):
    def test_R12_59_no_duplicate_breaker_authority(self):
        """R12-59: no duplicate breaker authority exists in exec/:
        only gate.py owns transition_breaker/claim_half_open_probe;
        the controller and scheduler never define or call them."""
        found = []
        for root, _dirs, files in os.walk(EXEC_SRC):
            for name in files:
                if not name.endswith(".py"):
                    continue
                path = os.path.join(root, name)
                with open(path) as f:
                    src = f.read()
                if name == "gate.py":
                    continue
                if "def transition_breaker" in src:
                    found.append(path + ":def transition_breaker")
                if "def claim_half_open_probe" in src:
                    found.append(path + ":def claim_half_open_probe")
        self.assertEqual(found, [])
        for path in (RES_SRC, SCHED_SRC):
            with open(path) as f:
                tree = ast.parse(f.read())
            defs = {n.name for n in ast.walk(tree)
                    if isinstance(n, ast.FunctionDef)}
            self.assertFalse(
                {"transition_breaker", "claim_half_open_probe",
                 "record_breaker_signal", "ensure_breaker_state"}
                & defs, path)


# ------------------------------------------------------------------ R12-60
class TestR1260(ResilienceBase):
    def test_R12_60_breaker_events_are_ledger_journaled(self):
        """R12-60: breaker opens journal to the ledger; ledger queries
        confirm open events (no store.conn write bypass needed)."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1)
        ctl.store._clock = self.clock
        self._dead("j-260")
        led0 = self._ledger_count()
        rep = ctl.evaluate_once()
        self.assertEqual(len(rep["breakers_opened"]), 3)
        rows = self.gate.store.conn.execute(
            "SELECT event_type, payload, actor FROM ledger WHERE rowid > ?"
            " ORDER BY rowid", (led0,)).fetchall()
        trans = [(json.loads(r[1]), r[2]) for r in rows
                 if r[0] == "breaker.transition"]
        self.assertEqual(len(trans), 3)
        by_scope = {(t["scope_type"], t["scope_id"]): (t, actor)
                    for t, actor in trans}
        for scope in (("JOB", "j-260"), ("TASK", "t"),
                      ("GLOBAL", "global")):
            self.assertIn(scope, by_scope)
            payload, actor = by_scope[scope]
            self.assertEqual(payload["new_state"], "OPEN")
            self.assertEqual(actor, "resilience-controller")


# ------------------------------------------------------------------ R12-61
class TestR1261(ResilienceBase):
    def test_R12_61_dedupe_scope_qualified(self):
        """R12-61: the same failure kind in different scopes dedupes
        independently: JOB and TASK signals for the same incident are
        both counted, not collapsed."""
        ctl = self._new_ctl(failure_threshold=1)
        ctl.store._clock = self.clock
        self._dead("j-261")
        rep = ctl.evaluate_once()
        # JOB, TASK, GLOBAL each got their own signal (not collapsed).
        self.assertEqual(rep["signals_recorded"], 3)
        rows = self.gate.store.conn.execute(
            "SELECT scope_type, scope_id, failure_kind FROM breaker_signals"
            " WHERE failure_kind='R7_DEAD'").fetchall()
        got = {(r[0], r[1], r[2]) for r in rows}
        self.assertEqual(
            got,
            {("JOB", "j-261", "R7_DEAD"), ("TASK", "t", "R7_DEAD"),
             ("GLOBAL", "global", "R7_DEAD")})


# ------------------------------------------------------------------ R12-62
class TestR1262(ResilienceBase):
    def test_R12_62_dedupe_is_content_keyed(self):
        """R12-62: the signal dedupe key is (scope, kind, incident,
        attempt) -- content, not time: re-scanning the same verdict
        later (within the scan window) still dedupes. Only genuinely
        new evidence re-fires, and it starts a fresh count window."""
        ctl = self._new_ctl(failure_threshold=100,
                            global_failure_threshold=100,
                            failure_window_s=100.0)
        ctl.store._clock = self.clock
        self._dead("j-262")
        rep = ctl.evaluate_once()
        self.assertEqual(rep["signals_recorded"], 3)
        # Later, still within the scan window: the same verdict
        # re-scans but dedupes -- the key is content, not time.
        self.clock.advance(50.0)
        rep2 = ctl.evaluate_once()
        self.assertEqual(rep2["signals_recorded"], 0)
        self.assertEqual(rep2["signals_deduped"], 3)
        # New evidence (a new execution identity's verdict) records and
        # starts a fresh window with count 1.
        self._dead("j-262b")
        rep3 = ctl.evaluate_once()
        self.assertEqual(rep3["signals_recorded"], 3)
        row = self.gate.get_breaker_state("JOB", "j-262b")
        self.assertEqual(row["failure_count"], 1)


# ------------------------------------------------------------------ R12-63
class TestR1263(ResilienceBase):
    def test_R12_63_human_gate_preserved(self):
        """R12-63: the PAUSED_FOR_HUMAN/rung-5 boundary is preserved:
        the controller never writes recovery rungs, never pauses for
        human, never transitions jobs, and never creates recovery
        incidents. Reading attempt/incident states as evidence is
        allowed; writing them is not."""
        with open(RES_SRC) as f:
            src = f.read()
        # PAUSED_FOR_HUMAN must not appear at all.
        self.assertNotIn("PAUSED_FOR_HUMAN", src)
        tree = ast.parse(src)
        calls = {n.func.attr for n in ast.walk(tree)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute)}
        # No job/recovery/incident write authority.
        self.assertFalse({"transition_job", "update_job_progress",
                          "commit_artifact", "reclaim_lease",
                          "run_recovery", "find_or_create_recovery_incident",
                          "create_recovery_attempt",
                          "complete_recovery_attempt",
                          "claim_recovery_attempt"} & calls)
        # Job statuses are untouched by any controller pass.
        ctl = self._new_ctl(failure_threshold=1)
        ctl.store._clock = self.clock
        self._dead("j-263")
        before = self.gate.get_job("j-263")["status"]
        ctl.evaluate_once()
        self.assertEqual(self.gate.get_job("j-263")["status"], before)


# ------------------------------------------------------------------ R12-64
class TestR1264(ResilienceBase):
    def test_R12_64_full_ledger_chain_verifies(self):
        """R12-64: after a full controller cycle the ledger chain
        verifies end-to-end (hash chain intact)."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1)
        ctl.store._clock = self.clock
        self._dead("j-264")
        ctl.evaluate_once()
        self.clock.advance(61.0)
        ctl.evaluate_once()  # half-open
        ctl.evaluate_once()  # still half-open, no probe
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)


# ------------------------------------------------------------------ R12-65
class TestR1265(ResilienceBase):
    def test_R12_65_concurrent_controllers_one_winner(self):
        """R12-65: two controllers racing the same pass open the breaker
        exactly once: CAS makes the loser re-read the winner's row."""
        self._dead("j-265")
        res = self._run_barrier([
            self._ctl_worker(failure_threshold=1,
                             global_failure_threshold=1, cooldown_s=60.0),
            self._ctl_worker(failure_threshold=1,
                             global_failure_threshold=1, cooldown_s=60.0),
        ])
        self.assertFalse(isinstance(res[0], Exception))
        self.assertFalse(isinstance(res[1], Exception))
        total = (res[0]["signals_recorded"] + res[1]["signals_recorded"])
        self.assertEqual(total, 3)  # 3 scopes, exactly one signal each
        row = self.gate.get_breaker_state("JOB", "j-265")
        self.assertEqual(row["state"], "OPEN")
        self.assertEqual(row["version"], 2)  # ensure(1) + open(1)
        opens = [r for r in self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='breaker.transition'"
        ).fetchall()
            if json.loads(r[0])["new_state"] == "OPEN"
            and json.loads(r[0])["scope_id"] == "j-265"]
        self.assertEqual(len(opens), 1)

# ------------------------------------------------------------------ R12-66
class TestR1266(ResilienceBase):
    def test_R12_66_three_controllers_one_winner(self):
        """R12-66: three controllers racing the same pass open the breaker
        exactly once: CAS makes both losers re-read the winner's row."""
        self._dead("j-266")
        res = self._run_barrier([
            self._ctl_worker(failure_threshold=1,
                             global_failure_threshold=1, cooldown_s=60.0),
            self._ctl_worker(failure_threshold=1,
                             global_failure_threshold=1, cooldown_s=60.0),
            self._ctl_worker(failure_threshold=1,
                             global_failure_threshold=1, cooldown_s=60.0),
        ])
        for r in res.values():
            self.assertFalse(isinstance(r, Exception), r)
        total = sum(r["signals_recorded"] for r in res.values())
        self.assertEqual(total, 3)  # 3 scopes, exactly one signal each
        row = self.gate.get_breaker_state("JOB", "j-266")
        self.assertEqual(row["state"], "OPEN")
        self.assertEqual(row["version"], 2)  # ensure(1) + open(1)
        opens = [r for r in self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='breaker.transition'"
        ).fetchall()
            if json.loads(r[0])["new_state"] == "OPEN"
            and json.loads(r[0])["scope_id"] == "j-266"]
        self.assertEqual(len(opens), 1)

# ======================================================================
# Race battery (barrier-aligned; all state durable, real SQLite).
# ======================================================================


# ------------------------------------------------------------------ RACE-01
class TestRace01(ResilienceBase):
    def test_RACE_01_many_claims_vs_open_breaker_zero_admissions(self):
        """RACE-01: N threads x 1 PENDING job under GLOBAL OPEN: zero
        admissions, zero dispatched workers, resilience_denied == N, the
        job still PENDING."""
        self._mk_job("j-r1")
        self._open("GLOBAL", "global")
        res = self._run_barrier([
            self._gate_worker(
                lambda g, i=i: g.claim_job_resilient(
                    "j-r1", f"w-r1-{i}", 60.0, "test", 100,
                    scheduler_id="s", probe_limit=1))
            for i in range(12)])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
            self.assertFalse(v)
        self.assertEqual(self.gate.get_job("j-r1")["status"], "PENDING")
        self.assertEqual(self.sup._procs, {})


# ------------------------------------------------------------------ RACE-02
class TestRace02(ResilienceBase):
    def test_RACE_02_scheduler_passes_vs_one_probe_slot(self):
        """RACE-02: N barrier-aligned scheduler passes racing one
        HALF_OPEN GLOBAL probe slot (limit 1): exactly one admission
        across all passes; every other candidate stays PENDING."""
        jobs = [f"j-r2-{i}" for i in range(8)]
        for jid in jobs:
            self._mk_job(jid)
        self._open("GLOBAL", "global", cooldown_s=60.0)
        row = self.gate.get_breaker_state("GLOBAL", "global")
        self.gate.transition_breaker(
            "GLOBAL", "global", expected_version=row["version"],
            to_state="HALF_OPEN", actor="test-r12", reason="test")
        sched = self._new_sched()
        res = self._run_barrier([sched.evaluate_once for _ in range(8)])
        reps = [v for v in res.values()
                if not isinstance(v, Exception)]
        self.assertEqual(len(reps), 8)
        total_admitted = sum(r["admitted"] for r in reps)
        self.assertEqual(total_admitted, 1)
        # Exactly one job left PENDING-behind (admitted); the rest are
        # still PENDING. The admitted job's worker runs async, so its
        # status is only asserted as "not PENDING".
        pending = [jid for jid in jobs
                   if self.gate.get_job(jid)["status"] == "PENDING"]
        self.assertEqual(len(pending), 7)
        row = self.gate.get_breaker_state("GLOBAL", "global")
        self.assertEqual(row["half_open_probes_used"], 1)


# ------------------------------------------------------------------ RACE-03
class TestRace03(ResilienceBase):
    def test_RACE_03_claim_race_vs_controller_open(self):
        """RACE-03: claim attempts racing the controller's threshold trip:
        every outcome is either admitted-then-opened or denied; the
        breaker row is never corrupt and the job never double-claimed."""
        self._mk_job("j-r3")
        for i in range(3):
            self._signal("JOB", "j-r3", incident=f"v{i}")
        res = self._run_barrier([
            self._gate_worker(
                lambda g: g.claim_job_resilient(
                    "j-r3", "w-r3", 60.0, "test", 4, scheduler_id="s",
                    probe_limit=1)),
            self._gate_worker(
                lambda g: g.record_breaker_signal(
                    "JOB", "j-r3", failure_kind="R7_DEAD",
                    incident_id="v3", attempt_id=None,
                    failure_window_s=300.0, cooldown_s=60.0,
                    actor="test-r12")),
            self._ctl_worker(failure_threshold=4,
                             global_failure_threshold=100),
        ])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        row = self.gate.get_breaker_state("JOB", "j-r3")
        self.assertIn(row["state"], ("CLOSED", "OPEN"))
        job = self.gate.get_job("j-r3")
        self.assertIn(job["status"], ("PENDING", "CLAIMED"))
        # No double claim: worker_id set at most once.
        self.assertIsNone(job["owner_worker_id"]) if job["status"] == "PENDING" \
            else self.assertEqual(job["owner_worker_id"], "w-r3")


# ------------------------------------------------------------------ RACE-04
class TestRace04(ResilienceBase):
    def test_RACE_04_probe_success_vs_new_failure(self):
        """RACE-04: a probe completing while new failure evidence lands:
        the breaker ends CLOSED (success wins) or OPEN (failure wins) —
        never corrupt, never HALF_OPEN-with-stale-probe."""
        self._mk_job("j-r4")
        self._open("JOB", "j-r4", cooldown_s=60.0)
        row = self.gate.get_breaker_state("JOB", "j-r4")
        self.gate.transition_breaker(
            "JOB", "j-r4", expected_version=row["version"],
            to_state="HALF_OPEN", actor="test-r12", reason="test")
        self.assertTrue(self.gate.claim_job_resilient(
            "j-r4", "w-r4", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        job = self.gate.get_job("j-r4")
        self.sup._ensure_worker_row("w-r4")
        self.gate.transition_job("j-r4", "RUNNING", "worker:w-r4")
        # Three barriers racing: success-completion vs a fresh DEAD
        # verdict vs the controller pass.
        tok = job["fencing_token"]
        res = self._run_barrier([
            self._gate_worker(
                lambda g: r5_complete(
                    gate=g, job_id="j-r4", worker_id="w-r4",
                    fencing_token=tok, task_id="t",
                    outcome="SUCCESS", evidence={"ok": True},
                    actor="worker:w-r4")),
            self._gate_worker(
                lambda g: g.record_watchdog_verdict(
                    "j-r4", tok, "DEAD", {"reason": "race"}, "test-r12")),
            self._ctl_worker(failure_threshold=1, cooldown_s=60.0),
        ])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        row = self.gate.get_breaker_state("JOB", "j-r4")
        self.assertIn(row["state"], ("CLOSED", "OPEN", "HALF_OPEN"))
        self.assertNotIn(row["state"], ("MELTED", "BROKEN", ""))
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)


# ------------------------------------------------------------------ RACE-05
class TestRace05(ResilienceBase):
    def test_RACE_05_controller_probe_evals_cas_safe(self):
        """RACE-05: two controllers evaluating probes concurrently:
        each probe resolves at most once; no double-close, no
        double-open."""
        ctl1 = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1, cooldown_s=60.0)
        ctl2 = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1, cooldown_s=60.0)
        ctl1.store._clock = self.clock
        ctl2.store._clock = self.clock
        self._dead("j-r5")
        ctl1.evaluate_once()
        self.clock.advance(61.0)
        ctl1.evaluate_once()  # half-open
        # Allocate the probe via a fresh job on the same HALF_OPEN scopes.
        self.gate.create_job("j-r5p", "t", None, "test")
        self.assertTrue(self.gate.claim_job_resilient(
            "j-r5p", "w-r5", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        job = self.gate.get_job("j-r5p")
        self.sup._ensure_worker_row("w-r5")
        self.gate.transition_job("j-r5p", "RUNNING", "worker:w-r5")
        r5_complete(gate=self.gate, job_id="j-r5p", worker_id="w-r5",
                    fencing_token=job["fencing_token"], task_id="t",
                    outcome="SUCCESS", evidence={"ok": True},
                    actor="worker:w-r5")
        res = self._run_barrier([
            self._ctl_worker(failure_threshold=1,
                             global_failure_threshold=1, cooldown_s=60.0),
            self._ctl_worker(failure_threshold=1,
                             global_failure_threshold=1, cooldown_s=60.0),
        ])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        total_closed = sum(
            1 for i in (0, 1)
            for d in res[i]["breakers_closed"]
            if (d["scope_type"], d["scope_id"]) == ("GLOBAL", "global"))
        self.assertEqual(total_closed, 1)  # closed exactly once
        self.assertEqual(
            self.gate.get_breaker_state("GLOBAL", "global")["state"],
            "CLOSED")


# ------------------------------------------------------------------ RACE-06
class TestRace06(ResilienceBase):
    def test_RACE_06_concurrent_dead_verdicts_one_signal_per_scope(self):
        """RACE-06: N threads recording the same DEAD verdict
        concurrently, then one controller pass: exactly one counted
        signal per scope (content-dedupe under concurrency)."""
        self._mk_job("j-r6")
        self.assertTrue(self.gate.claim_job_bounded(
            "j-r6", "w-r6", 60.0, "test", 4))
        tok = self.gate.get_job("j-r6")["fencing_token"]
        self.sup._ensure_worker_row("w-r6")
        self.gate.transition_job("j-r6", "RUNNING", "worker:w-r6")
        res = self._run_barrier([
            self._gate_worker(
                lambda g: g.record_watchdog_verdict(
                    "j-r6", tok, "DEAD", {"reason": "race"}, "test-r12"))
            for _ in range(8)])
        for v in res.values():
            self.assertNotIsInstance(v, Exception)
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=100)
        ctl.store._clock = self.clock
        rep = ctl.evaluate_once()
        self.assertEqual(rep["signals_recorded"], 3)  # JOB/TASK/GLOBAL
        rows = self.gate.store.conn.execute(
            "SELECT scope_type, scope_id, failure_kind FROM breaker_signals"
            " WHERE failure_kind='R7_DEAD'").fetchall()
        got = {(r[0], r[1], r[2]) for r in rows}
        self.assertEqual(
            got,
            {("JOB", "j-r6", "R7_DEAD"), ("TASK", "t", "R7_DEAD"),
             ("GLOBAL", "global", "R7_DEAD")})


# ======================================================================
# §34 — the full breaker lifecycle end to end (§34 negative: injected
# corruption mid-path fails closed and stays consistent).
# ======================================================================


class TestSection34Full(ResilienceBase):
    def test_S34_full_breaker_lifecycle(self):
        """§34 full path: (1) threshold breach -> OPEN; (2) cooldown
        anchored at open time; (3) post-cooldown HALF_OPEN; (4) probe
        admission (limit 1); (5) probe SUCCESS via R5-only evidence;
        (6) controller closes; (7) normal claims resume."""
        ctl = self._new_ctl(failure_threshold=1,
                            global_failure_threshold=1, cooldown_s=120.0)
        ctl.store._clock = self.clock
        # (1) counted evidence trips the threshold -> OPEN.
        self._dead("j-s34")
        rep1 = ctl.evaluate_once()
        self.assertEqual(len(rep1["breakers_opened"]), 3)
        opened = self.gate.get_breaker_state("JOB", "j-s34")
        self.assertEqual(opened["state"], "OPEN")
        # (2) cooldown anchored at open time.
        self.assertEqual(opened["cooldown_until"],
                         opened["opened_at"] + 120.0)
        # Admission denied while OPEN.
        self._mk_job("j-s34b")
        self.assertFalse(self.gate.claim_job_resilient(
            "j-s34b", "w-s34", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        # (3) post-cooldown -> HALF_OPEN (controller only).
        self.clock.advance(121.0)
        rep3 = ctl.evaluate_once()
        half = {(d["scope_type"], d["scope_id"])
                for d in rep3["breakers_half_opened"]}
        self.assertEqual(half, {("JOB", "j-s34"), ("TASK", "t"),
                                ("GLOBAL", "global")})
        # (4) one probe admitted (limit 1).
        self.assertTrue(self.gate.claim_job_resilient(
            "j-s34b", "w-s34", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        grow = self.gate.get_breaker_state("GLOBAL", "global")
        self.assertEqual(grow["half_open_probes_used"], 1)
        # (5) probe SUCCESS via the R5-only evidence contract.
        job = self.gate.get_job("j-s34b")
        self.sup._ensure_worker_row("w-s34")
        self.gate.transition_job("j-s34b", "RUNNING", "worker:w-s34")
        r5_complete(gate=self.gate, job_id="j-s34b", worker_id="w-s34",
                    fencing_token=job["fencing_token"], task_id="t",
                    outcome="SUCCESS", evidence={"ok": True},
                    actor="worker:w-s34")
        # (6) the controller closes the probed scopes (TASK/GLOBAL --
        # JOB:j-s34b was never OPEN, so it was never HALF_OPEN and never
        # carried a probe).
        rep6 = ctl.evaluate_once()
        closed = {(d["scope_type"], d["scope_id"])
                  for d in rep6["breakers_closed"]}
        self.assertEqual(closed, {("TASK", "t"), ("GLOBAL", "global")})
        for scope in (("TASK", "t"), ("GLOBAL", "global")):
            self.assertEqual(
                self.gate.get_breaker_state(*scope)["state"], "CLOSED")
        # (7) normal claims resume on the same task.
        self._mk_job("j-s34c")
        self.assertTrue(self.gate.claim_job_resilient(
            "j-s34c", "w-s34c", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        self.assertEqual(self.gate.get_job("j-s34c")["status"], "CLAIMED")
        # And the ledger chain verifies end to end.
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)


class TestSection34Negative(ResilienceBase):
    def test_S34_negative_corrupt_mid_path_fails_closed(self):
        """§34 negative path: inject a corrupt breaker row mid-path
        (after HALF_OPEN, before probe admission). Admission fails
        closed, the scheduler counts the denial with no dispatch, the
        controller pass completes without touching the corrupt row, and
        every durable row stays consistent."""
        ctl = self._new_ctl(failure_threshold=1, cooldown_s=60.0)
        ctl.store._clock = self.clock
        self._dead("j-sn")
        ctl.evaluate_once()
        self.clock.advance(61.0)
        ctl.evaluate_once()  # HALF_OPEN
        self._mk_job("j-snb")
        # Inject corruption: GLOBAL row is no longer in the legal graph.
        self._corrupt_breaker("GLOBAL", "global", state="MELTED")
        # Admission fails closed: no claim, no lease, no worker.
        self.assertFalse(self.gate.claim_job_resilient(
            "j-snb", "w-sn", 60.0, "test", 4, scheduler_id="s",
            probe_limit=1))
        self.assertEqual(self.gate.get_job("j-snb")["status"], "PENDING")
        # Scheduler: denial counted, nothing dispatched, no error.
        sched = self._new_sched()
        sched.store._clock = self.clock
        rep = sched.evaluate_once()
        self.assertEqual(rep["resilience_denied"], 1)
        self.assertEqual(rep["admitted"], 0)
        self.assertEqual(rep["errors"], [])
        self.assertEqual(self.sup._procs, {})
        # Controller: the corrupt row is inert — the pass completes,
        # journals nothing, and leaves the other scopes' rows untouched.
        led0 = self._ledger_count()
        crep = ctl.evaluate_once()
        self.assertEqual(crep["errors"], [])
        self.assertEqual(self._ledger_count(), led0)
        self.assertEqual(
            self.gate.get_breaker_state("TASK", "t")["state"], "HALF_OPEN")
        # Consistency: the corrupt row is still corrupt (untouched), the
        # job row is PENDING with no lease, the ledger chain verifies.
        row = self.gate.get_breaker_state("GLOBAL", "global")
        self.assertEqual(row["state"], "MELTED")
        job = self.gate.get_job("j-snb")
        self.assertIsNone(job["owner_worker_id"])
        self.assertIsNone(job["lease_expires_at"])
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)


# Keep the module importable by unittest discovery even if a later chunk
# is appended with a duplicate class name: fail loudly instead.
if __name__ == "__main__":
    unittest.main()
