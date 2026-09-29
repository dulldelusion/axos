"""Phase 1C R11 — desired-state reconciliation tests.

STANDARD: real SQLite/WAL temp DBs with full migrate() (v9); real
subprocesses where execution is involved (scheduler dispatch, worker
lifecycle); gate-level fixtures elsewhere. Deterministic fault injection
(wrapped gate methods, counting wrappers) is preferred over process
kills; exactly one real-process restart test (a new Reconciler over the
same DB file) plus real worker subprocesses for the dispatch/completion
legs. No blind sleeps: barrier-aligned threads and poll-based waits;
every spawned process is killed and reaped in tearDown.

Test IDs:
  R11-01  durable desired-state snapshot (head version bumps, snapshot
          hash changes, state visible via a fresh gate on the same file)
  R11-02  deterministic snapshot identity (same logical spec -> same
          head hash across DBs; tampered spec -> snapshot mismatch ->
          FAILED with zero mutation)
  R11-03  canonical job_id stable across runs and reconciler restarts
          (matches the axos-desired-job:v1 construction)
  R11-04  missing item detected (diff DESIRED_MISSING) + pure diff_item
          verdict table for every status
  R11-05  DESIRED_MISSING created exactly once as PENDING with the
          correct task_id/stage_id/max_attempts/policy envelope
  R11-06  N threads x N reconcilers, one missing item -> exactly one job
          row, one map row, one job.created event
  R11-07  PENDING recognized (DESIRED_ALREADY_PRESENT): no-op, job row
          byte-identical, no new ledger events for the job
  R11-08  CLAIMED recognized (DESIRED_ACTIVE): no-op, byte-identical
  R11-09  RUNNING recognized (DESIRED_ACTIVE): no-op, byte-identical
  R11-10  COMMITTING recognized (DESIRED_ACTIVE): no-op, byte-identical
  R11-11  VERIFYING recognized (DESIRED_ACTIVE): no-op, byte-identical
          (injected defensively: the gate graph has no VERIFYING job
          state; the reconciler must still treat it as active, never act)
  R11-12  COMPLETE satisfies desired (DESIRED_COMPLETE): no recreate
  R11-13  UNCERTAIN not recreated (DESIRED_UNCERTAIN): recorded only
  R11-14  FAILED stays (DESIRED_FAILED): no recovery bypass — no
          recovery attempt/incident/policy rows created by the pass
  R11-15  BLOCKED stays blocked (DESIRED_BLOCKED): recorded only
  R11-16  task PAUSED_FOR_HUMAN -> item blocked, no creation; sticky
          across reconciler restart
  R11-17  retired item -> OBSOLETE recorded
  R11-18  retired item: job row and all history untouched (no
          delete/transition ledger events for it)
  R11-19  reconciler never spawns (no worker.proc_spawned events; none
          attributable to a reconciler actor even with a live supervisor)
  R11-20  reconciler never terminates (AST: no kill verbs; behavioral: a
          live worker proc stays alive across reconcile)
  R11-21  reconciler never reclaims (AST + behavioral: expired lease
          untouched, no job.lease_reclaimed events)
  R11-22  reconciler never fences (AST + behavioral: no
          worker.fence_enforced events)
  R11-23  reconciler never touches recovery policy tables (counts
          unchanged; AST: no recovery/incident references)
  R11-24  reconciler never bypasses TransitionGate (AST: no write_txn /
          raw SQL; behavioral: every desired-created job has exactly one
          gate-journaled job.created event with actor "reconciler")
  R11-25  first reconcile creates PENDING only (no scheduler involved)
  R11-26  R10 Scheduler.evaluate_once() claims+dispatches it: claim and
          dispatch authority are the scheduler's, not the reconciler's
  R11-27  worker completes via the R5 contract -> COMPLETE; next
          reconcile -> DESIRED_COMPLETE satisfied, zero new jobs
  R11-28  re-run unchanged -> CONVERGED no-op (zero new job rows, zero
          new ledger events)
  R11-29  repeated runs converge (run-until-stable helper, bounded)
  R11-30  stale pin reconcile(desired_version=old) -> CONFLICT, run=None,
          zero mutation
  R11-31  version races: stale-pin entry refusal vs current-version run
          (deterministic final state); mid-pass stale rule via a
          deterministic counting wrapper (CONFLICT after the run opened,
          zero creations); older run's creates are idempotent
          same-identity under the newer version
  R11-32  2 reconcilers x 8 missing items -> no duplicates, deterministic
          final durable state
  R11-33  3 reconcilers x 6 missing items -> no duplicates, deterministic
          final durable state
  R11-34  crash before snapshot (list_desired_items raises) -> propagates;
          nothing created, no run rows
  R11-35  crash after snapshot before mutation (begin run raises) ->
          propagates; no jobs, no run rows
  R11-36  crash between batches (ensure raises on 3rd call) -> FAILED
          run with per-batch checkpoints; resume completes without
          duplicating batches 1..k
  R11-37  crash after job creation before run-record finish (finish
          raises) -> idempotent re-run -> CONVERGED, single created event
  R11-38  crash during run-record update (checkpoint raises on 2nd call)
          -> open RUNNING run observable; resume -> no duplicates
  R11-39  real-process restart: new Reconciler on the same DB after a
          completed pass -> CONVERGED, no duplicates
  R11-40  unreadable desired state (corrupt spec JSON; dropped
          desired_state table) -> FAILED, run=None, zero job mutation
  R11-41  unreadable actual state (dropped jobs table) -> FAILED, run
          finished FAILED (not left RUNNING), zero job mutation
  R11-42  contradictory desired (tampered spec -> snapshot mismatch) ->
          FAILED, run=None
  R11-43  contradictory actual (map-without-job via manual delete):
          reconcile records CONTRADICTORY with zero action (no adoption);
          the gate's ensure op raises DesiredStateConflict on the same
          state
  R11-44  canonical-identity collision (human-created job under the
          canonical dj- id): reconcile records CONTRADICTORY, no
          adoption, no overwrite; direct ensure -> DesiredStateConflict
  R11-45  deterministic ordering: shuffled insertion -> creation order
          follows sorted desired_work_id (ledger seq)
  R11-46  batching bounded: batch_size=2 over 5 items -> 3 batches, per-
          batch checkpoints advance
  R11-47  partial resume safe: reconciler dropped mid-run (open RUNNING
          run left behind); a new Reconciler completes the pass with no
          duplicates
  R11-48  no duplicate jobs after restart (close + new Reconciler)
  R11-49  reconciler cannot mark COMPLETE (AST: no commit path;
          behavioral: full lifecycle completes only via the R5 contract;
          a direct reconciler-actor commit attempt is fencing-rejected)
  R11-50  R1-R10 composition smoke: scheduler still admits; a FAILED job
          from a reconciled item is left for R8/R9 (no recovery incident
          created by the reconciler)
  R11-FULL  S30 full-path composition: desired item -> R11 PENDING ->
          R10 claims -> R10 dispatches real worker (fast success) -> R3
          heartbeat evidence -> R5 commit_artifact -> COMPLETE -> next
          R11 reconcile -> DESIRED_COMPLETE, zero new jobs; per-layer
          authority asserted from the ledger
Race extras (run x3 by the verification agent):
  RACE-1  reconciler racing a concurrent set_desired_item (head bump
          mid-pass): first run CHANGED or CONFLICT; settled state is
          deterministic (one job per item, follow-up CONVERGED)
  RACE-2  reconciler racing a concurrent retire: retired item ends
          OBSOLETE; its job (if created) is never transitioned/deleted
  RACE-3  6 threads x set_desired_item (distinct items, CAS head):
          all succeed, head version +6, snapshot verifies
  RACE-4  4 reconcilers x batch_size=1 x 6 items: no duplicates,
          checkpoints interleave safely
  RACE-5  reconcilers racing direct gate.ensure_job_for_desired_state
          calls on the same item: exactly one job row / map row /
          job.created event
"""
import ast
import hashlib
import json
import os
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.dirname(os.path.dirname(HERE))  # ~/workspace
sys.path.insert(0, WORKSPACE_ROOT)

from axos.exec.reconciler import (Reconciler, ReconcilerConfig,  # noqa: E402
                                  canonical_job_id, diff_item)
from axos.exec.scheduler import Scheduler, SchedulerConfig  # noqa: E402
from axos.exec.supervisor import Supervisor  # noqa: E402
from axos.store import (TransitionGate, TransitionRejected,  # noqa: E402
                        DesiredStateConflict, StoreError,
                        open_store, migrate)
from axos.tests.r5_helpers import r5_complete  # noqa: E402

REC_SRC = os.path.abspath(os.path.join(os.path.dirname(HERE), "exec",
                                       "reconciler.py"))
assert os.path.isfile(REC_SRC), REC_SRC


class ReconcilerBase(unittest.TestCase):
    H = 0.4

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r11-")
        self.db = os.path.join(self.tmp, "t.db")
        self._stores: list = []
        self._recs: list = []
        self._sups: list = []
        self._scheds: list = []
        self.store = open_store(self.db)
        migrate(self.store)
        self._stores.append(self.store)
        self.gate = TransitionGate(self.store)
        self.gate.create_task("t", {"objective": "r11"},
                              {"usd": 1}, "test")

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
        for r in self._recs:
            try:
                r.close()
            except Exception:
                pass
        for st in self._stores:
            try:
                st.close()
            except Exception:
                pass
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- helpers
    def _new_rec(self, batch_size=10, poll_interval_s=0.2,
                 actor="reconciler") -> Reconciler:
        rec = Reconciler(
            self.db,
            ReconcilerConfig(poll_interval_s=poll_interval_s,
                             batch_size=batch_size, actor=actor))
        self._recs.append(rec)
        return rec

    def _set(self, dwid, task_id="t", stage_id="s", max_attempts=3,
             policy=None, actor="test") -> dict:
        return self.gate.set_desired_item(
            dwid,
            {"task_id": task_id, "stage_id": stage_id,
             "max_attempts": max_attempts, "policy": policy or {}},
            actor)

    def _new_sup(self, H=None, actor=None) -> Supervisor:
        s = Supervisor(self.db,
                       actor=actor or f"test-r11-{len(self._sups)}",
                       heartbeat_interval_s=H or self.H)
        self._sups.append(s)
        return s

    def _new_sched(self, sup, scheduler_id=None, **kw) -> Scheduler:
        cfg = SchedulerConfig(
            poll_interval_s=kw.get("poll_interval_s", 0.2),
            lease_ttl_s=kw.get("lease_ttl_s", 60.0),
            max_concurrent_jobs=kw.get("max_concurrent_jobs", 4),
            batch_size=kw.get("batch_size", 10))
        sc = Scheduler(self.db, sup, cfg,
                       scheduler_id=scheduler_id or f"r11-{len(self._scheds)}",
                       boot_ready=(lambda: True))
        self._scheds.append(sc)
        return sc

    def _wait(self, pred, timeout=20.0, interval=0.02):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("timed out waiting for condition")

    def _wait_status(self, jid, states, timeout=30.0):
        if isinstance(states, str):
            states = (states,)
        self._wait(lambda: self.gate.get_job(jid)["status"] in states,
                   timeout)
        return self.gate.get_job(jid)

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

    def _job_events(self, jid, event_type=None):
        q = ("SELECT seq, event_type, actor, payload FROM ledger "
             "WHERE json_extract(payload,'$.job_id')=?")
        args: list = [jid]
        if event_type is not None:
            q += " AND event_type=?"
            args.append(event_type)
        q += " ORDER BY seq"
        return [dict(r)
                for r in self.gate.store.conn.execute(q, args).fetchall()]

    def _count(self, table):
        return self.gate.store.conn.execute(
            f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def _ledger_seq_max(self):
        row = self.gate.store.conn.execute(
            "SELECT MAX(seq) FROM ledger").fetchone()
        return row[0] or 0

    def _raw(self):
        """A raw sqlite3 handle for deliberate tamper/corruption ops."""
        return sqlite3.connect(self.db)

    def _pause_task(self, tid="t", actor="test"):
        self.gate.transition_task(tid, "AUTHORIZED", actor)
        return self.gate.transition_task(tid, "PAUSED_FOR_HUMAN", actor,
                                         reason="r11-test")

    def _run_until_stable(self, rec, max_passes=5):
        results = []
        for _ in range(max_passes):
            r = rec.reconcile()
            results.append(r["result"])
            if r["result"] == "CONVERGED":
                break
        return results

    @staticmethod
    def _rec_calls_attrs_defs():
        """(attribute-call names, attribute names, defined names) in
        exec/reconciler.py — AST based so docstring prose never trips it."""
        with open(REC_SRC) as f:
            tree = ast.parse(f.read())
        calls, attrs, defs = set(), set(), set()
        for n in ast.walk(tree):
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                defs.add(n.name)
            if isinstance(n, ast.Attribute):
                attrs.add(n.attr)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
                calls.add(n.func.attr)
        return calls, attrs, defs


# ------------------------------------------------------------------ R11-01
class TestR1101(ReconcilerBase):
    def test_R11_01_desired_state_snapshot_durable(self):
        """R11-01: every set/retire bumps the head version by exactly 1
        and changes the snapshot hash; the state is durable — a fresh
        gate on the same file sees the identical head and items."""
        h0 = self.gate.get_desired_head()
        self.assertEqual(h0["version"], 0)
        h1 = self._set("w-1")
        self.assertEqual(h1["version"], 1)
        self.assertNotEqual(h1["snapshot_hash"], h0["snapshot_hash"])
        h2 = self._set("w-2", stage_id="s2")
        self.assertEqual(h2["version"], 2)
        h3 = self.gate.retire_desired_item("w-1", "test")
        self.assertEqual(h3["version"], 3)
        self.assertNotEqual(h3["snapshot_hash"], h2["snapshot_hash"])
        # Durable: a brand-new store+gate on the same file agrees.
        store2 = open_store(self.db)
        migrate(store2)
        self._stores.append(store2)
        g2 = TransitionGate(store2)
        self.assertEqual(g2.get_desired_head(), h3)
        items = g2.list_desired_items(include_retired=True)
        self.assertEqual([r["desired_work_id"] for r in items],
                         ["w-1", "w-2"])
        self.assertEqual(items[0]["retired"], 1)
        self.assertEqual(items[1]["retired"], 0)
        spec = json.loads(items[1]["spec"])
        self.assertEqual(spec, {"task_id": "t", "stage_id": "s2",
                               "max_attempts": 3, "policy": {}})


# ------------------------------------------------------------------ R11-02
class TestR1102(ReconcilerBase):
    def test_R11_02_snapshot_hash_deterministic_and_tamper_evident(self):
        """R11-02: the same logical spec (written with defaults omitted
        vs explicit) yields the same head snapshot hash in two
        independent DBs; tampering with a stored spec breaks the
        snapshot -> reconcile fails closed (FAILED, run=None, zero
        mutation)."""
        db2 = os.path.join(self.tmp, "t2.db")
        store2 = open_store(db2)
        migrate(store2)
        self._stores.append(store2)
        g2 = TransitionGate(store2)
        g2.create_task("t", {"objective": "r11"}, {"usd": 1}, "test")
        # Same logical spec, different surface spelling.
        self.gate.set_desired_item("w-1", {"task_id": "t"}, "test")
        g2.set_desired_item("w-1", {"task_id": "t", "stage_id": None,
                                   "max_attempts": 3, "policy": {}},
                            "test")
        self.assertEqual(self.gate.get_desired_head()["snapshot_hash"],
                         g2.get_desired_head()["snapshot_hash"])
        # Tamper: rewrite the stored spec bytes out of band.
        raw = self._raw()
        raw.execute(
            "UPDATE desired_state SET spec=? WHERE desired_work_id='w-1'",
            (json.dumps({"task_id": "t", "stage_id": "evil",
                         "max_attempts": 3, "policy": {}}),))
        raw.commit()
        raw.close()
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "FAILED")
        self.assertIsNone(r["run"])
        self.assertEqual(r["items_created"], 0)
        self.assertEqual(r["discrepancies"][0]["type"],
                         "desired-state snapshot mismatch")
        self.assertEqual(self._count("jobs"), 0)
        self.assertEqual(self._count("desired_job_map"), 0)
        self.assertEqual(self._count("reconciliation_runs"), 0)


# ------------------------------------------------------------------ R11-03
class TestR1103(ReconcilerBase):
    def test_R11_03_canonical_job_id_stable(self):
        """R11-03: canonical_job_id matches the
        axos-desired-job:v1 construction, and the same desired state
        yields the same job identity across runs and across reconciler
        restarts (no second job, no second map row)."""
        expect = ("dj-" + hashlib.sha256(
            b"axos-desired-job:v1:w-1").hexdigest()[:32])
        self.assertEqual(canonical_job_id("w-1"), expect)
        self._set("w-1")
        rec = self._new_rec()
        r1 = rec.reconcile()
        self.assertEqual(r1["result"], "CHANGED")
        self.assertEqual(self.gate.get_desired_job_map("w-1")["job_id"],
                         expect)
        self.assertEqual(self.gate.get_job(expect)["status"], "PENDING")
        # Restart: a brand-new Reconciler over the same DB file.
        rec.close()
        rec2 = self._new_rec()
        self.assertIsNot(rec2.gate, rec.gate)
        r2 = rec2.reconcile()
        self.assertEqual(r2["result"], "CONVERGED")
        self.assertEqual(r2["items_created"], 0)
        self.assertEqual(self._count("jobs"), 1)
        self.assertEqual(self._count("desired_job_map"), 1)
        self.assertEqual(self.gate.get_desired_job_map("w-1")["job_id"],
                         expect)


# ------------------------------------------------------------------ R11-04
class TestR1104(ReconcilerBase):
    def test_R11_04_missing_item_detected(self):
        """R11-04: a declared item with no job diffs as DESIRED_MISSING;
        the pure diff_item verdict table covers every status (including
        the defensive VERIFYING branch, which the gate graph cannot
        produce)."""
        self._set("w-1")
        rec = self._new_rec()
        row = self.gate.get_desired_item("w-1")
        self.assertEqual(rec._diff_active(rec.gate, row), "DESIRED_MISSING")
        base = {"desired_work_id": "w", "retired": 0,
                "spec": {"task_id": "t"}}
        # retired is a tombstone no matter what the rows say
        self.assertEqual(
            diff_item({**base, "retired": 1},
                      {"job_id": "j", "status": "PENDING"},
                      {"job_id": "j"}, False), "OBSOLETE")
        # unreadable task row
        self.assertEqual(diff_item(base, None, None, True), "CONTRADICTORY")
        # map row without its job row
        self.assertEqual(diff_item(base, None, {"job_id": "j",
                                               "spec_hash": "x"}, False),
                         "CONTRADICTORY")
        # job under our canonical identity without a map row: never adopt
        self.assertEqual(diff_item(base, {"job_id": "j", "status": "PENDING"},
                                  None, False), "CONTRADICTORY")
        # spec drift under an existing mapping
        real_hash = hashlib.sha256(json.dumps(
            {"max_attempts": 3, "policy": {},
             "stage_id": None, "task_id": "t"},
            sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(
            diff_item(base, {"job_id": "j", "status": "PENDING"},
                      {"job_id": "j", "spec_hash": "deadbeef"}, False),
            "CONTRADICTORY")
        self.assertEqual(
            diff_item(base, {"job_id": "j", "status": "PENDING"},
                      {"job_id": "j", "spec_hash": real_hash}, False),
            "DESIRED_ALREADY_PRESENT")
        for status, want in (
                ("PENDING", "DESIRED_ALREADY_PRESENT"),
                ("CLAIMED", "DESIRED_ACTIVE"),
                ("RUNNING", "DESIRED_ACTIVE"),
                ("COMMITTING", "DESIRED_ACTIVE"),
                ("VERIFYING", "DESIRED_ACTIVE"),
                ("COMPLETE", "DESIRED_COMPLETE"),
                ("FAILED", "DESIRED_FAILED"),
                ("BLOCKED", "DESIRED_BLOCKED"),
                ("UNCERTAIN", "DESIRED_UNCERTAIN"),
                ("QUARANTINED", "CONTRADICTORY")):
            with self.subTest(status=status):
                self.assertEqual(
                    diff_item(base, {"job_id": "j", "status": status},
                              {"job_id": "j", "spec_hash": real_hash},
                              False), want)
        # unparseable desired spec is contradictory, never silently skipped
        bad = {**base, "spec": "{not json"}
        self.assertEqual(
            diff_item(bad, {"job_id": "j", "status": "PENDING"},
                      {"job_id": "j", "spec_hash": real_hash}, False),
            "CONTRADICTORY")
        self.assertEqual(diff_item("not-a-dict", None, None, False),
                         "CONTRADICTORY")


# ------------------------------------------------------------------ R11-05
class TestR1105(ReconcilerBase):
    def test_R11_05_created_exactly_once_with_correct_fields(self):
        """R11-05: DESIRED_MISSING is created exactly once as PENDING
        with the declared task_id/stage_id/max_attempts/policy (plus the
        gate-owned _desired envelope); a second pass creates nothing and
        the single job.created event carries actor "reconciler"."""
        self._set("w-1", stage_id="s9", max_attempts=5,
                  policy={"k": "v"})
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CHANGED")
        self.assertEqual(r["items_created"], 1)
        jid = canonical_job_id("w-1")
        job = self.gate.get_job(jid)
        self.assertEqual(job["status"], "PENDING")
        self.assertEqual(job["task_id"], "t")
        self.assertEqual(job["stage_id"], "s9")
        self.assertEqual(job["max_attempts"], 5)
        self.assertIsNone(job["owner_worker_id"])
        pol = json.loads(job["policy"])
        self.assertEqual(pol["k"], "v")
        self.assertEqual(pol["_desired"]["desired_work_id"], "w-1")
        self.assertEqual(pol["_desired"]["desired_version"], 1)
        self.assertIn("spec_hash", pol["_desired"])
        m = self.gate.get_desired_job_map("w-1")
        self.assertEqual(m["job_id"], jid)
        self.assertEqual(m["spec_hash"], pol["_desired"]["spec_hash"])
        evts = self._job_events(jid, "job.created")
        self.assertEqual(len(evts), 1)
        self.assertEqual(evts[0]["actor"], "reconciler")
        payload = json.loads(evts[0]["payload"])
        self.assertEqual(payload["desired_work_id"], "w-1")
        # Exactly once: a second pass is a no-op for this item.
        r2 = rec.reconcile()
        self.assertEqual(r2["result"], "CONVERGED")
        self.assertEqual(r2["items_created"], 0)
        self.assertEqual(self._count("jobs"), 1)
        self.assertEqual(len(self._job_events(jid, "job.created")), 1)


# ------------------------------------------------------------------ R11-06
class TestR1106(ReconcilerBase):
    def test_R11_06_concurrent_reconcilers_create_exactly_once(self):
        """R11-06: 4 threads x 4 reconcilers racing one missing item ->
        exactly one job row, one map row, one job.created event; exactly
        one pass reports CHANGED, the rest CONVERGED."""
        self._set("w-1")
        jid = canonical_job_id("w-1")

        def fn():
            rec = Reconciler(
                self.db, ReconcilerConfig(poll_interval_s=0.2,
                                         batch_size=10))
            try:
                return rec.reconcile()
            finally:
                rec.close()

        results = self._run_barrier([fn] * 4)
        for i, res in results.items():
            self.assertNotIsInstance(res, Exception, f"thread {i}: {res!r}")
        changed = [i for i, res in results.items()
                   if res["result"] == "CHANGED"]
        converged = [i for i, res in results.items()
                     if res["result"] == "CONVERGED"]
        self.assertEqual(len(changed), 1,
                         f"exactly one CHANGED: {results}")
        self.assertEqual(len(converged), 3)
        self.assertEqual(self._count("jobs"), 1)
        self.assertEqual(self._count("desired_job_map"), 1)
        self.assertEqual(self.gate.get_job(jid)["status"], "PENDING")
        self.assertEqual(len(self._job_events(jid, "job.created")), 1)


# ------------------------------------------------- R11-07..11 state no-ops
class TestR1107_11(ReconcilerBase):
    """R11-07..11: jobs already in PENDING/CLAIMED/RUNNING/COMMITTING/
    VERIFYING are recognized and left strictly alone: the job row is
    byte-identical afterwards and no new ledger events name the job."""

    def _reconciled_job(self, dwid="w-1"):
        self._set(dwid)
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CHANGED")
        return rec, canonical_job_id(dwid)

    def _claim(self, jid, wid="w-11"):
        self.assertTrue(self.gate.claim_job(jid, wid, 60.0, "test"))
        return int(self.gate.get_job(jid)["fencing_token"])

    def _assert_noop(self, rec, jid, expect_diff):
        before_row = dict(self.gate.get_job(jid))
        before_evts = self._job_events(jid)
        before_seq = self._ledger_seq_max()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED", r)
        self.assertEqual(r["items_created"], 0)
        d = [x for x in r["discrepancies"]
             if x.get("desired_work_id")]
        self.assertEqual(len(d), 1)
        self.assertEqual(d[0]["diff"], expect_diff, d)
        self.assertEqual(d[0]["action"], "none")
        self.assertEqual(dict(self.gate.get_job(jid)), before_row)
        self.assertEqual(self._job_events(jid), before_evts)
        self.assertEqual(self._ledger_seq_max(), before_seq)

    def test_R11_07_pending_recognized_noop(self):
        """R11-07: PENDING -> DESIRED_ALREADY_PRESENT, no-op."""
        rec, jid = self._reconciled_job()
        self._assert_noop(rec, jid, "DESIRED_ALREADY_PRESENT")

    def test_R11_08_claimed_recognized_noop(self):
        """R11-08: CLAIMED -> DESIRED_ACTIVE, no-op."""
        rec, jid = self._reconciled_job()
        self._claim(jid)
        self._assert_noop(rec, jid, "DESIRED_ACTIVE")

    def test_R11_09_running_recognized_noop(self):
        """R11-09: RUNNING -> DESIRED_ACTIVE, no-op."""
        rec, jid = self._reconciled_job()
        self._claim(jid)
        self.gate.transition_job(jid, "RUNNING", "worker:w-11")
        self._assert_noop(rec, jid, "DESIRED_ACTIVE")

    def test_R11_10_committing_recognized_noop(self):
        """R11-10: COMMITTING -> DESIRED_ACTIVE, no-op."""
        rec, jid = self._reconciled_job()
        tok = self._claim(jid)
        self.gate.transition_job(jid, "RUNNING", "worker:w-11")
        art = self.gate.stage_artifact(
            job_id=jid, worker_id="w-11", fencing_token=tok,
            task_id="t", kind="test", data=b"r11-10-bytes", actor="test")
        self.gate.begin_commit(jid, "w-11", tok,
                               artifact_id=art["artifact_id"], actor="test")
        self.assertEqual(self.gate.get_job(jid)["status"], "COMMITTING")
        self._assert_noop(rec, jid, "DESIRED_ACTIVE")

    def test_R11_11_verifying_recognized_noop(self):
        """R11-11: VERIFYING -> DESIRED_ACTIVE, no-op. The gate's job
        graph has no VERIFYING state, so the row is injected out of band
        (deliberate harness tamper, documented here): the reconciler
        must still treat it as active and never touch it."""
        rec, jid = self._reconciled_job()
        raw = self._raw()
        raw.execute("UPDATE jobs SET status='VERIFYING' WHERE job_id=?",
                    (jid,))
        raw.commit()
        raw.close()
        self.assertEqual(self.gate.get_job(jid)["status"], "VERIFYING")
        self._assert_noop(rec, jid, "DESIRED_ACTIVE")


# ------------------------------------------------------------------ R11-12
class TestR1112(ReconcilerBase):
    def test_R11_12_complete_satisfies_desired(self):
        """R11-12: COMPLETE -> DESIRED_COMPLETE: the pass creates
        nothing, the job row is byte-identical, no new ledger events."""
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        jid = canonical_job_id("w-1")
        self.assertTrue(self.gate.claim_job(jid, "w-12", 60.0, "test"))
        tok = int(self.gate.get_job(jid)["fencing_token"])
        self.gate.transition_job(jid, "RUNNING", "worker:w-12")
        r5_complete(self.gate, job_id=jid, worker_id="w-12",
                    fencing_token=tok, task_id="t", outcome="SUCCESS",
                    evidence={"ok": True}, actor="worker:w-12")
        self.assertEqual(self.gate.get_job(jid)["status"], "COMPLETE")
        before_row = dict(self.gate.get_job(jid))
        before_seq = self._ledger_seq_max()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        self.assertEqual(r["items_created"], 0)
        d = r["discrepancies"][0]
        self.assertEqual(d["diff"], "DESIRED_COMPLETE")
        self.assertEqual(d["action"], "none")
        self.assertEqual(dict(self.gate.get_job(jid)), before_row)
        self.assertEqual(self._ledger_seq_max(), before_seq)
        self.assertEqual(self._count("jobs"), 1)


# ------------------------------------------------------------------ R11-13
class TestR1113(ReconcilerBase):
    def test_R11_13_uncertain_not_recreated(self):
        """R11-13: UNCERTAIN -> DESIRED_UNCERTAIN: recorded only, never
        recreated, never transitioned."""
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        jid = canonical_job_id("w-1")
        self.assertTrue(self.gate.claim_job(jid, "w-13", 60.0, "test"))
        self.gate.transition_job(jid, "RUNNING", "worker:w-13")
        self.gate.transition_job(jid, "UNCERTAIN", "test")
        before_row = dict(self.gate.get_job(jid))
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        d = r["discrepancies"][0]
        self.assertEqual(d["diff"], "DESIRED_UNCERTAIN")
        self.assertEqual(d["action"], "none")
        self.assertEqual(dict(self.gate.get_job(jid)), before_row)
        self.assertEqual(self._count("jobs"), 1)
        self.assertEqual(self._count("desired_job_map"), 1)


# ------------------------------------------------------------------ R11-14
class TestR1114(ReconcilerBase):
    def test_R11_14_failed_stays_no_recovery_bypass(self):
        """R11-14: FAILED -> DESIRED_FAILED: the pass records it and
        does nothing else — no recovery attempt, incident, or policy
        row is created (recovery is R8/R9's domain, never the
        reconciler's)."""
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        jid = canonical_job_id("w-1")
        self.assertTrue(self.gate.claim_job(jid, "w-14", 60.0, "test"))
        tok = int(self.gate.get_job(jid)["fencing_token"])
        self.gate.transition_job(jid, "RUNNING", "worker:w-14")
        self.gate.fail_job_execution(jid, "w-14", tok, actor="worker:w-14",
                                     reason="r11-14")
        self.assertEqual(self.gate.get_job(jid)["status"], "FAILED")
        n_inc = self._count("incidents")
        n_att = self._count("recovery_attempts")
        n_pol = self._count("recovery_policy")
        before_row = dict(self.gate.get_job(jid))
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        d = r["discrepancies"][0]
        self.assertEqual(d["diff"], "DESIRED_FAILED")
        self.assertEqual(d["action"], "none")
        self.assertEqual(dict(self.gate.get_job(jid)), before_row)
        self.assertEqual(self._count("incidents"), n_inc)
        self.assertEqual(self._count("recovery_attempts"), n_att)
        self.assertEqual(self._count("recovery_policy"), n_pol)
        self.assertEqual(
            self.gate.open_recovery_incidents(scope="recovery"), [])


# ------------------------------------------------------------------ R11-15
class TestR1115(ReconcilerBase):
    def test_R11_15_blocked_stays_blocked(self):
        """R11-15: BLOCKED -> DESIRED_BLOCKED: recorded only, the row is
        byte-identical afterwards."""
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        jid = canonical_job_id("w-1")
        self.assertTrue(self.gate.claim_job(jid, "w-15", 60.0, "test"))
        tok = int(self.gate.get_job(jid)["fencing_token"])
        self.gate.transition_job(jid, "RUNNING", "worker:w-15")
        self.gate.fail_job_execution(jid, "w-15", tok, actor="worker:w-15",
                                     reason="r11-15")
        self.gate.transition_job(jid, "BLOCKED", "test")
        before_row = dict(self.gate.get_job(jid))
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        d = r["discrepancies"][0]
        self.assertEqual(d["diff"], "DESIRED_BLOCKED")
        self.assertEqual(d["action"], "none")
        self.assertEqual(dict(self.gate.get_job(jid)), before_row)


# ------------------------------------------------------------------ R11-16
class TestR1116(ReconcilerBase):
    def test_R11_16_paused_task_blocks_creation_sticky(self):
        """R11-16: a task in PAUSED_FOR_HUMAN blocks creation (result
        BLOCKED, zero jobs, zero map rows); the block is sticky across a
        reconciler restart."""
        self._pause_task("t")
        self._set("w-1")
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "BLOCKED")
        self.assertEqual(r["items_created"], 0)
        d = [x for x in r["discrepancies"]
             if x.get("desired_work_id") == "w-1"][0]
        self.assertEqual(d["diff"], "DESIRED_MISSING")
        self.assertEqual(d["action"], "blocked")
        self.assertEqual(self._count("jobs"), 0)
        self.assertEqual(self._count("desired_job_map"), 0)
        # Sticky across restart: a brand-new Reconciler still blocks.
        rec.close()
        rec2 = self._new_rec()
        r2 = rec2.reconcile()
        self.assertEqual(r2["result"], "BLOCKED")
        self.assertEqual(self._count("jobs"), 0)
        self.assertEqual(self.gate.get_task("t")["status"],
                         "PAUSED_FOR_HUMAN")


# ------------------------------------------------------- R11-17/18 retired
class TestR1117_18(ReconcilerBase):
    def test_R11_17_retired_item_obsolete(self):
        """R11-17: a retired desired item diffs as OBSOLETE and is only
        recorded."""
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        self.gate.retire_desired_item("w-1", "test")
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        d = [x for x in r["discrepancies"]
             if x.get("desired_work_id") == "w-1"][0]
        self.assertEqual(d["diff"], "OBSOLETE")
        self.assertEqual(d["action"], "none")

    def test_R11_18_retired_item_job_and_history_untouched(self):
        """R11-18: retiring an item never deletes or transitions its job:
        the job row is byte-identical, no delete/transition ledger events
        name it, the map row is kept."""
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        jid = canonical_job_id("w-1")
        before_row = dict(self.gate.get_job(jid))
        before_evts = self._job_events(jid)
        self.gate.retire_desired_item("w-1", "test")
        rec.reconcile()
        self.assertEqual(dict(self.gate.get_job(jid)), before_row)
        self.assertEqual(self._job_events(jid), before_evts)
        kinds = {e["event_type"] for e in self._job_events(jid)}
        self.assertNotIn("job.transition", kinds)
        self.assertTrue(all("delet" not in k for k in kinds), kinds)
        self.assertEqual(self._count("jobs"), 1)
        self.assertIsNotNone(self.gate.get_desired_job_map("w-1"))


# ------------------------------------------------------- R11-19..24 "never"
class TestR1119_24(ReconcilerBase):
    """R11-19..24: the reconciler owns no execution authority. Static
    (AST over exec/reconciler.py — docstring prose cannot trip it) plus
    behavioral evidence for each forbidden capability."""

    def _proc_spawned_count(self):
        return self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type="
            "'worker.proc_spawned'").fetchone()[0]

    def _reconciler_actors_on(self, event_type):
        return [r["actor"] for r in self.gate.store.conn.execute(
            "SELECT actor FROM ledger WHERE event_type=?", (event_type,)
        ).fetchall() if r["actor"] == "reconciler"
            or r["actor"].startswith("reconciler:")]

    def test_R11_19_never_spawns(self):
        """R11-19: reconcile never spawns a worker process: no new
        worker.proc_spawned events, and none ever carries a reconciler
        actor — even with a live supervisor in the same DB."""
        sup = self._new_sup(actor="test-r11-sup")
        # A real live worker to prove reconcile doesn't touch it.
        self.gate.create_job("j-19", "t", None, "test")
        sup.start_worker("w-19", "j-19",
                         {"kind": "heartbeat_loop", "duration_s": 30.0},
                         ttl_s=60.0, hb_interval_s=0.2)
        self._wait(lambda: len(self.gate.heartbeats_for("w-19")) >= 1,
                   timeout=15.0)
        self._set("w-1")
        rec = self._new_rec()
        n0 = self._proc_spawned_count()
        procs0 = set(sup._procs)
        r = rec.reconcile()
        self.assertEqual(r["result"], "CHANGED")
        self.assertEqual(self._proc_spawned_count(), n0)
        self.assertEqual(set(sup._procs), procs0)
        self.assertEqual(self._reconciler_actors_on("worker.proc_spawned"),
                         [])

    def test_R11_20_never_terminates(self):
        """R11-20: the reconciler has no kill path (AST) and a live
        worker process stays alive across a reconcile (behavioral)."""
        calls, attrs, _ = self._rec_calls_attrs_defs()
        for verb in ("kill", "terminate", "send_signal", "_kill_process_group",
                     "kill_worker", "stop_worker", "restart_worker"):
            self.assertNotIn(verb, calls, f"reconciler calls {verb}")
            self.assertNotIn(verb, attrs, f"reconciler references {verb}")
        self.assertNotIn("SIGKILL", attrs)
        self.assertNotIn("SIGTERM", attrs)
        sup = self._new_sup(actor="test-r11-sup")
        self.gate.create_job("j-20", "t", None, "test")
        sup.start_worker("w-20", "j-20",
                         {"kind": "heartbeat_loop", "duration_s": 60.0},
                         ttl_s=60.0, hb_interval_s=0.2)
        self._wait(lambda: len(self.gate.heartbeats_for("w-20")) >= 1,
                   timeout=15.0)
        proc = sup._procs["w-20"].popen
        self.assertIsNone(proc.poll())
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        self.assertIsNone(proc.poll(), "worker died across reconcile")

    def test_R11_21_never_reclaims(self):
        """R11-21: the reconciler never reclaims leases (AST) — an
        expired-lease claim survives a reconcile untouched, with no
        job.lease_reclaimed events (behavioral)."""
        calls, attrs, _ = self._rec_calls_attrs_defs()
        self.assertNotIn("reclaim_lease", calls)
        self.assertNotIn("reclaim_lease", attrs)
        self.assertNotIn("release_lease", calls)
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        jid = canonical_job_id("w-1")
        self.assertTrue(self.gate.claim_job(jid, "w-21", 0.05, "test"))
        tok0 = self.gate.get_job(jid)["fencing_token"]
        time.sleep(0.3)  # real lease expiry by store time
        before = dict(self.gate.get_job(jid))
        n0 = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type="
            "'job.lease_reclaimed'").fetchone()[0]
        rec.reconcile()
        after = self.gate.get_job(jid)
        self.assertEqual(after["status"], "CLAIMED")
        self.assertEqual(after["owner_worker_id"], "w-21")
        self.assertEqual(after["fencing_token"], tok0)
        self.assertEqual(dict(after), before)
        n1 = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type="
            "'job.lease_reclaimed'").fetchone()[0]
        self.assertEqual(n1, n0)

    def test_R11_22_never_fences(self):
        """R11-22: the reconciler never fences (AST) — no
        worker.fence_enforced events appear across a reconcile
        (behavioral)."""
        calls, attrs, _ = self._rec_calls_attrs_defs()
        for verb in ("fence_sweep", "_enforce_fence_tracked",
                     "_enforce_fence_adopted", "fence_enforced"):
            self.assertNotIn(verb, calls, f"reconciler calls {verb}")
            self.assertNotIn(verb, attrs)
        sup = self._new_sup(actor="test-r11-sup")
        self.gate.create_job("j-22", "t", None, "test")
        sup.start_worker("w-22", "j-22",
                         {"kind": "heartbeat_loop", "duration_s": 60.0},
                         ttl_s=60.0, hb_interval_s=0.2)
        self._wait(lambda: len(self.gate.heartbeats_for("w-22")) >= 1,
                   timeout=15.0)
        self._set("w-1")
        rec = self._new_rec()
        n0 = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type="
            "'worker.fence_enforced'").fetchone()[0]
        rec.reconcile()
        n1 = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type="
            "'worker.fence_enforced'").fetchone()[0]
        self.assertEqual(n1, n0)

    def test_R11_23_never_touches_recovery_policy_tables(self):
        """R11-23: recovery tables are byte-identical across a reconcile
        (behavioral); the reconciler never names them (AST)."""
        calls, attrs, _ = self._rec_calls_attrs_defs()
        for name in ("recovery_policy", "recovery_attempts", "incidents",
                     "create_recovery_attempt", "select_rung"):
            self.assertNotIn(name, calls, f"reconciler calls {name}")
            self.assertNotIn(name, attrs, f"reconciler references {name}")
        self._set("w-1")
        rec = self._new_rec()
        n_inc = self._count("incidents")
        n_att = self._count("recovery_attempts")
        n_pol = self._count("recovery_policy")
        rec.reconcile()
        self.assertEqual(self._count("incidents"), n_inc)
        self.assertEqual(self._count("recovery_attempts"), n_att)
        self.assertEqual(self._count("recovery_policy"), n_pol)

    def test_R11_24_never_bypasses_transition_gate(self):
        """R11-24: every mutation the reconciler causes flows through
        TransitionGate methods — AST shows no write_txn/raw-SQL/job
        INSERTs, and behaviorally every desired-created job has exactly
        one gate-journaled job.created event with actor "reconciler"."""
        calls, attrs, _ = self._rec_calls_attrs_defs()
        self.assertNotIn("write_txn", calls)
        self.assertNotIn("execute", calls)
        self.assertNotIn("sqlite3", attrs)
        with open(REC_SRC) as f:
            src = f.read()
        tree = ast.parse(src)
        code_strings = [n.value for n in ast.walk(tree)
                        if isinstance(n, ast.Constant)
                        and isinstance(n.value, str)]
        for s in code_strings:
            self.assertNotIn("INSERT INTO jobs", s)
            self.assertNotIn("DELETE FROM", s)
            self.assertNotIn("UPDATE jobs", s)
        for dwid in ("w-1", "w-2", "w-3"):
            self._set(dwid)
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["items_created"], 3)
        for dwid in ("w-1", "w-2", "w-3"):
            jid = canonical_job_id(dwid)
            evts = self._job_events(jid, "job.created")
            self.assertEqual(len(evts), 1, dwid)
            self.assertEqual(evts[0]["actor"], "reconciler")


# ------------------------------------------------- R11-25/26/27 handoff R10
class TestR1125_27(ReconcilerBase):
    """R11-25/26/27: the reconciler creates PENDING and stops — the R10
    scheduler owns claim+dispatch, and the worker owns completion."""

    def test_R11_25_first_reconcile_creates_pending_only(self):
        """R11-25: the first reconcile creates the job as PENDING with no
        owner; no scheduler actor appears anywhere in the ledger and no
        process is spawned."""
        self._set("w-1")
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CHANGED")
        jid = canonical_job_id("w-1")
        job = self.gate.get_job(jid)
        self.assertEqual(job["status"], "PENDING")
        self.assertIsNone(job["owner_worker_id"])
        self.assertEqual(job["fencing_token"], 0)  # pristine column default
        self.assertIsNone(job["lease_expires_at"])
        sched_actors = [row["actor"] for row in
                        self.gate.store.conn.execute(
                            "SELECT DISTINCT actor FROM ledger").fetchall()
                        if row["actor"].startswith("scheduler:")]
        self.assertEqual(sched_actors, [])
        self.assertEqual(self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type="
            "'worker.proc_spawned'").fetchone()[0], 0)

    def test_R11_26_scheduler_claims_and_dispatches(self):
        """R11-26: R10's evaluate_once() claims the reconciler-created
        PENDING job and dispatches it — the claim and the dispatch
        authority are the scheduler's (ledger actors), not the
        reconciler's."""
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        jid = canonical_job_id("w-1")
        sup = self._new_sup(actor="test-r11-sup")
        sched = self._new_sched(sup, scheduler_id="r11s26")
        rep = sched.evaluate_once()
        self.assertEqual(rep["detail"], "ok")
        self.assertEqual(rep["admitted"], 1)
        self.assertEqual(rep["errors"], [])
        job = self.gate.get_job(jid)
        wid = job["owner_worker_id"]
        self.assertTrue(wid.startswith("sched-r11s26-"), wid)
        # Claim authority: the scheduler.
        claimed = self._job_events(jid, "job.claimed")
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0]["actor"], "scheduler:r11s26")
        # Dispatch authority: the supervisor spawned exactly this worker
        # for this job; the reconciler spawned nothing.
        spawns = self.gate.store.conn.execute(
            "SELECT actor, payload FROM ledger WHERE event_type="
            "'worker.proc_spawned' AND json_extract(payload,'$.job_id')=?"
            " ORDER BY seq", (jid,)).fetchall()
        self.assertEqual(len(spawns), 1)
        self.assertEqual(spawns[0]["actor"], "test-r11-sup")
        self.assertEqual(json.loads(spawns[0]["payload"])["worker_id"], wid)
        self.assertIn(wid, sup._procs)
        # The reconciler's only ledger footprint on this job is creation.
        rec_actors = [(e["event_type"], e["actor"])
                      for e in self._job_events(jid)
                      if e["actor"] == "reconciler"
                      or e["actor"].startswith("reconciler:")]
        self.assertEqual([t for t, _ in rec_actors], ["job.created"])

    def test_R11_27_worker_completes_next_reconcile_satisfied(self):
        """R11-27: the dispatched worker completes through the R5
        contract -> COMPLETE; the next reconcile sees DESIRED_COMPLETE
        and creates nothing."""
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        jid = canonical_job_id("w-1")
        sup = self._new_sup(actor="test-r11-sup")
        sched = self._new_sched(sup, scheduler_id="r11s27")
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 1)
        self._wait_status(jid, "COMPLETE", timeout=30.0)
        committed = self._job_events(jid, "job.committed")
        self.assertTrue(committed, "no job.committed event")
        for e in committed:
            self.assertTrue(e["actor"].startswith("worker:"),
                            e["actor"])
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        self.assertEqual(r["items_created"], 0)
        d = r["discrepancies"][0]
        self.assertEqual(d["diff"], "DESIRED_COMPLETE")
        self.assertEqual(self._count("jobs"), 1)
        self.assertEqual(len(self._job_events(jid, "job.created")), 1)


# ------------------------------------------------- R11-28/29 convergence
class TestR1128_29(ReconcilerBase):
    def test_R11_28_rerun_unchanged_converged_noop(self):
        """R11-28: re-running over unchanged state -> CONVERGED with
        zero new job rows and zero new ledger events (run bookkeeping
        lives in reconciliation_runs, not the ledger)."""
        for dwid in ("w-1", "w-2"):
            self._set(dwid)
        rec = self._new_rec()
        r1 = rec.reconcile()
        self.assertEqual(r1["result"], "CHANGED")
        r2 = rec.reconcile()
        self.assertEqual(r2["result"], "CONVERGED")
        jobs0 = self._count("jobs")
        maps0 = self._count("desired_job_map")
        seq0 = self._ledger_seq_max()
        r3 = rec.reconcile()
        self.assertEqual(r3["result"], "CONVERGED")
        self.assertEqual(r3["items_examined"], 2)
        self.assertEqual(r3["items_created"], 0)
        self.assertEqual(self._count("jobs"), jobs0)
        self.assertEqual(self._count("desired_job_map"), maps0)
        self.assertEqual(self._ledger_seq_max(), seq0)

    def test_R11_29_repeated_runs_converge(self):
        """R11-29: run-until-stable converges in a bounded number of
        passes and then stays converged."""
        for dwid in ("w-1", "w-2", "w-3"):
            self._set(dwid)
        rec = self._new_rec()
        results = self._run_until_stable(rec)
        self.assertEqual(results, ["CHANGED", "CONVERGED"])
        self.assertEqual(self._count("jobs"), 3)
        # Still converged afterwards: the fixed point is stable.
        self.assertEqual(self._run_until_stable(rec), ["CONVERGED"])


# ------------------------------------------------- R11-30/31 stale versions
class TestR1130_31(ReconcilerBase):
    def test_R11_30_stale_pin_conflict_zero_mutation(self):
        """R11-30: reconcile(desired_version=<old>) after the head moved
        -> CONFLICT with run=None and zero mutation (no jobs, no map
        rows, no run rows; the head is untouched)."""
        self._set("w-1")  # head -> v1
        head_v1 = self.gate.get_desired_head()["version"]
        self.assertEqual(head_v1, 1)
        self._set("w-2")  # head -> v2
        self.assertEqual(self.gate.get_desired_head()["version"], 2)
        rec = self._new_rec()
        r = rec.reconcile(desired_version=1)
        self.assertEqual(r["result"], "CONFLICT")
        self.assertIsNone(r["run"])
        self.assertEqual(r["items_created"], 0)
        self.assertEqual(r["discrepancies"][0]["type"],
                         "stale desired-state pin")
        self.assertEqual(self._count("jobs"), 0)
        self.assertEqual(self._count("desired_job_map"), 0)
        self.assertEqual(self._count("reconciliation_runs"), 0)
        self.assertEqual(self.gate.get_desired_head()["version"], 2)

    def test_R11_31_version_races_deterministic_final_state(self):
        """R11-31: (a) a stale-pinned pass racing a current-version pass:
        the stale one refuses with zero mutation while the current one
        converges — deterministic final durable state. (b) the mid-pass
        stale rule, exercised deterministically via a counting wrapper on
        get_desired_head: the run opens, observes the move at the next
        batch boundary, stops creating, and finishes CONFLICT. (c) an
        older run's creates are idempotent same-identity under the newer
        version."""
        # --- (a) entry-refusal race: head already at v2 before threads
        self._set("w-a")  # v1
        self._set("w-b")  # v2
        rec_stale = Reconciler(
            self.db, ReconcilerConfig(poll_interval_s=0.2, batch_size=10))
        rec_cur = Reconciler(
            self.db, ReconcilerConfig(poll_interval_s=0.2, batch_size=10))
        self._recs.extend([rec_stale, rec_cur])
        results = self._run_barrier([
            lambda: rec_stale.reconcile(desired_version=1),
            lambda: rec_cur.reconcile(),
        ])
        stale_r, cur_r = results[0], results[1]
        self.assertNotIsInstance(stale_r, Exception)
        self.assertNotIsInstance(cur_r, Exception)
        self.assertEqual(stale_r["result"], "CONFLICT")
        self.assertIsNone(stale_r["run"])
        self.assertEqual(cur_r["result"], "CHANGED")
        self.assertEqual(cur_r["items_created"], 2)
        jids = {canonical_job_id("w-a"), canonical_job_id("w-b")}
        self.assertEqual(
            {r["job_id"] for r in self.gate.store.conn.execute(
                "SELECT job_id FROM jobs").fetchall()}, jids)
        for jid in jids:
            self.assertEqual(len(self._job_events(jid, "job.created")), 1)

        # --- (b) mid-pass stale rule, deterministic via counting wrapper
        rec2 = self._new_rec(batch_size=1)
        real_head = rec2.gate.get_desired_head
        calls = []

        def moving_head():
            calls.append(1)
            h = real_head()
            if len(calls) > 1:
                h = dict(h)
                h["version"] = h["version"] + 1000  # the head moved mid-pass
            return h

        rec2.gate.get_desired_head = moving_head
        r_mid = rec2.reconcile()
        self.assertEqual(r_mid["result"], "CONFLICT")
        self.assertEqual(r_mid["items_created"], 0)
        self.assertIsNotNone(r_mid["run"])
        self.assertEqual(r_mid["run"]["result"], "CONFLICT")
        self.assertTrue(
            any(x.get("type") == "stale desired-state pin"
                for x in r_mid["discrepancies"]),
            r_mid["discrepancies"])
        # Nothing was created by the conflicted pass.
        self.assertEqual(self._count("jobs"), 2)
        # A clean pass afterwards converges (both items already present).
        rec2.gate.get_desired_head = real_head
        r_clean = rec2.reconcile()
        self.assertEqual(r_clean["result"], "CONVERGED")

        # --- (c) older run's creates are idempotent under the new version
        self._set("w-c")  # head -> v3
        rec3 = self._new_rec()
        r3 = rec3.reconcile()
        self.assertEqual(r3["result"], "CHANGED")
        self.assertEqual(r3["items_created"], 1)  # only w-c was missing
        jid_c = canonical_job_id("w-c")
        self.assertEqual(len(self._job_events(jid_c, "job.created")), 1)
        self.assertEqual(self._count("jobs"), 3)


# ------------------------------------------------- R11-32/33 many-item races
class TestR1132_33(ReconcilerBase):
    def _race_many(self, n_threads, n_items, batch_size):
        dwids = [f"w-{i:02d}" for i in range(n_items)]
        for d in dwids:
            self._set(d)

        def fn():
            rec = Reconciler(
                self.db, ReconcilerConfig(poll_interval_s=0.2,
                                         batch_size=batch_size))
            try:
                return rec.reconcile()
            finally:
                rec.close()

        results = self._run_barrier([fn] * n_threads)
        for i, res in results.items():
            self.assertNotIsInstance(res, Exception, f"thread {i}: {res!r}")
            self.assertIn(res["result"], ("CHANGED", "CONVERGED"),
                          f"thread {i}: {res['result']}")
        # No duplicates: exactly one job row, one map row, one
        # job.created event per item — the deterministic final state.
        self.assertEqual(self._count("jobs"), n_items)
        self.assertEqual(self._count("desired_job_map"), n_items)
        for d in dwids:
            jid = canonical_job_id(d)
            self.assertEqual(self.gate.get_job(jid)["status"], "PENDING")
            self.assertEqual(len(self._job_events(jid, "job.created")), 1)
            self.assertEqual(self.gate.get_desired_job_map(d)["job_id"],
                             jid)
        # And the fixed point is stable.
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        self.assertEqual(r["items_created"], 0)

    def test_R11_32_two_reconcilers_eight_items(self):
        """R11-32: 2 reconcilers racing 8 missing items -> no
        duplicates, deterministic final durable state."""
        self._race_many(n_threads=2, n_items=8, batch_size=3)

    def test_R11_33_three_reconcilers_six_items(self):
        """R11-33: 3 reconcilers racing 6 missing items -> no
        duplicates, deterministic final durable state."""
        self._race_many(n_threads=3, n_items=6, batch_size=2)


# ------------------------------------------------- R11-34..39 crash points
class TestR1134_39(ReconcilerBase):
    """R11-34..39: crash points, simulated by deterministic fault
    injection (wrapped gate methods) plus one real-process restart
    (a new Reconciler over the same DB file)."""

    def _five_items(self):
        for d in ("w-1", "w-2", "w-3", "w-4", "w-5"):
            self._set(d)

    def test_R11_34_crash_before_snapshot(self):
        """R11-34: crash before the snapshot (list_desired_items raises)
        -> fail-closed FAILED with run=None: the desired state is
        unreadable, so the pass refuses before opening a run record.
        Zero mutations — no jobs, no map rows, no run rows — and the
        discrepancy identifies the injection point."""
        self._five_items()
        rec = self._new_rec()

        def boom(*a, **k):
            raise StoreError("injected: snapshot read failed")

        rec.gate.list_desired_items = boom
        r = rec.reconcile()
        self.assertEqual(r["result"], "FAILED")
        self.assertIsNone(r["run"])
        self.assertEqual(r["discrepancies"][0]["type"],
                         "unreadable desired state")
        self.assertIn("injected", r["discrepancies"][0]["error"])
        self.assertEqual(r["items_created"], 0)
        self.assertEqual(self._count("jobs"), 0)
        self.assertEqual(self._count("desired_job_map"), 0)
        self.assertEqual(self._count("reconciliation_runs"), 0)

    def test_R11_35_crash_after_snapshot_before_mutation(self):
        """R11-35: crash after the snapshot but before any mutation
        (begin_reconciliation_run raises) -> propagates; no jobs, no
        run rows."""
        self._five_items()
        rec = self._new_rec()

        def boom(*a, **k):
            raise StoreError("injected: run open failed")

        rec.gate.begin_reconciliation_run = boom
        with self.assertRaises(StoreError):
            rec.reconcile()
        self.assertEqual(self._count("jobs"), 0)
        self.assertEqual(self._count("desired_job_map"), 0)
        self.assertEqual(self._count("reconciliation_runs"), 0)

    def test_R11_36_crash_between_batches_then_resume(self):
        """R11-36: crash between batches (ensure raises on the 3rd call,
        i.e. inside batch 2) -> the run closes FAILED with per-batch
        checkpoints (last_item_id=w-3, examined=3, created=2); removing
        the fault and re-running completes without duplicating batches
        1..k — every job has exactly one job.created event."""
        self._five_items()
        rec = self._new_rec(batch_size=2)
        real_ensure = rec.gate.ensure_job_for_desired_state
        calls = []

        def flaky(**kw):
            calls.append(kw["desired_work_id"])
            if len(calls) == 3:
                raise StoreError("injected: create failed mid-pass")
            return real_ensure(**kw)

        rec.gate.ensure_job_for_desired_state = flaky
        r = rec.reconcile()
        self.assertEqual(r["result"], "FAILED")
        self.assertEqual(r["items_created"], 2)
        run = self.gate.get_reconciliation_run(r["run"]["reconciliation_id"])
        self.assertEqual(run["result"], "FAILED")
        self.assertEqual(run["last_item_id"], "w-3")
        self.assertEqual(run["items_examined"], 3)
        self.assertEqual(run["items_created"], 2)
        self.assertEqual(self._count("jobs"), 2)
        # Resume: the fault is gone; the pass completes idempotently.
        rec.gate.ensure_job_for_desired_state = real_ensure
        r2 = rec.reconcile()
        self.assertEqual(r2["result"], "CHANGED")
        self.assertEqual(r2["items_created"], 3)
        self.assertEqual(self._count("jobs"), 5)
        self.assertEqual(self._count("desired_job_map"), 5)
        for d in ("w-1", "w-2", "w-3", "w-4", "w-5"):
            jid = canonical_job_id(d)
            self.assertEqual(len(self._job_events(jid, "job.created")), 1,
                             d)

    def test_R11_37_crash_after_create_before_run_finish(self):
        """R11-37: crash after job creation but before the run record is
        finished (finish_reconciliation_run raises) -> propagates; the
        job+map rows are durable and the run is left open (observable);
        an idempotent re-run -> CONVERGED with a single created event."""
        self._set("w-1")
        rec = self._new_rec()

        def boom(*a, **k):
            raise StoreError("injected: run finish failed")

        rec.gate.finish_reconciliation_run = boom
        with self.assertRaises(StoreError):
            rec.reconcile()
        jid = canonical_job_id("w-1")
        self.assertEqual(self.gate.get_job(jid)["status"], "PENDING")
        self.assertIsNotNone(self.gate.get_desired_job_map("w-1"))
        open_runs = self.gate.open_reconciliation_runs()
        self.assertEqual(len(open_runs), 1)
        self.assertEqual(open_runs[0]["result"], "RUNNING")
        # Idempotent re-run: the existing job is recognized, nothing is
        # duplicated, and the new pass converges.
        rec2 = self._new_rec()
        r2 = rec2.reconcile()
        self.assertEqual(r2["result"], "CONVERGED")
        self.assertEqual(r2["items_created"], 0)
        self.assertEqual(self._count("jobs"), 1)
        self.assertEqual(len(self._job_events(jid, "job.created")), 1)
        # The crashed run is still observable as open (by inspection).
        self.assertEqual(len(self.gate.open_reconciliation_runs()), 1)

    def test_R11_38_crash_during_run_record_update(self):
        """R11-38: crash during a run-record update (checkpoint raises on
        the 2nd call) -> propagates; jobs created so far are durable and
        the open RUNNING run shows the last good checkpoint; resume ->
        no duplicates."""
        for d in ("w-1", "w-2", "w-3"):
            self._set(d)
        rec = self._new_rec(batch_size=1)
        real_ckpt = rec.gate.checkpoint_reconciliation_run
        calls = []

        def flaky(rid, last_item_id, examined, created):
            calls.append(last_item_id)
            if len(calls) == 2:
                raise StoreError("injected: checkpoint failed")
            return real_ckpt(rid, last_item_id, examined, created)

        rec.gate.checkpoint_reconciliation_run = flaky
        with self.assertRaises(StoreError):
            rec.reconcile()
        # Batch 1 (w-1) created and checkpointed; batch 2 (w-2) created,
        # then the checkpoint blew up.
        self.assertEqual(self._count("jobs"), 2)
        open_runs = self.gate.open_reconciliation_runs()
        self.assertEqual(len(open_runs), 1)
        self.assertEqual(open_runs[0]["last_item_id"], "w-1")
        self.assertEqual(open_runs[0]["items_created"], 1)
        # Resume with the fault gone: w-3 created, nothing duplicated.
        rec.gate.checkpoint_reconciliation_run = real_ckpt
        rec2 = self._new_rec(batch_size=1)
        r2 = rec2.reconcile()
        self.assertEqual(r2["result"], "CHANGED")
        self.assertEqual(r2["items_created"], 1)
        self.assertEqual(self._count("jobs"), 3)
        for d in ("w-1", "w-2", "w-3"):
            self.assertEqual(
                len(self._job_events(canonical_job_id(d), "job.created")),
                1, d)

    def test_R11_39_restart_new_reconciler_same_db(self):
        """R11-39: real-process restart — after a completed pass, a
        brand-new Reconciler over the same DB file reconstructs purely
        from durable state -> CONVERGED, zero new jobs, zero new ledger
        events."""
        for d in ("w-1", "w-2", "w-3"):
            self._set(d)
        rec = self._new_rec()
        r1 = rec.reconcile()
        self.assertEqual(r1["result"], "CHANGED")
        rec.close()
        seq0 = self._ledger_seq_max()
        rec2 = Reconciler(self.db, ReconcilerConfig(poll_interval_s=0.2,
                                                   batch_size=10))
        self._recs.append(rec2)
        r2 = rec2.reconcile()
        self.assertEqual(r2["result"], "CONVERGED")
        self.assertEqual(r2["items_created"], 0)
        self.assertEqual(self._count("jobs"), 3)
        self.assertEqual(self._ledger_seq_max(), seq0)


# --------------------------------------- R11-40..44 corruption/contradiction
class TestR1140_44(ReconcilerBase):
    def test_R11_40_unreadable_desired_state(self):
        """R11-40: unreadable desired state -> FAILED with run=None and
        zero job mutation. Two variants: (a) corrupt spec JSON in the
        desired_state row; (b) the desired_state table itself dropped."""
        # (a) corrupt spec JSON
        self._set("w-1")
        raw = self._raw()
        raw.execute("UPDATE desired_state SET spec='not-json'"
                    " WHERE desired_work_id='w-1'")
        raw.commit()
        raw.close()
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "FAILED")
        self.assertIsNone(r["run"])
        self.assertEqual(r["discrepancies"][0]["type"],
                         "unparseable desired-state row")
        self.assertEqual(self._count("jobs"), 0)
        self.assertEqual(self._count("desired_job_map"), 0)
        self.assertEqual(self._count("reconciliation_runs"), 0)
        rec.close()

        # (b) desired_state table dropped out from under the reconciler
        raw = self._raw()
        raw.execute("DROP TABLE desired_state")
        raw.commit()
        raw.close()
        rec2 = self._new_rec()
        r2 = rec2.reconcile()
        self.assertEqual(r2["result"], "FAILED")
        self.assertIsNone(r2["run"])
        self.assertEqual(r2["discrepancies"][0]["type"],
                         "unreadable desired state")
        self.assertEqual(r2["items_created"], 0)

    def test_R11_41_unreadable_actual_state(self):
        """R11-41: unreadable actual state (jobs table dropped) -> the
        pass fails closed: result FAILED, the run is finished FAILED
        (never left RUNNING), zero job mutation (no jobs created, no map
        rows, no job ledger events)."""
        self._set("w-1")
        raw = self._raw()
        raw.execute("DROP TABLE jobs")
        raw.commit()
        raw.close()
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "FAILED")
        self.assertIsNotNone(r["run"])
        self.assertEqual(r["run"]["result"], "FAILED")
        self.assertEqual(r["items_created"], 0)
        self.assertEqual(self.gate.open_reconciliation_runs(), [])
        self.assertEqual(self._count("desired_job_map"), 0)
        self.assertEqual(self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type LIKE 'job.%'"
        ).fetchone()[0], 0)

    def test_R11_42_contradictory_desired_state(self):
        """R11-42: tampered spec (valid JSON, different bytes) breaks the
        head snapshot -> FAILED with run=None: the desired state is
        internally contradictory, so the pass refuses before opening a
        run record."""
        self._set("w-1")
        raw = self._raw()
        raw.execute(
            "UPDATE desired_state SET spec=? WHERE desired_work_id='w-1'",
            (json.dumps({"task_id": "t", "stage_id": "tampered",
                         "max_attempts": 3, "policy": {}}),))
        raw.commit()
        raw.close()
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "FAILED")
        self.assertIsNone(r["run"])
        self.assertEqual(r["discrepancies"][0]["type"],
                         "desired-state snapshot mismatch")
        self.assertEqual(self._count("jobs"), 0)
        self.assertEqual(self._count("reconciliation_runs"), 0)

    def test_R11_43_contradictory_actual_map_without_job(self):
        """R11-43: map-without-job (job row manually deleted) is
        contradictory actual state. The reconcile pass records
        CONTRADICTORY with zero action — no adoption, no recreate — and
        the gate's ensure op raises DesiredStateConflict on the same
        state (fail closed, never adopt a foreign/missing job)."""
        self._set("w-1")
        rec = self._new_rec()
        rec.reconcile()
        jid = canonical_job_id("w-1")
        raw = self._raw()
        raw.execute("DELETE FROM jobs WHERE job_id=?", (jid,))
        raw.commit()
        raw.close()
        with self.assertRaises(TransitionRejected):
            self.gate.get_job(jid)
        r = rec.reconcile()
        d = [x for x in r["discrepancies"]
             if x.get("desired_work_id") == "w-1"][0]
        self.assertEqual(d["diff"], "CONTRADICTORY")
        self.assertEqual(d["action"], "none")
        self.assertEqual(r["items_created"], 0)
        # No adoption: the job row was NOT recreated.
        with self.assertRaises(TransitionRejected):
            self.gate.get_job(jid)
        self.assertIsNotNone(self.gate.get_desired_job_map("w-1"))
        self.assertEqual(len(self._job_events(jid, "job.created")), 1)
        # The gate op itself fails closed on the same state.
        head_v = self.gate.get_desired_head()["version"]
        with self.assertRaises(DesiredStateConflict):
            self.gate.ensure_job_for_desired_state(
                desired_work_id="w-1", task_id="t", stage_id="s",
                max_attempts=3, policy={}, desired_version=head_v,
                actor="reconciler")

    def test_R11_44_canonical_identity_collision(self):
        """R11-44: a human-created job squatting the canonical dj- id
        for a different purpose is never adopted: reconcile records
        CONTRADICTORY, the job row is byte-identical (no overwrite, no
        _desired envelope), no map row is created; direct ensure ->
        DesiredStateConflict."""
        jid = canonical_job_id("w-collide")
        self.gate.create_job(jid, "t", "s", "human")
        before_row = dict(self.gate.get_job(jid))
        self._set("w-collide")
        rec = self._new_rec()
        r = rec.reconcile()
        d = [x for x in r["discrepancies"]
             if x.get("desired_work_id") == "w-collide"][0]
        self.assertEqual(d["diff"], "CONTRADICTORY")
        self.assertEqual(d["action"], "none")
        self.assertEqual(r["items_created"], 0)
        # No adoption, no overwrite.
        self.assertEqual(dict(self.gate.get_job(jid)), before_row)
        self.assertNotIn("_desired", json.loads(before_row["policy"]))
        self.assertIsNone(self.gate.get_desired_job_map("w-collide"))
        self.assertEqual(
            [e["actor"] for e in self._job_events(jid, "job.created")],
            ["human"])
        head_v = self.gate.get_desired_head()["version"]
        with self.assertRaises(DesiredStateConflict):
            self.gate.ensure_job_for_desired_state(
                desired_work_id="w-collide", task_id="t", stage_id="s",
                max_attempts=3, policy={}, desired_version=head_v,
                actor="reconciler")


# --------------------------------------- R11-45..48 ordering/batching/resume
class TestR1145_48(ReconcilerBase):
    def test_R11_45_deterministic_creation_order(self):
        """R11-45: items inserted in shuffled order are still created in
        sorted desired_work_id order — asserted via the ledger's total
        order (job.created seq)."""
        shuffled = ["w-delta", "w-alpha", "w-echo", "w-bravo", "w-charlie"]
        for d in shuffled:
            self._set(d)
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CHANGED")
        rows = self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='job.created'"
            " ORDER BY seq").fetchall()
        order = [json.loads(r_["payload"])["desired_work_id"]
                 for r_ in rows]
        self.assertEqual(order, sorted(shuffled))

    def test_R11_46_batching_bounded_checkpoints_advance(self):
        """R11-46: batch_size=2 over 5 items -> 3 batches; the durable
        per-batch checkpoint advances each time (counted
        deterministically via a wrapper)."""
        for d in ("w-1", "w-2", "w-3", "w-4", "w-5"):
            self._set(d)
        rec = self._new_rec(batch_size=2)
        real_ckpt = rec.gate.checkpoint_reconciliation_run
        calls = []

        def counting(rid, last_item_id, examined, created):
            calls.append((last_item_id, examined, created))
            return real_ckpt(rid, last_item_id, examined, created)

        rec.gate.checkpoint_reconciliation_run = counting
        r = rec.reconcile()
        self.assertEqual(r["result"], "CHANGED")
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[0], ("w-2", 2, 2))
        self.assertEqual(calls[1], ("w-4", 4, 4))
        self.assertEqual(calls[2], ("w-5", 5, 5))
        run = self.gate.get_reconciliation_run(r["run"]["reconciliation_id"])
        self.assertEqual(run["items_examined"], 5)
        self.assertEqual(run["items_created"], 5)
        self.assertEqual(run["last_item_id"], "w-5")

    def test_R11_47_partial_resume_safe_after_reconciler_drop(self):
        """R11-47: the reconciler is dropped mid-run (its stores closed)
        with an open RUNNING run row left behind; a brand-new Reconciler
        on the same DB completes the pass with no duplicates."""
        for d in ("w-1", "w-2", "w-3", "w-4"):
            self._set(d)
        rec = self._new_rec(batch_size=1)
        real_ensure = rec.gate.ensure_job_for_desired_state
        calls = []

        def die_on_second(**kw):
            calls.append(kw["desired_work_id"])
            if len(calls) == 2:
                # Simulate process death mid-pass: close every store
                # handle without finishing the run.
                rec.close()
                raise RuntimeError("simulated reconciler death")
            return real_ensure(**kw)

        rec.gate.ensure_job_for_desired_state = die_on_second
        with self.assertRaises(RuntimeError):
            rec.reconcile()
        self._recs.remove(rec)  # already closed; don't close twice
        # w-1's job+map are durable; the run is observably open.
        self.assertEqual(self.gate.get_job(
            canonical_job_id("w-1"))["status"], "PENDING")
        open_runs = self.gate.open_reconciliation_runs()
        self.assertEqual(len(open_runs), 1)
        self.assertEqual(open_runs[0]["result"], "RUNNING")
        self.assertEqual(open_runs[0]["last_item_id"], "w-1")
        # A new reconciler resumes and completes: no duplicates.
        rec2 = self._new_rec(batch_size=1)
        r2 = rec2.reconcile()
        self.assertEqual(r2["result"], "CHANGED")
        self.assertEqual(r2["items_created"], 3)
        self.assertEqual(self._count("jobs"), 4)
        for d in ("w-1", "w-2", "w-3", "w-4"):
            self.assertEqual(
                len(self._job_events(canonical_job_id(d), "job.created")),
                1, d)

    def test_R11_48_no_duplicate_jobs_after_restart(self):
        """R11-48: close + brand-new Reconciler on the same DB ->
        CONVERGED, still exactly one job/map row per item, one
        job.created event each."""
        for d in ("w-1", "w-2"):
            self._set(d)
        rec = self._new_rec()
        rec.reconcile()
        rec.close()
        rec2 = self._new_rec()
        r = rec2.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        self.assertEqual(self._count("jobs"), 2)
        self.assertEqual(self._count("desired_job_map"), 2)
        for d in ("w-1", "w-2"):
            jid = canonical_job_id(d)
            self.assertEqual(len(self._job_events(jid, "job.created")), 1)


# ------------------------------------------------------------------ R11-49
class TestR1149(ReconcilerBase):
    def test_R11_49_reconciler_cannot_mark_complete(self):
        """R11-49: the reconciler has no completion path. Static: no
        commit/stage/begin/verify/fail/transition calls in
        exec/reconciler.py. Behavioral: a full lifecycle completes only
        through the R5 contract (job.committed actor is worker:*), the
        reconciler's footprint stays creation-only, and a direct
        reconciler-actor commit attempt is fencing-rejected."""
        calls, attrs, defs = self._rec_calls_attrs_defs()
        for name in ("commit_artifact", "stage_artifact", "begin_commit",
                     "verify_artifact", "fail_job_execution",
                     "transition_job"):
            self.assertNotIn(name, calls, f"reconciler calls {name}")
            self.assertNotIn(name, attrs, f"reconciler references {name}")
        self.assertNotIn("commit_artifact", defs)
        rec = Reconciler(self.db, ReconcilerConfig(poll_interval_s=0.2,
                                                 batch_size=10))
        self._recs.append(rec)
        for name in ("commit_artifact", "stage_artifact", "begin_commit",
                     "complete_job", "mark_complete"):
            self.assertFalse(hasattr(rec, name), f"Reconciler.{name}")
        # Full lifecycle: only the R5 contract can complete.
        self._set("w-1")
        rec.reconcile()
        jid = canonical_job_id("w-1")
        self.assertTrue(self.gate.claim_job(jid, "w-49", 60.0, "test"))
        tok = int(self.gate.get_job(jid)["fencing_token"])
        self.gate.transition_job(jid, "RUNNING", "worker:w-49")
        # A direct misuse attempt with reconciler-flavored identity is
        # fencing-rejected: the reconciler is not the owner.
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact(jid, "reconciler", tok,
                                      artifact_id="nope",
                                      actor="reconciler", evidence={})
        r5_complete(self.gate, job_id=jid, worker_id="w-49",
                    fencing_token=tok, task_id="t", outcome="SUCCESS",
                    evidence={"ok": True}, actor="worker:w-49")
        self.assertEqual(self.gate.get_job(jid)["status"], "COMPLETE")
        committed = self._job_events(jid, "job.committed")
        self.assertTrue(committed)
        for e in committed:
            self.assertTrue(e["actor"].startswith("worker:"), e["actor"])
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        self.assertEqual(r["discrepancies"][0]["diff"], "DESIRED_COMPLETE")
        rec_footprint = [e["event_type"] for e in self._job_events(jid)
                         if e["actor"] == "reconciler"
                         or e["actor"].startswith("reconciler:")]
        self.assertEqual(rec_footprint, ["job.created"])


# ------------------------------------------------------------------ R11-50
class TestR1150(ReconcilerBase):
    def test_R11_50_composition_smoke(self):
        """R11-50: R1-R10 composition smoke. The scheduler still admits
        reconciler-created work; a FAILED job from a reconciled item is
        left for R8/R9 — the reconciler creates no recovery incident,
        attempt, or policy row, and the scheduler keeps working."""
        self._set("w-a")
        self._set("w-b")
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CHANGED")
        jid_a = canonical_job_id("w-a")
        # Drive w-a to FAILED through the real execution path shape
        # (claim -> RUNNING -> fail).
        self.assertTrue(self.gate.claim_job(jid_a, "w-50", 60.0, "test"))
        tok = int(self.gate.get_job(jid_a)["fencing_token"])
        self.gate.transition_job(jid_a, "RUNNING", "worker:w-50")
        self.gate.fail_job_execution(jid_a, "w-50", tok,
                                     actor="worker:w-50", reason="r11-50")
        n_inc = self._count("incidents")
        n_att = self._count("recovery_attempts")
        n_pol = self._count("recovery_policy")
        r2 = rec.reconcile()
        d = [x for x in r2["discrepancies"]
             if x.get("desired_work_id") == "w-a"][0]
        self.assertEqual(d["diff"], "DESIRED_FAILED")
        # Left for R8/R9: the reconciler created no recovery state.
        self.assertEqual(self._count("incidents"), n_inc)
        self.assertEqual(self._count("recovery_attempts"), n_att)
        self.assertEqual(self._count("recovery_policy"), n_pol)
        self.assertEqual(
            self.gate.open_recovery_incidents(scope="recovery"), [])
        # The scheduler still admits the healthy reconciled item and
        # never touches the FAILED one.
        sup = self._new_sup(actor="test-r11-sup")
        sched = self._new_sched(sup, scheduler_id="r11s50")
        rep = sched.evaluate_once()
        self.assertEqual(rep["detail"], "ok")
        self.assertEqual(rep["admitted"], 1)
        jid_b = canonical_job_id("w-b")
        self.assertEqual(self.gate.get_job(jid_b)["status"], "CLAIMED")
        self.assertEqual(self.gate.get_job(jid_a)["status"], "FAILED")
        self._wait_status(jid_b, "COMPLETE", timeout=30.0)


# ------------------------------------------------- §30 full-path composition
class TestR11FullPath(ReconcilerBase):
    def test_R11_FULL_s30_full_path_composition(self):
        """§30 FULL-PATH: desired item -> R11 creates PENDING -> R10
        claims -> R10 dispatches a real worker (fast success) -> R3
        heartbeat evidence -> R5 commit_artifact -> COMPLETE -> next R11
        reconcile -> DESIRED_COMPLETE satisfied, zero new jobs.
        Per-layer authority is asserted from the ledger: the reconciler
        created only PENDING, the scheduler claimed/dispatched, the
        worker committed, and the reconciler never touched execution
        states."""
        # R11: desired -> PENDING, nothing else.
        self._set("w-full")
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CHANGED")
        self.assertEqual(r["items_created"], 1)
        jid = canonical_job_id("w-full")
        self.assertEqual(self.gate.get_job(jid)["status"], "PENDING")
        # R10: claim + dispatch a real worker.
        sup = self._new_sup(actor="test-r11-sup")
        sched = self._new_sched(sup, scheduler_id="r11full")
        rep = sched.evaluate_once()
        self.assertEqual(rep["detail"], "ok")
        self.assertEqual(rep["admitted"], 1)
        self.assertEqual(rep["errors"], [])
        wid = self.gate.get_job(jid)["owner_worker_id"]
        self.assertTrue(wid.startswith("sched-r11full-"), wid)
        # R5 (via the real worker): commit -> COMPLETE.
        self._wait_status(jid, "COMPLETE", timeout=30.0)
        # R3: heartbeat evidence from the worker's execution.
        beats = self.gate.heartbeats_for(wid)
        self.assertGreaterEqual(len(beats), 1)
        # Next R11 pass: DESIRED_COMPLETE satisfied, zero new jobs.
        r2 = rec.reconcile()
        self.assertEqual(r2["result"], "CONVERGED")
        self.assertEqual(r2["items_created"], 0)
        d = r2["discrepancies"][0]
        self.assertEqual(d["diff"], "DESIRED_COMPLETE")
        self.assertEqual(self._count("jobs"), 1)
        # Per-layer authority from the ledger.
        created = self._job_events(jid, "job.created")
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]["actor"], "reconciler")
        claimed = self._job_events(jid, "job.claimed")
        self.assertEqual([e["actor"] for e in claimed],
                         ["scheduler:r11full"])
        committed = self._job_events(jid, "job.committed")
        self.assertTrue(committed, "no job.committed event")
        for e in committed:
            self.assertTrue(e["actor"].startswith("worker:"),
                            e["actor"])
            self.assertFalse(e["actor"].startswith("scheduler:"))
        spawns = self.gate.store.conn.execute(
            "SELECT actor, payload FROM ledger WHERE event_type="
            "'worker.proc_spawned' AND json_extract(payload,'$.job_id')=?"
            " ORDER BY seq", (jid,)).fetchall()
        self.assertEqual(len(spawns), 1)
        self.assertEqual(spawns[0]["actor"], "test-r11-sup")
        # The reconciler never touched execution states: its only
        # footprint on this job is the creation event.
        for e in self._job_events(jid):
            if (e["actor"] == "reconciler"
                    or e["actor"].startswith("reconciler:")):
                self.assertEqual(e["event_type"], "job.created", e)


# ------------------------------------------------- race extras (run x3)
class TestR11Races(ReconcilerBase):
    """RACE_1..5: races discovered during development. Outcomes that are
    inherently racy assert membership/invariants; final durable state is
    always asserted deterministically."""

    def test_RACE_1_reconcile_racing_head_bump(self):
        """RACE-1: a set_desired_item (head bump) racing a mid-pass
        reconcile. The first pass reports CHANGED or CONFLICT; after it
        settles, one follow-up pass converges and every item owns
        exactly one job."""
        for d in ("w-1", "w-2", "w-3"):
            self._set(d)
        rec = Reconciler(self.db, ReconcilerConfig(poll_interval_s=0.2,
                                                 batch_size=1))
        self._recs.append(rec)
        outcome: dict = {}

        def do_reconcile():
            outcome["r"] = rec.reconcile()

        def do_bump():
            s = open_store(self.db)
            migrate(s)
            try:
                TransitionGate(s).set_desired_item(
                    "w-4", {"task_id": "t"}, "test")
            finally:
                s.close()

        barrier = threading.Barrier(2)

        def w1():
            barrier.wait(timeout=15)
            do_reconcile()

        def w2():
            barrier.wait(timeout=15)
            do_bump()

        t1, t2 = threading.Thread(target=w1), threading.Thread(target=w2)
        t1.start()
        t2.start()
        t1.join(timeout=60)
        t2.join(timeout=60)
        self.assertFalse(t1.is_alive() or t2.is_alive(), "thread hung")
        r = outcome["r"]
        self.assertIn(r["result"], ("CHANGED", "CONFLICT"), r["result"])
        # Settle: the first post-race pass may still be doing creation
        # work (the bump can land after the first pass pinned v1); the
        # pass after that must converge. The final durable state is
        # deterministic on every interleaving.
        r2 = rec.reconcile()
        self.assertIn(r2["result"], ("CHANGED", "CONVERGED"), r2["result"])
        r3 = rec.reconcile()
        self.assertEqual(r3["result"], "CONVERGED")
        self.assertEqual(r3["items_created"], 0)
        self.assertEqual(self._count("jobs"), 4)
        for d in ("w-1", "w-2", "w-3", "w-4"):
            self.assertEqual(
                len(self._job_events(canonical_job_id(d), "job.created")),
                1, d)

    def test_RACE_2_reconcile_racing_retire(self):
        """RACE-2: a retire racing a mid-pass reconcile. The retired item
        ends OBSOLETE; its job — whether or not the pass created it
        before observing the tombstone — is never transitioned or
        deleted."""
        for d in ("w-1", "w-2"):
            self._set(d)
        rec = Reconciler(self.db, ReconcilerConfig(poll_interval_s=0.2,
                                                 batch_size=1))
        self._recs.append(rec)

        def do_retire():
            s = open_store(self.db)
            migrate(s)
            try:
                TransitionGate(s).retire_desired_item("w-2", "test")
            finally:
                s.close()

        barrier = threading.Barrier(2)

        def w1():
            barrier.wait(timeout=15)
            rec.reconcile()

        def w2():
            barrier.wait(timeout=15)
            do_retire()

        t1, t2 = threading.Thread(target=w1), threading.Thread(target=w2)
        t1.start()
        t2.start()
        t1.join(timeout=60)
        t2.join(timeout=60)
        self.assertFalse(t1.is_alive() or t2.is_alive(), "thread hung")
        # Settle and inspect the final durable state: the first post-race
        # pass may still be doing creation work; the pass after that
        # must converge.
        r = rec.reconcile()
        self.assertIn(r["result"], ("CHANGED", "CONVERGED"), r["result"])
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        d = [x for x in r["discrepancies"]
             if x.get("desired_work_id") == "w-2"][0]
        self.assertEqual(d["diff"], "OBSOLETE")
        jid2 = canonical_job_id("w-2")
        n2 = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE job_id=?", (jid2,)).fetchone()[0]
        self.assertLessEqual(n2, 1)
        if n2 == 1:
            # Created before the tombstone landed: still PENDING, never
            # transitioned, never deleted.
            self.assertEqual(self.gate.get_job(jid2)["status"], "PENDING")
            kinds = {e["event_type"] for e in self._job_events(jid2)}
            self.assertNotIn("job.transition", kinds)
        jid1 = canonical_job_id("w-1")
        self.assertEqual(self.gate.get_job(jid1)["status"], "PENDING")
        self.assertEqual(len(self._job_events(jid1, "job.created")), 1)

    def test_RACE_3_concurrent_set_desired_item_cas(self):
        """RACE-3: 6 threads x set_desired_item on distinct items. BEGIN
        IMMEDIATE serializes the writers, so every CAS bump lands: head
        version +6, all items present, snapshot verifies (a reconcile
        proceeds normally)."""
        def fn(i):
            s = open_store(self.db)
            migrate(s)
            try:
                return TransitionGate(s).set_desired_item(
                    f"w-{i}", {"task_id": "t"}, "test")
            finally:
                s.close()

        results = self._run_barrier(
            [lambda i=i: fn(i) for i in range(6)])
        for i, res in results.items():
            self.assertNotIsInstance(res, Exception, f"thread {i}: {res!r}")
        self.assertEqual(self.gate.get_desired_head()["version"], 6)
        self.assertEqual(len(self.gate.list_desired_items()), 6)
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CHANGED")
        self.assertEqual(r["items_created"], 6)

    def test_RACE_4_four_reconcilers_batch_size_1(self):
        """RACE-4: 4 reconcilers x batch_size=1 x 6 items (maximum
        batch-boundary exposure): no duplicates, checkpoints interleave
        safely, final pass CONVERGED."""
        dwids = [f"w-{i}" for i in range(6)]
        for d in dwids:
            self._set(d)

        def fn():
            rec = Reconciler(self.db,
                             ReconcilerConfig(poll_interval_s=0.2,
                                              batch_size=1))
            try:
                return rec.reconcile()
            finally:
                rec.close()

        results = self._run_barrier([fn] * 4)
        for i, res in results.items():
            self.assertNotIsInstance(res, Exception, f"thread {i}: {res!r}")
            self.assertIn(res["result"], ("CHANGED", "CONVERGED"),
                          f"thread {i}")
        self.assertEqual(self._count("jobs"), 6)
        self.assertEqual(self._count("desired_job_map"), 6)
        for d in dwids:
            self.assertEqual(
                len(self._job_events(canonical_job_id(d), "job.created")),
                1, d)
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        self.assertEqual(r["items_created"], 0)

    def test_RACE_5_reconcile_racing_direct_ensure(self):
        """RACE-5: reconcilers racing direct gate.ensure calls on the
        same item: exactly one winner across all callers — one job row,
        one map row, one job.created event."""
        self._set("w-1")
        head_v = self.gate.get_desired_head()["version"]

        def do_rec():
            rec = Reconciler(self.db,
                             ReconcilerConfig(poll_interval_s=0.2,
                                              batch_size=10))
            try:
                return ("rec", rec.reconcile())
            finally:
                rec.close()

        def do_ensure():
            s = open_store(self.db)
            migrate(s)
            try:
                g = TransitionGate(s)
                return ("ensure", g.ensure_job_for_desired_state(
                    desired_work_id="w-1", task_id="t", stage_id="s",
                    max_attempts=3, policy={}, desired_version=head_v,
                    actor="reconciler"))
            finally:
                s.close()

        results = self._run_barrier([do_rec, do_rec, do_ensure, do_ensure])
        for i, res in results.items():
            self.assertNotIsInstance(res, Exception, f"thread {i}: {res!r}")
        jid = canonical_job_id("w-1")
        self.assertEqual(self._count("jobs"), 1)
        self.assertEqual(self._count("desired_job_map"), 1)
        self.assertEqual(len(self._job_events(jid, "job.created")), 1)
        winners = 0
        for i, (kind, res) in results.items():
            if kind == "rec":
                winners += res["items_created"]
            else:
                _job, created = res
                winners += 1 if created else 0
        self.assertEqual(winners, 1, f"exactly one creator: {results}")


if __name__ == "__main__":
    unittest.main()
