"""Phase 1C R13 — release finalization tests.

STANDARD: real SQLite/WAL temp DBs with full migrate() (v11); gate-level
fixtures for determinism (real subprocesses only where the composition
tests need a real worker, mirroring the R11-FULL pattern); deterministic
fault injection (wrapped gate methods, raw-SQL tamper) for crash points —
never blind sleeps: barrier-aligned threads and poll-based waits; every
spawned process is killed and reaped in tearDown.

Conventions:
- `blockers` on run rows is canonical JSON TEXT — always json.loads it.
- Fault injection via raw SQL is deliberate and documented per test (the
  fault being modeled); the gate itself can never produce such rows.
- Private gate helpers (_canonical_finalization_task_id,
  _release_checkpoint_entries/_id) are used only to construct the EXACT
  deterministic identities the implementation defines, never to bypass
  authority.

Test IDs:
  R13-01  fresh generation: begin -> OPEN, version 1, idempotent re-begin
  R13-02  durable run identity: canonical fin- id stable across restart
  R13-03  deterministic snapshot: same logical spec -> same snapshot
          hash across DBs; manifest desired_state_hash == head hash
  R13-04  desired-satisfaction gate: item with no job -> BLOCKED
          (DESIRED_WORK_UNSATISFIED)
  R13-05  PENDING job blocks
  R13-06  CLAIMED job blocks
  R13-07  RUNNING job blocks
  R13-08  COMMITTING job blocks
  R13-09  VERIFYING job blocks (injected defensively: the gate graph
          has no VERIFYING job state; the evaluator must still treat it
          as active, never ignore it)
  R13-10  UNCERTAIN job blocks (UNCERTAIN_EXECUTION)
  R13-11  FAILED job blocks
  R13-12  BLOCKED job blocks (DESIRED_WORK_UNSATISFIED + HUMAN_GATE)
  R13-13  COMPLETE satisfies: a COMPLETE item contributes zero blockers
  R13-14  valid artifact -> READY, zero blockers
  R13-15  MISSING_ARTIFACT: COMPLETE job with no result_artifact_id
  R13-16  corrupt artifact: staged bytes absent
  R13-17  wrong-hash artifact: staged bytes do not re-hash
  R13-18  bad provenance: artifact staged for a different job
  R13-19  missing validator receipt -> INVALID_ARTIFACT
  R13-20  checkpoint bound to a foreign task -> CONTRADICTORY_STATE
  R13-21  checkpoint identity mismatch (tampered canonical_manifest) ->
          CONTRADICTORY_STATE
  R13-22  VERIFIED checkpoint without a release attestation ->
          INVALID_CHECKPOINT
  R13-23  CORRUPT checkpoint -> INVALID_CHECKPOINT
  R13-24  UNVERIFIED release checkpoint is verified by evaluation (not
          a blocker) -> READY
  R13-25  UNCERTAIN recovery attempt -> UNCERTAIN_EXECUTION (+ ACTIVE)
  R13-26  open recovery incident blocks (ACTIVE_RECOVERY)
  R13-27  RUNNING recovery attempt blocks
  R13-28  escalated incident with no terminal R9 policy state blocks
  R13-29  escalated incident WITH terminal policy state does not block
  R13-30  incident outcome without terminal policy (unconsumed) blocks
  R13-31  OPEN breaker on GLOBAL blocks
  R13-32  OPEN breaker on a generation JOB scope blocks
  R13-33  OPEN breaker on an unrelated scope does NOT block
  R13-34  HALF_OPEN breaker on a required scope blocks
  R13-35  task PAUSED_FOR_HUMAN blocks (HUMAN_GATE)
  R13-36  task MANUAL_REVIEW blocks (HUMAN_GATE, injected)
  R13-37  job BLOCKED: finalizer never auto-clears the human gate
  R13-38  evaluate with a stale expected_version -> FinalizationConflict
  R13-39  publish with a stale expected_version -> FinalizationConflict
  R13-40  generation/version canonical binding (mismatch rejected,
          contradictory re-begin fails closed)
  R13-41  stale generation: head moved between evaluate and publish ->
          TransitionRejected (STALE_GENERATION), zero mutation
  R13-42  manifest determinism: identical fixtures in two DBs -> same
          manifest_hash
  R13-43  manifest changes when an artifact content hash changes
  R13-44  manifest changes when the generation changes
  R13-45  publish revalidates the release checkpoint record (success)
  R13-46  publish detects checkpoint-record mutation -> fails closed
  R13-47  publication atomicity: one txn flips state/version/
          completed_at/result + exactly one ledger event
  R13-48  no FINALIZED without a checkpoint: READY with NULL
          checkpoint_id -> publish refuses
  R13-49  no checkpoint publication without verification: UNVERIFIED
          checkpoint at publish -> publish refuses
  R13-50  2 finalizers racing -> exactly one publication
  R13-51  3 finalizers racing -> exactly one publication
  R13-52  CAS loser observes the winner (re-read, never overwrite)
  R13-53  crash before evaluation -> restart reconstructs, converges
  R13-54  crash after evidence snapshot -> restart converges
  R13-55  crash after manifest construction -> restart converges
  R13-56  crash after checkpoint staging -> restart verifies, converges
  R13-57  crash after checkpoint verification -> restart converges
  R13-58  crash after READY recorded, before publish -> re-evaluate is
          a no-op, publish converges
  R13-59  crash immediately before the publication txn -> zero mutation,
          restart publishes
  R13-60  crash DURING the publication txn -> rolled back, zero partial
          mutation, restart publishes
  R13-61  crash immediately after the publication txn -> restart
          observes FINALIZED; re-publish idempotent, zero writes
  R13-62  crash before observation by another process -> a fresh gate
          observes FINALIZED; idempotent re-publish, zero writes
  R13-63  idempotent re-finalize: second publish -> same row, zero
          writes, no duplicate ledger event; a contradictory finalized
          record (tampered checkpoint) fails closed
  R13-64  post-finalization desired mutation names a NEW generation;
          the old run row is byte-identical (old evidence never
          overwritten)
  R13-65  R8/R9 history (incidents/attempts/policy) intact across
          finalization
  R13-66  reconciler/scheduler/R12 activity cannot silently mutate
          finalized evidence
  R13-67  unreadable desired state (corrupt spec JSON) -> FAILED, never
          FINALIZED; publish fails closed
  R13-68  unreadable actual state (dropped jobs table) -> FAILED, never
          FINALIZED
  R13-69  unreadable artifact evidence (dropped artifacts table) ->
          FAILED, never FINALIZED
  R13-70  unreadable checkpoint evidence (dropped checkpoints table) ->
          FAILED, never FINALIZED
  R13-71  contradictory process identity: squatted finalization task id
          -> TransitionRejected, run stays OPEN
  R13-72  unreaped worker.proc_spawned evidence -> ACTIVE_EXECUTION
  R13-73  deterministic blocker ordering: sorted by (category,
          identity), stable across evaluations
  R13-74  no-op re-evaluation writes nothing (row byte-identical, zero
          new ledger events)
  R13-75  finalizer never creates/claims/dispatches/kills/fences/
          reclaims (AST over exec/finalizer.py)
  R13-76  finalizer never changes desired state / recovery policy /
          breaker policy (behavioral: tables byte-identical)
  R13-77  R1-R12 composition: reconciler -> scheduler dispatches a REAL
          worker -> R5 completion -> watchdog -> R12 healthy ->
          finalizer FINALIZED; per-layer ledger authority
  R13-78  complete e2e finalization path (gate-level): desired ->
          reconciled -> executed -> READY -> deterministic manifest ->
          release checkpoint -> revalidation -> atomic FINALIZED
          publication
  R13-79  S31 END-TO-END: R11 -> R10 claim -> R3 heartbeats -> R4
          advisory expiry -> R7 detection -> R8/R9 incident+attempt+
          policy -> R5 verification -> R12 healthy -> R13 evaluate ->
          manifest -> checkpoint -> revalidation -> atomic FINALIZED ->
          second finalization idempotent
  R13-80  S31 NEGATIVE: desired change -> new generation; old
          finalization unusable for the new generation; new
          reconciliation/finalization required; a stale finalizer fails
          CAS
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
sys.path.insert(0, os.path.dirname(os.path.dirname(HERE)))  # ~/workspace

from axos.store import (  # noqa: E402
    open_store, migrate, TransitionGate, TransitionRejected, LeaseError,
    FinalizationConflict, canonical_release_generation,
    canonical_finalization_id, build_finalization_manifest,
    finalization_manifest_hash,
)
from axos.store.gate import (  # noqa: E402
    _canonical_finalization_task_id, _canonical_desired_job_id,
    _release_checkpoint_entries, _release_checkpoint_id,
)
import axos.store.gate as gate_mod  # noqa: E402
from axos.exec.finalizer import (  # noqa: E402
    Finalizer, FinalizationConfig,
)
from axos.exec.reconciler import (  # noqa: E402
    Reconciler, ReconcilerConfig, canonical_job_id,
)
from axos.exec.scheduler import Scheduler, SchedulerConfig  # noqa: E402
from axos.exec.supervisor import Supervisor  # noqa: E402
from axos.exec.resilience import (  # noqa: E402
    ResilienceConfig, ResilienceController,
)
from axos.exec.watchdog import Watchdog, WatchdogConfig  # noqa: E402
from axos.tests.r5_helpers import r5_complete  # noqa: E402

FIN_SRC = os.path.join(os.path.dirname(HERE), "exec", "finalizer.py")
assert os.path.isfile(FIN_SRC), FIN_SRC


class _Crashed(Exception):
    """Test-only crash sentinel: raised at an injected crash point to
    simulate process death. Never caught by the implementation (it is
    not _EvidenceFailure), so it propagates like a real crash."""


class FakeClock:
    def __init__(self, t: float):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        assert dt >= 0, "FakeClock never moves backward"
        self.t += dt


class FinalizationBase(unittest.TestCase):
    H = 0.4

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r13-")
        self.db = os.path.join(self.tmp, "t.db")
        # Start ahead of wall time so store-time stays on the fake clock
        # even when real-time components commit.
        self.clock = FakeClock(t=time.time() + 10_000.0)
        self.store = open_store(self.db, clock=self.clock)
        migrate(self.store)
        self._stores = [self.store]
        self.gate = TransitionGate(self.store)
        self._sups: list = []
        self._scheds: list = []
        self._recs: list = []
        self._ctls: list = []
        self._wds: list = []
        self._fins: list = []
        self.gate.create_task("t", {"objective": "r13"}, {"usd": 1},
                              "test")

    def tearDown(self):
        for fin in self._fins:
            try:
                fin.close()
            except Exception:
                pass
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
        for st in self._stores:
            try:
                st.close()
            except Exception:
                pass
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------- helpers
    def _mk_task(self, tid, actor="test"):
        return self.gate.create_task(tid, {"objective": "r13"},
                                     {"usd": 1}, actor)

    def _set(self, dwid, task_id="t", stage_id="s", max_attempts=3,
             policy=None, actor="test") -> dict:
        return self.gate.set_desired_item(
            dwid,
            {"task_id": task_id, "stage_id": stage_id,
             "max_attempts": max_attempts, "policy": policy or {}},
            actor)

    def _head_version(self) -> int:
        return self.gate.get_desired_head()["version"]

    def _ensure(self, dwid, actor="reconciler") -> dict:
        job, _created = self.gate.ensure_job_for_desired_state(
            desired_work_id=dwid, task_id="t", stage_id="s",
            max_attempts=3, policy={},
            desired_version=self._head_version(), actor=actor)
        return job

    def _claim_run(self, jid, wid=None, ttl=60.0):
        wid = wid or f"w-{jid}"
        self.assertTrue(
            self.gate.claim_job_bounded(jid, wid, ttl, "test", 4))
        self.gate.transition_job(jid, "RUNNING", f"worker:{wid}")
        return wid, self.gate.get_job(jid)["fencing_token"]

    def _complete(self, jid, wid=None, task_id="t", data=None):
        wid = wid or f"w-{jid}"
        tok = self.gate.get_job(jid)["fencing_token"]
        return r5_complete(gate=self.gate, job_id=jid, worker_id=wid,
                           fencing_token=tok, task_id=task_id,
                           outcome="SUCCESS", evidence={"ok": True},
                           actor=f"worker:{wid}", data=data)

    def _healthy_job(self, dwid, data=None) -> str:
        """One desired item -> COMPLETE with a valid artifact. Returns
        the canonical job_id."""
        self._set(dwid)
        job = self._ensure(dwid)
        jid = job["job_id"]
        wid, _tok = self._claim_run(jid)
        self._complete(jid, wid, data=data)
        self.assertEqual(self.gate.get_job(jid)["status"], "COMPLETE")
        return jid

    def _healthy_generation(self, dwids, data=None):
        """All desired items set first (one head), then every job run to
        COMPLETE. Returns (generation, [job_ids])."""
        for d in dwids:
            self._set(d)
        gen = canonical_release_generation(self._head_version())
        jids = []
        for d in dwids:
            job = self._ensure(d)
            jid = job["job_id"]
            wid, _tok = self._claim_run(jid)
            self._complete(jid, wid, data=data)
            jids.append(jid)
        return gen, jids

    def _begin(self, gen=None, version=None, actor="test") -> dict:
        version = self._head_version() if version is None else version
        gen = (canonical_release_generation(version)
               if gen is None else gen)
        return self.gate.begin_finalization_run(gen, version, actor)

    def _eval(self, gen, version=None, actor="test") -> dict:
        if version is None:
            version = self.gate.get_finalization_run(gen)["version"]
        return self.gate.evaluate_finalization(gen, version, actor)

    def _publish(self, gen, version=None, manifest_hash=None,
                 actor="test") -> dict:
        row = self.gate.get_finalization_run(gen)
        if version is None:
            version = row["version"]
        if manifest_hash is None:
            manifest_hash = row["manifest_hash"]
        return self.gate.publish_finalization(gen, version,
                                              manifest_hash, actor)

    @staticmethod
    def _blockers(run) -> list:
        return json.loads(run["blockers"] or "[]")

    def _cats(self, run) -> list:
        return [b["category"] for b in self._blockers(run)]

    def _count_events(self, event_type) -> int:
        return self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?",
            (event_type,)).fetchone()[0]

    def _ledger_seq_max(self):
        row = self.gate.store.conn.execute(
            "SELECT MAX(seq) FROM ledger").fetchone()
        return row[0] or 0

    def _count(self, table) -> int:
        return self.gate.store.conn.execute(
            f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def _raw(self):
        """A raw sqlite3 handle for deliberate tamper/corruption ops."""
        return sqlite3.connect(self.db)

    def _table_dump(self, table):
        rows = self.gate.store.conn.execute(
            f"SELECT * FROM {table}").fetchall()
        return sorted(tuple(r) for r in rows)

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

    def _new_fin(self, actor="finalizer", poll_interval_s=0.2,
                 batch_size=10, boot_ready=None) -> Finalizer:
        fin = Finalizer(
            self.db, None,
            FinalizationConfig(poll_interval_s=poll_interval_s,
                               batch_size=batch_size, actor=actor),
            boot_ready=(lambda: True) if boot_ready is None
            else boot_ready)
        self._fins.append(fin)
        return fin

    def _new_sup(self, H=None, actor=None) -> Supervisor:
        s = Supervisor(self.db,
                       actor=actor or f"test-r13-{len(self._sups)}",
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
                       scheduler_id=scheduler_id or f"r13-{len(self._scheds)}",
                       boot_ready=(lambda: True))
        self._scheds.append(sc)
        return sc

    def _new_rec(self, batch_size=10, poll_interval_s=0.2,
                 actor="reconciler") -> Reconciler:
        rec = Reconciler(
            self.db,
            ReconcilerConfig(poll_interval_s=poll_interval_s,
                             batch_size=batch_size, actor=actor))
        self._recs.append(rec)
        return rec

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

    def _open_breaker(self, scope_type, scope_id, cooldown_s=60.0):
        self.gate.ensure_breaker_state(
            scope_type, scope_id, cooldown_s=cooldown_s, actor="test")
        row = self.gate.get_breaker_state(scope_type, scope_id)
        return self.gate.transition_breaker(
            scope_type, scope_id, expected_version=row["version"],
            to_state="OPEN", actor="test", reason="r13-test")

    def _half_open_breaker(self, scope_type, scope_id):
        opened = self._open_breaker(scope_type, scope_id)
        return self.gate.transition_breaker(
            scope_type, scope_id, expected_version=opened["version"],
            to_state="HALF_OPEN", actor="test", reason="r13-test")

    def _restart(self):
        """Simulate a process crash + restart: close every handle held
        by this test, then reopen a fresh store+gate over the same DB
        file. Crash tests never run live subprocesses, so only stores
        and finalizers need re-establishing."""
        for fin in self._fins:
            try:
                fin.close()
            except Exception:
                pass
        self._fins = []
        for st in self._stores:
            try:
                st.close()
            except Exception:
                pass
        self._stores = []
        self.store = open_store(self.db, clock=self.clock)
        migrate(self.store)
        self._stores.append(self.store)
        self.gate = TransitionGate(self.store)

    def _release_entries_for(self, gen, jids):
        """The exact release-checkpoint manifest entries for a healthy
        generation's artifacts (public rows only)."""
        entries = []
        for jid in jids:
            job = self.gate.get_job(jid)
            entries.append({"artifact_id": job["result_artifact_id"],
                            "content_hash": job["content_hash"],
                            "job_id": jid})
        return entries

    def _fin_task_id(self, gen):
        return _canonical_finalization_task_id(gen)


# ------------------------------------------------------------------ R13-01
class TestR1301(FinalizationBase):
    def test_R13_01_fresh_generation_begins_open(self):
        """R13-01: a fresh generation begins OPEN at version 1 with no
        blockers; re-begin is idempotent (same row, no version bump, no
        duplicate finalization.begun ledger event)."""
        self._set("w-1")
        gen = canonical_release_generation(1)
        row = self.gate.begin_finalization_run(gen, 1, "test")
        self.assertEqual(row["release_generation"], gen)
        self.assertEqual(row["state"], "OPEN")
        self.assertEqual(row["version"], 1)
        self.assertEqual(row["desired_state_version"], 1)
        self.assertIsNone(row["manifest_hash"])
        self.assertIsNone(row["checkpoint_id"])
        self.assertEqual(self._blockers(row), [])
        self.assertEqual(row["finalization_id"],
                         canonical_finalization_id(gen))
        begun = self._count_events("finalization.begun")
        self.assertEqual(begun, 1)
        # Idempotent re-begin: the current row, unchanged.
        row2 = self.gate.begin_finalization_run(gen, 1, "test")
        self.assertEqual(dict(row2), dict(row))
        self.assertEqual(self._count_events("finalization.begun"), 1)
        # A mismatched generation/version pair is a caller bug.
        with self.assertRaises(TransitionRejected):
            self.gate.begin_finalization_run(gen, 2, "test")
        # A re-begin pinning a different version is a contradiction.
        with self.assertRaises(TransitionRejected):
            self.gate.begin_finalization_run(
                canonical_release_generation(2), 1, "test")


# ------------------------------------------------------------------ R13-02
class TestR1302(FinalizationBase):
    def test_R13_02_durable_run_identity(self):
        """R13-02: the finalization identity is canonical and durable — a
        fresh gate after a restart observes the identical run row."""
        self._set("w-1")
        gen = "ds-v1"
        expected_id = ("fin-" + hashlib.sha256(
            ("axos-finalization:v1:" + gen).encode()).hexdigest()[:32])
        self.assertEqual(canonical_finalization_id(gen), expected_id)
        row = self._begin(gen)
        self.assertEqual(row["finalization_id"], expected_id)
        self._restart()
        row2 = self.gate.get_finalization_run(gen)
        self.assertIsNotNone(row2)
        self.assertEqual(dict(row2), dict(row))
        # Re-begin after restart converges on the same row, not a
        # duplicate.
        row3 = self._begin(gen)
        self.assertEqual(dict(row3), dict(row))
        self.assertEqual(self._count("finalization_runs"), 1)


# ------------------------------------------------------------------ R13-03
class TestR1303(FinalizationBase):
    def test_R13_03_deterministic_snapshot(self):
        """R13-03: the same logical desired spec yields the same head
        snapshot hash in two independent DBs; the release manifest pins
        exactly that hash as desired_state_hash."""
        db2 = os.path.join(self.tmp, "t2.db")
        store2 = open_store(db2, clock=self.clock)
        migrate(store2)
        self._stores.append(store2)
        g2 = TransitionGate(store2)
        g2.create_task("t", {"objective": "r13"}, {"usd": 1}, "test")
        self.gate.set_desired_item("w-1", {"task_id": "t"}, "test")
        g2.set_desired_item("w-1", {"task_id": "t", "stage_id": None,
                                    "max_attempts": 3, "policy": {}},
                            "test")
        h1 = self.gate.get_desired_head()
        h2 = g2.get_desired_head()
        self.assertEqual(h1["version"], 1)
        self.assertEqual(h2["version"], 1)
        self.assertEqual(h1["snapshot_hash"], h2["snapshot_hash"])
        # The release manifest pins exactly the head's snapshot hash:
        # re-collect the (read-only) evidence and rebuild the manifest
        # with the public pure helpers — the row's manifest_hash must
        # equal the independently recomputed one. Retire the leftover
        # w-1 item first so the only active item is the completed w-3
        # (an active item with no materialized job would BLOCK).
        self.gate.retire_desired_item("w-1", "test")
        gen, _jids = self._healthy_generation(["w-3"])
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "READY")
        head = self.gate.get_desired_head()
        ev = self.gate._collect_finalization_evidence(
            self.gate.store.conn, gen, row["desired_state_version"])
        self.assertEqual(ev["desired_state_hash"],
                         head["snapshot_hash"])
        manifest = build_finalization_manifest(ev, [])
        self.assertEqual(manifest["desired_state_hash"],
                         head["snapshot_hash"])
        self.assertEqual(finalization_manifest_hash(manifest),
                         row["manifest_hash"])
        store2.close()


# ------------------------------------------------------------------ R13-04
class TestR1304(FinalizationBase):
    def test_R13_04_missing_job_blocks(self):
        """R13-04: a desired item with no materialized job blocks with
        DESIRED_WORK_UNSATISFIED (never silently releasable)."""
        self._set("w-1")
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        blockers = self._blockers(row)
        self.assertEqual(len(blockers), 1)
        self.assertEqual(blockers[0]["category"],
                         "DESIRED_WORK_UNSATISFIED")
        self.assertEqual(blockers[0]["identity"], "w-1")
        # Still blocked after a no-op re-evaluation.
        row2 = self._eval(gen)
        self.assertEqual(row2["state"], "BLOCKED")
        self.assertEqual(self._blockers(row2), blockers)


# ------------------------------------------------------------------ R13-05
class TestR1305(FinalizationBase):
    def test_R13_05_pending_job_blocks(self):
        """R13-05: a PENDING desired job blocks."""
        self._set("w-1")
        job = self._ensure("w-1")
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        cats = self._cats(row)
        self.assertIn("DESIRED_WORK_UNSATISFIED", cats)
        ids = [b["identity"] for b in self._blockers(row)
               if b["category"] == "DESIRED_WORK_UNSATISFIED"]
        self.assertIn(job["job_id"], ids)


# ------------------------------------------------------------------ R13-06
class TestR1306(FinalizationBase):
    def test_R13_06_claimed_job_blocks(self):
        """R13-06: a CLAIMED desired job blocks (desired-unsatisfied +
        active execution)."""
        self._set("w-1")
        job = self._ensure("w-1")
        jid = job["job_id"]
        self.assertTrue(
            self.gate.claim_job_bounded(jid, "w-1", 60.0, "test", 4))
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        cats = self._cats(row)
        self.assertIn("DESIRED_WORK_UNSATISFIED", cats)
        self.assertIn("ACTIVE_EXECUTION", cats)
        self.assertIn(jid, [b["identity"] for b in self._blockers(row)
                            if b["category"] == "ACTIVE_EXECUTION"])


# ------------------------------------------------------------------ R13-07
class TestR1307(FinalizationBase):
    def test_R13_07_running_job_blocks(self):
        """R13-07: a RUNNING desired job blocks."""
        self._set("w-1")
        job = self._ensure("w-1")
        jid = job["job_id"]
        self._claim_run(jid)
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("ACTIVE_EXECUTION", self._cats(row))
        self.assertIn(jid, [b["identity"] for b in self._blockers(row)
                            if b["category"] == "ACTIVE_EXECUTION"])


# ------------------------------------------------------------------ R13-08
class TestR1308(FinalizationBase):
    def test_R13_08_committing_job_blocks(self):
        """R13-08: a COMMITTING desired job blocks (completion in
        flight is still active execution)."""
        self._set("w-1")
        job = self._ensure("w-1")
        jid = job["job_id"]
        wid, tok = self._claim_run(jid)
        art = self.gate.stage_artifact(
            job_id=jid, worker_id=wid, fencing_token=tok, task_id="t",
            kind="test", data=b"r13-08-bytes", actor=f"worker:{wid}")
        self.gate.begin_commit(jid, wid, tok,
                               artifact_id=art["artifact_id"],
                               actor=f"worker:{wid}")
        self.assertEqual(self.gate.get_job(jid)["status"], "COMMITTING")
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("ACTIVE_EXECUTION", self._cats(row))


# ------------------------------------------------------------------ R13-09
class TestR1309(FinalizationBase):
    def test_R13_09_verifying_job_blocks(self):
        """R13-09: a VERIFYING job row blocks. The gate graph has no
        VERIFYING job state, so this is injected defensively via raw
        SQL (the fault being modeled): the evaluator must still treat
        it as active execution, never ignore it."""
        self._set("w-1")
        job = self._ensure("w-1")
        jid = job["job_id"]
        self._claim_run(jid)
        raw = self._raw()
        raw.execute("UPDATE jobs SET status='VERIFYING' WHERE job_id=?",
                    (jid,))
        raw.commit()
        raw.close()
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("ACTIVE_EXECUTION", self._cats(row))
        self.assertIn(jid, [b["identity"] for b in self._blockers(row)
                            if b["category"] == "ACTIVE_EXECUTION"])


# ------------------------------------------------------------------ R13-10
class TestR1310(FinalizationBase):
    def test_R13_10_uncertain_job_blocks(self):
        """R13-10: an UNCERTAIN desired job blocks with
        UNCERTAIN_EXECUTION (completion state unknown)."""
        self._set("w-1")
        job = self._ensure("w-1")
        jid = job["job_id"]
        self._claim_run(jid)
        self.gate.transition_job(jid, "UNCERTAIN", "test",
                                 reason="r13-test")
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("UNCERTAIN_EXECUTION", self._cats(row))
        self.assertIn(jid, [b["identity"] for b in self._blockers(row)
                            if b["category"] == "UNCERTAIN_EXECUTION"])


# ------------------------------------------------------------------ R13-11
class TestR1311(FinalizationBase):
    def test_R13_11_failed_job_blocks(self):
        """R13-11: a FAILED desired job blocks (desired work
        unsatisfied; no recovery bypass)."""
        self._set("w-1")
        job = self._ensure("w-1")
        jid = job["job_id"]
        wid, tok = self._claim_run(jid)
        self.gate.fail_job_execution(jid, wid, tok, actor="test",
                                     reason="boom", evidence={})
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("DESIRED_WORK_UNSATISFIED", self._cats(row))


# ------------------------------------------------------------------ R13-12
class TestR1312(FinalizationBase):
    def test_R13_12_blocked_job_blocks(self):
        """R13-12: a BLOCKED desired job blocks with both
        DESIRED_WORK_UNSATISFIED and HUMAN_GATE."""
        self._set("w-1")
        job = self._ensure("w-1")
        jid = job["job_id"]
        wid, tok = self._claim_run(jid)
        self.gate.fail_job_execution(jid, wid, tok, actor="test",
                                     reason="boom", evidence={})
        self.gate.transition_job(jid, "BLOCKED", "test",
                                 reason="human decision needed")
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        cats = self._cats(row)
        self.assertIn("DESIRED_WORK_UNSATISFIED", cats)
        self.assertIn("HUMAN_GATE", cats)
        self.assertIn(jid, [b["identity"] for b in self._blockers(row)
                            if b["category"] == "HUMAN_GATE"])


# ------------------------------------------------------------------ R13-13
class TestR1313(FinalizationBase):
    def test_R13_13_complete_satisfies(self):
        """R13-13: a COMPLETE desired item contributes zero blockers —
        only the still-pending item blocks."""
        self._set("w-13a")
        self._set("w-13b")
        gen = canonical_release_generation(2)
        ja = self._ensure("w-13a")
        self._ensure("w-13b")
        wid, _tok = self._claim_run(ja["job_id"])
        self._complete(ja["job_id"], wid)
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        for b in self._blockers(row):
            self.assertNotIn(ja["job_id"], (b["identity"],),
                             f"COMPLETE job must not block: {b}")
            self.assertNotEqual(b["identity"], "w-13a")
        self.assertIn("DESIRED_WORK_UNSATISFIED", self._cats(row))


# ------------------------------------------------------------------ R13-14
class TestR1314(FinalizationBase):
    def test_R13_14_valid_artifact_ready(self):
        """R13-14: the healthy path — one desired item COMPLETE with a
        valid artifact evaluates READY with zero blockers, a staged and
        VERIFIED release checkpoint, and a deterministic manifest hash."""
        gen, jids = self._healthy_generation(["w-14"])
        jid = jids[0]
        art = self.gate.get_artifact(
            self.gate.get_job(jid)["result_artifact_id"])
        self.assertEqual(art["status"], "VALIDATED")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(self._blockers(row), [])
        self.assertIsNotNone(row["manifest_hash"])
        self.assertEqual(len(row["manifest_hash"]), 64)
        self.assertIsNotNone(row["checkpoint_id"])
        ck = self.gate.store.conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?",
            (row["checkpoint_id"],)).fetchone()
        self.assertIsNotNone(ck)
        self.assertEqual(ck["verification_status"], "VERIFIED")
        receipt = json.loads(ck["verification_receipt"])
        self.assertTrue(receipt["release"])
        self.assertEqual(receipt["checkpoint_id"], row["checkpoint_id"])
        self.assertEqual(ck["trigger"], "pre_risky_operation")
        # The release checkpoint identity is the pure R5 identity over
        # the generation's artifacts.
        self.assertEqual(
            row["checkpoint_id"],
            _release_checkpoint_id(self._release_entries_for(gen, jids)))


# ------------------------------------------------------------------ R13-15
class TestR1315(FinalizationBase):
    def test_R13_15_missing_artifact_blocks(self):
        """R13-15: a COMPLETE job with no recorded artifact (fault
        injected: result_artifact_id nulled) blocks with
        MISSING_ARTIFACT."""
        gen, jids = self._healthy_generation(["w-15"])
        jid = jids[0]
        raw = self._raw()
        raw.execute("UPDATE jobs SET result_artifact_id=NULL"
                    " WHERE job_id=?", (jid,))
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("MISSING_ARTIFACT", self._cats(row))
        self.assertIn(jid, [b["identity"] for b in self._blockers(row)
                            if b["category"] == "MISSING_ARTIFACT"])


# ------------------------------------------------------------------ R13-16
class TestR1316(FinalizationBase):
    def test_R13_16_absent_artifact_bytes_block(self):
        """R13-16: a COMPLETE job whose staged artifact bytes are gone
        (fault injected: bytes file deleted) blocks with
        INVALID_ARTIFACT."""
        gen, jids = self._healthy_generation(["w-16"])
        jid = jids[0]
        aid = self.gate.get_job(jid)["result_artifact_id"]
        uri = self.gate.get_artifact(aid)["uri"]
        os.remove(uri)
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        bad = [b for b in self._blockers(row)
               if b["category"] == "INVALID_ARTIFACT"]
        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0]["identity"], aid)
        self.assertIn("bytes missing", bad[0]["detail"])


# ------------------------------------------------------------------ R13-17
class TestR1317(FinalizationBase):
    def test_R13_17_tampered_artifact_bytes_block(self):
        """R13-17: staged bytes that no longer re-hash to the recorded
        content hash (fault injected: bytes overwritten) block with
        INVALID_ARTIFACT — post-verification mutation is detected."""
        gen, jids = self._healthy_generation(["w-17"])
        jid = jids[0]
        aid = self.gate.get_job(jid)["result_artifact_id"]
        uri = self.gate.get_artifact(aid)["uri"]
        with open(uri, "wb") as f:
            f.write(b"tampered-by-r13-17")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        bad = [b for b in self._blockers(row)
               if b["category"] == "INVALID_ARTIFACT"]
        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0]["identity"], aid)
        self.assertIn("re-hash", bad[0]["detail"])


# ------------------------------------------------------------------ R13-18
class TestR1318(FinalizationBase):
    def test_R13_18_bad_provenance_blocks(self):
        """R13-18: an artifact staged for a DIFFERENT job (fault
        injected: artifacts.job_id rewritten) blocks with
        INVALID_ARTIFACT."""
        gen, jids = self._healthy_generation(["w-18"])
        jid = jids[0]
        aid = self.gate.get_job(jid)["result_artifact_id"]
        raw = self._raw()
        raw.execute("UPDATE artifacts SET job_id='some-other-job'"
                    " WHERE artifact_id=?", (aid,))
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        bad = [b for b in self._blockers(row)
               if b["category"] == "INVALID_ARTIFACT"]
        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0]["identity"], aid)
        self.assertIn("not", bad[0]["detail"])


# ------------------------------------------------------------------ R13-19
class TestR1319(FinalizationBase):
    def test_R13_19_missing_validator_receipt_blocks(self):
        """R13-19: a COMPLETE job whose PASS validation receipt is gone
        (fault injected: validations row deleted) blocks with
        INVALID_ARTIFACT."""
        gen, jids = self._healthy_generation(["w-19"])
        jid = jids[0]
        aid = self.gate.get_job(jid)["result_artifact_id"]
        raw = self._raw()
        raw.execute("DELETE FROM validations WHERE artifact_id=?",
                    (aid,))
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        bad = [b for b in self._blockers(row)
               if b["category"] == "INVALID_ARTIFACT"]
        self.assertEqual(len(bad), 1)
        self.assertIn("axos-structural/1", bad[0]["detail"])


# ------------------------------------------------------------------ R13-20
class TestR1320(FinalizationBase):
    def test_R13_20_foreign_task_checkpoint_blocks(self):
        """R13-20: a release checkpoint bound to a FOREIGN task (fault
        injected: staged under another task id with the exact release
        identity) blocks with CONTRADICTORY_STATE."""
        gen, jids = self._healthy_generation(["w-20"])
        entries = self._release_entries_for(gen, jids)
        self._mk_task("t-foreign")
        staged = self.gate.stage_checkpoint(
            "t-foreign", actor="test", manifest=entries,
            trigger="pre_risky_operation",
            versions={"methodology_version": "r13-release-v1"})
        self.assertEqual(staged["checkpoint_id"],
                         _release_checkpoint_id(entries))
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        bad = [b for b in self._blockers(row)
               if b["category"] == "CONTRADICTORY_STATE"]
        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0]["identity"], staged["checkpoint_id"])


# ------------------------------------------------------------------ R13-21
class TestR1321(FinalizationBase):
    def test_R13_21_checkpoint_identity_mismatch_blocks(self):
        """R13-21: a checkpoint row whose canonical_manifest no longer
        hashes to its checkpoint_id (fault injected: manifest tampered)
        blocks with CONTRADICTORY_STATE."""
        gen, jids = self._healthy_generation(["w-21"])
        entries = self._release_entries_for(gen, jids)
        self.gate._ensure_finalization_task(gen, "test")
        staged = self.gate.stage_checkpoint(
            self._fin_task_id(gen), actor="test", manifest=entries,
            trigger="pre_risky_operation",
            versions={"methodology_version": "r13-release-v1"})
        raw = self._raw()
        raw.execute("UPDATE checkpoints SET canonical_manifest=?"
                    " WHERE checkpoint_id=?",
                    ("tampered", staged["checkpoint_id"]))
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        bad = [b for b in self._blockers(row)
               if b["category"] == "CONTRADICTORY_STATE"]
        self.assertEqual(len(bad), 1)
        self.assertIn("sha256", bad[0]["detail"])


# ------------------------------------------------------------------ R13-22
class TestR1322(FinalizationBase):
    def test_R13_22_verified_without_release_attestation_blocks(self):
        """R13-22: a VERIFIED release checkpoint that carries no release
        attestation (verified with release=False) blocks with
        INVALID_CHECKPOINT."""
        gen, jids = self._healthy_generation(["w-22"])
        entries = self._release_entries_for(gen, jids)
        self.gate._ensure_finalization_task(gen, "test")
        staged = self.gate.stage_checkpoint(
            self._fin_task_id(gen), actor="test", manifest=entries,
            trigger="pre_risky_operation",
            versions={"methodology_version": "r13-release-v1"})
        verified = self.gate.verify_checkpoint(
            staged["checkpoint_id"], actor="test", release=False)
        self.assertEqual(verified["verification_status"], "VERIFIED")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        bad = [b for b in self._blockers(row)
               if b["category"] == "INVALID_CHECKPOINT"]
        self.assertEqual(len(bad), 1)
        self.assertIn("attestation", bad[0]["detail"])


# ------------------------------------------------------------------ R13-23
class TestR1323(FinalizationBase):
    def test_R13_23_corrupt_checkpoint_blocks(self):
        """R13-23: a CORRUPT release checkpoint (fault injected: status
        forced CORRUPT — terminal per R5) blocks with
        INVALID_CHECKPOINT."""
        gen, jids = self._healthy_generation(["w-23"])
        entries = self._release_entries_for(gen, jids)
        self.gate._ensure_finalization_task(gen, "test")
        staged = self.gate.stage_checkpoint(
            self._fin_task_id(gen), actor="test", manifest=entries,
            trigger="pre_risky_operation",
            versions={"methodology_version": "r13-release-v1"})
        raw = self._raw()
        raw.execute("UPDATE checkpoints SET verification_status='CORRUPT'"
                    " WHERE checkpoint_id=?", (staged["checkpoint_id"],))
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        bad = [b for b in self._blockers(row)
               if b["category"] == "INVALID_CHECKPOINT"]
        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0]["identity"], staged["checkpoint_id"])


# ------------------------------------------------------------------ R13-24
class TestR1324(FinalizationBase):
    def test_R13_24_unverified_checkpoint_gets_verified(self):
        """R13-24: a staged-but-UNVERIFIED release checkpoint (e.g. left
        by an interrupted evaluation) is verified by the next
        evaluation — not a blocker — and the run goes READY."""
        gen, jids = self._healthy_generation(["w-24"])
        entries = self._release_entries_for(gen, jids)
        self.gate._ensure_finalization_task(gen, "test")
        staged = self.gate.stage_checkpoint(
            self._fin_task_id(gen), actor="test", manifest=entries,
            trigger="pre_risky_operation",
            versions={"methodology_version": "r13-release-v1"})
        self.assertEqual(staged["verification_status"], "UNVERIFIED")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(self._blockers(row), [])
        ck = self.gate.store.conn.execute(
            "SELECT verification_status, verification_receipt"
            " FROM checkpoints WHERE checkpoint_id=?",
            (row["checkpoint_id"],)).fetchone()
        self.assertEqual(ck["verification_status"], "VERIFIED")
        self.assertTrue(json.loads(ck["verification_receipt"])["release"])


# ------------------------------------------------------------------ R13-25
class TestR1325(FinalizationBase):
    def test_R13_25_uncertain_recovery_attempt_blocks(self):
        """R13-25: an UNCERTAIN recovery attempt blocks with both
        ACTIVE_RECOVERY and UNCERTAIN_EXECUTION."""
        gen, jids = self._healthy_generation(["w-25"])
        self.gate.create_job("s-25", "t", None, "test")
        self.assertTrue(
            self.gate.claim_job_bounded("s-25", "w-25", 600.0, "test", 10))
        tok = self.gate.get_job("s-25")["fencing_token"]
        self.gate.transition_job("s-25", "RUNNING", "worker:w-25")
        inc = self.gate.find_or_create_recovery_incident(
            "s-25", "DEAD", {"reason": "r13-25"}, "test")
        att = self.gate.create_recovery_attempt(
            incident_id=inc["incident_id"], job_id="s-25",
            fencing_token=tok, rung=1, rung_name="requeue",
            action={"name": "requeue"}, failure_class="DEAD",
            success_criterion="s", failure_criterion="f",
            evidence_before={}, actor="test")
        self.assertTrue(
            self.gate.claim_recovery_attempt(att["attempt_id"], "test"))
        self.gate.mark_recovery_attempt_uncertain(
            att["attempt_id"], "r13-25", "test")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        cats = self._cats(row)
        self.assertIn("ACTIVE_RECOVERY", cats)
        self.assertIn("UNCERTAIN_EXECUTION", cats)
        unc = [b for b in self._blockers(row)
               if b["category"] == "UNCERTAIN_EXECUTION"]
        self.assertIn(att["attempt_id"],
                      [b["identity"] for b in unc])
        # The scratch job itself is RUNNING -> also ACTIVE_EXECUTION.
        self.assertIn("ACTIVE_EXECUTION", cats)


# ------------------------------------------------------------------ R13-26
class TestR1326(FinalizationBase):
    def test_R13_26_open_incident_blocks(self):
        """R13-26: an open recovery incident (no outcome) blocks with
        ACTIVE_RECOVERY."""
        gen, _jids = self._healthy_generation(["w-26"])
        inc = self.gate.find_or_create_recovery_incident(
            "scratch-26", "DEAD", {"reason": "r13-26"}, "test")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        rec = [b for b in self._blockers(row)
               if b["category"] == "ACTIVE_RECOVERY"]
        self.assertEqual(len(rec), 1)
        self.assertEqual(rec[0]["identity"], inc["incident_id"])


# ------------------------------------------------------------------ R13-27
class TestR1327(FinalizationBase):
    def test_R13_27_running_recovery_attempt_blocks(self):
        """R13-27: a RUNNING recovery attempt blocks with
        ACTIVE_RECOVERY."""
        gen, _jids = self._healthy_generation(["w-27"])
        inc = self.gate.find_or_create_recovery_incident(
            "scratch-27", "DEAD", {"reason": "r13-27"}, "test")
        att = self.gate.create_recovery_attempt(
            incident_id=inc["incident_id"], job_id="scratch-27",
            fencing_token=7, rung=1, rung_name="requeue",
            action={"name": "requeue"}, failure_class="DEAD",
            success_criterion="s", failure_criterion="f",
            evidence_before={}, actor="test")
        self.assertTrue(
            self.gate.claim_recovery_attempt(att["attempt_id"], "test"))
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        rec = [b for b in self._blockers(row)
               if b["category"] == "ACTIVE_RECOVERY"]
        self.assertIn(att["attempt_id"],
                      [b["identity"] for b in rec])


# ------------------------------------------------------------------ R13-28
class TestR1328(FinalizationBase):
    def test_R13_28_escalated_without_policy_blocks(self):
        """R13-28: an escalated incident with no terminal R9 policy
        state blocks with ACTIVE_RECOVERY."""
        gen, _jids = self._healthy_generation(["w-28"])
        inc = self.gate.find_or_create_recovery_incident(
            "scratch-28", "DEAD", {"reason": "r13-28"}, "test")
        self.gate.set_incident_escalated(inc["incident_id"],
                                         "r13-28", "test")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        rec = [b for b in self._blockers(row)
               if b["category"] == "ACTIVE_RECOVERY"]
        self.assertEqual(len(rec), 1)
        self.assertEqual(rec[0]["identity"], inc["incident_id"])
        self.assertIn("terminal", rec[0]["detail"])


# ------------------------------------------------------------------ R13-29
class TestR1329(FinalizationBase):
    def test_R13_29_escalated_with_terminal_policy_ok(self):
        """R13-29: an escalated incident WITH a terminal R9 policy state
        does not block — the escalation was consumed by policy."""
        gen, _jids = self._healthy_generation(["w-29"])
        inc = self.gate.find_or_create_recovery_incident(
            "scratch-29", "DEAD", {"reason": "r13-29"}, "test")
        self.gate.set_incident_escalated(inc["incident_id"],
                                         "r13-29", "test")
        pol = self.gate.ensure_recovery_policy(
            inc["incident_id"], policy_version="r9-v1", current_rung=5,
            rung_name="escalate", incident_budget=3,
            per_rung_budgets={"5": 3}, actor="test")
        self.gate.cas_update_recovery_policy(
            inc["incident_id"], pol["version"],
            {"terminal_state": "PAUSED_FOR_HUMAN"}, "test")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(self._blockers(row), [])


# ------------------------------------------------------------------ R13-30
class TestR1330(FinalizationBase):
    def test_R13_30_unconsumed_outcome_blocks(self):
        """R13-30: an incident with an outcome but no terminal policy
        row state (unconsumed by R9) blocks with ACTIVE_RECOVERY."""
        gen, _jids = self._healthy_generation(["w-30"])
        inc = self.gate.find_or_create_recovery_incident(
            "scratch-30", "DEAD", {"reason": "r13-30"}, "test")
        self.gate.ensure_recovery_policy(
            inc["incident_id"], policy_version="r9-v1", current_rung=1,
            rung_name="requeue", incident_budget=3,
            per_rung_budgets={"1": 3}, actor="test")
        self.gate.set_incident_outcome(inc["incident_id"], "recovered",
                                       "r13-30", "test")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        rec = [b for b in self._blockers(row)
               if b["category"] == "ACTIVE_RECOVERY"]
        self.assertEqual(len(rec), 1)
        self.assertEqual(rec[0]["identity"], inc["incident_id"])


# ------------------------------------------------------------------ R13-31
class TestR1331(FinalizationBase):
    def test_R13_31_open_global_breaker_blocks(self):
        """R13-31: an OPEN breaker on GLOBAL blocks the release."""
        gen, _jids = self._healthy_generation(["w-31"])
        self._open_breaker("GLOBAL", "global")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        brk = [b for b in self._blockers(row)
               if b["category"] == "OPEN_BREAKER"]
        self.assertEqual(len(brk), 1)
        self.assertEqual(brk[0]["identity"], "GLOBAL:global")


# ------------------------------------------------------------------ R13-32
class TestR1332(FinalizationBase):
    def test_R13_32_open_job_breaker_blocks(self):
        """R13-32: an OPEN breaker on a generation JOB scope blocks."""
        gen, jids = self._healthy_generation(["w-32"])
        self._open_breaker("JOB", jids[0])
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        brk = [b for b in self._blockers(row)
               if b["category"] == "OPEN_BREAKER"]
        self.assertEqual(len(brk), 1)
        self.assertEqual(brk[0]["identity"], f"JOB:{jids[0]}")


# ------------------------------------------------------------------ R13-33
class TestR1333(FinalizationBase):
    def test_R13_33_unrelated_breaker_does_not_block(self):
        """R13-33: an OPEN breaker on an unrelated scope does NOT block
        the release."""
        gen, _jids = self._healthy_generation(["w-33"])
        self._open_breaker("JOB", "some-other-job")
        self._open_breaker("DESIRED", "some-other-item")
        self._open_breaker("TASK", "some-other-task")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(self._blockers(row), [])


# ------------------------------------------------------------------ R13-34
class TestR1334(FinalizationBase):
    def test_R13_34_half_open_required_scope_blocks(self):
        """R13-34: a HALF_OPEN breaker on a required scope (the
        generation's task) blocks."""
        gen, _jids = self._healthy_generation(["w-34"])
        self._half_open_breaker("TASK", "t")
        self.assertEqual(
            self.gate.get_breaker_state("TASK", "t")["state"],
            "HALF_OPEN")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        brk = [b for b in self._blockers(row)
               if b["category"] == "OPEN_BREAKER"]
        self.assertEqual(len(brk), 1)
        self.assertEqual(brk[0]["identity"], "TASK:t")


# ------------------------------------------------------------------ R13-35
class TestR1335(FinalizationBase):
    def test_R13_35_paused_for_human_blocks(self):
        """R13-35: a task PAUSED_FOR_HUMAN blocks with HUMAN_GATE."""
        self.gate.transition_task("t", "AUTHORIZED", "test")
        self.gate.transition_task("t", "PAUSED_FOR_HUMAN", "test",
                                  reason="r13-test")
        gen, _jids = self._healthy_generation(["w-35"])
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        hg = [b for b in self._blockers(row)
              if b["category"] == "HUMAN_GATE"]
        self.assertTrue(hg)
        self.assertIn("t", [b["identity"] for b in hg])


# ------------------------------------------------------------------ R13-36
class TestR1336(FinalizationBase):
    def test_R13_36_manual_review_blocks(self):
        """R13-36: a task in MANUAL_REVIEW (injected defensively via raw
        SQL — no gate transition produces it) blocks with HUMAN_GATE."""
        gen, _jids = self._healthy_generation(["w-36"])
        raw = self._raw()
        raw.execute("UPDATE tasks SET status='MANUAL_REVIEW'"
                    " WHERE task_id='t'")
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        hg = [b for b in self._blockers(row)
              if b["category"] == "HUMAN_GATE"]
        self.assertTrue(hg)
        self.assertIn("t", [b["identity"] for b in hg])


# ------------------------------------------------------------------ R13-37
class TestR1337(FinalizationBase):
    def test_R13_37_human_gate_never_auto_cleared(self):
        """R13-37: the finalizer never clears a human gate — after
        evaluation the task is still PAUSED_FOR_HUMAN, the job still
        BLOCKED, and a second evaluation still reports HUMAN_GATE."""
        self.gate.transition_task("t", "AUTHORIZED", "test")
        self.gate.transition_task("t", "PAUSED_FOR_HUMAN", "test",
                                  reason="r13-test")
        gen, jids = self._healthy_generation(["w-37"])
        jid = jids[0]
        # The job itself is COMPLETE; the human gate is on the task.
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("HUMAN_GATE", self._cats(row))
        self.assertEqual(self.gate.get_task("t")["status"],
                         "PAUSED_FOR_HUMAN")
        # A finalizer pass changes nothing about the gate.
        fin = self._new_fin()
        fin.evaluate_once()
        self.assertEqual(self.gate.get_task("t")["status"],
                         "PAUSED_FOR_HUMAN")
        row2 = self._eval(gen)
        self.assertEqual(row2["state"], "BLOCKED")
        self.assertIn("HUMAN_GATE", self._cats(row2))
        # Publish refuses: the run is not READY.
        with self.assertRaises(TransitionRejected):
            self._publish(gen)
        # And no task transition was ever journaled by the finalizer.
        for r in self.gate.store.conn.execute(
                "SELECT actor, event_type FROM ledger WHERE"
                " event_type LIKE 'task.%'").fetchall():
            self.assertNotEqual(r["actor"], "finalizer")


# ------------------------------------------------------------------ R13-38
class TestR1338(FinalizationBase):
    def test_R13_38_evaluate_version_mismatch_conflicts(self):
        """R13-38: evaluating with a stale expected_version raises
        FinalizationConflict; the authoritative row is untouched."""
        gen, _jids = self._healthy_generation(["w-38"])
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "READY")
        before = dict(self.gate.get_finalization_run(gen))
        with self.assertRaises(FinalizationConflict):
            self.gate.evaluate_finalization(gen, 1, "test")
        self.assertEqual(dict(self.gate.get_finalization_run(gen)),
                         before)
        # Invalid versions are caller bugs, not conflicts.
        for bad in (0, -1, True, "2"):
            with self.assertRaises(TransitionRejected):
                self.gate.evaluate_finalization(gen, bad, "test")


# ------------------------------------------------------------------ R13-39
class TestR1339(FinalizationBase):
    def test_R13_39_publish_version_mismatch_conflicts(self):
        """R13-39: publishing with a stale expected_version raises
        FinalizationConflict with zero mutation; the correct version
        then publishes."""
        gen, _jids = self._healthy_generation(["w-39"])
        self._begin(gen)
        ready = self._eval(gen)
        self.assertEqual(ready["state"], "READY")
        with self.assertRaises(FinalizationConflict):
            self.gate.publish_finalization(gen, 1,
                                           ready["manifest_hash"],
                                           "test")
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(row["version"], ready["version"])
        published = self._publish(gen)
        self.assertEqual(published["state"], "FINALIZED")


# ------------------------------------------------------------------ R13-40
class TestR1340(FinalizationBase):
    def test_R13_40_generation_version_binding(self):
        """R13-40: the release_generation is the canonical identity of
        the desired_state_version ("ds-v" + version) — a mismatched
        begin pair is rejected, and re-beginning an existing run with
        a different pinned version fails closed."""
        self._set("w-40")
        gen = canonical_release_generation(1)
        # Mismatched pair: generation names v2, version says 1.
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.begin_finalization_run("ds-v2", 1, "test")
        self.assertIn("not the canonical identity", str(ctx.exception))
        # Correct pair begins.
        row = self.gate.begin_finalization_run(gen, 1, "test")
        self.assertEqual(row["state"], "OPEN")
        # Re-begin with the same pair is idempotent.
        again = self.gate.begin_finalization_run(gen, 1, "test")
        self.assertEqual(dict(again), dict(row))
        # Contradictory re-begin: the canonical check passes for
        # (gen, v1), but the existing row pins a different version
        # (fault injected) — fails closed.
        raw = self._raw()
        raw.execute("UPDATE finalization_runs SET desired_state_version=2"
                    " WHERE release_generation=?", (gen,))
        raw.commit()
        raw.close()
        with self.assertRaises(TransitionRejected) as ctx2:
            self.gate.begin_finalization_run(gen, 1, "test")
        self.assertIn("contradictory", str(ctx2.exception))
        # Non-string / malformed generations are rejected too.
        for bad in (None, "", "v1", "ds-v", "ds-vX", "DS-V1"):
            with self.assertRaises((TransitionRejected, TypeError)):
                self.gate.begin_finalization_run(bad, 1, "test")
        self.assertEqual(
            self.gate.get_finalization_run(gen)["state"], "OPEN")


# ------------------------------------------------------------------ R13-41
class TestR1341(FinalizationBase):
    def test_R13_41_stale_generation_fails_closed(self):
        """R13-41: when the desired-state head moves after evaluation,
        both evaluate and publish fail closed with STALE_GENERATION —
        zero mutation, the READY verdict is not recorded over."""
        gen, _jids = self._healthy_generation(["w-41"])
        self._begin(gen)
        ready = self._eval(gen)
        self.assertEqual(ready["state"], "READY")
        version = ready["version"]
        self._set("w-41b")  # head moves: v1 -> v2
        self.assertEqual(self._head_version(), 2)
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.evaluate_finalization(gen, version, "test")
        self.assertIn("STALE_GENERATION", str(ctx.exception))
        with self.assertRaises(TransitionRejected) as ctx2:
            self.gate.publish_finalization(gen, version,
                                           ready["manifest_hash"],
                                           "test")
        self.assertIn("STALE_GENERATION", str(ctx2.exception))
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(row["version"], version)
        self.assertEqual(row["manifest_hash"], ready["manifest_hash"])


# ------------------------------------------------------------------ R13-42
class TestR1342(FinalizationBase):
    def test_R13_42_manifest_deterministic_across_dbs(self):
        """R13-42: identical fixtures in two independent DBs produce
        the identical manifest_hash — the manifest is a pure function
        of the evidence."""
        db2 = os.path.join(self.tmp, "t2.db")
        store2 = open_store(db2, clock=self.clock)
        migrate(store2)
        self._stores.append(store2)
        g2 = TransitionGate(store2)
        g2.create_task("t", {"objective": "r13"}, {"usd": 1}, "test")
        data = b"r13-42-deterministic-bytes"
        for gate, dwid in ((self.gate, "w-42"), (g2, "w-42")):
            gate.set_desired_item(
                dwid, {"task_id": "t", "stage_id": "s",
                       "max_attempts": 3, "policy": {}}, "test")
            job, _ = gate.ensure_job_for_desired_state(
                desired_work_id=dwid, task_id="t", stage_id="s",
                max_attempts=3, policy={}, desired_version=1,
                actor="reconciler")
            jid = job["job_id"]
            gate.claim_job_bounded(jid, "w-42", 60.0, "test", 4)
            gate.transition_job(jid, "RUNNING", "worker:w-42")
            tok = gate.get_job(jid)["fencing_token"]
            r5_complete(gate=gate, job_id=jid, worker_id="w-42",
                        fencing_token=tok, task_id="t",
                        outcome="SUCCESS", evidence={"ok": True},
                        actor="worker:w-42", data=data)
        gen = "ds-v1"
        r1 = self.gate.begin_finalization_run(gen, 1, "test")
        r1 = self.gate.evaluate_finalization(
            gen, r1["version"], "test")
        r2 = g2.begin_finalization_run(gen, 1, "test")
        r2 = g2.evaluate_finalization(gen, r2["version"], "test")
        self.assertEqual(r1["state"], "READY")
        self.assertEqual(r2["state"], "READY")
        self.assertEqual(r1["manifest_hash"], r2["manifest_hash"])
        self.assertEqual(r1["checkpoint_id"], r2["checkpoint_id"])
        store2.close()


# ------------------------------------------------------------------ R13-43
class TestR1343(FinalizationBase):
    def test_R13_43_manifest_changes_with_artifact_hash(self):
        """R13-43: the same logical generation with different artifact
        bytes produces a different manifest_hash."""
        hashes = []
        for i, data in ((0, b"r13-43-bytes-A"), (1, b"r13-43-bytes-B")):
            db = os.path.join(self.tmp, f"t43-{i}.db")
            st = open_store(db, clock=self.clock)
            migrate(st)
            self._stores.append(st)
            g = TransitionGate(st)
            g.create_task("t", {"objective": "r13"}, {"usd": 1}, "test")
            g.set_desired_item("w-43", {"task_id": "t"}, "test")
            job, _ = g.ensure_job_for_desired_state(
                desired_work_id="w-43", task_id="t", stage_id="s",
                max_attempts=3, policy={}, desired_version=1,
                actor="reconciler")
            jid = job["job_id"]
            g.claim_job_bounded(jid, "w-43", 60.0, "test", 4)
            g.transition_job(jid, "RUNNING", "worker:w-43")
            tok = g.get_job(jid)["fencing_token"]
            r5_complete(gate=g, job_id=jid, worker_id="w-43",
                        fencing_token=tok, task_id="t",
                        outcome="SUCCESS", evidence={"ok": True},
                        actor="worker:w-43", data=data)
            row = g.begin_finalization_run("ds-v1", 1, "test")
            row = g.evaluate_finalization("ds-v1", row["version"],
                                           "test")
            self.assertEqual(row["state"], "READY")
            hashes.append(row["manifest_hash"])
        self.assertNotEqual(hashes[0], hashes[1])


# ------------------------------------------------------------------ R13-44
class TestR1344(FinalizationBase):
    def test_R13_44_manifest_changes_with_generation(self):
        """R13-44: a new generation (desired-state change) produces a
        different manifest — a finalized manifest can never silently
        describe a later generation."""
        gen1, _jids = self._healthy_generation(["w-44a"])
        self._begin(gen1)
        h1 = self._eval(gen1)["manifest_hash"]
        self._set("w-44b")
        gen2 = canonical_release_generation(2)
        self.assertEqual(gen2, "ds-v2")
        job = self._ensure("w-44b")
        wid, _tok = self._claim_run(job["job_id"])
        self._complete(job["job_id"], wid)
        self._begin(gen2)
        h2 = self._eval(gen2)["manifest_hash"]
        self.assertNotEqual(h1, h2)


# ------------------------------------------------------------------ R13-45
class TestR1345(FinalizationBase):
    def test_R13_45_publish_revalidates_checkpoint(self):
        """R13-45: publish revalidates the release checkpoint record
        (identity, generation binding, release attestation) and
        succeeds when it is intact."""
        gen, jids = self._healthy_generation(["w-45"])
        self._begin(gen)
        ready = self._eval(gen)
        ck = self.gate.store.conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?",
            (ready["checkpoint_id"],)).fetchone()
        self.assertIsNotNone(ck)
        self.assertEqual(ck["verification_status"], "VERIFIED")
        self.assertEqual(ck["task_id"], self._fin_task_id(gen))
        receipt = json.loads(ck["verification_receipt"])
        self.assertTrue(receipt["release"])
        self.assertEqual(receipt["checkpoint_id"],
                         ready["checkpoint_id"])
        published = self._publish(gen)
        self.assertEqual(published["state"], "FINALIZED")
        self.assertEqual(published["checkpoint_id"],
                         ready["checkpoint_id"])


# ------------------------------------------------------------------ R13-46
class TestR1346(FinalizationBase):
    def test_R13_46_publish_detects_checkpoint_mutation(self):
        """R13-46: a checkpoint record mutated between evaluation and
        publication (fault injected: canonical_manifest tampered) is
        detected at publish — TransitionRejected, zero mutation, the
        run stays READY."""
        gen, _jids = self._healthy_generation(["w-46"])
        self._begin(gen)
        ready = self._eval(gen)
        self.assertEqual(ready["state"], "READY")
        raw = self._raw()
        raw.execute("UPDATE checkpoints SET canonical_manifest=?"
                    " WHERE checkpoint_id=?",
                    ("tampered", ready["checkpoint_id"]))
        raw.commit()
        raw.close()
        with self.assertRaises(TransitionRejected) as ctx:
            self._publish(gen)
        self.assertIn("identity mismatch", str(ctx.exception))
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(row["version"], ready["version"])
        self.assertEqual(self._count_events("finalization.published"), 0)


# ------------------------------------------------------------------ R13-47
class TestR1347(FinalizationBase):
    def test_R13_47_publication_atomicity(self):
        """R13-47: publication flips state/version/completed_at/result
        in one transaction with exactly one ledger event; the manifest
        and checkpoint are carried over unchanged."""
        gen, _jids = self._healthy_generation(["w-47"])
        self._begin(gen)
        ready = self._eval(gen)
        version = ready["version"]
        seq_before = self._ledger_seq_max()
        published = self._publish(gen)
        self.assertEqual(published["state"], "FINALIZED")
        self.assertEqual(published["version"], version + 1)
        self.assertIsNotNone(published["completed_at"])
        self.assertEqual(published["result"], "FINALIZED")
        self.assertEqual(published["manifest_hash"],
                         ready["manifest_hash"])
        self.assertEqual(published["checkpoint_id"],
                         ready["checkpoint_id"])
        pubs = [dict(r) for r in self.gate.store.conn.execute(
            "SELECT seq, payload FROM ledger WHERE event_type=?"
            " AND seq > ?", ("finalization.published",
                             seq_before)).fetchall()]
        self.assertEqual(len(pubs), 1)
        payload = json.loads(pubs[0]["payload"])
        self.assertEqual(payload["release_generation"], gen)
        self.assertEqual(payload["manifest_hash"],
                         ready["manifest_hash"])
        self.assertEqual(payload["checkpoint_id"],
                         ready["checkpoint_id"])
        self.assertEqual(payload["desired_state_version"], 1)


# ------------------------------------------------------------------ R13-48
class TestR1348(FinalizationBase):
    def test_R13_48_no_finalized_without_checkpoint(self):
        """R13-48: a READY run whose checkpoint_id was nulled (fault
        injected) cannot publish — TransitionRejected, zero mutation."""
        gen, _jids = self._healthy_generation(["w-48"])
        self._begin(gen)
        ready = self._eval(gen)
        self.assertEqual(ready["state"], "READY")
        raw = self._raw()
        raw.execute("UPDATE finalization_runs SET checkpoint_id=NULL"
                    " WHERE release_generation=?", (gen,))
        raw.commit()
        raw.close()
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.publish_finalization(gen, ready["version"],
                                           ready["manifest_hash"],
                                           "test")
        self.assertIn("checkpoint", str(ctx.exception).lower())
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(self._count_events("finalization.published"), 0)


# ------------------------------------------------------------------ R13-49
class TestR1349(FinalizationBase):
    def test_R13_49_no_publication_without_verification(self):
        """R13-49: a READY run whose checkpoint regressed to UNVERIFIED
        (fault injected) cannot publish — TransitionRejected."""
        gen, _jids = self._healthy_generation(["w-49"])
        self._begin(gen)
        ready = self._eval(gen)
        raw = self._raw()
        raw.execute("UPDATE checkpoints SET verification_status="
                    "'UNVERIFIED' WHERE checkpoint_id=?",
                    (ready["checkpoint_id"],))
        raw.commit()
        raw.close()
        with self.assertRaises(TransitionRejected) as ctx:
            self._publish(gen)
        self.assertIn("not VERIFIED", str(ctx.exception))
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(self._count_events("finalization.published"), 0)


# ------------------------------------------------------------------ R13-50
class TestR1350(FinalizationBase):
    def test_R13_50_two_finalizer_race_one_publication(self):
        """R13-50: two finalizers racing full passes produce exactly
        one publication — exactly one finalization.published event,
        the run FINALIZED, both finalizers observe the same record."""
        gen, _jids = self._healthy_generation(["w-50"])
        f1 = self._new_fin(actor="fin-a")
        f2 = self._new_fin(actor="fin-b")
        res = self._run_barrier([f1.evaluate_once, f2.evaluate_once])
        for i, v in res.items():
            self.assertNotIsInstance(v, Exception, f"finalizer {i}: {v}")
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)
        for v in res.values():
            fins = [f for f in v["finalized"]
                    if f["release_generation"] == gen]
            for f in fins:
                self.assertEqual(f["manifest_hash"],
                                 row["manifest_hash"])


# ------------------------------------------------------------------ R13-51
class TestR1351(FinalizationBase):
    def test_R13_51_three_finalizer_race_one_publication(self):
        """R13-51: three finalizers racing full passes produce exactly
        one publication."""
        gen, _jids = self._healthy_generation(["w-51"])
        fins = [self._new_fin(actor=f"fin-{i}") for i in range(3)]
        res = self._run_barrier([f.evaluate_once for f in fins])
        for i, v in res.items():
            self.assertNotIsInstance(v, Exception, f"finalizer {i}: {v}")
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)
        # Every publication report agrees on the one manifest.
        seen = {f["manifest_hash"]
                for v in res.values() for f in v["finalized"]}
        self.assertEqual(seen, {row["manifest_hash"]})


# ------------------------------------------------------------------ R13-52
class TestR1352(FinalizationBase):
    def test_R13_52_cas_loser_observes_winner(self):
        """R13-52: two gates evaluating on the same expected_version —
        exactly one wins the CAS; the loser gets FinalizationConflict,
        re-reads the winner's verdict, and never overwrites it. Each
        thread opens its own store (SQLite connections are
        thread-bound), mirroring the Finalizer's _gate_for_thread."""
        gen, _jids = self._healthy_generation(["w-52"])
        self._begin(gen)

        def evaluate_in_thread():
            st = open_store(self.db, clock=self.clock)
            try:
                g = TransitionGate(st)
                return g.evaluate_finalization(gen, 1, "test")
            finally:
                st.close()

        res = self._run_barrier([evaluate_in_thread,
                                 evaluate_in_thread])
        conflicts = [v for v in res.values()
                     if isinstance(v, FinalizationConflict)]
        winners = [v for v in res.values()
                   if not isinstance(v, Exception)]
        self.assertEqual(len(conflicts), 1,
                         f"results: {res}")
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0]["state"], "READY")
        self.assertEqual(winners[0]["version"], 2)
        # The loser re-reads: it observes the winner's READY, and a
        # re-evaluation on the current version is a no-op.
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(row["version"], 2)
        again = self.gate.evaluate_finalization(gen, 2, "test")
        self.assertEqual(again["state"], "READY")
        self.assertEqual(again["version"], 2)
        self.assertEqual(dict(again), dict(row))


# ------------------------------------------------------------------ R13-53
class TestR1353(FinalizationBase):
    def test_R13_53_crash_before_evaluation(self):
        """R13-53 crash point 1/10 — crash after begin, before any
        evaluation: the run is still OPEN; after restart the
        finalization converges to FINALIZED with exactly one
        publication."""
        gen, _jids = self._healthy_generation(["w-53"])
        self._begin(gen)
        # Crash: drop every handle without evaluating.
        self._restart()
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "OPEN")
        self.assertEqual(row["version"], 1)
        ready = self._eval(gen)
        self.assertEqual(ready["state"], "READY")
        published = self._publish(gen)
        self.assertEqual(published["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)


# ------------------------------------------------------------------ R13-54
class TestR1354(FinalizationBase):
    def test_R13_54_crash_after_evidence_snapshot(self):
        """R13-54 crash point 2/10 — crash right after the evidence
        snapshot is collected: _Crashed propagates (it is NOT
        converted to a FAILED verdict); after restart the evaluation
        converges to FINALIZED."""
        gen, _jids = self._healthy_generation(["w-54"])
        self._begin(gen)
        orig = TransitionGate._collect_finalization_evidence

        def crash_after_snapshot(self, conn, generation, version):
            ev = orig(self, conn, generation, version)
            raise _Crashed("after evidence snapshot")

        TransitionGate._collect_finalization_evidence = \
            crash_after_snapshot
        try:
            with self.assertRaises(_Crashed):
                self._eval(gen)
        finally:
            TransitionGate._collect_finalization_evidence = orig
        # No verdict was recorded: the run is still OPEN.
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "OPEN")
        self._restart()
        ready = self._eval(gen)
        self.assertEqual(ready["state"], "READY")
        published = self._publish(gen)
        self.assertEqual(published["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)


# ------------------------------------------------------------------ R13-55
class TestR1355(FinalizationBase):
    def test_R13_55_crash_after_manifest(self):
        """R13-55 crash point 3/10 — crash right after the manifest is
        built: no verdict recorded; restart converges to FINALIZED."""
        gen, _jids = self._healthy_generation(["w-55"])
        self._begin(gen)
        orig = gate_mod.build_finalization_manifest

        def crash_after_manifest(evidence, blockers):
            m = orig(evidence, blockers)
            raise _Crashed("after manifest")

        gate_mod.build_finalization_manifest = crash_after_manifest
        try:
            with self.assertRaises(_Crashed):
                self._eval(gen)
        finally:
            gate_mod.build_finalization_manifest = orig
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "OPEN")
        self.assertIsNone(row["manifest_hash"])
        self._restart()
        self.assertEqual(self._eval(gen)["state"], "READY")
        self.assertEqual(self._publish(gen)["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)


# ------------------------------------------------------------------ R13-56
class TestR1356(FinalizationBase):
    def test_R13_56_crash_after_checkpoint_staging(self):
        """R13-56 crash point 4/10 — crash after the release checkpoint
        is staged but before verification: the UNVERIFIED checkpoint
        row survives; after restart the evaluation re-stages
        idempotently, verifies it, and converges to FINALIZED."""
        gen, jids = self._healthy_generation(["w-56"])
        self._begin(gen)
        orig = TransitionGate.stage_checkpoint

        def crash_after_stage(self, task_id, **kw):
            staged = orig(self, task_id, **kw)
            raise _Crashed("after checkpoint staging")

        TransitionGate.stage_checkpoint = crash_after_stage
        try:
            with self.assertRaises(_Crashed):
                self._eval(gen)
        finally:
            TransitionGate.stage_checkpoint = orig
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "OPEN")
        # The staged (UNVERIFIED) checkpoint survived the crash.
        ck = self.gate.store.conn.execute(
            "SELECT verification_status FROM checkpoints").fetchone()
        self.assertIsNotNone(ck)
        self.assertEqual(ck["verification_status"], "UNVERIFIED")
        created_before = self._count_events("checkpoint.created")
        self._restart()
        ready = self._eval(gen)
        self.assertEqual(ready["state"], "READY")
        # Re-staging was idempotent: no second checkpoint.created event.
        self.assertEqual(self._count_events("checkpoint.created"),
                         created_before)
        ck2 = self.gate.store.conn.execute(
            "SELECT verification_status FROM checkpoints").fetchone()
        self.assertEqual(ck2["verification_status"], "VERIFIED")
        self.assertEqual(self._publish(gen)["state"], "FINALIZED")


# ------------------------------------------------------------------ R13-57
class TestR1357(FinalizationBase):
    def test_R13_57_crash_after_checkpoint_verification(self):
        """R13-57 crash point 5/10 — crash after the release checkpoint
        is VERIFIED but before the verdict write: the VERIFIED
        checkpoint survives; restart converges to FINALIZED."""
        gen, _jids = self._healthy_generation(["w-57"])
        self._begin(gen)
        orig = TransitionGate.verify_checkpoint

        def crash_after_verify(self, checkpoint_id, **kw):
            verified = orig(self, checkpoint_id, **kw)
            raise _Crashed("after checkpoint verification")

        TransitionGate.verify_checkpoint = crash_after_verify
        try:
            with self.assertRaises(_Crashed):
                self._eval(gen)
        finally:
            TransitionGate.verify_checkpoint = orig
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "OPEN")
        ck = self.gate.store.conn.execute(
            "SELECT verification_status FROM checkpoints").fetchone()
        self.assertEqual(ck["verification_status"], "VERIFIED")
        self._restart()
        self.assertEqual(self._eval(gen)["state"], "READY")
        self.assertEqual(self._publish(gen)["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)


# ------------------------------------------------------------------ R13-58
class TestR1358(FinalizationBase):
    def test_R13_58_crash_after_ready_recorded(self):
        """R13-58 crash point 6/10 — crash after the READY verdict is
        durably recorded but before publish: after restart the READY
        row is intact and re-evaluation is a zero-write no-op; publish
        then converges to FINALIZED."""
        gen, _jids = self._healthy_generation(["w-58"])
        self._begin(gen)
        orig = TransitionGate._record_evaluation_outcome

        def crash_after_record(self, *a, **k):
            row = orig(self, *a, **k)
            raise _Crashed("after READY recorded")

        TransitionGate._record_evaluation_outcome = crash_after_record
        try:
            with self.assertRaises(_Crashed):
                self._eval(gen)
        finally:
            TransitionGate._record_evaluation_outcome = orig
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(row["version"], 2)
        self._restart()
        # Re-evaluation with the current version: zero writes.
        seq_before = self._ledger_seq_max()
        again = self._eval(gen)
        self.assertEqual(again["state"], "READY")
        self.assertEqual(again["version"], 2)
        self.assertEqual(dict(again),
                         dict(self.gate.get_finalization_run(gen)))
        self.assertEqual(self._ledger_seq_max(), seq_before)
        self.assertEqual(self._publish(gen)["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)


# ------------------------------------------------------------------ R13-59
class TestR1359(FinalizationBase):
    def test_R13_59_crash_before_publication_txn(self):
        """R13-59 crash point 7/10 — crash immediately before the
        publication transaction: zero mutation; restart publishes."""
        gen, _jids = self._healthy_generation(["w-59"])
        self._begin(gen)
        ready = self._eval(gen)
        orig = TransitionGate.publish_finalization

        def crash_before_txn(self, *a, **k):
            raise _Crashed("immediately before publication txn")

        TransitionGate.publish_finalization = crash_before_txn
        try:
            with self.assertRaises(_Crashed):
                self._publish(gen)
        finally:
            TransitionGate.publish_finalization = orig
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(dict(row), dict(ready))
        self._restart()
        self.assertEqual(self._publish(gen)["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)


# ------------------------------------------------------------------ R13-60
class TestR1360(FinalizationBase):
    def test_R13_60_crash_during_publication_txn(self):
        """R13-60 crash point 8/10 — crash DURING the publication
        transaction (after the UPDATE, before the ledger event): the
        write_txn rolls back atomically — the run is still READY at
        the same version with no partial mutation; restart publishes."""
        gen, _jids = self._healthy_generation(["w-60"])
        self._begin(gen)
        ready = self._eval(gen)
        orig = TransitionGate._append_event
        armed = {"on": True}

        def crash_during_txn(self, conn, now, event_type, payload,
                             actor):
            if armed["on"] and event_type == "finalization.published":
                raise _Crashed("during publication txn")
            return orig(self, conn, now, event_type, payload, actor)

        TransitionGate._append_event = crash_during_txn
        try:
            with self.assertRaises(_Crashed):
                self._publish(gen)
        finally:
            armed["on"] = False
            TransitionGate._append_event = orig
        # Atomic rollback: READY, same version, no published event.
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(row["version"], ready["version"])
        self.assertEqual(row["manifest_hash"], ready["manifest_hash"])
        self.assertIsNone(row["completed_at"])
        self.assertEqual(self._count_events("finalization.published"), 0)
        self._restart()
        published = self._publish(gen)
        self.assertEqual(published["state"], "FINALIZED")
        self.assertEqual(published["version"], ready["version"] + 1)
        self.assertEqual(self._count_events("finalization.published"), 1)


# ------------------------------------------------------------------ R13-61
class TestR1361(FinalizationBase):
    def test_R13_61_crash_after_publication_txn(self):
        """R13-61 crash point 9/10 — crash immediately AFTER the
        publication transaction committed: after restart the run is
        FINALIZED; an idempotent re-publish verifies and returns it
        with zero writes and no duplicate ledger event."""
        gen, _jids = self._healthy_generation(["w-61"])
        self._begin(gen)
        ready = self._eval(gen)
        orig = TransitionGate.publish_finalization

        def crash_after_txn(self, *a, **k):
            row = orig(self, *a, **k)
            raise _Crashed("immediately after publication txn")

        TransitionGate.publish_finalization = crash_after_txn
        try:
            with self.assertRaises(_Crashed):
                self._publish(gen)
        finally:
            TransitionGate.publish_finalization = orig
        self._restart()
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "FINALIZED")
        seq_before = self._ledger_seq_max()
        pubs_before = self._count_events("finalization.published")
        again = self._publish(gen)
        self.assertEqual(again["state"], "FINALIZED")
        self.assertEqual(dict(again), dict(row))
        self.assertEqual(self._ledger_seq_max(), seq_before)
        self.assertEqual(self._count_events("finalization.published"),
                         pubs_before)


# ------------------------------------------------------------------ R13-62
class TestR1362(FinalizationBase):
    def test_R13_62_crash_before_observation_by_another_process(self):
        """R13-62 crash point 10/10 — the finalizing process dies before
        any other process observes the result: a fresh gate ('another
        process') observes the FINALIZED record; idempotent re-publish
        performs zero writes."""
        gen, _jids = self._healthy_generation(["w-62"])
        self._begin(gen)
        ready = self._eval(gen)
        published = self._publish(gen)
        self.assertEqual(published["state"], "FINALIZED")
        # Process A dies here, before any other process reads the row.
        self._restart()
        # Process B: a brand-new gate over the same file.
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "FINALIZED")
        self.assertEqual(row["manifest_hash"], ready["manifest_hash"])
        self.assertEqual(row["checkpoint_id"], ready["checkpoint_id"])
        seq_before = self._ledger_seq_max()
        again = self.gate.publish_finalization(
            gen, row["version"], row["manifest_hash"], "test")
        self.assertEqual(again["state"], "FINALIZED")
        self.assertEqual(dict(again), dict(row))
        self.assertEqual(self._ledger_seq_max(), seq_before)
        # evaluate_finalization on a FINALIZED run verifies, zero writes.
        verified = self.gate.evaluate_finalization(
            gen, row["version"], "test")
        self.assertEqual(verified["state"], "FINALIZED")
        self.assertEqual(self._ledger_seq_max(), seq_before)


# ------------------------------------------------------------------ R13-63
class TestR1363(FinalizationBase):
    def test_R13_63_idempotent_refinalize_and_contradiction(self):
        """R13-63: publishing a FINALIZED run is idempotent — the same
        row, zero writes, no duplicate ledger event. A contradictory
        finalized record (checkpoint regressed to UNVERIFIED) fails
        closed on both evaluate and publish."""
        gen, _jids = self._healthy_generation(["w-63"])
        self._begin(gen)
        self._eval(gen)
        published = self._publish(gen)
        seq_before = self._ledger_seq_max()
        pubs_before = self._count_events("finalization.published")
        again = self._publish(gen)
        self.assertEqual(dict(again), dict(published))
        self.assertEqual(self._ledger_seq_max(), seq_before)
        self.assertEqual(self._count_events("finalization.published"),
                         pubs_before)
        # Contradictory finalized record: the release checkpoint is no
        # longer VERIFIED — a finalized claim is never returned over
        # degraded evidence.
        raw = self._raw()
        raw.execute("UPDATE checkpoints SET verification_status="
                    "'UNVERIFIED' WHERE checkpoint_id=?",
                    (published["checkpoint_id"],))
        raw.commit()
        raw.close()
        with self.assertRaises(TransitionRejected):
            self.gate.evaluate_finalization(
                gen, published["version"], "test")
        with self.assertRaises(TransitionRejected):
            self._publish(gen)
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"),
                         pubs_before)


# ------------------------------------------------------------------ R13-64
class TestR1364(FinalizationBase):
    def test_R13_64_desired_mutation_names_new_generation(self):
        """R13-64: after FINALIZED, a desired-state mutation bumps the
        head and names a NEW generation; the old run row (and its
        evidence) is byte-identical — old evidence is never
        overwritten."""
        gen, _jids = self._healthy_generation(["w-64"])
        self._begin(gen)
        self._eval(gen)
        published = self._publish(gen)
        snapshot = dict(published)
        ck_before = self.gate.store.conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?",
            (published["checkpoint_id"],)).fetchone()
        ck_snapshot = dict(ck_before)
        arts_before = self._table_dump("artifacts")
        self._set("w-64b")
        self.assertEqual(self._head_version(), 2)
        new_gen = canonical_release_generation(2)
        self.assertEqual(new_gen, "ds-v2")
        self.assertIsNone(self.gate.get_finalization_run(new_gen))
        old = self.gate.get_finalization_run(gen)
        self.assertEqual(dict(old), snapshot)
        ck_after = dict(self.gate.store.conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?",
            (published["checkpoint_id"],)).fetchone())
        self.assertEqual(ck_after, ck_snapshot)
        self.assertEqual(self._table_dump("artifacts"), arts_before)
        self.assertEqual(len(self.gate.list_finalization_runs()), 1)


# ------------------------------------------------------------------ R13-65
class TestR1365(FinalizationBase):
    def test_R13_65_recovery_history_intact(self):
        """R13-65: R8/R9 history (incidents, attempts, policy rows) is
        byte-identical across finalization — the finalizer never
        touches recovery state."""
        gen, _jids = self._healthy_generation(["w-65"])
        inc = self.gate.find_or_create_recovery_incident(
            "scratch-65", "DEAD", {"reason": "r13-65"}, "test")
        att = self.gate.create_recovery_attempt(
            incident_id=inc["incident_id"], job_id="scratch-65",
            fencing_token=3, rung=1, rung_name="requeue",
            action={"name": "requeue"}, failure_class="DEAD",
            success_criterion="s", failure_criterion="f",
            evidence_before={}, actor="test")
        pol = self.gate.ensure_recovery_policy(
            inc["incident_id"], policy_version="r9-v1", current_rung=1,
            rung_name="requeue", incident_budget=3,
            per_rung_budgets={"1": 3}, actor="test")
        self.gate.set_incident_outcome(inc["incident_id"], "recovered",
                                       "r13-65", "test")
        self.gate.cas_update_recovery_policy(
            inc["incident_id"], pol["version"],
            {"terminal_state": "RESOLVED"}, "test")
        before = {
            "incidents": self._table_dump("incidents"),
            "recovery_attempts": self._table_dump("recovery_attempts"),
            "recovery_policy": self._table_dump("recovery_policy"),
        }
        fin = self._new_fin()
        fin.evaluate_once()
        fin.evaluate_once()
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "FINALIZED")
        for table, dump in before.items():
            self.assertEqual(self._table_dump(table), dump, table)


# ------------------------------------------------------------------ R13-66
class TestR1366(FinalizationBase):
    def test_R13_66_finalized_evidence_immutable(self):
        """R13-66: after FINALIZED, reconciler/scheduler/R12 activity
        cannot silently mutate the finalized evidence — the run row,
        the checkpoint row, and the artifact rows are byte-identical
        and no new finalization.* ledger events appear."""
        gen, _jids = self._healthy_generation(["w-66"])
        fin = self._new_fin()
        fin.evaluate_once()
        fin.evaluate_once()
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "FINALIZED")
        snapshot = dict(row)
        ck_snapshot = dict(self.gate.store.conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?",
            (row["checkpoint_id"],)).fetchone())
        arts_snapshot = self._table_dump("artifacts")
        fin_events_before = self._count_events("finalization.published")
        eval_events_before = self._count_events("finalization.evaluated")
        # Reconciler pass: CONVERGED, nothing to create.
        rec = self._new_rec()
        r = rec.reconcile()
        self.assertEqual(r["result"], "CONVERGED")
        # Scheduler pass: nothing to admit.
        sup = self._new_sup(actor="test-r13-sup")
        sched = self._new_sched(sup, scheduler_id="r13-66")
        rep = sched.evaluate_once()
        self.assertEqual(rep["admitted"], 0)
        # R12: breaker signals + transitions on UNRELATED scopes.
        self.gate.record_breaker_signal(
            "JOB", "unrelated-66", failure_kind="R7_DEAD",
            incident_id="i-66", attempt_id=None,
            failure_window_s=300.0, cooldown_s=60.0, actor="test")
        self._open_breaker("TASK", "unrelated-task-66")
        # R8: a scratch incident lifecycle.
        inc = self.gate.find_or_create_recovery_incident(
            "scratch-66", "DEAD", {"reason": "r13-66"}, "test")
        self.gate.set_incident_outcome(inc["incident_id"], "recovered",
                                       "r13-66", "test")
        # Finalized evidence untouched.
        self.assertEqual(dict(self.gate.get_finalization_run(gen)),
                         snapshot)
        self.assertEqual(dict(self.gate.store.conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?",
            (row["checkpoint_id"],)).fetchone()), ck_snapshot)
        self.assertEqual(self._table_dump("artifacts"), arts_snapshot)
        self.assertEqual(self._count_events("finalization.published"),
                         fin_events_before)
        self.assertEqual(self._count_events("finalization.evaluated"),
                         eval_events_before)


# ------------------------------------------------------------------ R13-67
class TestR1367(FinalizationBase):
    def test_R13_67_unreadable_desired_state_fails_closed(self):
        """R13-67: unreadable desired state (fault injected: spec JSON
        corrupted) -> evaluate records FAILED (CORRUPT_AUTHORITY), never
        FINALIZED; publish fails closed."""
        gen, _jids = self._healthy_generation(["w-67"])
        raw = self._raw()
        raw.execute("UPDATE desired_state SET spec='not-json{{'"
                    " WHERE desired_work_id='w-67'")
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "FAILED")
        bad = self._blockers(row)
        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0]["category"], "CORRUPT_AUTHORITY")
        with self.assertRaises(TransitionRejected):
            self._publish(gen)
        self.assertEqual(
            self.gate.get_finalization_run(gen)["state"], "FAILED")


# ------------------------------------------------------------------ R13-68
class TestR1368(FinalizationBase):
    def test_R13_68_unreadable_actual_state_fails_closed(self):
        """R13-68: unreadable actual state (fault injected: jobs table
        dropped) -> FAILED, never FINALIZED."""
        gen, _jids = self._healthy_generation(["w-68"])
        raw = self._raw()
        raw.execute("DROP TABLE jobs")
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "FAILED")
        self.assertEqual(self._blockers(row)[0]["category"],
                         "CORRUPT_AUTHORITY")
        with self.assertRaises(TransitionRejected):
            self._publish(gen)


# ------------------------------------------------------------------ R13-69
class TestR1369(FinalizationBase):
    def test_R13_69_unreadable_artifact_evidence_fails_closed(self):
        """R13-69: unreadable artifact evidence (fault injected:
        artifacts table dropped) -> FAILED, never FINALIZED."""
        gen, _jids = self._healthy_generation(["w-69"])
        raw = self._raw()
        raw.execute("DROP TABLE artifacts")
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "FAILED")
        self.assertEqual(self._blockers(row)[0]["category"],
                         "CORRUPT_AUTHORITY")
        with self.assertRaises(TransitionRejected):
            self._publish(gen)


# ------------------------------------------------------------------ R13-70
class TestR1370(FinalizationBase):
    def test_R13_70_unreadable_checkpoint_evidence_fails_closed(self):
        """R13-70: unreadable checkpoint evidence (fault injected:
        checkpoints table dropped) -> FAILED, never FINALIZED."""
        gen, _jids = self._healthy_generation(["w-70"])
        raw = self._raw()
        raw.execute("DROP TABLE checkpoints")
        raw.commit()
        raw.close()
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "FAILED")
        self.assertEqual(self._blockers(row)[0]["category"],
                         "CORRUPT_AUTHORITY")
        with self.assertRaises(TransitionRejected):
            self._publish(gen)


# ------------------------------------------------------------------ R13-71
class TestR1371(FinalizationBase):
    def test_R13_71_squatted_finalization_task_fails_closed(self):
        """R13-71: contradictory process identity — a foreign task row
        squatting the deterministic finalization task id (fault
        injected) makes evaluation fail closed with TransitionRejected;
        the run stays OPEN and no checkpoint is staged."""
        gen, _jids = self._healthy_generation(["w-71"])
        squat_id = self._fin_task_id(gen)
        self.gate.create_task(squat_id, {"objective": "squat"},
                              {"usd": 1}, "test")
        self._begin(gen)
        with self.assertRaises(TransitionRejected) as ctx:
            self._eval(gen)
        self.assertIn("squatted", str(ctx.exception))
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "OPEN")
        self.assertEqual(row["version"], 1)
        self.assertEqual(self._count("checkpoints"), 0)


# ------------------------------------------------------------------ R13-72
class TestR1372(FinalizationBase):
    def test_R13_72_unreaped_spawn_blocks(self):
        """R13-72: an unreaped worker.proc_spawned milestone (no
        proc_reaped) is possibly-live execution -> ACTIVE_EXECUTION
        blocker; once reaped, the generation is releasable."""
        gen, jids = self._healthy_generation(["w-72"])
        self.gate.append_event(
            "worker.proc_spawned",
            {"worker_id": "w-ghost", "proc_id": "p-ghost",
             "job_id": jids[0], "pid": 4242}, "test")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        spawns = [b for b in self._blockers(row)
                  if b["category"] == "ACTIVE_EXECUTION"
                  and b["identity"].startswith("spawn:")]
        self.assertEqual(len(spawns), 1)
        self.assertEqual(spawns[0]["identity"],
                         "spawn:w-ghost:p-ghost")
        # Reaped: the evidence is reconciled, the generation releases.
        self.gate.append_event(
            "worker.proc_reaped",
            {"worker_id": "w-ghost", "proc_id": "p-ghost",
             "job_id": jids[0], "pid": 4242}, "test")
        row2 = self._eval(gen)
        self.assertEqual(row2["state"], "READY")


# ------------------------------------------------------------------ R13-73
class TestR1373(FinalizationBase):
    def test_R13_73_deterministic_blocker_ordering(self):
        """R13-73: the blocker list is sorted by (category, identity)
        and byte-stable across evaluations."""
        self._set("w-73a")
        self._set("w-73b")
        gen = canonical_release_generation(2)
        ja = self._ensure("w-73a")
        self._ensure("w-73b")  # stays PENDING
        self._claim_run(ja["job_id"])  # RUNNING
        self.gate.find_or_create_recovery_incident(
            "scratch-73", "DEAD", {"reason": "r13-73"}, "test")
        self._open_breaker("GLOBAL", "global")
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        blockers = self._blockers(row)
        keys = [(b["category"], b["identity"]) for b in blockers]
        self.assertEqual(keys, sorted(keys))
        self.assertGreaterEqual(len(blockers), 4)
        cats = {b["category"] for b in blockers}
        self.assertTrue(
            {"DESIRED_WORK_UNSATISFIED", "ACTIVE_EXECUTION",
             "ACTIVE_RECOVERY", "OPEN_BREAKER"} <= cats)
        row2 = self._eval(gen)
        self.assertEqual(self._blockers(row2), blockers)


# ------------------------------------------------------------------ R13-74
class TestR1374(FinalizationBase):
    def test_R13_74_noop_reevaluation_writes_nothing(self):
        """R13-74: re-evaluating a READY run with unchanged evidence
        performs zero writes — the row is byte-identical and no new
        ledger events appear."""
        gen, _jids = self._healthy_generation(["w-74"])
        self._begin(gen)
        ready = self._eval(gen)
        self.assertEqual(ready["state"], "READY")
        seq_before = self._ledger_seq_max()
        again = self._eval(gen)
        self.assertEqual(dict(again), dict(ready))
        self.assertEqual(self._ledger_seq_max(), seq_before)
        # And a finalizer pass over the READY run changes nothing until
        # it publishes.
        fin = self._new_fin()
        rep = fin.evaluate_once()
        self.assertEqual(rep["detail"], "ok")
        finalized = [f for f in rep["finalized"]
                     if f["release_generation"] == gen]
        self.assertEqual(len(finalized), 1)
        self.assertEqual(finalized[0]["manifest_hash"],
                         ready["manifest_hash"])


# ------------------------------------------------------------------ R13-75
class TestR1375(FinalizationBase):
    def test_R13_75_finalizer_authority_ast(self):
        """R13-75: AST — exec/finalizer.py never calls any job/task/
        worker/desired/recovery/breaker/artifact/checkpoint mutation
        API, never touches SQL, and only drives the finalization gate
        ops plus read-only evidence reads."""
        with open(FIN_SRC) as f:
            tree = ast.parse(f.read())
        calls, attrs, names = set(), set(), set()
        for n in ast.walk(tree):
            if isinstance(n, ast.Name):
                names.add(n.id)
            if isinstance(n, ast.Attribute):
                attrs.add(n.attr)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute):
                calls.add(n.func.attr)
        forbidden = {
            "create_job", "claim_job", "claim_job_bounded",
            "claim_job_resilient", "transition_job", "create_task",
            "transition_task", "set_desired_item", "retire_desired_item",
            "record_breaker_signal", "transition_breaker",
            "ensure_breaker_state", "claim_half_open_probe",
            "create_recovery_attempt", "transition_recovery_attempt",
            "complete_recovery_attempt", "mark_recovery_attempt_uncertain",
            "claim_recovery_attempt", "create_incident",
            "set_incident_outcome", "set_incident_escalated",
            "ensure_recovery_policy", "cas_update_recovery_policy",
            "consume_policy_attempts", "reclaim_lease", "release_lease",
            "renew_lease", "stage_artifact", "begin_commit",
            "verify_artifact", "commit_artifact", "fail_job_execution",
            "create_worker", "transition_worker", "mark_worker_seen",
            "ingest_heartbeat", "record_watchdog_verdict",
            "observe_expired_leases", "expired_leases",
            "stage_checkpoint", "verify_checkpoint", "create_checkpoint",
            "set_checkpoint_verification", "create_approval",
            "decide_approval", "append_event", "execute", "write_txn",
            "kill", "terminate", "fence",
        }
        hit = (calls | attrs) & forbidden
        self.assertEqual(hit, set(), f"forbidden authority: {hit}")
        self.assertNotIn("sqlite3", names | attrs)
        # The finalizer only drives the finalization surface (plus
        # read-only head/run reads).
        for op in ("begin_finalization_run", "evaluate_finalization",
                   "publish_finalization"):
            self.assertIn(op, calls, op)


# ------------------------------------------------------------------ R13-76
class TestR1376(FinalizationBase):
    def test_R13_76_finalizer_never_mutates_other_authority(self):
        """R13-76: behavioral — finalizer passes over a healthy head
        generation (FINALIZED) and a stale blocked older generation
        (left BLOCKED, error collected, zero writes) leave every
        non-finalization table byte-identical, and every ledger event
        under the finalizer actor is a finalization/checkpoint event."""
        # Older generation: pending job -> evaluated BLOCKED.
        self._set("w-76b")
        self._ensure("w-76b")  # stays PENDING
        old_gen = canonical_release_generation(1)
        self.gate.begin_finalization_run(old_gen, 1, "test")
        old_row = self.gate.evaluate_finalization(old_gen, 1, "test")
        self.assertEqual(old_row["state"], "BLOCKED")
        # Retire the blocked item so the head generation is healthy.
        self.gate.retire_desired_item("w-76b", "test")
        self._set("w-76a")
        gen = canonical_release_generation(self._head_version())
        self.assertEqual(gen, "ds-v3")
        job = self._ensure("w-76a")
        wid, _tok = self._claim_run(job["job_id"], wid="w-76a")
        self._complete(job["job_id"], wid)
        tables = ["jobs", "desired_state", "desired_job_map", "artifacts",
                  "validations", "breaker_state", "breaker_signals",
                  "incidents", "recovery_attempts", "recovery_policy",
                  "workers", "tasks"]
        before = {t: self._table_dump(t) for t in tables}
        fin = self._new_fin()
        rep = fin.evaluate_once()
        self.assertEqual(rep["detail"], "ok")
        fin.evaluate_once()
        self.assertEqual(
            self.gate.get_finalization_run(gen)["state"], "FINALIZED")
        # The stale older run was evaluated (STALE -> error collected
        # under its generation scope, never fatal) and left BLOCKED
        # with zero writes.
        self.assertEqual(
            self.gate.get_finalization_run(old_gen)["state"], "BLOCKED")
        stale_errors = [e for e in rep["errors"]
                        if e["scope"] == old_gen
                        and e["phase"] == "evaluate"]
        self.assertEqual(len(stale_errors), 1)
        self.assertIn("STALE_GENERATION", stale_errors[0]["error"])
        for t, dump in before.items():
            if t == "tasks":
                # Only the deterministic finalization container tasks
                # may appear; no other task row may change.
                after = {r for r in self._table_dump(t)}
                before_set = {r for r in dump}
                new = after - before_set
                for r in new:
                    self.assertIn("task-fin-", r[0], r)
                continue
            self.assertEqual(self._table_dump(t), dump, t)
        allowed = {"finalization.begun", "finalization.evaluated",
                   "finalization.published", "finalization.task_ensured",
                   "checkpoint.created", "checkpoint.verification"}
        for r in self.gate.store.conn.execute(
                "SELECT event_type, actor FROM ledger"
                " WHERE actor='finalizer'").fetchall():
            self.assertIn(r["event_type"], allowed, r["event_type"])


# ------------------------------------------------------------------ R13-77
class TestR1377(FinalizationBase):
    def test_R13_77_lease_reclaim_is_evidence(self):
        """R13-77 (R1 composition): the R1 lease-reclaim primitive is
        finalization evidence — a RUNNING job reclaimed to UNCERTAIN
        (token bumped, owner cleared) BLOCKS the generation as
        possibly-live; the stale worker epoch cannot forge completion
        against the bumped token."""
        self._set("w-77")
        job = self._ensure("w-77")
        jid = job["job_id"]
        wid, tok = self._claim_run(jid, wid="w-77")
        # R1: forced reclaim of the live lease -> UNCERTAIN.
        self.gate.reclaim_lease(jid, actor="test", reason="R9_STALL",
                                expected_owner=wid, expected_token=tok,
                                force=True, verdict="DEAD",
                                incident_id="i-77")
        reclaimed = self.gate.get_job(jid)
        self.assertEqual(reclaimed["status"], "UNCERTAIN")
        self.assertIsNone(reclaimed["owner_worker_id"])
        self.assertEqual(reclaimed["fencing_token"], tok + 1)
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertTrue(
            any(b["category"] == "UNCERTAIN_EXECUTION"
                for b in self._blockers(row)))
        # The stale epoch cannot forge completion against the bumped
        # token — fencing holds, and the generation never finalizes.
        with self.assertRaises(LeaseError):
            r5_complete(gate=self.gate, job_id=jid, worker_id=wid,
                        fencing_token=tok, task_id="t",
                        outcome="SUCCESS", evidence={"ok": True},
                        actor=f"worker:{wid}",
                        data=b"r13-77-stale-forge")
        self.assertEqual(self.gate.get_job(jid)["status"], "UNCERTAIN")
        row2 = self._eval(gen)
        self.assertEqual(row2["state"], "BLOCKED")
        self.assertEqual(self._count_events("finalization.published"), 0)


# ------------------------------------------------------------------ R13-78
class TestR1378(FinalizationBase):
    def test_R13_78_heartbeat_evidence_gates_finalization(self):
        """R13-78 (R1/R3 composition): a claimed job with a live R3
        heartbeat BLOCKS; after the R1 reclaim (CLAIMED->PENDING, token
        bumped) the generation is still blocked — now on the
        unsatisfied desired item — until a new worker epoch completes
        it through the R5 contract."""
        self._set("w-78")
        job = self._ensure("w-78")
        jid = job["job_id"]
        self.assertTrue(
            self.gate.claim_job_bounded(jid, "w-78", 60.0, "test", 4))
        tok = self.gate.get_job(jid)["fencing_token"]
        # R3: durable heartbeat evidence from the live worker.
        self.gate.create_worker("w-78", "test")
        self.gate.ingest_heartbeat("w-78", "p-78", jid, tok, 1,
                                   "RUNNING", "r13-78-op",
                                   "worker:w-78")
        self.assertEqual(len(self.gate.heartbeats_for("w-78", "p-78")),
                         1)
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertTrue(
            any(b["category"] == "ACTIVE_EXECUTION"
                for b in self._blockers(row)))
        # Heartbeat stops; R1 reclaims CLAIMED->PENDING (token bumped).
        self.gate.reclaim_lease(jid, actor="test", reason="R9_STALL",
                                expected_owner="w-78",
                                expected_token=tok,
                                force=True, verdict="DEAD",
                                incident_id="i-78")
        self.assertEqual(self.gate.get_job(jid)["status"], "PENDING")
        self.assertEqual(self.gate.get_job(jid)["fencing_token"],
                         tok + 1)
        row2 = self._eval(gen)
        self.assertEqual(row2["state"], "BLOCKED")
        pend = [b for b in self._blockers(row2)
                if b["category"] == "DESIRED_WORK_UNSATISFIED"]
        self.assertEqual(len(pend), 1)
        self.assertEqual(pend[0]["identity"], jid)
        self.assertIn("PENDING", pend[0]["detail"])
        # A new worker epoch completes the job; the generation releases.
        wid, new_tok = self._claim_run(jid, wid="w-78b")
        self.assertGreater(new_tok, tok)  # reclaim + reclaim bumped it
        self._complete(jid, wid)
        self.assertEqual(self._eval(gen)["state"], "READY")
        self.assertEqual(self._publish(gen)["state"], "FINALIZED")


# ------------------------------------------------------------------ R13-79
class TestR1379(FinalizationBase):
    def test_R13_79_full_recovery_ladder_then_finalize(self):
        """R13-79 (R1/R5/R8/R9 composition): a RUNNING job whose worker
        staged and verified its artifact is reclaimed to UNCERTAIN
        (R1); the R8 incident plus the R9 escalated policy BLOCK the
        generation; the recovery authority then adopts the pre-reclaim
        artifact through the D5 resolver, the attempt completes with
        real progress, the incident/policy resolve, and the generation
        FINALIZES with recovery history intact."""
        self._set("w-79")
        job = self._ensure("w-79")
        jid = job["job_id"]
        wid, tok = self._claim_run(jid, wid="w-79")
        # The worker stages and verifies its artifact BEFORE the
        # reclaim, so the artifact's token predates the UNCERTAIN
        # state (D5 resolver precondition).
        staged = self.gate.stage_artifact(
            job_id=jid, worker_id=wid, fencing_token=tok, task_id="t",
            kind="result", data=b"r13-79-bytes", actor=f"worker:{wid}")
        aid = staged["artifact_id"]
        self.gate.verify_artifact(aid, actor="test")
        # R8: incident + recovery attempt for the stalled job.
        inc = self.gate.find_or_create_recovery_incident(
            jid, "STALLED", {"reason": "r13-79"}, "test")
        att = self.gate.create_recovery_attempt(
            incident_id=inc["incident_id"], job_id=jid,
            fencing_token=tok, rung=2, rung_name="reclaim",
            action={"name": "reclaim"}, failure_class="STALLED",
            success_criterion="lease revoked",
            failure_criterion="lease still live",
            evidence_before={"fencing_token": tok}, actor="test")
        self.gate.transition_recovery_attempt(
            att["attempt_id"], ("CREATED",), "RUNNING", "test")
        # R1: the reclaim rung revokes the live lease -> UNCERTAIN.
        self.gate.reclaim_lease(jid, actor="recovery-controller",
                                reason="R9_STALL", expected_owner=wid,
                                expected_token=tok, force=True,
                                verdict="DEAD",
                                incident_id=inc["incident_id"])
        self.assertEqual(self.gate.get_job(jid)["status"], "UNCERTAIN")
        # R9: the attempt goes uncertain; the policy escalates.
        self.gate.mark_recovery_attempt_uncertain(
            att["attempt_id"], "r13-79 dispatch ambiguous", "test")
        pol = self.gate.ensure_recovery_policy(
            inc["incident_id"], policy_version="r9-v1", current_rung=5,
            rung_name="terminal", incident_budget=1,
            per_rung_budgets={"5": 1}, actor="test")
        self.gate.cas_update_recovery_policy(
            inc["incident_id"], pol["version"],
            {"terminal_state": "ESCALATED"}, "test")
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        row = self._eval(gen)
        self.assertEqual(row["state"], "BLOCKED")
        cats = {b["category"] for b in self._blockers(row)}
        self.assertTrue({"UNCERTAIN_EXECUTION", "ACTIVE_RECOVERY"}
                        <= cats)
        # Recovery: the authority adopts the pre-reclaim artifact via
        # the D5 resolver (artifact token < job token, owner cleared).
        new_tok = self.gate.get_job(jid)["fencing_token"]
        self.assertEqual(new_tok, tok + 1)
        self.gate.commit_artifact(jid, None, new_tok, artifact_id=aid,
                                  actor="recovery-controller",
                                  evidence={"resolver": "D5",
                                            "reason": "r13-79"})
        self.assertEqual(self.gate.get_job(jid)["status"], "COMPLETE")
        # The reclaim genuinely revoked the lease: the attempt
        # completes with real observed progress (I-18).
        self.gate.complete_recovery_attempt(
            att["attempt_id"], decision="success",
            observed_effect="lease revoked, token bumped",
            progress_delta=1.0, resulting_state="RECOVERED",
            evidence_after={"fencing_token": new_tok},
            actor="recovery-controller")
        pol2 = self.gate.get_recovery_policy(inc["incident_id"])
        self.gate.cas_update_recovery_policy(
            inc["incident_id"], pol2["version"],
            {"terminal_state": "RESOLVED"}, "test")
        self.gate.set_incident_outcome(inc["incident_id"], "recovered",
                                       "r13-79", "test")
        row2 = self._eval(gen)
        self.assertEqual(row2["state"], "READY")
        self.assertEqual(self._publish(gen)["state"], "FINALIZED")
        # Recovery history survived finalization.
        self.assertEqual(
            self.gate.get_recovery_incident(
                inc["incident_id"])["outcome"], "recovered")
        self.assertIsNotNone(
            self.gate.get_recovery_attempt(att["attempt_id"]))


# ------------------------------------------------------------------ R13-80
class TestR1380(FinalizationBase):
    def test_R13_80_injection_matrix_path_to_finalized(self):
        """R13-80 (R12/R14 composition): the full R12 resilience path —
        breakers record signals and trip, an HALF_OPEN probe completes
        the job via R5, and the R13 finalizer converges to FINALIZED —
        proving the resilience and finalization kernels compose."""
        self._set("w-80")
        job = self._ensure("w-80")
        jid = job["job_id"]
        wid, _tok = self._claim_run(jid, wid="w-80")
        # R12: signals trip the GLOBAL breaker open.
        for i in range(3):
            self.gate.record_breaker_signal(
                "GLOBAL", "global", failure_kind="R7_DEAD",
                incident_id=f"i-80-{i}", attempt_id=None,
                failure_window_s=300.0, cooldown_s=60.0, actor="test")
        self._open_breaker("GLOBAL", "global")
        self.assertEqual(
            self.gate.get_breaker_state("GLOBAL",
                                        "global")["state"], "OPEN")
        gen = canonical_release_generation(self._head_version())
        self._begin(gen)
        self.assertEqual(self._eval(gen)["state"], "BLOCKED")
        # HALF_OPEN probe: one attempt may pass through the gate.
        opened = self.gate.get_breaker_state("GLOBAL", "global")
        half = self.gate.transition_breaker(
            "GLOBAL", "global", expected_version=opened["version"],
            to_state="HALF_OPEN", actor="test", reason="r13-80-probe")
        probe = self.gate.claim_half_open_probe(
            "GLOBAL", "global", expected_version=half["version"],
            probe_id="probe-80", probe_limit=1, baseline=None,
            actor="test")
        self.assertIsNotNone(probe)
        self._complete(jid, wid)
        # Probe success closes the breaker.
        closed = self.gate.get_breaker_state("GLOBAL", "global")
        self.gate.transition_breaker(
            "GLOBAL", "global", expected_version=closed["version"],
            to_state="CLOSED", actor="test", reason="r13-80-probe-ok")
        self.assertEqual(
            self.gate.get_breaker_state("GLOBAL",
                                        "global")["state"], "CLOSED")
        row = self._eval(gen)
        self.assertEqual(row["state"], "READY")
        self.assertEqual(self._publish(gen)["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)


# ============================================================ §31 flows
class TestR13S31Full(FinalizationBase):
    def test_S31_full_end_to_end_path(self):
        """§31 full path: desired-state declaration -> reconciler
        materializes jobs -> workers claim/heartbeat/run the R5 commit
        -> finalizer begins, evaluates (READY), publishes (FINALIZED).
        Every ledger link is present and the manifest pins the evidence."""
        self._set("w-s31a")
        self._set("w-s31b")
        gen = canonical_release_generation(self._head_version())
        # Reconciler materializes the jobs for the new generation.
        rec = self._new_rec()
        rep = rec.reconcile()
        self.assertEqual(rep["result"], "CHANGED")
        self.assertEqual(rep["items_created"], 2)
        for dwid in ("w-s31a", "w-s31b"):
            job = self.gate.get_job(
                _canonical_desired_job_id(dwid))
            self.assertEqual(job["status"], "PENDING")
        # Workers claim, heartbeat, and complete via the R5 contract.
        for dwid in ("w-s31a", "w-s31b"):
            job = self.gate.get_job(
                _canonical_desired_job_id(dwid))
            jid = job["job_id"]
            wid, tok = self._claim_run(jid, wid=dwid)
            self.gate.create_worker(dwid, "test")
            self.gate.ingest_heartbeat(dwid, f"p-{dwid}", jid, tok, 1,
                                       "RUNNING", "s31-op",
                                       f"worker:{dwid}")
            self._complete(jid, wid)
        # Finalizer converges the whole path.
        fin = self._new_fin()
        rep1 = fin.evaluate_once()
        self.assertEqual(rep1["detail"], "ok")
        rep2 = fin.evaluate_once()
        self.assertEqual(rep2["detail"], "ok")
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(row["state"], "FINALIZED")
        self.assertEqual(row["result"], "FINALIZED")
        # Ledger linkage: begun -> evaluated -> published.
        types = [r["event_type"] for r in
                 self.gate.store.conn.execute(
                     "SELECT event_type FROM ledger"
                     " WHERE actor='finalizer'"
                     " ORDER BY seq").fetchall()]
        for need in ("finalization.begun", "finalization.evaluated",
                     "finalization.published"):
            self.assertIn(need, types)
        # The manifest pins the desired-state hash of this generation.
        head = self.gate.get_desired_head()
        self.assertEqual(head["version"], 2)
        ev = self.gate._collect_finalization_evidence(
            self.gate.store.conn, gen,
            row["desired_state_version"])
        self.assertEqual(ev["desired_state_hash"],
                         head["snapshot_hash"])
        self.assertEqual(finalization_manifest_hash(
            build_finalization_manifest(ev, [])), row["manifest_hash"])


class TestR13S31Negative(FinalizationBase):
    def test_S31_negative_desired_generation_change(self):
        """§31 negative flow: desired generation changes between
        evaluation and publication -> both evaluate and publish fail
        closed with STALE_GENERATION; the READY verdict is preserved,
        no publication occurs, and the new generation gets its own
        run."""
        gen, _jids = self._healthy_generation(["w-neg"])
        self._begin(gen)
        ready = self._eval(gen)
        self.assertEqual(ready["state"], "READY")
        # The desired generation changes (operator adds work).
        self._set("w-neg2")
        self.assertEqual(self._head_version(), 2)
        # The old verdict is stale: re-evaluation and publication fail
        # closed, zero mutation.
        with self.assertRaises(TransitionRejected) as ctx:
            self._eval(gen)
        self.assertIn("STALE_GENERATION", str(ctx.exception))
        with self.assertRaises(TransitionRejected):
            self._publish(gen)
        row = self.gate.get_finalization_run(gen)
        self.assertEqual(dict(row), dict(ready))
        self.assertEqual(self._count_events("finalization.published"), 0)
        # The new generation proceeds on its own track.
        new_gen = canonical_release_generation(2)
        self._ensure("w-neg2")
        job = self.gate.get_job(
            _canonical_desired_job_id("w-neg2"))
        wid, _tok = self._claim_run(job["job_id"], wid="w-neg2")
        self._complete(job["job_id"], wid)
        self._begin(new_gen)
        self.assertEqual(self._eval(new_gen)["state"], "READY")
        self.assertEqual(self._publish(new_gen)["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)
        self.assertEqual(
            self.gate.get_finalization_run(gen)["state"], "READY")
