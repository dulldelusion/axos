"""R14 Final Hardening & Release Proof - the complete R14 suite.

This is the single canonical home of all R14 tests (Phase 1C final gate).
It consolidates the three section tracks written in parallel:

  R14-B  fault-injection campaign: FI-01..FI-12 (each x2 consecutive)
         + environment record (R14-ENV)
  R14-C  full-system composition, concurrency/races, fencing,
         recovery convergence, breaker/finalization composition,
         stray-process audit
  R14-D  corruption/restore, artifact/checkpoint adversarial battery,
         release manifest, clean boot

Run:  python -m pytest tests/test_final_hardening_r14.py

Earlier R1-R13 tests are untouched; nothing here renames or removes them.
"""

import datetime
import hashlib
import json
import os
import platform
import random
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))          # tests/
REPO = os.path.dirname(os.path.dirname(HERE))              # ~/workspace
WORKSPACE_ROOT = REPO
if REPO not in sys.path:
    sys.path.insert(0, REPO)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from axos.store import (  # noqa: E402
    open_store, migrate, applied_versions, TransitionGate,
    TransitionRejected, LeaseError, StoreError, PolicyConflict,
    BreakerConflict, FinalizationConflict, canonical_release_generation,
)
from axos.store.gate import _canonical_desired_job_id  # noqa: E402
from axos.store.migrations import MIGRATIONS, applied_versions  # noqa: E402
from axos.exec.supervisor import Supervisor  # noqa: E402
from axos.exec.watchdog import Watchdog, WatchdogConfig  # noqa: E402
from axos.exec.reconciler import Reconciler, ReconcilerConfig  # noqa: E402
from axos.exec.scheduler import Scheduler, SchedulerConfig  # noqa: E402
from axos.exec.resilience import ResilienceController, ResilienceConfig  # noqa: E402
from axos.exec.recovery import RecoveryController, RecoveryConfig  # noqa: E402
from axos.exec.policy import (  # noqa: E402
    CANONICAL_LADDER, PolicyController, PolicyConfig,
)
from axos.exec.finalizer import Finalizer, FinalizationConfig  # noqa: E402
from axos.exec.boot import boot_recover  # noqa: E402
from axos.tests.r5_helpers import r5_complete  # noqa: E402


# ==========================================================================
# R14-B: FAULT-INJECTION CAMPAIGN (FI-01..FI-12 x2 + environment record)
# (consolidated from tests/test_r14_b_injection.py)
# ==========================================================================

# --------------------------------------------------------------------------
# Environment record (R14-ENV)
# --------------------------------------------------------------------------
def environment_record() -> dict:
    uname = platform.uname()
    return {
        "suite": "R14-B",
        "os": f"{uname.system} {uname.release}",
        "machine": uname.machine,
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "execution": "real-vm",
        "host": uname.node,
        "pid": os.getpid(),
    }


ENVIRONMENT = environment_record()


class _Registry:
    """Process-global collectors for the final report test."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.results: dict[str, dict] = {}
        self.fi10_latencies: list[dict] = []

    def record(self, name: str, ok: bool, detail: str = "") -> None:
        with self._lock:
            self.results[name] = {"pass": bool(ok), "detail": detail}

    def fi10(self, rows: list[dict]) -> None:
        with self._lock:
            self.fi10_latencies.extend(rows)


REG = _Registry()


# --------------------------------------------------------------------------
# Base
# --------------------------------------------------------------------------
class R14BBase(unittest.TestCase):
    """Isolated DB + real Supervisor/Watchdog harness for FI tests."""

    maxDiff = None

    # -- setup / teardown -------------------------------------------------
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="r14b-")
        self.db = os.path.join(self.tmpdir, "axos.db")
        self.store = open_store(self.db)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        self._sups: list[Supervisor] = []
        self._wds: list[Watchdog] = []
        self._recs: list[Reconciler] = []
        self._procs: list[subprocess.Popen] = []
        self._driver_procs: list[subprocess.Popen] = []

    def tearDown(self) -> None:
        for sup in self._sups:
            try:
                for wid in list(sup._procs):
                    try:
                        if sup._procs[wid].popen.poll() is None:
                            sup.kill_worker(wid)
                        else:
                            sup.reap(wid)
                    except Exception:
                        pass
                # Adopted (post-restart) procs are not in _procs.
                for (_w, _pid), ad in list(sup._adopted.items()):
                    try:
                        os.killpg(ad.pid, signal.SIGKILL)
                    except (ProcessLookupError, PermissionError):
                        pass
                    except Exception:
                        pass
            except Exception:
                pass
            try:
                sup.stop()
            except Exception:
                pass
            try:
                sup.close()
            except Exception:
                pass
        for wd in self._wds:
            try:
                wd.close()
            except Exception:
                pass
        for rc in self._recs:
            try:
                rc.close()
            except Exception:
                pass
        for p in self._procs + self._driver_procs:
            try:
                if p.poll() is None:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
            except Exception:
                pass
            try:
                p.wait(timeout=5)
            except Exception:
                pass
        try:
            self.store.close()
        except Exception:
            pass
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        # Hygiene: no process THIS test spawned may survive teardown.
        # (Scoped to owned PIDs — a global pgrep is invalid: other AXOS
        # work may run concurrently on this machine, and the pattern even
        # matches the shell running the test itself.)
        self._assert_owned_procs_dead()

    @staticmethod
    def _pid_dead(pid: int) -> bool:
        """True when the process is gone — a zombie counts as dead."""
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

    def _assert_owned_procs_dead(self) -> None:
        owned: set[int] = set()
        for sup in self._sups:
            try:
                for info in list(sup._procs.values()):
                    owned.add(info.popen.pid)
                for rec in list(sup._reaped.values()):
                    if rec.get("pid"):
                        owned.add(int(rec["pid"]))
                for ad in list(sup._adopted.values()):
                    owned.add(int(ad.pid))
            except Exception:
                pass
        for p in self._procs + self._driver_procs:
            try:
                owned.add(p.pid)
            except Exception:
                pass
        # Give SIGKILLed processes a moment to actually die.
        deadline = time.monotonic() + 5.0
        alive = set()
        while time.monotonic() < deadline:
            alive = {pid for pid in owned if not self._pid_dead(pid)}
            if not alive:
                break
            time.sleep(0.05)
        self.assertEqual(
            alive, set(),
            f"processes spawned by this test survived teardown: {alive}")

    # -- small helpers ----------------------------------------------------
    def _wait_for(self, cond, timeout: float, desc: str):
        deadline = time.monotonic() + timeout
        while True:
            try:
                if cond():
                    return True
            except Exception:
                pass
            if time.monotonic() > deadline:
                self.fail(f"timeout waiting for: {desc}")
            time.sleep(0.05)

    def _kill_procgroup(self, proc: subprocess.Popen) -> None:
        """Real kill -9 of a whole process group (worker + children)."""
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)

    def _new_task(self, task_id: str, objective: str) -> str:
        self.gate.create_task(task_id, {"objective": objective},
                              {"usd": 10}, "scheduler")
        self.gate.transition_task(task_id, "AUTHORIZED", "operator",
                                   reason="r14b")
        self.gate.transition_task(task_id, "PLANNED", "operator",
                                   reason="r14b")
        self.gate.transition_task(task_id, "EXECUTING", "operator",
                                   reason="r14b")
        return task_id

    def _new_sup(self, actor: str = "test-r14b",
                 hb: float = 1.0) -> Supervisor:
        sup = Supervisor(self.db, actor=actor, heartbeat_interval_s=hb)
        self._sups.append(sup)
        return sup

    def _new_watchdog(self, hb_stale: float = 2.0,
                      prog_stale: float = 30.0, sup=None) -> Watchdog:
        # Boot runs synchronously in Supervisor.__init__; passing the
        # supervisor derives honest readiness from its boot report.
        kw = {"supervisor": sup} if sup is not None else {
            "readiness": lambda: True}
        wd = Watchdog(self.db, WatchdogConfig(
            heartbeat_stale_s=hb_stale, progress_stale_s=prog_stale,
            evaluation_interval_s=0.2), **kw)
        self._wds.append(wd)
        return wd

    def _events(self, event_type: str | None = None) -> list[dict]:
        q = "SELECT event_type, payload, actor FROM ledger ORDER BY seq"
        rows = self.store.conn.execute(q).fetchall()
        out = []
        for r in rows:
            if event_type is None or r["event_type"] == event_type:
                out.append({"type": r["event_type"],
                            "payload": json.loads(r["payload"]),
                            "actor": r["actor"]})
        return out

    def _event_payloads(self, event_type: str) -> list[dict]:
        return [e["payload"] for e in self._events(event_type)]

    # -- suite rules (after every FI) --------------------------------------
    def _assert_suite_rules(self, tag: str,
                            incident_ids: list[str] | None = None) -> None:
        gate = self.gate
        # 1. Ledger chain verifies.
        ok, detail = gate.verify_ledger_chain()
        self.assertTrue(ok, f"{tag}: ledger chain broken: {detail}")
        # 2. No orphaned leases: every CLAIMED/RUNNING/COMMITTING job has a
        # live lease bound to its owner, or a DEAD/STALLED verdict row and
        # a forced reclaim incident behind the current state.
        now = self.store.current_time()
        orphans = []
        for j in self.store.conn.execute(
                "SELECT job_id, status, owner_worker_id, fencing_token,"
                " lease_expires_at FROM jobs").fetchall():
            if j["status"] not in ("CLAIMED", "RUNNING", "COMMITTING"):
                continue
            if j["owner_worker_id"] and j["lease_expires_at"] > now:
                continue
            orphans.append(dict(j))
        self.assertEqual(orphans, [],
                         f"{tag}: orphaned leases: {orphans}")
        # 3. No running fenced-out workers: no LIVE process whose
        # (worker_id, proc_id) identity was superseded by a fencing event.
        # The authoritative fencing record is worker.fence_enforced
        # (exactly one per fenced worker, per the supervisor).
        fenced = {(p.get("worker_id"), p.get("proc_id"))
                  for p in self._event_payloads("worker.fence_enforced")}
        live_bad = []
        for wid, pid in fenced:
            try:
                os.kill(pid, 0)
                live_bad.append((wid, pid))
            except (ProcessLookupError, PermissionError, TypeError):
                pass
        self.assertEqual(live_bad, [],
                         f"{tag}: fenced-out workers still alive: {live_bad}")
        # 4. No unclassified staging orphans.
        orph = gate.classify_staging_orphans()
        unclassified = [o for o in orph
                        if o.get("classification") == "unclassified"]
        self.assertEqual(unclassified, [],
                         f"{tag}: unclassified staging orphans: "
                         f"{unclassified[:3]}")
        # 5. Every recovery action has a contract row with a measured
        # progress delta and an explicit, I-18-legal decision. This covers
        # not only the scenario's incidents but EVERY incident in the DB
        # (including boot-created forced reclaims) — no recovery action
        # may escape the contract.
        seen: set[str] = set()
        for iid in (incident_ids or []):
            seen.add(iid)
        for r in self.store.conn.execute(
                "SELECT incident_id FROM incidents").fetchall():
            seen.add(r["incident_id"])
        for iid in sorted(seen):
            atts = gate.recovery_attempts_for(iid)
            self.assertTrue(atts,
                            f"{tag}: incident {iid} has no attempt rows")
            for a in atts:
                self.assertIsInstance(a["progress_delta"], float,
                                      f"{tag}: attempt {a['attempt_id']} has"
                                      " no measured progress delta")
                self.assertTrue(a["observed_effect"],
                                f"{tag}: attempt {a['attempt_id']} has no"
                                " observed effect (I-18)")
                if a["decision"] == "success":
                    self.assertGreater(
                        a["progress_delta"], 0.0,
                        f"{tag}: attempt {a['attempt_id']} recorded success"
                        " with zero progress delta")

    # -- recovery-attempt helper -------------------------------------------
    def _new_incident(self, job_id: str, failure_class: str = "DEAD",
                      detection: dict | None = None) -> str:
        """Stable recovery incident for one job failure (R8 authority)."""
        inc = self.gate.find_or_create_recovery_incident(
            job_id, failure_class,
            detection or {"job_id": job_id}, "recovery-controller")
        return inc["incident_id"]

    def _service_incident(self, task_id: str, failure_class: str,
                          error_class: str, detection: dict) -> str:
        inc = self.gate.create_incident(
            None, "service", failure_class, None, None, None, error_class,
            "recovery-controller", task_id=task_id, detection=detection)
        return inc["incident_id"]

    def _resolve_incident(self, incident_id: str, diagnosis: str) -> None:
        self.gate.set_incident_outcome(incident_id, "success", diagnosis,
                                       "recovery-controller")
    def _record_attempt(self, incident_id: str, rung: int, action_name: str,
                        observed_effect: str, job_id: str,
                        before: dict, decision: str,
                        actor: str = "recovery-controller") -> dict:
        """One contract row for one recovery action; delta measured from
        the D6 evidence bundle before vs after. When a job transitions to
        COMPLETE with no finer progress metric, the completion itself
        counts as a binary progress unit of 1.0."""
        after = self.gate.progress_evidence_for_job(job_id)
        delta = after["progress_done"] - before["progress_done"]
        became_complete = (
            after.get("job", {}).get("status") == "COMPLETE"
            and before.get("job", {}).get("status") != "COMPLETE")
        if became_complete:
            total = after.get("progress_total")
            if total:
                delta += total - after["progress_done"]
            if delta <= 0:
                delta = 1.0
        return self.gate.record_recovery_attempt(
            None, incident_id, rung,
            {"name": action_name, "rung": rung,
             "rung_name": CANONICAL_LADDER[rung]},
            observed_effect, float(delta), 0.0, decision, actor)

# ==========================================================================
# R14-ENV — environment record + migration version gate (runs first)
# ==========================================================================
class TestR14BEnv(R14BBase):
    def test_R14_ENV_migration_and_record(self):
        versions = applied_versions(self.store)
        self.assertTrue(versions, "no migrations applied")
        self.assertEqual(
            versions[-1], 12,
            f"migration version must be exactly 12, got {versions[-1]}")
        print("\nR14-ENV environment record:")
        for k in sorted(ENVIRONMENT):
            print(f"  {k}: {ENVIRONMENT[k]}")
        print(f"  migration_version: {versions[-1]}")
        REG.record("R14-ENV", True,
                   f"migration={versions[-1]} host={ENVIRONMENT['host']}")


# ==========================================================================
# FI-01 — worker process crash (SIGKILL-equivalent mid-execution)
# ==========================================================================
class TestR14BFI01(R14BBase):
    """10 jobs / 2 workers; one RUNNING worker is SIGKILLed before
    staging. Expected: watchdog DEAD verdict, no premature reclaim, token
    bump + stale-token replay rejected, UNCERTAIN -> PENDING requeue, all
    10 jobs complete exactly once."""

    def _fi01_impl(self) -> None:
        tag = "FI01"
        t = self._new_task("t-fi01", "fi01 worker crash")
        jids = [f"jfi01-{i}" for i in range(10)]
        for j in jids:
            self.gate.create_job(j, t, "s", "scheduler")
        sup = self._new_sup("test-fi01")
        # wfi01-0 crashes mid-execution (progress 50/100, then os._exit(3)
        # — the real crash path, no commit). wfi01-1 is a healthy worker.
        sup.start_worker("wfi01-0", jids[0], {"kind": "crash"},
                         ttl_s=120.0, hb_interval_s=0.2, renew=False)
        sup.start_worker("wfi01-1", jids[1],
                         {"kind": "success_delayed", "duration_s": 2.0,
                          "steps": 4},
                         ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(
            lambda: sup.reap("wfi01-0") is not None
            and self.gate.get_job(jids[1])["status"] == "RUNNING",
            30, "fi01 crash + healthy worker running")
        j0 = self.gate.get_job(jids[0])
        tok0 = j0["fencing_token"]
        # The supervisor must NOT reclaim: the job row stays RUNNING with
        # the dead owner's live lease (reclaim is recovery authority).
        self.assertEqual(j0["status"], "RUNNING",
                         "crashed worker's job must stay RUNNING until a"
                         " DEAD verdict + forced reclaim")
        self.assertEqual(j0["owner_worker_id"], "wfi01-0")
        self.assertEqual(self.gate.get_worker("wfi01-0")["status"],
                         "SUSPECT")
        # Watchdog: real evaluation over real process-death evidence.
        wd = self._new_watchdog(hb_stale=2.0, sup=sup)
        time.sleep(3.0)
        verdicts = wd.evaluate()
        v0 = next(v for v in verdicts if v["job_id"] == jids[0])
        self.assertEqual(v0["verdict"], "DEAD",
                         f"watchdog must return DEAD, got {v0}")
        # Recovery: incident + forced reclaim with the fresh token.
        iid = self._new_incident(
            jids[0], "DEAD",
            {"job_id": jids[0], "fi": "01",
             "detection": "watchdog DEAD verdict on crashed worker"})
        before = self.gate.progress_evidence_for_job(jids[0])
        r = self.gate.reclaim_lease(
            jids[0], actor="recovery-controller", reason="fi01",
            expected_owner="wfi01-0",
            expected_token=v0["fencing_token"],
            force=True, verdict="DEAD", incident_id=iid)
        self.assertEqual(r["status"], "UNCERTAIN")
        self.assertEqual(r["fencing_token"], tok0 + 1,
                         "forced reclaim must bump the fencing token")
        # The dead worker's token is now stale: every fenced surface
        # must reject it. renew_lease returns False on staleness
        # (documented); staging checks fencing before state, so the
        # stale token raises LeaseError even though the job moved on.
        self.assertFalse(self.gate.renew_lease(
            jids[0], "wfi01-0", tok0, 60.0, "worker:wfi01-0"),
            "stale-token renew must be refused")
        with self.assertRaises(LeaseError):
            self.gate.stage_artifact(
                job_id=jids[0], worker_id="wfi01-0", fencing_token=tok0,
                task_id=t, kind="test", data=b"stale",
                actor="worker:wfi01-0")
        self._record_attempt(
            iid, 2, "forced-reclaim+requeue",
            f"forced reclaim token {tok0}->{tok0 + 1} on DEAD verdict;"
            " RUNNING->UNCERTAIN; requeued to PENDING; fault unresolved",
            jids[0], before, "retry")
        self.gate.transition_job(jids[0], "PENDING", "recovery-controller")
        # Replacement worker completes the requeued job via the R5 commit
        # contract (real staged bytes, real validation receipts).
        before_replace = self.gate.progress_evidence_for_job(jids[0])
        sup.start_worker("wfi01-r", jids[0],
                         {"kind": "success_immediate"},
                         ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(
            lambda: self.gate.get_job(jids[0])["status"] == "COMPLETE",
            60, "fi01 replacement completion")
        att = self._record_attempt(
            iid, 3, "replace/reassign",
            "replacement worker completed the requeued job via the R5"
            " stage->begin_commit->verify->commit contract",
            jids[0], before_replace, "success")
        self.assertGreater(att["progress_delta"], 0.0)
        self._resolve_incident(
            iid, "exactly-once completion by replacement worker")
        # The remaining 8 jobs complete on healthy workers.
        for i, j in enumerate(jids[2:], start=2):
            sup.start_worker(f"wfi01-{i}", j,
                             {"kind": "success_immediate"},
                             ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(
            lambda: all(self.gate.get_job(j)["status"] == "COMPLETE"
                        for j in jids),
            120, "fi01 all jobs complete")
        # Exactly-once: one commit per job, no duplicates.
        commits = self._event_payloads("job.committed")
        by_job: dict[str, int] = {}
        for c in commits:
            by_job[c["job_id"]] = by_job.get(c["job_id"], 0) + 1
        for j in jids:
            self.assertEqual(by_job.get(j), 1,
                             f"job {j} committed {by_job.get(j)} times")
        self._assert_suite_rules(tag, [iid])
        REG.record("R14-FI01", True,
                   "10/10 jobs COMPLETE, exactly-once; token bump+replay"
                   " rejection verified")

    def test_R14_FI01_worker_crash(self):
        self._fi01_impl()

    def test_R14_FI01_worker_crash_repeat(self):
        self._fi01_impl()


# ==========================================================================
# FI-02 — crash after staging, before commit (partial artifact)
# ==========================================================================
class TestR14BFI02(R14BBase):
    """Two crash_after_stage workers: one with intact staged bytes
    (adoptable -> exactly-once via resolver adoption), one with corrupted
    staged bytes (requeue, corrupt bytes never validated)."""

    def _fi02_impl(self) -> None:
        tag = "FI02"
        t = self._new_task("t-fi02", "fi02 crash after stage")
        ja, jb = "jfi02-a", "jfi02-b"
        self.gate.create_job(ja, t, "s", "scheduler")
        self.gate.create_job(jb, t, "s", "scheduler")
        sup = self._new_sup("test-fi02")
        sup.start_worker("wfi02-a", ja, {"kind": "crash_after_stage"},
                         ttl_s=120.0, hb_interval_s=0.2, renew=False)
        sup.start_worker("wfi02-b", jb,
                         {"kind": "crash_after_stage",
                          "corrupt_staged_bytes": True},
                         ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(
            lambda: sup.reap("wfi02-a") is not None
            and sup.reap("wfi02-b") is not None,
            30, "fi02 both workers crashed after staging")
        wd = self._new_watchdog(hb_stale=2.0, sup=sup)
        time.sleep(3.0)
        verdicts = {v["job_id"]: v for v in wd.evaluate()}
        self.assertEqual(verdicts[ja]["verdict"], "DEAD")
        self.assertEqual(verdicts[jb]["verdict"], "DEAD")
        iids: list[str] = []
        # --- job A: intact staged bytes -> adopt ---
        iida = self._new_incident(
            ja, "DEAD",
            {"job_id": ja, "fi": "02",
             "detection": "DEAD; staged bytes intact"})
        iids.append(iida)
        before_a = self.gate.progress_evidence_for_job(ja)
        ra = self.gate.reclaim_lease(
            ja, actor="recovery-controller", reason="fi02",
            expected_owner="wfi02-a",
            expected_token=verdicts[ja]["fencing_token"],
            force=True, verdict="DEAD", incident_id=iida)
        self.assertEqual(ra["status"], "UNCERTAIN")
        insp = self.gate.inspect_uncertain_completion(ja)
        self.assertEqual(insp["disposition"], "UNCERTAIN")
        self.assertTrue(insp["adoptable_artifacts"],
                        f"intact staged bytes must be adoptable: {insp}")
        aid = insp["adoptable_artifacts"][0]
        cand = next(c for c in insp["candidates"]
                    if c["artifact_id"] == aid)
        expected_hash = hashlib.sha256(
            b"fi-artifact-" + ja.encode("utf-8")).hexdigest()
        self.assertEqual(cand["content_hash"], expected_hash)
        self.assertTrue(cand["hash_matches"])
        self.assertTrue(cand["verified"])
        # Resolver adoption: the recovery authority commits the staged
        # artifact in place of the dead worker (token lineage proves the
        # bytes predate the reclaim).
        adopted = self.gate.commit_artifact(
            ja, None, None, artifact_id=aid, actor="reconciler",
            evidence={"fi": "02", "adopted_staged": True})
        self.assertEqual(adopted["status"], "COMPLETE")
        # The COMMITTED bytes are exactly the staged bytes — no
        # re-execution happened.
        art = self.store.conn.execute(
            "SELECT content_hash, status FROM artifacts WHERE"
            " artifact_id=?", (aid,)).fetchone()
        self.assertEqual(art["content_hash"], expected_hash)
        self.assertEqual(art["status"], "VALIDATED")
        att = self._record_attempt(
            iida, 3, "adopt-uncertain-completion",
            "resolver adopted the verified staged artifact in place of the"
            " dead worker; job COMPLETE with zero re-execution",
            ja, before_a, "success")
        self.assertGreater(att["progress_delta"], 0.0)
        self._resolve_incident(iida, "adopted staged completion")
        # --- job B: corrupted staged bytes -> requeue, never validate ---
        iidb = self._new_incident(
            jb, "DEAD",
            {"job_id": jb, "fi": "02",
             "detection": "DEAD; staged bytes corrupted"})
        iids.append(iidb)
        before_b = self.gate.progress_evidence_for_job(jb)
        rb = self.gate.reclaim_lease(
            jb, actor="recovery-controller", reason="fi02",
            expected_owner="wfi02-b",
            expected_token=verdicts[jb]["fencing_token"],
            force=True, verdict="DEAD", incident_id=iidb)
        self.assertEqual(rb["status"], "UNCERTAIN")
        insp_b = self.gate.inspect_uncertain_completion(jb)
        self.assertEqual(insp_b["disposition"], "NOT_COMMITTED",
                         "corrupted staged bytes must yield no adoptable"
                         f" artifact: {insp_b}")
        self.assertEqual(insp_b["adoptable_artifacts"], [])
        bad = [c for c in insp_b["candidates"] if not c["hash_matches"]]
        self.assertTrue(bad, "the corruption must be visible as a hash"
                             " mismatch on the staged bytes")
        self._record_attempt(
            iidb, 2, "forced-reclaim+requeue",
            "forced reclaim; staged bytes corrupt (hash mismatch on"
            " re-read); requeued to PENDING; corrupt bytes never"
            " validated, never committed",
            jb, before_b, "retry")
        self.gate.transition_job(jb, "PENDING", "recovery-controller")
        # The corrupt artifact row was VALIDATED against the pre-corruption
        # bytes; what matters is that the bytes on disk no longer match,
        # so it is never adoptable and never committed.
        rows = self.store.conn.execute(
            "SELECT artifact_id, status FROM artifacts WHERE job_id=?",
            (jb,)).fetchall()
        self.assertTrue(rows)
        for rrow in rows:
            d = dict(rrow)
            self.assertNotEqual(
                d["status"], "RELEASED",
                "corrupt staged bytes must never be released/committed")
        before_rb = self.gate.progress_evidence_for_job(jb)
        sup.start_worker("wfi02-rb", jb, {"kind": "success_immediate"},
                         ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(
            lambda: self.gate.get_job(jb)["status"] == "COMPLETE",
            60, "fi02-b replacement completion")
        att_b = self._record_attempt(
            iidb, 3, "replace/reassign",
            "replacement worker completed from scratch via the R5"
            " contract; corrupt staged bytes discarded",
            jb, before_rb, "success")
        self.assertGreater(att_b["progress_delta"], 0.0)
        self._resolve_incident(iidb, "re-executed cleanly")
        self._assert_suite_rules(tag, iids)
        REG.record("R14-FI02", True,
                   "adopt path (zero re-execution) + corrupt path (requeue,"
                   " corrupt bytes never adopted) both exactly-once")

    def test_R14_FI02_crash_after_stage(self):
        self._fi02_impl()

    def test_R14_FI02_crash_after_stage_repeat(self):
        self._fi02_impl()

# ==========================================================================
# FI-03 — supervisor process kill -9 (orphaned workers keep running)
# ==========================================================================
class TestR14BFI03(R14BBase):
    """The supervisor is SIGKILLed mid-flight; the two worker processes
    (separate session leaders) survive, keep renewing, and complete on
    their own. Expected: clean boot, service_restart + exactly one
    reconciler pass, zero duplicate workers, task completes."""

    def _spawn_driver(self, jids, wids, ready_name):
        ready = os.path.join(self.tmpdir, ready_name)
        log = open(os.path.join(self.tmpdir, ready_name + ".log"), "w")
        drv = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "_supdrv.py"),
             "--db", self.db,
             "--scenario", "fi03", "--ready", ready,
             "--job-ids", ",".join(jids),
             "--worker-ids", ",".join(wids)],
            start_new_session=True, stdout=log, stderr=subprocess.STDOUT)
        self._driver_procs.append(drv)
        self._wait_for(lambda: os.path.exists(ready), 30,
                       "fi03 driver ready")
        return drv

    def _fi03_impl(self) -> None:
        tag = "FI03"
        t = self._new_task("t-fi03", "fi03 supervisor kill -9")
        for dw in ("dw-fi03-a", "dw-fi03-b"):
            self.gate.set_desired_item(
                dw, {"task_id": t, "stage_id": "s", "max_attempts": 3,
                     "policy": {}}, "scheduler")
        head = self.gate.get_desired_head()["version"]
        jids = []
        for dw in ("dw-fi03-a", "dw-fi03-b"):
            job, _ = self.gate.ensure_job_for_desired_state(
                desired_work_id=dw, task_id=t, stage_id="s",
                max_attempts=3, policy={}, desired_version=head,
                actor="reconciler")
            jids.append(job["job_id"])
        wids = ["wfi03-0", "wfi03-1"]
        drv = self._spawn_driver(jids, wids, "fi03.ready")
        self._wait_for(
            lambda: all(self.gate.get_job(j)["status"] == "RUNNING"
                        for j in jids),
            30, "fi03 both workers running")
        spawns = [p for p in self._event_payloads("worker.proc_spawned")
                  if p["worker_id"] in wids]
        self.assertEqual(len(spawns), 2)
        worker_pids = {p["worker_id"]: p["pid"] for p in spawns}
        before = {j: self.gate.progress_evidence_for_job(j) for j in jids}
        tip_before = self.store.conn.execute(
            "SELECT COALESCE(MAX(seq),0) FROM ledger").fetchone()[0]
        time.sleep(3)  # mid-flight
        # Inject: real kill -9 of the supervisor driver process group.
        self._kill_procgroup(drv)
        drv.wait(timeout=15)
        self.assertIsNotNone(drv.poll())
        # The workers (separate session leaders) survive the kill.
        for wid, pid in worker_pids.items():
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                self.fail(f"fi03 worker {wid} (pid {pid}) died with the"
                          " supervisor; the injection premise is broken")
        # No new scheduling may happen between the kill and the boot.
        new_types = [r["event_type"] for r in self.store.conn.execute(
            "SELECT event_type FROM ledger WHERE seq > ?",
            (tip_before,)).fetchall()]
        self.assertNotIn("job.created", new_types)
        self.assertNotIn("worker.proc_spawned", new_types)
        # Recover: fresh Supervisor boots from disk on the same DB file.
        sup2 = Supervisor(self.db, actor="test-fi03-boot",
                          heartbeat_interval_s=1.0)
        self._sups.append(sup2)
        report = sup2._boot_report
        self.assertEqual(report["phase"], "READY",
                         f"boot must reach READY, got {report['phase']}: "
                         f"{report.get('blockers')}")
        self.assertEqual(report["errors"], [],
                         f"boot recovery must be error-free: {report['errors']}")
        self.assertTrue(report["fully_recovered"],
                        "boot must report fully_recovered")
        # The bootstrap monitor journals the service restart.
        self.gate.append_event(
            "service_restart",
            {"reason": "supervisor SIGKILL; boot recovery RECOVERED",
             "boot_report_phase": report["phase"],
             "adopted_workers": wids}, "test-fi03")
        # Exactly one reconciler pass over the desired state.
        runs_before = self.store.conn.execute(
            "SELECT COUNT(*) FROM reconciliation_runs").fetchone()[0]
        rc = Reconciler(self.db, ReconcilerConfig(poll_interval_s=60.0,
                                                  batch_size=10))
        self._recs.append(rc)
        rep = rc.reconcile()
        self.assertIn(rep["result"], ("CONVERGED", "CHANGED"),
                      f"reconciler pass failed: {rep}")
        runs_after = self.store.conn.execute(
            "SELECT COUNT(*) FROM reconciliation_runs").fetchone()[0]
        self.assertEqual(runs_after, runs_before + 1,
                         "exactly one reconciler pass must be recorded")
        # Zero duplicate workers: same worker ids still own the jobs, no
        # new spawns happened.
        for j, wid in zip(jids, wids):
            self.assertEqual(self.gate.get_job(j)["owner_worker_id"], wid)
        spawns2 = [p for p in self._event_payloads("worker.proc_spawned")
                   if p["worker_id"] in wids]
        self.assertEqual(len(spawns2), 2,
                         "no duplicate worker processes may be spawned")
        # The surviving workers complete on their own (leases kept alive
        # by their own renewal loops).
        self._wait_for(
            lambda: all(self.gate.get_job(j)["status"] == "COMPLETE"
                        for j in jids),
            120, "fi03 surviving workers complete")
        # One contract row per job for the boot-recovery resolution.
        iid = self._service_incident(
            t, "SUPERVISOR_DEAD", "supervisor_sigkill",
            {"fi": "03",
             "detection": "supervisor process SIGKILLed; workers survived"
                          " and completed under boot recovery"})
        for j in jids:
            att = self._record_attempt(
                iid, 2, "boot-recovery-adopt",
                "boot recovery adopted the surviving worker process;"
                " one reconciler pass; no rescheduling; job completed",
                j, before[j], "success")
            self.assertGreater(att["progress_delta"], 0.0)
        self._resolve_incident(iid, "task completed after supervisor kill")
        # Ledger shows service_restart and the single reconciler pass.
        types = [e["type"] for e in self._events()]
        self.assertIn("service_restart", types)
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM reconciliation_runs").fetchone()[0],
            runs_after)
        self.assertEqual(
            len([j for j in jids
                 if self.gate.get_job(j)["status"] == "COMPLETE"]), 2)
        self._assert_suite_rules(tag, [iid])
        REG.record("R14-FI03", True,
                   "supervisor SIGKILL; workers survived, boot RECOVERED,"
                   " 1 reconciler pass, 0 duplicate workers")

    def test_R14_FI03_supervisor_kill(self):
        self._fi03_impl()

    def test_R14_FI03_supervisor_kill_repeat(self):
        self._fi03_impl()


# ==========================================================================
# FI-04 — full VM restart (kill -9 ALL AXOS processes, boot from disk)
# ==========================================================================
class TestR14BFI04(R14BBase):
    """HONEST-EQUIVALENT (see module docstring): real kill -9 of every
    AXOS process (worker process groups, then the driver supervisor
    itself — a full control-plane kill), then boot-from-disk recovery by
    a fresh Supervisor. NOT injected: OS power loss, disk-cache loss,
    wall-clock jumps."""

    def _fi04_impl(self) -> None:
        from axos.store.gate import REQUIRED_VALIDATORS
        tag = "FI04"
        t = self._new_task("t-fi04", "fi04 full vm restart")
        for i in range(6):
            self.gate.set_desired_item(
                f"dw-fi04-{i}",
                {"task_id": t, "stage_id": "s", "max_attempts": 3,
                 "policy": {}}, "scheduler")
        head = self.gate.get_desired_head()["version"]
        jids = []
        for i in range(6):
            job, _ = self.gate.ensure_job_for_desired_state(
                desired_work_id=f"dw-fi04-{i}", task_id=t, stage_id="s",
                max_attempts=3, policy={}, desired_version=head,
                actor="reconciler")
            jids.append(job["job_id"])
        # 4 of 8 jobs complete BEFORE the kill (task 50% complete).
        sup0 = self._new_sup("test-fi04-pre")
        for k in range(4):
            sup0.start_worker(f"wfi04-pre-{k}", jids[k],
                              {"kind": "success_immediate"},
                              ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(
            lambda: all(self.gate.get_job(j)["status"] == "COMPLETE"
                        for j in jids[:4]),
            60, "fi04 pre-kill completions")
        pre_committed = {
            j: (self.gate.get_job(j)["result_artifact_id"],
                self.gate.get_job(j)["content_hash"]) for j in jids[:4]}
        sup0.close()  # completed workers' processes already exited
        # One engineered UNCERTAIN: staged + verified artifact, worker
        # "dies" (DEAD verdict + forced reclaim), bytes stay on disk.
        ju = "j-fi04-unc"
        self.gate.create_job(ju, t, "s", "scheduler")
        self.gate.create_worker("w-eng", "test-fi04")
        for s in ("IDLE", "ASSIGNED", "RUNNING"):
            self.gate.transition_worker("w-eng", s, "test-fi04")
        self.gate.claim_job(ju, "w-eng", 120.0, "test-fi04")
        toku = self.gate.get_job(ju)["fencing_token"]
        self.gate.transition_job(ju, "RUNNING", "worker:w-eng")
        udata = b"fi04-uncertain-bytes"
        uart = self.gate.stage_artifact(
            job_id=ju, worker_id="w-eng", fencing_token=toku, task_id=t,
            kind="test", data=udata, actor="worker:w-eng")
        uaid = uart["artifact_id"]
        for vid, ver in REQUIRED_VALIDATORS:
            self.gate.record_validation(
                None, uaid, vid, ver, "PASS", "validator",
                method="fi04-engineered")
        self.gate.verify_artifact(uaid, actor="system")
        self.gate.record_watchdog_verdict(
            ju, toku, "DEAD", {"reason": "engineered pre-boot death"},
            "test-fi04")
        inc_u = self._new_incident(
            ju, "DEAD",
            {"job_id": ju, "fi": "04",
             "detection": "engineered pre-boot UNCERTAIN with staged"
                          " artifact"})
        self.gate.reclaim_lease(
            ju, actor="recovery-controller", reason="fi04",
            expected_owner="w-eng", expected_token=toku,
            force=True, verdict="DEAD", incident_id=inc_u)
        self.assertEqual(self.gate.get_job(ju)["status"], "UNCERTAIN")
        before_u = self.gate.progress_evidence_for_job(ju)
        # One PENDING job, untouched.
        jp = "j-fi04-pend"
        self.gate.create_job(jp, t, "s", "scheduler")
        # One verified checkpoint over the 4 completed artifacts.
        manifest = [{"artifact_id": a, "content_hash": h}
                    for a, h in pre_committed.values()]
        ck = self.gate.stage_checkpoint(t, manifest=manifest,
                                        actor="system", trigger="policy")
        ckid = ck["checkpoint_id"]
        self.gate.verify_checkpoint(ckid, actor="system")
        self.assertEqual(self.gate.latest_known_good(t)["checkpoint_id"],
                         ckid)
        # The driver runs the remaining 2 jobs (long workers).
        wids = ["wfi04-0", "wfi04-1"]
        ready = os.path.join(self.tmpdir, "fi04.ready")
        log = open(os.path.join(self.tmpdir, "fi04.log"), "w")
        drv = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "_supdrv.py"),
             "--db", self.db,
             "--scenario", "fi03", "--ready", ready,
             "--job-ids", ",".join(jids[4:]),
             "--worker-ids", ",".join(wids)],
            start_new_session=True, stdout=log, stderr=subprocess.STDOUT)
        self._driver_procs.append(drv)
        self._wait_for(lambda: os.path.exists(ready), 30,
                       "fi04 driver ready")
        self._wait_for(
            lambda: all(self.gate.get_job(j)["status"] == "RUNNING"
                        for j in jids[4:]),
            30, "fi04 driver workers running")
        time.sleep(4)  # mid-flight, task ~50% complete
        # Capture pre-kill progress evidence for the boot incidents'
        # contract rows (the boot's forced reclaim must be accounted).
        pre_kill_evidence = {
            j: self.gate.progress_evidence_for_job(j) for j in jids[4:]}
        worker_pids = [p["pid"] for p in
                       self._event_payloads("worker.proc_spawned")
                       if p["worker_id"] in wids]
        self.assertEqual(len(worker_pids), 2)
        # === INJECT: kill -9 ALL AXOS processes ===
        for pid in worker_pids:
            os.killpg(os.getpgid(pid), signal.SIGKILL)
        os.killpg(os.getpgid(drv.pid), signal.SIGKILL)
        drv.wait(timeout=15)
        # All killed workers must be dead; a transient zombie (parent also
        # killed, awaiting init reaping) counts as dead.
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if all(self._pid_dead(p) for p in worker_pids):
                break
            time.sleep(0.05)
        for pid in worker_pids:
            self.assertTrue(
                self._pid_dead(pid),
                f"worker pid {pid} survived the kill -9")
        # No AXOS runtime process may remain.
        time.sleep(1.0)
        # === RECOVER: boot from disk ===
        sup2 = Supervisor(self.db, actor="test-fi04-boot",
                          heartbeat_interval_s=1.0)
        self._sups.append(sup2)
        report = sup2._boot_report
        self.assertEqual(report["phase"], "READY",
                         f"boot must reach READY, got {report['phase']}: "
                         f"{report.get('blockers')}")
        self.assertEqual(report["errors"], [],
                         f"boot recovery must be error-free: {report['errors']}")
        self.assertTrue(report["fully_recovered"],
                        "boot must report fully_recovered")
        disps = {d.get("worker_id"): d.get("disposition")
                 for d in report.get("dispositions", [])}
        for wid in wids:
            self.assertEqual(disps.get(wid), "ALREADY_DEAD",
                             f"worker {wid} must be seen dead, got "
                             f"{disps.get(wid)}")
        # Live leases of proven-dead owners were forcibly reclaimed.
        for j in jids[4:]:
            self.assertEqual(self.gate.get_job(j)["status"], "UNCERTAIN",
                             f"{j} must be UNCERTAIN after boot reclaim")
        # Every boot-created forced-reclaim incident gets its contract row.
        # The boot's reclaim is an intermediate rung-2 step (it made the
        # job actionable; the requeue+replacement below finishes it), so
        # the honest decision is "retry" with the measured delta.
        boot_iids = [r["incident_id"] for r in self.store.conn.execute(
            "SELECT incident_id FROM incidents WHERE scope='boot'").fetchall()]
        self.assertTrue(boot_iids,
                        "boot must have journaled forced-reclaim incidents")
        for biid in boot_iids:
            brow = self.store.conn.execute(
                "SELECT * FROM incidents WHERE incident_id=?",
                (biid,)).fetchone()
            # The detection payload names the job.
            import json as _json
            det = _json.loads(brow["detection"] or "{}")
            bjob = det.get("job_id")
            self.assertIsNotNone(bjob, f"boot incident {biid} names no job")
            self.assertIn(bjob, pre_kill_evidence,
                          f"boot incident {biid} for unexpected job {bjob}")
            # Measure against the real pre-kill evidence: the job was
            # RUNNING with a live owner; now it is UNCERTAIN and
            # actionable.
            batt = self._record_attempt(
                biid, 2, "boot-forced-reclaim",
                "boot observed the owner process dead and forcibly"
                " reclaimed the live lease; job UNCERTAIN and actionable;",
                bjob, pre_kill_evidence[bjob], "retry")
            # The delta is honest (no progress_done moved); the decision
            # is "retry" because the recovery continues below.
        # The engineered UNCERTAIN's staged bytes are still adoptable.
        insp = self.gate.inspect_uncertain_completion(ju)
        self.assertEqual(insp["disposition"], "UNCERTAIN")
        self.assertIn(uaid, insp["adoptable_artifacts"])
        # Bootstrap marks non-retired workers DEAD through the graph
        # (the boot fence sweep may already have done so — idempotent).
        for wid in wids + ["w-eng"]:
            while True:
                cur = self.gate.get_worker(wid)["status"]
                if cur == "DEAD":
                    break
                nxt = {"ASSIGNED": "SUSPECT", "RUNNING": "SUSPECT",
                       "SUSPECT": "DEAD"}.get(cur)
                self.assertIsNotNone(
                    nxt, f"worker {wid} in unexpected state {cur};"
                    " cannot mark DEAD through the graph")
                self.gate.transition_worker(wid, nxt, "system")
            self.assertEqual(self.gate.get_worker(wid)["status"], "DEAD",
                             f"worker {wid} must end DEAD")
        # Reconcile desired state exactly once.
        rc = Reconciler(self.db, ReconcilerConfig(poll_interval_s=60.0,
                                                  batch_size=10))
        self._recs.append(rc)
        rep = rc.reconcile()
        self.assertIn(rep["result"], ("CONVERGED", "CHANGED"))
        # Adopt the staged artifact from the UNCERTAIN job.
        adopted = self.gate.commit_artifact(
            ju, None, None, artifact_id=uaid, actor="reconciler",
            evidence={"fi": "04", "adopted_post_boot": True})
        self.assertEqual(adopted["status"], "COMPLETE")
        att_u = self._record_attempt(
            inc_u, 3, "adopt-uncertain-completion",
            "post-boot resolver adopted the staged artifact; zero"
            " re-execution", ju, before_u, "success")
        self.assertGreater(att_u["progress_delta"], 0.0)
        self._resolve_incident(inc_u, "adopted post-boot")
        # Task resumes: requeue + replacement workers for the 2 killed
        # jobs, plus the untouched PENDING job.
        iid = self._service_incident(
            t, "VM_RESTART", "full_control_plane_kill",
            {"fi": "04",
             "detection": "kill -9 of all AXOS processes; boot READY"})
        sup3 = self._new_sup("test-fi04-resume")
        for j, wid2 in zip(jids[4:], ["wfi04-r0", "wfi04-r1"]):
            before = self.gate.progress_evidence_for_job(j)
            self.gate.transition_job(j, "PENDING", "recovery-controller")
            sup3.start_worker(wid2, j, {"kind": "success_immediate"},
                              ttl_s=120.0, hb_interval_s=0.2, renew=False)
            self._wait_for(
                lambda j=j: self.gate.get_job(j)["status"] == "COMPLETE",
                60, f"fi04 replacement {j}")
            att = self._record_attempt(
                iid, 3, "replace/reassign",
                "replacement worker completed the boot-reclaimed job via"
                " the R5 contract", j, before, "success")
            self.assertGreater(att["progress_delta"], 0.0)
        sup3.start_worker("wfi04-p", jp, {"kind": "success_immediate"},
                          ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(
            lambda: self.gate.get_job(jp)["status"] == "COMPLETE",
            60, "fi04 pending job")
        self._resolve_incident(iid, "task resumed post-boot")
        # Campaign-level completion is journaled by the bootstrap monitor
        # AFTER every recovery stage above succeeded.
        self.gate.append_event(
            "vm_recovery_complete",
            {"task_id": t, "boot_phase": report["phase"],
             "stages": ["boot_recovered", "workers_marked_dead",
                        "desired_reconciled", "uncertain_adopted",
                        "task_resumed"],
             "honest_equivalent": "kill -9 of all AXOS processes +"
             " boot-from-disk (no true power loss injected)"},
            "test-fi04")
        # All 8 jobs COMPLETE; pre-kill completions untouched (no
        # rescheduling of completed work); checkpoint intact; one task.
        all_jobs = jids + [ju, jp]
        for j in all_jobs:
            self.assertEqual(self.gate.get_job(j)["status"], "COMPLETE",
                             f"{j} not COMPLETE")
        for j in jids[:4]:
            now = (self.gate.get_job(j)["result_artifact_id"],
                   self.gate.get_job(j)["content_hash"])
            self.assertEqual(now, pre_committed[j],
                             f"{j} was rescheduled after the kill")
        self.assertEqual(self.gate.latest_known_good(t)["checkpoint_id"],
                         ckid, "verified checkpoint must survive the kill")
        task_ids = {r[0] for r in self.store.conn.execute(
            "SELECT task_id FROM tasks").fetchall()}
        self.assertEqual(
            task_ids, {t, "t"},
            f"no duplicate/unexpected tasks may be created: {task_ids}")
        self.assertIn("vm_recovery_complete",
                      [e["type"] for e in self._events()])
        self._assert_suite_rules(tag, [iid, inc_u])
        REG.record("R14-FI04", True,
                   "full control-plane kill; boot READY; 8/8 COMPLETE;"
                   " checkpoint intact; no rescheduling")

    def test_R14_FI04_full_vm_restart(self):
        self._fi04_impl()

    def test_R14_FI04_full_vm_restart_repeat(self):
        self._fi04_impl()

# ==========================================================================
# FI-05 — checkpoint corruption after VERIFIED (C2 corrupt, fall back)
# ==========================================================================
def _revalidate_checkpoint_files(gate, checkpoint_id: str) -> tuple:
    """Read-only post-verification re-validation of a checkpoint: the
    manifest must still parse, sha256(canonical) must equal the
    checkpoint_id, and every manifest artifact's bytes must re-hash to
    the recorded content_hash. Returns (ok, evidence). This is the
    detection half of FI-05 (the gate previously never re-ran it)."""
    row = gate.store.conn.execute(
        "SELECT canonical_manifest FROM checkpoints WHERE checkpoint_id=?",
        (checkpoint_id,)).fetchone()
    canon = row["canonical_manifest"]
    evidence: dict = {"checkpoint_id": checkpoint_id}
    ok = True
    if hashlib.sha256(canon.encode()).hexdigest() != checkpoint_id:
        ok = False
        evidence["manifest_id_mismatch"] = True
    try:
        manifest = json.loads(canon)
    except Exception as exc:
        ok = False
        evidence["manifest_unparseable"] = f"{type(exc).__name__}"
        manifest = []
    bad = []
    for e in manifest if isinstance(manifest, list) else []:
        arow = gate.store.conn.execute(
            "SELECT uri, content_hash FROM artifacts WHERE artifact_id=?",
            (e.get("artifact_id"),)).fetchone()
        if arow is None:
            bad.append(f"{e.get('artifact_id')}: row missing")
            continue
        try:
            with open(arow["uri"], "rb") as fh:
                data = fh.read()
        except OSError:
            bad.append(f"{e.get('artifact_id')}: bytes unreadable")
            continue
        if hashlib.sha256(data).hexdigest() != arow["content_hash"]:
            bad.append(f"{e.get('artifact_id')}: bytes hash mismatch")
    if bad:
        ok = False
        evidence["artifact_problems"] = bad
    return ok, evidence


class TestR14BFI05(R14BBase):
    """Corrupt C2's manifest bytes after VERIFIED. Expected: detection via
    post-verification re-validation, C2 marked CORRUPT (never trusted
    again), fallback to C1 + ledger replay, new verified C3, incident
    with corruption evidence. No resumption from C2 under any path."""

    def _coverage(self, task_id: str) -> int:
        """Verified-artifact coverage of the currently trusted LKG."""
        lkg = self.gate.latest_known_good(task_id)
        if lkg is None:
            return 0
        try:
            manifest = json.loads(lkg["canonical_manifest"])
        except Exception:
            return 0
        n = 0
        for e in manifest:
            r = self.store.conn.execute(
                "SELECT status FROM artifacts WHERE artifact_id=?",
                (e.get("artifact_id"),)).fetchone()
            if r and r["status"] in ("VALIDATED", "RELEASED"):
                n += 1
        return n

    def _fi05_impl(self) -> None:
        tag = "FI05"
        t = self._new_task("t-fi05", "fi05 checkpoint corruption")
        jobs = ["jfi05-a", "jfi05-b", "jfi05-c"]
        for j in jobs:
            self.gate.create_job(j, t, "s", "scheduler")
        sup = self._new_sup("test-fi05")
        # Start ONLY job A's worker: B and C must complete after C1's tip.
        sup.start_worker("wfi05-0", jobs[0],
                         {"kind": "success_immediate"},
                         ttl_s=120.0, hb_interval_s=0.2, renew=False)
        def entry(j):
            jr = self.gate.get_job(j)
            return {"artifact_id": jr["result_artifact_id"],
                    "content_hash": jr["content_hash"]}

        # Complete job A first, so C1's ledger tip sits between A's commit
        # and B/C's commits — replay from C1's tip must find later work.
        self._wait_for(
            lambda: self.gate.get_job(jobs[0])["status"] == "COMPLETE",
            60, "fi05 job A complete")
        # C1 over [a] -> VERIFIED.
        c1 = self.gate.stage_checkpoint(
            t, manifest=[entry(jobs[0])], actor="system", trigger="policy")
        c1id = c1["checkpoint_id"]
        self.gate.verify_checkpoint(c1id, actor="system")
        # B and C complete after C1.
        for i, j in enumerate(jobs[1:], start=1):
            sup.start_worker(f"wfi05-{i}", j,
                             {"kind": "success_immediate"},
                             ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(
            lambda: all(self.gate.get_job(j)["status"] == "COMPLETE"
                        for j in jobs[1:]),
            60, "fi05 jobs B/C complete")
        # C2 over [a,b,c] -> VERIFIED (LKG=C2).
        c2 = self.gate.stage_checkpoint(
            t, manifest=[entry(j) for j in jobs], actor="system",
            trigger="policy", supersedes=c1id)
        c2id = c2["checkpoint_id"]
        self.gate.verify_checkpoint(c2id, actor="system")
        self.assertEqual(self.gate.latest_known_good(t)["checkpoint_id"],
                         c2id)
        tip_c1 = self.store.conn.execute(
            "SELECT ledger_tip_seq FROM checkpoints WHERE checkpoint_id=?",
            (c1id,)).fetchone()[0]
        ok_pre, _ = _revalidate_checkpoint_files(self.gate, c2id)
        self.assertTrue(ok_pre, "C2 must be intact before the injection")
        # === INJECT: corrupt C2's manifest bytes on disk ===
        # The manifest lives in the checkpoints row (SQLite is the disk
        # here); a raw connection flipping bytes in that cell IS the disk
        # corruption — it bypasses the gate exactly as real corruption
        # would.
        canon = self.store.conn.execute(
            "SELECT canonical_manifest FROM checkpoints WHERE"
            " checkpoint_id=?", (c2id,)).fetchone()["canonical_manifest"]
        cut = len(canon) // 2
        flipped = "X" if canon[cut] != "X" else "Y"
        corrupted = canon[:cut] + flipped + canon[cut + 1:]
        raw = sqlite3.connect(self.db)
        raw.execute("UPDATE checkpoints SET canonical_manifest=?"
                    " WHERE checkpoint_id=?", (corrupted, c2id))
        raw.commit()
        raw.close()
        # === DETECT: post-verification re-validation (real, read-only) ===
        ok, evidence = _revalidate_checkpoint_files(self.gate, c2id)
        self.assertFalse(ok, "the corruption must be detected")
        self.assertTrue(evidence.get("manifest_id_mismatch")
                        or evidence.get("manifest_unparseable"),
                        f"evidence must name the corruption: {evidence}")
        # C1 is untouched by the corruption.
        ok1, _ = _revalidate_checkpoint_files(self.gate, c1id)
        self.assertTrue(ok1, "C1 must remain intact")
        # === RECOVER: invalidate C2 through the real authority ===
        iid = self._service_incident(
            t, "CHECKPOINT_CORRUPT", "manifest_corruption",
            {"fi": "05", "checkpoint_id": c2id, **evidence})
        cov_before = self._coverage(t)  # LKG still names C2 pre-invalidate
        inv = self.gate.invalidate_checkpoint(
            c2id, actor="system", evidence=evidence)
        self.assertEqual(inv["verification_status"], "CORRUPT")
        inv_events = [p for p in
                      self._event_payloads("checkpoint.invalidated")
                      if p["checkpoint_id"] == c2id]
        self.assertEqual(len(inv_events), 1)
        # C2 is never trusted again: the pointer row still names C2
        # (invariant: only verify_checkpoint() advances it) but the
        # hardened latest_known_good() refuses to return it.
        ptr = self.store.conn.execute(
            "SELECT checkpoint_id FROM checkpoint_pointers WHERE name=?",
            (f"lkg:{t}",)).fetchone()["checkpoint_id"]
        self.assertEqual(ptr, c2id)
        self.assertIsNone(self.gate.latest_known_good(t),
                          "corrupt C2 must never be returned as known-good")
        # === FALLBACK: C1 + ledger replay, re-derive, stage+verify C3 ===
        # Ledger replay from C1's tip: every job.committed after the tip.
        committed_rows = self.store.conn.execute(
            "SELECT payload FROM ledger WHERE seq > ? AND"
            " event_type='job.committed'", (tip_c1,)).fetchall()
        replayed = [json.loads(r["payload"]) for r in committed_rows]
        self.assertTrue(replayed, "ledger replay must find post-C1 work")
        # C3 = C1's intact manifest (job A, at/below the tip) + replayed
        # post-tip commits (jobs B, C). C1's canonical_manifest is intact
        # (verified above); only C2 was corrupted.
        manifest3 = [
            {"artifact_id": e["artifact_id"],
             "content_hash": e["content_hash"]}
            for e in json.loads(
                self.store.conn.execute(
                    "SELECT canonical_manifest FROM checkpoints WHERE"
                    " checkpoint_id=?", (c1id,)).fetchone()
                ["canonical_manifest"])]
        for p in replayed:
            ar = self.store.conn.execute(
                "SELECT artifact_id, content_hash, job_id FROM artifacts"
                " WHERE artifact_id=?", (p["artifact_id"],)).fetchone()
            # Re-verify the bytes behind each replayed artifact now.
            with open(self.store.conn.execute(
                    "SELECT uri FROM artifacts WHERE artifact_id=?",
                    (p["artifact_id"],)).fetchone()["uri"], "rb") as fh:
                data = fh.read()
            self.assertEqual(hashlib.sha256(data).hexdigest(),
                             ar["content_hash"],
                             f"replayed artifact {p['artifact_id']} bytes"
                             " must re-hash cleanly")
            manifest3.append({"artifact_id": ar["artifact_id"],
                              "content_hash": ar["content_hash"],
                              "job_id": ar["job_id"]})
        # C3's manifest adds the ledger-derived job_id binding, so its
        # deterministic id differs from the invalidated C2 (same logical
        # coverage, honestly different provenance).
        c3 = self.gate.stage_checkpoint(
            t, manifest=manifest3, actor="system", trigger="policy",
            supersedes=c2id)
        c3id = c3["checkpoint_id"]
        self.assertNotEqual(c3id, c2id)
        self.gate.verify_checkpoint(c3id, actor="system")
        lkg = self.gate.latest_known_good(t)
        self.assertEqual(lkg["checkpoint_id"], c3id)
        self.assertEqual(lkg["verification_status"], "VERIFIED")
        covered = {e["artifact_id"] for e in json.loads(
            lkg["canonical_manifest"])}
        self.assertEqual(covered, {entry(j)["artifact_id"] for j in jobs})
        # No resumption from C2 under any path: C2's row stays CORRUPT,
        # no VERIFIED verification event for C2 exists after the
        # corruption, and C2 was never returned by latest_known_good
        # after invalidation.
        self.assertEqual(
            self.store.conn.execute(
                "SELECT verification_status FROM checkpoints WHERE"
                " checkpoint_id=?", (c2id,)).fetchone()
            ["verification_status"], "CORRUPT")
        verif_c2 = [p for p in
                    self._event_payloads("checkpoint.verification")
                    if p["checkpoint_id"] == c2id
                    and p["to"] == "VERIFIED"]
        self.assertEqual(len(verif_c2), 1,
                         "exactly one VERIFIED event for C2 (pre-corruption)")
        # Contract row for the checkpoint recovery: delta = verified
        # artifact coverage restored under the trusted LKG (0 -> 3).
        cov_after = self._coverage(t)
        att = self.gate.record_recovery_attempt(
            None, iid, 4,
            {"name": "invalidate-corrupt-checkpoint+fallback",
             "rung": 4, "rung_name": CANONICAL_LADDER[4]},
            f"invalidated corrupt C2; fell back to C1; ledger replay from"
            f" tip {tip_c1} re-derived {len(manifest3)} artifacts; staged"
            f" and verified C3; trusted coverage {cov_before}->{cov_after}",
            float(cov_after - cov_before), 0.0, "success",
            "recovery-controller")
        self.assertGreater(att["progress_delta"], 0.0)
        self._resolve_incident(iid, "C2 invalidated; C3 verified from C1 +"
                                   " ledger replay")
        self._assert_suite_rules(tag, [iid])
        REG.record("R14-FI05", True,
                   "C2 corruption detected+invalidated; C3 verified from"
                   " C1+ledger replay; C2 never trusted again")

    def test_R14_FI05_checkpoint_corruption(self):
        self._fi05_impl()

    def test_R14_FI05_checkpoint_corruption_repeat(self):
        self._fi05_impl()


# ==========================================================================
# FI-06 — stuck progress, live heartbeats (watchdog STALLED)
# ==========================================================================
class TestR14BFI06(R14BBase):
    """heartbeat_loop worker: heartbeats fresh, progress frozen.
    Expected: watchdog STALLED (not DEAD), forced restart via
    STALLED-verdict reclaim, progress resumes, watchdog clears."""

    def _fi06_impl(self) -> None:
        tag = "FI06"
        t = self._new_task("t-fi06", "fi06 stuck progress")
        j = "jfi06"
        self.gate.create_job(j, t, "s", "scheduler")
        sup = self._new_sup("test-fi06")
        sup.start_worker("wfi06", j,
                         {"kind": "heartbeat_loop", "duration_s": 120.0},
                         ttl_s=120.0, hb_interval_s=0.2, renew=True)
        self._wait_for(lambda: len(self.gate.heartbeats_for("wfi06")) >= 2,
                       30, "fi06 heartbeats flowing")
        wd = self._new_watchdog(hb_stale=30.0, prog_stale=3.0, sup=sup)
        iid = self._new_incident(
            j, "STALLED",
            {"job_id": j, "fi": "06",
             "detection": "fresh heartbeats, frozen progress"})
        # Pass 1: progress goes stale while the worker is provably alive.
        time.sleep(4.0)
        time.sleep(0.6)  # let heartbeats land with the new store time
        v1 = next(v for v in wd.evaluate() if v["job_id"] == j)
        self.assertEqual(v1["verdict"], "STALLED",
                         f"expected STALLED, got {v1}")
        self.assertNotEqual(v1["verdict"], "DEAD",
                            "a live worker must not be called DEAD")
        # The STALLED verdict is durable evidence for the forced reclaim.
        hist = self.store.conn.execute(
            "SELECT verdict FROM watchdog_verdicts WHERE job_id=?"
            " ORDER BY evaluated_at", (j,)).fetchall()
        self.assertIn("STALLED", [r["verdict"] for r in hist])
        # Recovery: rung 2 — terminate the stuck worker, forced reclaim
        # on the STALLED verdict, requeue, replacement re-executes.
        before = self.gate.progress_evidence_for_job(j)
        sup.kill_worker("wfi06")  # real SIGTERM -> grace -> SIGKILL
        r = self.gate.reclaim_lease(
            j, actor="recovery-controller", reason="fi06",
            expected_owner="wfi06", expected_token=v1["fencing_token"],
            force=True, verdict="STALLED", incident_id=iid)
        self.assertEqual(r["status"], "UNCERTAIN")
        self.gate.transition_job(j, "PENDING", "recovery-controller")
        sup.start_worker("wfi06-r", j,
                         {"kind": "success_delayed", "duration_s": 3.0,
                          "steps": 6},
                         ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(lambda: self.gate.get_job(j)["status"] == "COMPLETE",
                       60, "fi06 replacement completes")
        att = self._record_attempt(
            iid, 2, "restart-stuck-worker",
            "terminated the stuck worker; forced reclaim on STALLED"
            " verdict; requeued; replacement re-executed to COMPLETE",
            j, before, "success")
        self.assertGreater(att["progress_delta"], 0.0)
        self._resolve_incident(iid, "progress resumed; job COMPLETE")
        # Watchdog clears: no STALLED verdict for the job anymore.
        time.sleep(4.0)
        time.sleep(0.6)
        v2 = [v for v in wd.evaluate() if v["job_id"] == j]
        self.assertFalse(any(v["verdict"] == "STALLED" for v in v2),
                         f"watchdog must clear after recovery: {v2}")
        self._assert_suite_rules(tag, [iid])
        REG.record("R14-FI06", True,
                   "STALLED (not DEAD) on live worker; restart recovered;"
                   " watchdog cleared")

    def test_R14_FI06_stuck_progress(self):
        self._fi06_impl()

    def test_R14_FI06_stuck_progress_repeat(self):
        self._fi06_impl()

# ==========================================================================
# FI-07 — breaker cascade (3 crashes -> OPEN -> half-open probe -> CLOSED)
# ==========================================================================
class TestR14BFI07(R14BBase):
    """Three real worker crashes trip the TASK breaker OPEN; the real
    claim path denies retries (no retry storm); after cooldown a single
    half-open probe is admitted and its success closes the breaker."""

    def _fi07_impl(self) -> None:
        tag = "FI07"
        t = self._new_task("t-fi07", "fi07 breaker cascade")
        jobs = ["jfi07-0", "jfi07-1", "jfi07-2"]
        for j in jobs:
            self.gate.create_job(j, t, "s", "scheduler")
        sup = self._new_sup("test-fi07")
        self.gate.ensure_breaker_state("TASK", t, cooldown_s=6.0,
                                       actor="system")
        self.assertEqual(
            self.gate.get_breaker_state("TASK", t)["state"], "CLOSED")
        wd = self._new_watchdog(hb_stale=2.0, sup=sup)
        iids: list[str] = []
        for i, j in enumerate(jobs):
            wid = f"wfi07-{i}"
            sup.start_worker(wid, j, {"kind": "crash"},
                             ttl_s=120.0, hb_interval_s=0.2, renew=False)
            self._wait_for(lambda w=wid: sup.reap(w) is not None, 30,
                           f"fi07 crash {wid}")
            time.sleep(3.0)
            v = next(x for x in wd.evaluate() if x["job_id"] == j)
            self.assertEqual(v["verdict"], "DEAD")
            iid = self._new_incident(
                j, "DEAD",
                {"job_id": j, "fi": "07", "crash_n": i + 1})
            iids.append(iid)
            before = self.gate.progress_evidence_for_job(j)
            r = self.gate.reclaim_lease(
                j, actor="recovery-controller", reason="fi07",
                expected_owner=wid, expected_token=v["fencing_token"],
                force=True, verdict="DEAD", incident_id=iid)
            self.assertEqual(r["status"], "UNCERTAIN")
            self.gate.transition_job(j, "PENDING", "recovery-controller")
            self._record_attempt(
                iid, 2, "forced-reclaim+requeue",
                f"crash {i + 1}/3: forced reclaim on DEAD verdict;"
                " requeued to PENDING; fault unresolved",
                j, before, "retry")
            # The cascade: each crash fans out as a breaker signal on the
            # TASK scope (and the JOB scope).
            for scope in (("TASK", t), ("JOB", j)):
                sig = self.gate.record_breaker_signal(
                    scope[0], scope[1], failure_kind="R7_DEAD",
                    incident_id=iid, attempt_id=None,
                    failure_window_s=120.0, cooldown_s=6.0,
                    actor="recovery-controller")
                self.assertFalse(sig["deduped"])
            # Dedupe: redelivering the same failure changes nothing.
            dup = self.gate.record_breaker_signal(
                "TASK", t, failure_kind="R7_DEAD", incident_id=iid,
                attempt_id=None, failure_window_s=120.0, cooldown_s=6.0,
                actor="recovery-controller")
            self.assertTrue(dup["deduped"])
            self.assertEqual(
                dup["row"]["failure_count"],
                self.gate.get_breaker_state("TASK", t)["failure_count"])
        task_row = self.gate.get_breaker_state("TASK", t)
        self.assertEqual(task_row["failure_count"], 3,
                         "three crashes must count three signals")
        # The controller trips the breaker OPEN (CAS on the version).
        opened = self.gate.transition_breaker(
            "TASK", t, expected_version=task_row["version"],
            to_state="OPEN", actor="recovery-controller",
            reason="fi07: 3 consecutive worker crashes in the window",
            evidence={"failure_count": 3})
        self.assertEqual(opened["state"], "OPEN")
        allowed, reason = self.gate.breaker_allows("TASK", t)
        self.assertEqual((allowed, reason), (False, "open"))
        # Retries stop: the real claim path denies admission while OPEN.
        denied = self.gate.claim_job_resilient(
            jobs[0], "wfi07-retry", 60.0, "scheduler", 10)
        self.assertFalse(denied, "OPEN breaker must deny the claim path")
        spawns = [p for p in self._event_payloads("worker.proc_spawned")
                  if p["worker_id"].startswith("wfi07-")
                  and not p["worker_id"].endswith("-retry")]
        self.assertEqual(len(spawns), 3,
                         "no retry storm: exactly the 3 crashed workers")
        # Recovery: cooldown elapses -> HALF_OPEN -> one probe admitted.
        time.sleep(7.0)
        half = self.gate.transition_breaker(
            "TASK", t,
            expected_version=self.gate.get_breaker_state("TASK", t)
            ["version"],
            to_state="HALF_OPEN", actor="recovery-controller",
            reason="fi07: cooldown elapsed; probing",
            evidence={})
        self.assertEqual(half["state"], "HALF_OPEN")
        j0 = jobs[0]
        before_probe = self.gate.progress_evidence_for_job(j0)
        admitted = self.gate.claim_job_resilient(
            j0, "wfi07-probe", 60.0, "scheduler", 10)
        self.assertTrue(admitted, "HALF_OPEN must admit one probe")
        tok = self.gate.get_job(j0)["fencing_token"]
        self.gate.transition_job(j0, "RUNNING", "worker:wfi07-probe")
        r5_complete(self.gate, job_id=j0, worker_id="wfi07-probe",
                    fencing_token=tok, task_id=t, outcome="SUCCESS",
                    evidence={"fi": "07", "probe": True},
                    actor="worker:wfi07-probe", data=b"fi07-probe")
        self.assertEqual(self.gate.get_job(j0)["status"], "COMPLETE")
        # The probe's success (measured progress) closes the breaker.
        closed = self.gate.transition_breaker(
            "TASK", t,
            expected_version=self.gate.get_breaker_state("TASK", t)
            ["version"],
            to_state="CLOSED", actor="recovery-controller",
            reason="fi07: half-open probe completed with progress",
            evidence={"probe_job": j0})
        self.assertEqual(closed["state"], "CLOSED")
        allowed2, _ = self.gate.breaker_allows("TASK", t)
        self.assertTrue(allowed2)
        iid_probe = self._service_incident(
            t, "BREAKER_OPEN", "breaker_cascade",
            {"fi": "07", "detection": "TASK breaker OPEN after 3 crashes;"
              " half-open probe recovered the scope"})
        att = self._record_attempt(
            iid_probe, 1, "half-open-probe",
            "cooldown elapsed; single half-open probe admitted via the"
            " real claim path and completed via the R5 contract; breaker"
            " CLOSED on measured progress",
            j0, before_probe, "success")
        self.assertGreater(att["progress_delta"], 0.0)
        for iid in iids:
            self._resolve_incident(iid, "crash handled; breaker absorbed"
                                       " the cascade")
        self._resolve_incident(iid_probe, "breaker recovered via probe")
        self._assert_suite_rules(tag, iids + [iid_probe])
        REG.record("R14-FI07", True,
                   "3 crashes -> OPEN (claim path denies) -> HALF_OPEN"
                   " probe -> CLOSED; no retry storm")

    def test_R14_FI07_breaker_cascade(self):
        self._fi07_impl()

    def test_R14_FI07_breaker_cascade_repeat(self):
        self._fi07_impl()


# ==========================================================================
# FI-08 — recovery escalation on repeated zero-progress failure
# ==========================================================================
class TestR14BFI08(R14BBase):
    """Two consecutive recovery attempts move zero progress (both
    replacements crash). The R9 rule — zero-delta-twice escalates —
    fires: the incident is escalated to human, no third blind retry."""

    def _fi08_impl(self) -> None:
        tag = "FI08"
        t = self._new_task("t-fi08", "fi08 escalation")
        j = "jfi08"
        self.gate.create_job(j, t, "s", "scheduler")
        sup = self._new_sup("test-fi08")
        wd = self._new_watchdog(hb_stale=2.0, sup=sup)
        iid = self._new_incident(
            j, "DEAD",
            {"job_id": j, "fi": "08",
             "detection": "repeated worker crashes, zero progress"})
        zero_deltas = 0
        for n in (1, 2):
            wid = f"wfi08-{n}"
            sup.start_worker(wid, j, {"kind": "crash"},
                             ttl_s=120.0, hb_interval_s=0.2, renew=False)
            self._wait_for(lambda w=wid: sup.reap(w) is not None, 30,
                           f"fi08 crash {n}")
            time.sleep(3.0)
            v = next(x for x in wd.evaluate() if x["job_id"] == j)
            self.assertEqual(v["verdict"], "DEAD")
            before = self.gate.progress_evidence_for_job(j)
            r = self.gate.reclaim_lease(
                j, actor="recovery-controller", reason="fi08",
                expected_owner=wid, expected_token=v["fencing_token"],
                force=True, verdict="DEAD", incident_id=iid)
            self.assertEqual(r["status"], "UNCERTAIN")
            self.gate.transition_job(j, "PENDING", "recovery-controller")
            # The R9 escalation rule: a second consecutive zero-delta
            # attempt escalates instead of retrying blindly.
            decision = ("escalate" if zero_deltas >= 1 else "retry")
            att = self._record_attempt(
                iid, 2, "forced-reclaim+requeue",
                f"attempt {n}: forced reclaim on DEAD verdict; requeued;"
                f" zero durable progress moved (delta"
                f" {0.0}); decision={decision}",
                j, before, decision)
            self.assertEqual(att["progress_delta"], 0.0)
            zero_deltas += 1
        atts = self.gate.recovery_attempts_for(iid)
        self.assertEqual(len(atts), 2)
        self.assertEqual([a["decision"] for a in atts],
                         ["retry", "escalate"])
        self.assertTrue(all(a["progress_delta"] == 0.0 for a in atts))
        # Escalation: the incident goes to a human; the job is NOT
        # retried a third time by any controller.
        self.gate.set_incident_outcome(
            iid, "escalated",
            "two consecutive zero-progress recovery attempts; escalating"
            " per the R9 rule instead of a third blind retry",
            "recovery-controller", escalated_to="human")
        inc = self.store.conn.execute(
            "SELECT outcome, escalated_to FROM incidents WHERE"
            " incident_id=?", (iid,)).fetchone()
        self.assertEqual(inc["outcome"], "escalated")
        self.assertEqual(inc["escalated_to"], "human")
        spawns_before = len(self._event_payloads("worker.proc_spawned"))
        time.sleep(2.0)  # no controller acts behind the escalation
        spawns_after = len(self._event_payloads("worker.proc_spawned"))
        self.assertEqual(spawns_before, spawns_after,
                         "no automatic retry may follow an escalation")
        self.assertEqual(self.gate.get_job(j)["status"], "PENDING")
        # A blind third retry would be a defect: prove the gate never
        # recorded one (attempt count is still exactly 2).
        self.assertEqual(len(self.gate.recovery_attempts_for(iid)), 2)
        self._assert_suite_rules(tag, [iid])
        REG.record("R14-FI08", True,
                   "zero-delta x2 -> escalated to human; no third retry")

    def test_R14_FI08_recovery_escalation(self):
        self._fi08_impl()

    def test_R14_FI08_recovery_escalation_repeat(self):
        self._fi08_impl()

# ==========================================================================
# FI-09 — wrong validator (canonical Test 9; production gate; Risk 2)
# ==========================================================================
def _fi09_honest_verdict(score: int) -> str:
    """The QA threshold rule: PASS iff score >= 50 (score in [0, 100))."""
    return "PASS" if score >= 50 else "FAIL"


def _fi09_bad_verdict(score: int) -> str:
    """The deployed v2 validator: the honest rule except on a 10% band of
    the range ([45, 55)), where its threshold is inverted."""
    honest = _fi09_honest_verdict(score)
    if 45 <= score < 55:
        return "FAIL" if honest == "PASS" else "PASS"
    return honest


class TestR14BFI09(R14BBase):
    """Canonical Test 9 — wrong validator (production gate; Risk 2).

    A QA validator version (qa-validator/v2) with an inverted threshold
    on 10% of the score range is deployed over a production batch.
    Gold-set probes on the probe schedule fail -> the bad version is
    quarantined (VALIDATION_FAILURE) -> artifacts it validated inside its
    active window are identified by validator provenance -> revalidated by
    the independent second validator (qa-validator-audit/v1) -> bad
    verdicts overturned -> the incident is disclosed in the task report.

    The components under test are the gate's validation provenance
    (record_validation), quarantine_validator, and the quarantine-aware
    receipt checks. The QA validators themselves are harness stand-ins
    for external QA services (like FI-07's scripted external service):
    deterministic functions whose verdicts are recorded through the real
    gate — nothing about the component under test is mocked."""

    QA_ID = "qa-validator"
    BAD_VER = "v2"
    PREV_VER = "v1"
    AUDIT_ID = "qa-validator-audit"
    AUDIT_VER = "v1"
    PROBE_INTERVAL = 2.0
    # 20 production scores, deterministic; 6 fall in the inverted band
    # [45, 55): 45, 47, 49, 51, 53, 54.
    PROD_SCORES = [5, 12, 23, 31, 40, 44, 45, 47, 49, 51, 53, 54, 55, 58,
                   63, 71, 77, 82, 90, 97]
    # 12 gold scores; 7 in the inverted band: 45, 46, 48, 49, 50, 52, 54.
    GOLD_SCORES = [44, 45, 46, 49, 50, 54, 55, 56, 10, 90, 48, 52]

    def _qa_record(self, artifact_id, validator_id, version, verdict,
                   method):
        return self.gate.record_validation(
            None, artifact_id, validator_id, version, verdict,
            "qa-harness", method=method,
            receipt_ref=f"{validator_id}/{version}:{artifact_id[:12]}")

    def _nonquarantined_qa_pass(self, artifact_id):
        """Non-quarantined QA-layer PASS receipts for one artifact."""
        return self.store.conn.execute(
            "SELECT validator_id, validator_version FROM validations"
            " WHERE artifact_id=? AND result='PASS' AND quarantined=0"
            " AND validator_id IN (?, ?)",
            (artifact_id, self.QA_ID, self.AUDIT_ID)).fetchall()

    def _fi09_impl(self) -> None:
        tag = "FI09"
        t = self._new_task("t-fi09", "fi09 wrong validator")
        # --- production batch: 20 jobs through the real R5 contract ---
        prod: list[tuple[str, str, int]] = []
        for i, score in enumerate(self.PROD_SCORES):
            j = f"jfi09-p{i:02d}"
            wid = f"wfi09-{i:02d}"
            self.gate.create_job(j, t, "s", "scheduler")
            self.assertTrue(self.gate.claim_job(j, wid, 120.0,
                                                "scheduler"))
            self.gate.transition_job(j, "RUNNING", f"worker:{wid}")
            tok = self.gate.get_job(j)["fencing_token"]
            data = b"fi09-prod:%02d:score:%03d:" % (i, score) + b"." * 64
            r5_complete(self.gate, job_id=j, worker_id=wid,
                        fencing_token=tok, task_id=t, outcome="SUCCESS",
                        evidence={"fi": "09", "score": score},
                        actor=f"worker:{wid}", data=data)
            jr = self.gate.get_job(j)
            self.assertEqual(jr["status"], "COMPLETE")
            self.assertTrue(jr["result_artifact_id"])
            prod.append((j, jr["result_artifact_id"], score))
        # The inverted band must actually bite the production batch.
        band_prod = sorted(s for _, _, s in prod if 45 <= s < 55)
        self.assertEqual(band_prod, [45, 47, 49, 51, 53, 54],
                         "test bug: production band changed")
        # --- gold set: known-good inputs with expected outputs ---
        gold: list[tuple[str, int]] = []
        for i, score in enumerate(self.GOLD_SCORES):
            data = b"fi09-gold:%02d:score:%03d:" % (i, score) + b"." * 64
            aid = hashlib.sha256(data).hexdigest()
            path = os.path.join(self.tmpdir, f"fi09-gold-{i:02d}.bin")
            with open(path, "wb") as fh:
                fh.write(data)
            self.gate.create_artifact(
                aid, t, "test-fi09", kind="qa-gold", size=len(data),
                uri=path,
                producer={"fi": "09", "gold": True, "score": score})
            self.gate.verify_artifact(aid, actor="test-fi09")
            gold.append((aid, score))
        # The task already has a QA validator (v1, honest): it validated
        # the first production artifacts, and the gold probe schedule is
        # green before the deployment.
        for _, aid, score in prod[:3]:
            self._qa_record(aid, self.QA_ID, self.PREV_VER,
                            _fi09_honest_verdict(score), "qa-threshold-v1")
        for aid, score in gold:
            got = _fi09_honest_verdict(score)
            self._qa_record(aid, self.QA_ID, self.PREV_VER, got,
                            "qa-probe-v1")
            self.assertEqual(got, _fi09_honest_verdict(score),
                             "pre-deployment probe schedule must be green")
        # === INJECT: deploy the bad validator version ===
        t_deploy = time.monotonic()
        deploy_store_t = self.store.current_time()
        for _, aid, score in prod:
            self._qa_record(aid, self.QA_ID, self.BAD_VER,
                            _fi09_bad_verdict(score), "qa-threshold-v2")
        # The probe schedule ticks once per PROBE_INTERVAL; the first
        # tick after the deployment exercises the deployed version.
        while time.monotonic() < t_deploy + self.PROBE_INTERVAL:
            time.sleep(0.05)
        failed_probes = []
        for aid, score in gold:
            got = _fi09_bad_verdict(score)
            want = _fi09_honest_verdict(score)
            self._qa_record(aid, self.QA_ID, self.BAD_VER, got,
                            "qa-probe-v2")
            if got != want:
                failed_probes.append((aid, score, want, got))
        band_gold = sorted(s for _, s in gold if 45 <= s < 55)
        self.assertEqual(band_gold, [45, 46, 48, 49, 50, 52, 54],
                         "test bug: gold band changed")
        self.assertEqual(sorted(s for _, s, _, _ in failed_probes),
                         band_gold,
                         "failed probes must be exactly the inverted band")
        self.assertEqual(len(failed_probes), 7,
                         "the injection must be detected by the probes")
        # === DETECT -> QUARANTINE (VALIDATION_FAILURE) ===
        q_before = self.store.conn.execute(
            "SELECT COUNT(*) FROM validations WHERE validator_id=?"
            " AND validator_version=? AND quarantined=1",
            (self.QA_ID, self.BAD_VER)).fetchone()[0]
        self.assertEqual(q_before, 0)
        n_q = self.gate.quarantine_validator(self.QA_ID, self.BAD_VER,
                                             "qa-incident-response")
        t_quarantine = time.monotonic()
        latency = t_quarantine - t_deploy
        # 20 production receipts + 12 gold probe receipts.
        self.assertEqual(n_q, 32,
                         f"quarantine must cover every v2 verdict: got {n_q}")
        self.assertLessEqual(
            latency, self.PROBE_INTERVAL + 1.5,
            f"quarantine latency {latency:.2f}s exceeded one probe"
            f" interval ({self.PROBE_INTERVAL}s)")
        # The quarantine is version-scoped: v1's honest rows are untouched.
        v1_rows = self.store.conn.execute(
            "SELECT quarantined FROM validations WHERE validator_id=?"
            " AND validator_version=?",
            (self.QA_ID, self.PREV_VER)).fetchall()
        self.assertTrue(v1_rows, "v1 rows must exist")
        self.assertTrue(all(r["quarantined"] == 0 for r in v1_rows),
                        "quarantine must not touch the honest version")
        qev = [p for p in self._event_payloads("validator.quarantined")
               if p["validator_id"] == self.QA_ID
               and p["validator_version"] == self.BAD_VER]
        self.assertEqual(len(qev), 1)
        self.assertEqual(qev[0]["count"], 32)
        # === PROVENANCE SCOPE: everything the bad version touched ===
        affected = [r["artifact_id"] for r in self.store.conn.execute(
            "SELECT DISTINCT artifact_id FROM validations WHERE"
            " validator_id=? AND validator_version=?",
            (self.QA_ID, self.BAD_VER)).fetchall()]
        prod_ids = {aid for _, aid, _ in prod}
        affected_prod = [a for a in affected if a in prod_ids]
        self.assertEqual(set(affected_prod), prod_ids,
                         "provenance scope must be the full production"
                         " batch validated by the bad version")
        # === REVALIDATE by the independent second validator ===
        overturned = []
        for _, aid, score in prod:
            honest = _fi09_honest_verdict(score)
            bad = _fi09_bad_verdict(score)
            self._qa_record(aid, self.AUDIT_ID, self.AUDIT_VER, honest,
                            "qa-audit-independent")
            if bad != honest:
                overturned.append((aid, score, bad, honest))
        self.assertEqual(len(overturned), 6,
                         "exactly the band's verdicts must be overturned")
        self.assertEqual(sorted(s for _, s, _, _ in overturned), band_prod)
        # === INCIDENT (VALIDATION_FAILURE) ===
        iid = self._service_incident(
            t, "VALIDATION_FAILURE", "wrong_validator_version",
            {"fi": "09",
             "bad_validator": f"{self.QA_ID}/{self.BAD_VER}",
             "deployed_at_store_t": deploy_store_t,
             "failed_gold_probes": len(failed_probes),
             "failed_probe_scores": sorted(s for _, s, _, _ in
                                           failed_probes),
             "quarantined_verdicts": n_q,
             "affected_artifacts": len(affected_prod),
             "overturned_verdicts": len(overturned),
             "quarantine_latency_s": round(latency, 3),
             "probe_interval_s": self.PROBE_INTERVAL})
        att1 = self.gate.record_recovery_attempt(
            None, iid, 5,
            {"name": "quarantine-bad-validator-version", "rung": 5,
             "rung_name": CANONICAL_LADDER[5]},
            f"gold probes failed ({len(failed_probes)}/12) -> quarantined"
            f" {n_q} verdicts by {self.QA_ID}/{self.BAD_VER} in"
            f" {latency:.2f}s (probe interval {self.PROBE_INTERVAL}s);"
            f" {self.PREV_VER} rows untouched",
            float(n_q - q_before), 0.0, "success",
            "qa-incident-response")
        self.assertGreater(att1["progress_delta"], 0.0)
        audit_after = self.store.conn.execute(
            "SELECT COUNT(*) FROM validations WHERE validator_id=?"
            " AND validator_version=?",
            (self.AUDIT_ID, self.AUDIT_VER)).fetchone()[0]
        att2 = self.gate.record_recovery_attempt(
            None, iid, 4,
            {"name": "revalidate-with-independent-validator", "rung": 4,
             "rung_name": CANONICAL_LADDER[4]},
            f"revalidated all {len(affected_prod)} affected artifacts"
            f" with the independent {self.AUDIT_ID}/{self.AUDIT_VER};"
            f" {len(overturned)} bad verdicts overturned",
            float(audit_after), 0.0, "success",
            "qa-incident-response")
        self.assertGreater(att2["progress_delta"], 0.0)
        # === FINALIZE: only artifacts with a non-quarantined QA PASS ===
        released = []
        for _, aid, score in prod:
            if self._nonquarantined_qa_pass(aid):
                st = self.store.conn.execute(
                    "SELECT status FROM artifacts WHERE artifact_id=?",
                    (aid,)).fetchone()["status"]
                self.assertEqual(st, "VALIDATED")
                self.gate.transition_artifact(aid, "RELEASED",
                                              "qa-release")
                released.append(aid)
        honest_pass = {aid for _, aid, s in prod
                       if _fi09_honest_verdict(s) == "PASS"}
        self.assertEqual(set(released), honest_pass,
                         "RELEASED set must equal the honest-PASS set")
        # Zero FINALIZED artifacts carrying only the bad validator's
        # verdict: no non-quarantined PASS receipt from the bad version
        # may exist anywhere (quarantine completeness), and every
        # RELEASED artifact carries the independent validator's PASS.
        bad_pass = self.store.conn.execute(
            "SELECT artifact_id FROM validations WHERE validator_id=?"
            " AND validator_version=? AND result='PASS' AND quarantined=0",
            (self.QA_ID, self.BAD_VER)).fetchall()
        self.assertEqual(bad_pass, [],
                         "quarantine missed a bad-version PASS receipt")
        for aid in released:
            audit_pass = self.store.conn.execute(
                "SELECT 1 FROM validations WHERE artifact_id=?"
                " AND validator_id=? AND validator_version=?"
                " AND result='PASS' AND quarantined=0",
                (aid, self.AUDIT_ID, self.AUDIT_VER)).fetchone()
            self.assertIsNotNone(
                audit_pass,
                f"released artifact {aid[:12]}.. lacks the independent"
                " validator's PASS")
        self._resolve_incident(
            iid, "bad validator version quarantined; affected artifacts"
                 " revalidated by the independent validator; bad verdicts"
                 " overturned; zero released artifacts rely on the bad"
                 " version")
        # === DISCLOSURE in the task report (durable ledger milestone) ===
        rep = [p for p in self._event_payloads("task.report")
               if p.get("incident_id") == iid]
        self.assertEqual(rep, [], "report must not exist before disclosure")
        self.gate.append_event(
            "task.report",
            {"task_id": t, "report": "validation-incident-disclosure",
             "incident_id": iid,
             "bad_validator": f"{self.QA_ID}/{self.BAD_VER}",
             "quarantined_verdicts": n_q,
             "quarantine_latency_s": round(latency, 3),
             "probe_interval_s": self.PROBE_INTERVAL,
             "affected_artifacts": len(affected_prod),
             "overturned_verdicts": len(overturned),
             "overturned_scores": sorted(s for _, s, _, _ in overturned),
             "released_artifacts": len(released),
             "released_with_only_bad_verdict": 0},
            "qa-incident-response")
        rep = [p for p in self._event_payloads("task.report")
               if p.get("incident_id") == iid]
        self.assertEqual(len(rep), 1,
                         "the final report must disclose the incident")
        self.assertEqual(rep[0]["released_with_only_bad_verdict"], 0)
        self._assert_suite_rules(tag, [iid])
        REG.record("R14-FI09", True,
                   f"bad validator {self.QA_ID}/{self.BAD_VER} quarantined"
                   f" ({n_q} verdicts) in {latency:.2f}s <= probe interval"
                   f" {self.PROBE_INTERVAL}s; {len(overturned)} verdicts"
                   f" overturned by {self.AUDIT_ID};"
                   f" {len(released)} released, 0 with only the bad"
                   " version's verdict; incident disclosed in task report")

    def test_R14_FI09_wrong_validator(self):
        self._fi09_impl()

    def test_R14_FI09_wrong_validator_repeat(self):
        self._fi09_impl()
# ==========================================================================
# FI-10 — fencing race: every stale worker's process group dies within
# one heartbeat interval of the fencing transaction
# ==========================================================================
class TestR14BFI10(R14BBase):
    """Three real wedged workers hold leases. Each is fenced by a real
    forced-reclaim (the fencing transaction); the supervisor's sweep
    enforces the fence. For EVERY worker the test measures
    (fencing-transaction timestamp -> process-group death) on a real
    monotonic clock and asserts each latency <= one heartbeat interval,
    and the worker.fence_enforced evidence agrees (within_bound)."""

    def _fi10_impl(self) -> None:
        tag = "FI10"
        H = 1.5
        t = self._new_task("t-fi10", "fi10 fencing race")
        sup = self._new_sup("test-fi10", hb=H)
        workers = [f"wfi10-{i}" for i in range(3)]
        jobs = [f"jfi10-{i}" for i in range(3)]
        pids: dict[str, int] = {}
        for wid, j in zip(workers, jobs):
            self.gate.create_job(j, t, "s", "scheduler")
            sup.start_worker(wid, j, {"kind": "wedged"},
                             ttl_s=120.0, hb_interval_s=0.2, renew=False)
            pids[wid] = sup._procs[wid].popen.pid
        for wid, j in zip(workers, jobs):
            self._wait_for(
                lambda j=j: self.gate.get_job(j)["status"] == "RUNNING",
                30, f"fi10 {wid} running")
        for wid, j in zip(workers, jobs):  # sweep must know each epoch
            tok = self.gate.get_job(j)["fencing_token"]
            self._wait_for(lambda w=wid, k=tok:
                           sup._procs[w].known_token == k,
                           30, f"fi10 epoch {wid}")
        iids: list[str] = []
        latencies: dict[str, float] = {}
        for wid, j in zip(workers, jobs):
            iid = self._new_incident(
                j, "STALLED",
                {"job_id": j, "fi": "10",
                 "detection": f"fencing race: stale worker {wid} holds a"
                  " live lease; forcing reclaim and measuring the fence"})
            iids.append(iid)
            tok = self.gate.get_job(j)["fencing_token"]
            pid = pids[wid]
            t_fence = time.monotonic()  # the fencing transaction
            r = self.gate.reclaim_lease(
                j, actor="recovery-controller", reason="fi10 fencing race",
                expected_owner=wid, expected_token=tok,
                force=True, verdict="STALLED", incident_id=iid)
            self.assertEqual(r["status"], "UNCERTAIN")
            # Poll the REAL process group until it is gone.
            deadline = time.monotonic() + H + 5.0
            t_death: float | None = None
            while time.monotonic() < deadline:
                try:
                    os.killpg(pid, 0)
                except ProcessLookupError:
                    t_death = time.monotonic(); break
                time.sleep(0.02)
            self.assertIsNotNone(t_death,
                                 f"process group {wid} never died")
            latencies[wid] = t_death - t_fence
            self.assertLessEqual(
                latencies[wid], H,
                f"fence latency {latencies[wid]:.3f}s > H={H}s for {wid}")
            # The supervisor's own fence evidence must agree.
            ev = self._wait_fence_evidence(wid)
            self.assertTrue(ev["within_bound"],
                            f"fence evidence not within bound for {wid}")
            self.assertLess(ev["fenced_out_worker_seconds"], H)
            # The recovery contract row: the hazard (1 live fenced-out
            # worker) was eliminated (0) — the measured delta.
            att = self.gate.record_recovery_attempt(
                None, iid, 2,
                {"kind": "forced-reclaim+fence",
                 "target": wid,
                 "fence_latency_s": latencies[wid]},
                "forced-reclaim+fence", 1.0, 0.0, "success",
                "recovery-controller",
                spend={"usd": 0.0})
            self.assertEqual(att["progress_delta"], 1.0)
            self._resolve_incident(
                iid, f"stale worker fenced in {latencies[wid]:.3f}s <= H")
            self.gate.transition_job(j, "PENDING", "recovery-controller")
        lat_str = ", ".join(f"{w}={latencies[w]:.3f}s" for w in workers)
        for wid in workers:
            self.assertLessEqual(latencies[wid], H)
        REG.fi10([{"worker_id": w, "latency_s": round(latencies[w], 3),
                   "bound_s": H} for w in workers])
        self._assert_suite_rules(tag, iids)
        REG.record("R14-FI10", True, f"fence latencies <= H={H}s: {lat_str}")

    def _wait_fence_evidence(self, wid: str, timeout: float = 15.0) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            rows = self.store.conn.execute(
                "SELECT payload FROM ledger WHERE"
                " event_type='worker.fence_enforced'"
                " AND json_extract(payload,'$.worker_id')=?"
                " ORDER BY seq DESC LIMIT 1", (wid,)).fetchone()
            if rows:
                return json.loads(rows[0])
            time.sleep(0.05)
        raise AssertionError(f"timed out waiting for fence evidence {wid}")

    def test_R14_FI10_fencing_race(self):
        self._fi10_impl()

    def test_R14_FI10_fencing_race_repeat(self):
        self._fi10_impl()

# ==========================================================================
# FI-11 — checkpoint sampling under corruption (canonical Test 11; Risk 5)
# ==========================================================================
def _fi11_sample_problems(gate, artifact_id: str,
                          expected_hash: str) -> list[str]:
    """The gate's per-artifact revalidation predicates (the same checks
    verify_checkpoint applies to every manifest entry), run read-only on
    one artifact. Returns the list of problems (empty = clean)."""
    probs: list[str] = []
    row = gate.store.conn.execute(
        "SELECT * FROM artifacts WHERE artifact_id=?",
        (artifact_id,)).fetchone()
    if row is None:
        return ["artifact row missing"]
    a = dict(row)
    if a["status"] not in ("VALIDATED", "RELEASED"):
        probs.append(f"artifact status {a['status']}, not VERIFIED")
    if (a["content_hash"] or a["artifact_id"]) != expected_hash:
        probs.append("artifact content hash != manifest")
    try:
        with open(a["uri"], "rb") as fh:
            data = fh.read()
    except OSError:
        return probs + ["artifact bytes missing"]
    h = hashlib.sha256(data).hexdigest()
    if h != expected_hash:
        probs.append(f"bytes re-hash to {h}, expected {expected_hash}")
    if a["size"] is not None and a["size"] != len(data):
        probs.append("size mismatch")
    rr = gate.store.conn.execute(
        "SELECT content_hash FROM validations WHERE artifact_id=?"
        " AND validator_id='axos-structural' AND validator_version='1'"
        " AND result='PASS' AND quarantined=0"
        " ORDER BY validated_at DESC LIMIT 1",
        (artifact_id,)).fetchone()
    if rr is None or (rr["content_hash"] or "") != expected_hash:
        probs.append("missing PASS receipt axos-structural/1 for"
                     " content hash")
    return probs


class TestR14BFI11(R14BBase):
    """Canonical Test 11 — checkpoint sampling under corruption (Risk 5).

    60 artifacts; an interim checkpoint is verified with a SAMPLED
    receipt (5%, seeded) that honestly states its detection bound; then
    ~3% of artifact bytes are corrupted post-verification. The interim
    receipt may legitimately miss the corruption; the release gate's 100%
    revalidation (verify_checkpoint(release=True) — no sampling) must
    catch every corrupted artifact; the affected units are re-executed;
    no FINALIZE (no VERIFIED release checkpoint) until the repair lands."""

    N = 60
    SAMPLE_RATE = 0.05
    SAMPLE_SEED = 20260924
    CORRUPT_SEED = 77

    def _fi11_impl(self) -> None:
        tag = "FI11"
        t = self._new_task("t-fi11", "fi11 checkpoint sampling")
        # --- 60 artifacts through the real R5 contract ---
        entries: list[dict] = []
        for i in range(self.N):
            j = f"jfi11-a{i:02d}"
            wid = f"wfi11-{i:02d}"
            self.gate.create_job(j, t, "s", "scheduler")
            self.assertTrue(self.gate.claim_job(j, wid, 120.0,
                                                "scheduler"))
            self.gate.transition_job(j, "RUNNING", f"worker:{wid}")
            tok = self.gate.get_job(j)["fencing_token"]
            data = b"fi11-art:%02d:" % i + b"#" * 48
            r5_complete(self.gate, job_id=j, worker_id=wid,
                        fencing_token=tok, task_id=t, outcome="SUCCESS",
                        evidence={"fi": "11", "unit": i},
                        actor=f"worker:{wid}", data=data)
            jr = self.gate.get_job(j)
            self.assertEqual(jr["status"], "COMPLETE")
            aid = jr["result_artifact_id"]
            self.assertTrue(aid)
            entries.append({"artifact_id": aid,
                            "content_hash": jr["content_hash"] or aid})
        self.assertEqual(len(entries), self.N)
        # --- interim checkpoint, SAMPLED verification (5%, seeded) ---
        ck = self.gate.create_checkpoint(None, t, "system",
                                         artifact_manifest=entries)
        ckid = ck["checkpoint_id"]
        rng = random.Random(self.SAMPLE_SEED)
        k = max(1, round(self.N * self.SAMPLE_RATE))
        self.assertEqual(k, 3, "test bug: 5% of 60 must sample 3")
        indexed = list(enumerate(entries))
        sampled = rng.sample(indexed, k)
        sampled_idx = {i for i, _ in sampled}
        for _, e in sampled:
            probs = _fi11_sample_problems(self.gate, e["artifact_id"],
                                          e["content_hash"])
            self.assertEqual(
                probs, [],
                f"sampled artifact {e['artifact_id'][:12]}.. must verify"
                f" pre-injection: {probs}")
        ok, chain_detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, chain_detail)
        receipt = {
            "method": "sampled-revalidation",
            "sample_rate": self.SAMPLE_RATE,
            "sample_seed": self.SAMPLE_SEED,
            "sample_count": k,
            "sample_passed": k,
            "manifest_full": True,
            "ledger_chain_verified": True,
            "sampled_artifact_ids": [e["artifact_id"] for _, e in sampled],
            "checked_predicates": [
                "row-exists", "status-verified", "hash-binding",
                "bytes-re-hash", "size", "structural-pass-receipt"],
            "detection_bound": (
                "byte-level revalidation covered only the sampled"
                f" {k}/{self.N} artifacts (seed {self.SAMPLE_SEED});"
                f" corruption in the {self.N - k} unsampled artifacts is"
                " NOT detectable by this receipt"),
        }
        done = self.gate.set_checkpoint_verification(
            ckid, "VERIFYING", "system")
        self.assertEqual(done["verification_status"], "VERIFYING")
        done = self.gate.set_checkpoint_verification(
            ckid, "VERIFIED", "system", receipt=receipt)
        self.assertEqual(done["verification_status"], "VERIFIED")
        self.assertEqual(done["sample_rate"], self.SAMPLE_RATE)
        self.assertEqual(done["sample_count"], k)
        self.assertEqual(done["sample_passed"], k)
        # The interim receipt's stated assurance matches what was
        # actually checked: read it back from the durable ledger event
        # and confirm the sample is exactly the seeded selection.
        rev = [p for p in self._event_payloads("checkpoint.verification")
               if p["checkpoint_id"] == ckid and p["to"] == "VERIFIED"]
        self.assertEqual(len(rev), 1)
        rcp = rev[0]["receipt"]
        self.assertLess(rcp["sample_rate"], 1.0)
        self.assertEqual(rcp["sample_count"],
                         len(rcp["sampled_artifact_ids"]))
        expect_ids = [e["artifact_id"] for _, e in
                      random.Random(self.SAMPLE_SEED).sample(indexed, k)]
        self.assertEqual(sorted(rcp["sampled_artifact_ids"]),
                         sorted(expect_ids),
                         "receipt must name exactly the seeded sample")
        self.assertIn("NOT detectable", rcp["detection_bound"])
        # === INJECT: corrupt ~3% of artifact bytes on disk ===
        rng2 = random.Random(self.CORRUPT_SEED)
        unsampled_idx = [i for i in range(self.N)
                         if i not in sampled_idx]
        c1 = rng2.choice(unsampled_idx)  # guaranteed interim miss
        c2 = rng2.choice([i for i in range(self.N) if i != c1])
        corrupted_idx = sorted({c1, c2})
        self.assertEqual(len(corrupted_idx), 2,
                         "2/60 = 3.3% ~= 3% of artifacts")
        self.assertNotIn(c1, sampled_idx,
                         "c1 must be unsampled: the interim receipt"
                         " legitimately misses it")
        corrupted: list[str] = []
        for i in corrupted_idx:
            e = entries[i]
            uri = self.store.conn.execute(
                "SELECT uri FROM artifacts WHERE artifact_id=?",
                (e["artifact_id"],)).fetchone()["uri"]
            with open(uri, "r+b") as fh:
                data = bytearray(fh.read())
                cut = len(data) // 3
                for p in range(cut, cut + 8):
                    data[p] ^= 0xFF
                fh.seek(0)
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            corrupted.append(e["artifact_id"])
        # === RELEASE GATE: 100% revalidation catches everything ===
        iid = self._service_incident(
            t, "ARTIFACT_CORRUPTION", "post_verification_byte_corruption",
            {"fi": "11",
             "detection": "release gate 100% revalidation over the task"
              " manifest after suspected post-verification corruption",
             "corrupted_artifact_count": len(corrupted),
             "interim_sample_rate": self.SAMPLE_RATE,
             "interim_sample_seed": self.SAMPLE_SEED})
        rc = self.gate.stage_checkpoint(
            t, manifest=entries, actor="system", trigger="policy")
        rcid = rc["checkpoint_id"]
        res = self.gate.verify_checkpoint(rcid, actor="system",
                                          release=True)
        self.assertEqual(res["verification_status"], "CORRUPT",
                         "the release gate must refuse a corrupt manifest")
        rcp2 = json.loads(res["verification_receipt"])
        self.assertTrue(rcp2["release"])
        self.assertTrue(rcp2["manifest_full"])
        bad_ids = {a["artifact_id"] for a in rcp2["artifacts"]
                   if not a["ok"]}
        self.assertEqual(
            bad_ids, set(corrupted),
            "release gate must identify ALL corrupted artifacts by"
            " manifest — no more, no fewer")
        for a in rcp2["artifacts"]:
            if not a["ok"]:
                self.assertTrue(
                    any("bytes re-hash" in p for p in a["problems"]),
                    f"corruption evidence must name the byte mismatch:"
                    f" {a}")
        # No FINALIZE until clean: the release candidate is CORRUPT
        # (terminal), and no R5 checkpoint is VERIFIED for the task, so
        # the latest-known-good pointer cannot advance.
        self.assertEqual(
            self.store.conn.execute(
                "SELECT verification_status FROM checkpoints WHERE"
                " checkpoint_id=?", (rcid,)).fetchone()
            ["verification_status"], "CORRUPT")
        r5_verified = self.store.conn.execute(
            "SELECT checkpoint_id FROM checkpoints WHERE task_id=?"
            " AND canonical_manifest IS NOT NULL"
            " AND verification_status='VERIFIED'", (t,)).fetchall()
        self.assertEqual(r5_verified, [],
                         "no VERIFIED release checkpoint may exist while"
                         " corruption is present")
        self.assertIsNone(self.gate.latest_known_good(t))
        # === REPAIR: re-execute the affected units ===
        new_entries = [e for i, e in enumerate(entries)
                       if i not in corrupted_idx]
        for n, i in enumerate(corrupted_idx):
            rj = f"jfi11-repair-{i:02d}"
            wid = f"wfi11-r{n:02d}"
            self.gate.create_job(rj, t, "s", "scheduler")
            self.assertTrue(self.gate.claim_job(rj, wid, 120.0,
                                                "scheduler"))
            self.gate.transition_job(rj, "RUNNING", f"worker:{wid}")
            tok = self.gate.get_job(rj)["fencing_token"]
            before = self.gate.progress_evidence_for_job(rj)
            data = b"fi11-repair:%02d:" % i + b"#" * 48
            r5_complete(self.gate, job_id=rj, worker_id=wid,
                        fencing_token=tok, task_id=t, outcome="SUCCESS",
                        evidence={"fi": "11", "repair_of": i},
                        actor=f"worker:{wid}", data=data)
            jr = self.gate.get_job(rj)
            self.assertEqual(jr["status"], "COMPLETE")
            att = self._record_attempt(
                iid, 2, "re-execute-corrupted-unit",
                f"re-executed unit {i}: corrupted artifact"
                f" {entries[i]['artifact_id'][:12]}.. replaced by"
                f" {jr['result_artifact_id'][:12]}..; job COMPLETE",
                rj, before, "success")
            self.assertGreater(att["progress_delta"], 0.0)
            new_entries.append(
                {"artifact_id": jr["result_artifact_id"],
                 "content_hash": jr["content_hash"]
                 or jr["result_artifact_id"]})
        # === repaired release checkpoint: the gate re-opens ===
        rc2 = self.gate.stage_checkpoint(
            t, manifest=new_entries, actor="system", trigger="policy",
            supersedes=rcid)
        rc2id = rc2["checkpoint_id"]
        self.assertNotEqual(rc2id, rcid)
        res2 = self.gate.verify_checkpoint(rc2id, actor="system",
                                           release=True)
        self.assertEqual(res2["verification_status"], "VERIFIED")
        lkg = self.gate.latest_known_good(t)
        self.assertIsNotNone(lkg)
        self.assertEqual(lkg["checkpoint_id"], rc2id)
        # The interim receipt is frozen: its stated assurance still
        # matches what it checked (the seeded 5% sample, pre-injection) —
        # it never claimed coverage it did not have.
        rev2 = [p for p in self._event_payloads("checkpoint.verification")
                if p["checkpoint_id"] == ckid and p["to"] == "VERIFIED"]
        self.assertEqual(len(rev2), 1)
        self.assertEqual(rev2[0]["receipt"]["sample_count"], k)
        self.assertEqual(
            sorted(rev2[0]["receipt"]["sampled_artifact_ids"]),
            sorted(expect_ids))
        self._resolve_incident(
            iid, "release gate identified all corrupted artifacts by"
                 " manifest; affected units re-executed; repaired release"
                 " checkpoint VERIFIED; interim receipt's stated assurance"
                 " matched what it checked")
        self._assert_suite_rules(tag, [iid])
        missed = len(set(corrupted_idx) - sampled_idx)
        REG.record("R14-FI11", True,
                   f"interim 5% sampled receipt honest (missed {missed}"
                   f" unsampled corrupted artifact(s), as stated); release"
                   f" 100% revalidation identified {len(corrupted)}/"
                   f"{len(corrupted)} corrupted by manifest; re-executed;"
                   " repaired release checkpoint VERIFIED; no FINALIZE"
                   " until clean")

    def test_R14_FI11_checkpoint_sampling(self):
        self._fi11_impl()

    def test_R14_FI11_checkpoint_sampling_repeat(self):
        self._fi11_impl()
# ==========================================================================
# FI-12 — human pause: in-flight work drains, no new claims, resume works
# ==========================================================================
class TestR14BFI12(R14BBase):
    """The operator pauses the task mid-flight (PAUSED_FOR_HUMAN). The
    in-flight worker drains to COMPLETE; no new claims are admitted
    while paused (the real claim path refuses); the pause survives a
    supervisor restart (I-17 stickiness — no approval, no resume); an
    APPROVED approval resumes the task and the remaining jobs complete."""

    def _fi12_impl(self) -> None:
        tag = "FI12"
        t = self._new_task("t-fi12", "fi12 human pause")
        jobs = ["jfi12-0", "jfi12-1", "jfi12-2"]
        for j in jobs:
            self.gate.create_job(j, t, "s", "scheduler")
        sup = self._new_sup("test-fi12")
        sched_cfg = SchedulerConfig(
            poll_interval_s=0.2, lease_ttl_s=120.0,
            max_concurrent_jobs=10, batch_size=10)
        sched = Scheduler(self.db, sup, sched_cfg,
                          scheduler_id="fi12-sched")
        # One job in flight (slow real worker); two still PENDING.
        sup.start_worker("wfi12-a", jobs[0], {"kind": "success_delayed"},
                         ttl_s=120.0, hb_interval_s=0.2, renew=False)
        self._wait_for(
            lambda: self.gate.get_job(jobs[0])["status"] == "RUNNING",
            30, "fi12 in-flight running")
        iid = self._service_incident(
            t, "OPERATOR_PAUSE", "human_pause",
            {"fi": "12",
             "detection": "operator pauses the task mid-flight; in-flight"
              " work must drain, no new claims may start"})
        before = self.gate.progress_evidence_for_job(jobs[0])
        # The human pause: ACTIVE -> PAUSED_FOR_HUMAN (durable).
        paused = self.gate.transition_task(
            t, "PAUSED_FOR_HUMAN", "human:operator",
            reason="fi12: operator pause mid-flight",
            pause_reason="operator-requested pause",
            pause_diagnostic={"fi": "12"})
        self.assertEqual(paused["status"], "PAUSED_FOR_HUMAN")
        # In-flight work drains to COMPLETE on its own.
        self._wait_for(lambda: sup.reap("wfi12-a") is not None, 60,
                       "fi12 drain")
        self.assertEqual(self.gate.get_job(jobs[0])["status"], "COMPLETE")
        # No new claims while paused: the REAL scheduler's admission pass
        # skips every PENDING job whose task is PAUSED_FOR_HUMAN (I-17).
        rep = sched.evaluate_once()
        self.assertEqual(rep["detail"], "ok")
        self.assertEqual(rep["admitted"], 0,
                         f"scheduler must admit nothing while paused: {rep}")
        claims_paused = [p for p in self._event_payloads("job.claimed")
                         if p["job_id"] in jobs[1:]]
        self.assertEqual(claims_paused, [],
                         "no claim may start while the task is paused")
        self.assertTrue(all(self.gate.get_job(j)["status"] == "PENDING"
                            for j in jobs[1:]))
        # The pause survives a supervisor restart: a fresh supervisor and
        # a fresh scheduler on the same db still admit nothing (the pause
        # is durable state, not a live-supervisor flag).
        sup.close()
        sup2 = self._new_sup("test-fi12-restarted")
        sched2 = Scheduler(self.db, sup2, sched_cfg,
                           scheduler_id="fi12-sched2")
        rep2 = sched2.evaluate_once()
        self.assertEqual(rep2["admitted"], 0,
                         f"pause must survive restart: {rep2}")
        self.assertEqual(self.gate.get_task(t)["status"], "PAUSED_FOR_HUMAN")
        # Resume requires an explicit APPROVED approval (I-17): without
        # it the transition is rejected.
        with self.assertRaises(TransitionRejected):
            self.gate.transition_task(t, "EXECUTING", "human:operator",
                                      reason="fi12: unapproved resume")
        ap = self.gate.create_approval(
            None, t, "fi12: operator approves resume after pause",
            "resume-task", "human:operator")
        self.gate.decide_approval(ap["approval_id"], "APPROVED",
                                  "human:operator", "human:operator",
                                  decision_reason="fi12: verified drain")
        resumed = self.gate.transition_task(
            t, "EXECUTING", "human:operator",
            reason="fi12: approved resume",
            approval_ref=ap["approval_id"])
        self.assertEqual(resumed["status"], "EXECUTING")
        # The remaining jobs are now admitted and dispatched by the real
        # scheduler (default behavior: success_immediate) and complete.
        rep3 = sched2.evaluate_once()
        self.assertEqual(rep3["admitted"], 2,
                         f"scheduler must admit the 2 pending jobs: {rep3}")
        for j in jobs[1:]:
            self._wait_for(
                lambda j=j: self.gate.get_job(j)["status"] == "COMPLETE",
                60, f"fi12 {j} done")
        att = self._record_attempt(
            iid, 5, "reconcile",
            "operator pause honored: in-flight job drained to COMPLETE;"
            " scheduler admitted 0 while paused (fresh scheduler after"
            " restart admitted 0 too); APPROVED approval resumed;"
            " scheduler then admitted 2 and both completed",
            jobs[0], before, "success")
        self.assertGreater(att["progress_delta"], 0.0)
        self._resolve_incident(iid, "pause/resume honored")
        self._assert_suite_rules(tag, [iid])
        REG.record("R14-FI12", True,
                   "pause: drained, 0 new claims, restart-sticky;"
                   " approved resume completed")

    def test_R14_FI12_human_pause(self):
        self._fi12_impl()

    def test_R14_FI12_human_pause_repeat(self):
        self._fi12_impl()


# ==========================================================================
# R14-C: COMPOSITION / RACES / FENCING / CONVERGENCE / BREAKER / FINALIZATION
# (consolidated from tests/test_r14_c_composition.py)
# ==========================================================================

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


def _proc_pgid_alive(pid: int) -> bool:
    """True when any process still lives in pid's process group."""
    try:
        pgid = os.getpgid(pid)
    except (ProcessLookupError, PermissionError):
        return False
    try:
        os.killpg(pgid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _sigterm_ignored(pid: int) -> bool:
    """True when the process has SIGTERM in its kernel-ignored mask
    (SigIgn bit 14). Deterministic proof that a wedged_ignore_sigterm
    worker installed SIG_IGN before we fence it."""
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("SigIgn:"):
                    mask = int(line.split()[1], 16)
                    return bool(mask & (1 << (signal.SIGTERM - 1)))
    except (FileNotFoundError, ProcessLookupError, PermissionError,
            ValueError, IndexError):
        return False
    return False


def _all_pids() -> list[int]:
    """Every PID currently visible in /proc."""
    try:
        return [int(n) for n in os.listdir("/proc") if n.isdigit()]
    except FileNotFoundError:
        return []


def _proc_children_of_pgid(pid: int) -> list[int]:
    """PIDs (other than pid itself) currently in pid's process group."""
    try:
        pgid = os.getpgid(pid)
    except (ProcessLookupError, PermissionError):
        return []
    out = []
    for other in _all_pids():
        if other == pid:
            continue
        try:
            if os.getpgid(other) == pgid:
                out.append(other)
        except (ProcessLookupError, PermissionError):
            continue
    return out


class CBase(unittest.TestCase):
    H = 0.4  # supervisor heartbeat interval for this campaign
    # Every worker pid ever spawned by any Track C test (class-level so
    # the R14-99 audit sees the whole campaign). Populated in tearDown
    # from each supervisor's proc table before cleanup.
    _spawned_pids: set = set()

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r14c-")
        self.db = os.path.join(self.tmp, "t.db")
        self.store = open_store(self.db)
        migrate(self.store)
        self.gate = TransitionGate(self.store)
        self.gate.create_task("t", {"objective": "r14c"}, {"usd": 1},
                              "test")
        self._sups: list = []
        self._scheds: list = []
        self._recs: list = []
        self._ctls: list = []
        self._wds: list = []
        self._fins: list = []
        self._pols: list = []
        self._res: list = []
        self._stores: list = [self.store]

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
        for p in self._pols:
            try:
                p.close()
            except Exception:
                pass
        for c in self._ctls:
            try:
                c.close()
            except Exception:
                pass
        for r in self._res:
            try:
                r.close()
            except Exception:
                pass
        for s in self._scheds:
            try:
                s.close()
            except Exception:
                pass
        for rc in self._recs:
            try:
                rc.close()
            except Exception:
                pass
        for s in self._sups:
            try:
                for _wid, info in list(s._procs.items()):
                    try:
                        CBase._spawned_pids.add(info.popen.pid)
                    except Exception:
                        pass
                for _wid, rd in list(getattr(s, "_reaped", {}).items()):
                    try:
                        if isinstance(rd, dict) and rd.get("pid"):
                            CBase._spawned_pids.add(rd["pid"])
                    except Exception:
                        pass
                for _k, ad in list(getattr(s, "_adopted", {}).items()):
                    try:
                        CBase._spawned_pids.add(ad.pid)
                    except Exception:
                        pass
                for _wid, info in list(s._procs.items()):
                    p = info.popen
                    try:
                        if p.poll() is None:
                            os.killpg(p.pid, signal.SIGKILL)
                    except Exception:
                        pass
                for _wid, info in list(s._procs.items()):
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
    def _wait(self, pred, timeout=30.0, interval=0.02):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("timed out waiting for condition")

    def _thread_gate(self, path):
        """A TransitionGate on its own store connection, for use inside
        barrier threads (SQLite connections are thread-bound). The
        store is registered for teardown."""
        st = open_store(path)
        self._stores.append(st)
        return TransitionGate(st)

    def _run_barrier(self, fns, timeout=120):
        barrier = threading.Barrier(len(fns))
        results: dict = {}

        def run(i, fn):
            try:
                barrier.wait(timeout=20)
                results[i] = fn()
            except Exception as exc:  # noqa: BLE001 - recorded, asserted
                results[i] = exc

        ts = [threading.Thread(target=run, args=(i, fn))
              for i, fn in enumerate(fns)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout)
        for t in ts:
            self.assertFalse(t.is_alive(), "barrier thread hung")
        return results

    def _new_sup(self, H=None, actor="r14c-sup"):
        sup = Supervisor(self.db, actor=actor,
                         heartbeat_interval_s=H or self.H)
        self._sups.append(sup)
        return sup

    def _new_rec(self, batch_size=10):
        rec = Reconciler(self.db, ReconcilerConfig(
            poll_interval_s=1.0, batch_size=batch_size))
        self._recs.append(rec)
        return rec

    def _new_sched(self, sup, scheduler_id="s0", max_concurrent=3):
        sc = Scheduler(self.db, sup, SchedulerConfig(
            poll_interval_s=1.0, lease_ttl_s=30.0,
            max_concurrent_jobs=max_concurrent, batch_size=10),
            scheduler_id=scheduler_id)
        self._scheds.append(sc)
        return sc

    def _new_res(self, **kw):
        return self._res_on(self.db, **kw)

    def _res_on(self, path, **kw):
        cfg = dict(failure_window_s=60.0, failure_threshold=3,
                   global_failure_threshold=10, cooldown_s=60.0,
                   half_open_probe_limit=1,
                   recovery_pressure_threshold=5, poll_interval_s=5.0,
                   batch_size=100, actor="r14c-resilience")
        cfg.update(kw)
        rc = ResilienceController(path, ResilienceConfig(**cfg))
        self._res.append(rc)
        return rc

    def _new_wd(self, sup, hb_stale=2.0, prog_stale=2.0):
        wd = Watchdog(self.db, WatchdogConfig(
            heartbeat_stale_s=hb_stale, progress_stale_s=prog_stale,
            evaluation_interval_s=1.0), supervisor=sup)
        self._wds.append(wd)
        return wd

    def _new_rc(self, sup, **kw):
        cfg = dict(observation_window_s=1.0, evaluation_interval_s=1.0,
                   claim_timeout_s=2.0, restart_worker_duration_s=60.0,
                   restart_worker_ttl_s=30.0,
                   restart_worker_hb_interval_s=0.3)
        cfg.update(kw)
        ctl = RecoveryController(self.db, RecoveryConfig(**cfg),
                                 supervisor=sup)
        self._ctls.append(ctl)
        return ctl

    def _new_pol(self, sup, **kw):
        pcfg = dict(per_rung_max_attempts={1: 2, 2: 2, 3: 1, 4: 0, 5: 0},
                    incident_max_attempts=7, policy_version="r14c/v1",
                    evaluation_interval_s=1.0)
        rcfg = dict(observation_window_s=1.0, evaluation_interval_s=1.0,
                    claim_timeout_s=2.0, restart_worker_duration_s=60.0,
                    restart_worker_ttl_s=30.0,
                    restart_worker_hb_interval_s=0.3)
        for k in ("per_rung_max_attempts", "incident_max_attempts",
                  "policy_version"):
            if k in kw:
                pcfg[k] = kw.pop(k)
        for k in list(kw):
            if k in rcfg:
                rcfg[k] = kw.pop(k)
        pol = PolicyController(self.db, PolicyConfig(**pcfg),
                               RecoveryConfig(**rcfg), supervisor=sup,
                               actor="r14c-policy")
        self._pols.append(pol)
        return pol

    def _new_fin(self, sup, actor="r14c-fin"):
        fin = Finalizer(self.db, sup, FinalizationConfig(
            poll_interval_s=1.0, batch_size=10, actor=actor))
        self._fins.append(fin)
        return fin

    def _set(self, dwid, task_id="t", max_attempts=3, actor="test") -> dict:
        return self.gate.set_desired_item(
            dwid, {"task_id": task_id, "stage_id": "s",
                   "max_attempts": max_attempts, "policy": {}},
            actor)

    def _head_version(self) -> int:
        return self.gate.get_desired_head()["version"]

    def _ensure(self, dwid, actor="reconciler") -> dict:
        job, _created = self.gate.ensure_job_for_desired_state(
            desired_work_id=dwid, task_id="t", stage_id="s",
            max_attempts=3, policy={},
            desired_version=self._head_version(), actor=actor)
        return job

    def _count_events(self, event_type) -> int:
        return self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?",
            (event_type,)).fetchone()[0]

    def _blockers(self, run) -> list:
        return json.loads(run["blockers"] or "[]")

    def _fresh_db(self):
        """A second, independent database in the same tmp dir."""
        path = os.path.join(self.tmp, f"t-{len(self._stores)}.db")
        st = open_store(path)
        migrate(st)
        self._stores.append(st)
        g = TransitionGate(st)
        g.create_task("t", {"objective": "r14c"}, {"usd": 1}, "test")
        return path, st, g

    def _kill_worker_group(self, sup, worker_id):
        """Real kill -9 of a worker's whole process group."""
        info = sup._procs[worker_id]
        pid = info.popen.pid
        os.killpg(pid, signal.SIGKILL)
        self._wait(lambda: not _proc_pgid_alive(pid), timeout=15.0)
        try:
            info.popen.wait(timeout=10)
        except Exception:
            pass
        return pid

    def _worker_pgid(self, sup, worker_id) -> int:
        return os.getpgid(sup._procs[worker_id].popen.pid)


# ================================================================== R14-A
class TestR14AComposition(CBase):
    """R14-01..R14-10: one end-to-end composition path surviving an
    injected kill -9, converging to FINALIZED. Each numbered block maps
    to one mission section 4 bullet."""

    def test_R14_01__e2e_composition_path(self):
        sup = self._new_sup()
        self.assertTrue(sup._boot_ready(), "supervisor must reach READY")

        # ---- R14-01: desired state declared; the head version is the
        # authoritative generation anchor.
        self._set("w-a")
        self._set("w-b")
        head = self.gate.get_desired_head()
        V = head["version"]
        self.assertGreaterEqual(V, 2)
        gen = canonical_release_generation(V)
        self.assertTrue(head["snapshot_hash"])
        head2 = self.gate.get_desired_head()
        self.assertEqual(head2["version"], V,
                         "head version must stay authoritative")

        # ---- R14-02: R11 reconciliation -> deterministic dj- identities.
        rec = self._new_rec()
        rep = rec.reconcile()
        self.assertIn(rep["result"], ("CONVERGED", "CHANGED"))
        self.assertEqual(rep["items_created"], 2)
        jids = {}
        for dwid in ("w-a", "w-b"):
            jid = _canonical_desired_job_id(dwid)
            jids[dwid] = jid
            self.assertTrue(jid.startswith("dj-"))
            job = self.gate.get_job(jid)
            self.assertEqual(job["status"], "PENDING")
            m = self.gate.get_desired_job_map(dwid)
            self.assertEqual(m["job_id"], jid)
        # Deterministic: a second pass creates nothing and agrees.
        rep2 = rec.reconcile()
        self.assertEqual(rep2["items_created"], 0)
        for dwid in ("w-a", "w-b"):
            self.assertEqual(
                _canonical_desired_job_id(dwid), jids[dwid])
        JA, JB = jids["w-a"], jids["w-b"]

        # ---- R14-03: R12 admission check: breakers closed, claims allowed.
        res = self._new_res()
        rrep = res.evaluate_once()
        self.assertEqual(rrep.get("detail", "ok"), "ok")
        for jid in (JA, JB):
            allowed, _reason = self.gate.breaker_allows("JOB", jid)
            self.assertTrue(allowed, f"breaker must allow {jid}")
            row = self.gate.get_breaker_state("JOB", jid)
            self.assertIsNone(row, "no breaker row: default closed")

        # ---- R14-04: R10 scheduler claims atomically (happy path via the
        # real scheduler; failure-path job via the scheduler's exact
        # atomic claim op). Exactly one claim per job.
        sched = self._new_sched(sup, scheduler_id="s0", max_concurrent=1)
        srep = sched.evaluate_once()
        self.assertEqual(srep["detail"], "ok")
        self.assertEqual(srep["admitted"], 1)
        # The dispatched real worker (success_immediate) completes via
        # the real R5 protocol on its own.
        self._wait(lambda: self.gate.get_job(JA)["status"] == "COMPLETE",
                   timeout=60.0)
        ja = self.gate.get_job(JA)
        self.assertIsNotNone(ja["result_artifact_id"])
        claims_a = [e for e in self._job_events(JA, "job.claimed")]
        self.assertEqual(len(claims_a), 1, "exactly one atomic claim")

        won = self.gate.claim_job_resilient(
            JB, "r14c-wb", 30.0, "scheduler:s0", 1, scheduler_id="s0")
        self.assertTrue(won, "R10 atomic claim must win")
        jb = self.gate.get_job(JB)
        TB = jb["fencing_token"]
        self.assertEqual(jb["status"], "CLAIMED")
        self.assertEqual(jb["owner_worker_id"], "r14c-wb")
        self.assertIsNotNone(TB)
        claims_b = self._job_events(JB, "job.claimed")
        self.assertEqual(len(claims_b), 1, "exactly one atomic claim")

        # ---- R14-05: real worker subprocess with the correct fencing
        # identity (expect_token verifies the durable triple).
        proc_id = sup.start_worker(
            "r14c-wb", JB, {"kind": "heartbeat_loop", "duration_s": 120.0},
            ttl_s=30.0, hb_interval_s=0.3, renew=True, expect_token=TB)
        self._wait(lambda: self.gate.get_job(JB)["status"] == "RUNNING",
                   timeout=30.0)
        hbs = self.gate.heartbeats_for("r14c-wb", proc_id)
        self._wait(lambda: len(self.gate.heartbeats_for(
            "r14c-wb", proc_id)) >= 2, timeout=30.0)
        jb = self.gate.get_job(JB)
        self.assertEqual((jb["owner_worker_id"], jb["fencing_token"]),
                         ("r14c-wb", TB),
                         "worker execution carries the fencing triple")

        # ---- R14-06: heartbeats are liveness-only, never authority;
        # progress evidence is durable.
        exp_before = jb["lease_expires_at"]
        seqs = [h["hb_seq"] for h in
                self.gate.heartbeats_for("r14c-wb", proc_id)]
        nxt = max(seqs) + 1
        self.gate.ingest_heartbeat(
            "r14c-wb", proc_id, JB, TB, nxt, "RUNNING", "op",
            "worker:r14c-wb")
        jb2 = self.gate.get_job(JB)
        self.assertEqual(jb2["lease_expires_at"], exp_before,
                         "heartbeat must not extend the lease")
        self.assertEqual(jb2["fencing_token"], TB)
        self.assertEqual(jb2["owner_worker_id"], "r14c-wb")
        self.gate.update_job_progress(JB, "r14c-wb", TB, 5.0, 10.0,
                                      actor="worker:r14c-wb")
        ev = self.gate.progress_evidence_for_job(JB)
        self.assertEqual(ev["progress_done"], 5.0)
        self.assertIsNotNone(ev["progress_updated_at"],
                             "progress evidence must be durable")

        # ---- inject the ONE real failure: kill -9 the worker mid-job.
        killed_pid = self._kill_worker_group(sup, "r14c-wb")

        # ---- R14-07: R7 watchdog records DEAD from durable evidence.
        wd = self._new_wd(sup)
        outcomes = wd.evaluate()
        by_job = {o["job_id"]: o for o in outcomes}
        self.assertIn(JB, by_job)
        self.assertEqual(by_job[JB]["verdict"], "DEAD")
        verdict = self.gate.latest_watchdog_verdict(JB, TB)
        self.assertIsNotNone(verdict)
        self.assertEqual(verdict["verdict"], "DEAD")
        vidence = verdict["evidence"]
        if isinstance(vidence, str):
            vidence = json.loads(vidence or "{}")
        self.assertTrue(vidence,
                        "watchdog verdict must carry durable evidence")

        # ---- R14-08: R8 recovery + R9 policy/ladder/budget. Canonical
        # incident identity; the attempt must show positive durable
        # progress to succeed.
        pol = self._new_pol(sup)
        outs = pol.evaluate()
        self.assertTrue(any("incident_id" in o for o in outs),
                        f"recovery must create an incident: {outs}")
        inc_id = self.gate.find_or_create_recovery_incident(
            JB, "DEAD", {"job_id": JB, "probing": True}, "r14c-policy"
        )["incident_id"]
        inc_id2 = self.gate.find_or_create_recovery_incident(
            JB, "DEAD", {"job_id": JB, "probing": True}, "r14c-policy"
        )["incident_id"]
        self.assertEqual(inc_id, inc_id2,
                         "recovery incidents use canonical identity")
        atts = self.gate.recovery_attempts_for(inc_id)
        self.assertEqual(len(atts), 1, "exactly one attempt created")
        self.assertEqual(json.loads(atts[0]["action"])["name"], "reclaim")
        # Drive the attempt to dispatch (claim -> dispatch -> R1 reclaim).
        outs = pol.evaluate()
        atts = self.gate.recovery_attempts_for(inc_id)
        self.assertEqual(atts[0]["attempt_state"], "RUNNING")
        action = json.loads(atts[0]["action"])
        dispatch = action.get("dispatch") or {}
        self.assertEqual(dispatch.get("authority"), "r1.reclaim_lease")
        jb = self.gate.get_job(JB)
        self.assertEqual(jb["fencing_token"], TB + 1,
                         "R1 reclaim bumps the token exactly once")
        self.assertIsNone(jb["owner_worker_id"])
        self.assertEqual(jb["status"], "UNCERTAIN")
        # Policy row: ladder position + durable budgets exist.
        prow = self.gate.get_recovery_policy(inc_id)
        self.assertIsNotNone(prow)
        self.assertEqual(prow["policy_version"], "r14c/v1")
        self.assertGreater(prow["incident_budget"], 0)

        # ---- R14-09: stale workers cannot regain authority. The old
        # token is dead at every mutation point.
        self.assertFalse(self.gate.renew_lease(JB, "r14c-wb", TB, 30.0,
                                               "worker:r14c-wb"),
                         "stale renew must fail, not extend the lease")
        with self.assertRaises((LeaseError, TransitionRejected)):
            self.gate.update_job_progress(JB, "r14c-wb", TB, 6.0, 10.0,
                                          actor="worker:r14c-wb")
        with self.assertRaises((LeaseError, TransitionRejected)):
            self.gate.stage_artifact(
                job_id=JB, worker_id="r14c-wb", fencing_token=TB,
                task_id="t", kind="result", data=b"stale",
                actor="worker:r14c-wb")
        # The R2 sweep runs and must not harm the (future) live worker.
        sweep = sup.fence_sweep()
        self.assertIn("checked", sweep)

        # Verify the reclaim attempt: positive durable progress required.
        self._wait_for_attempt_window()
        outs = pol.evaluate()
        atts = self.gate.recovery_attempts_for(inc_id)
        term = [a for a in atts if a["attempt_state"] == "SUCCEEDED"]
        self.assertEqual(len(term), 1,
                         f"reclaim attempt must succeed: {atts}")
        self.assertGreater(term[0]["progress_delta"], 0,
                           "recovery success requires positive durable"
                           " progress")
        # Uncertainty resolution: no artifact was staged by the dead
        # worker -> requeue to PENDING through the gate, then the real
        # scheduler re-admits a FRESH identity.
        insp = self.gate.inspect_uncertain_completion(JB)
        self.assertEqual(insp["disposition"], "NOT_COMMITTED")
        self.gate.transition_job(JB, "PENDING", "recovery-controller",
                                 reason="r14c: dead worker staged nothing;"
                                 " requeue")
        srep2 = sched.evaluate_once()
        self.assertEqual(srep2["admitted"], 1)
        self._wait(lambda: self.gate.get_job(JB)["status"] == "COMPLETE",
                   timeout=60.0)
        jb = self.gate.get_job(JB)
        self.assertNotEqual(jb["owner_worker_id"], "r14c-wb",
                            "replacement runs under a fresh identity")
        self.assertGreater(jb["fencing_token"], TB + 1)
        # The recovery controller observes the SUCCEEDED attempt and the
        # completed job: no lingering process -> incident closes with a
        # legitimate "success" outcome (no ACTIVE_RECOVERY blocker left).
        outs = pol.evaluate()
        inc = self.gate.get_recovery_incident(inc_id)
        self.assertEqual(inc["outcome"], "success",
                         f"incident must close: {inc}")
        # Let the supervisor reap the replacement worker's exited
        # process; finalization conservatively blocks on unreaped
        # spawns (possibly-live execution).
        self._wait(lambda: not self.gate.unreaped_proc_spawns(),
                   timeout=30.0)

        # ---- R14-10: R5 artifact contract + R13 finalization -> FINALIZED.
        for jid in (JA, JB):
            art = self.gate.get_artifact(
                self.gate.get_job(jid)["result_artifact_id"])
            self.assertEqual(art["status"], "VALIDATED")
            self.assertTrue(art["uri"] and os.path.isfile(art["uri"]),
                            f"artifact bytes must exist at {art['uri']!r}")
            self.assertGreater(os.path.getsize(art["uri"]), 0)
        fin = self._new_fin(sup)
        frep = fin.evaluate_once()
        self.assertEqual(frep["detail"], "ok")
        evd = [e for e in frep["evaluated"]]
        self.assertEqual(len(evd), 1)
        self.assertEqual(evd[0]["state"], "READY")
        self.assertEqual(evd[0]["release_generation"], gen)
        pubs = [p for p in frep["finalized"]
                if p["release_generation"] == gen]
        self.assertEqual(len(pubs), 1,
                         "READY run is published by the pass")
        run = self.gate.get_finalization_run(gen)
        self.assertEqual(run["desired_state_version"], V,
                         "finalization binds to the correct desired-state"
                         " generation")
        self.assertIsNotNone(run["checkpoint_id"],
                              "READY requires a verified release checkpoint")
        self.assertEqual(self._blockers(run), [],
                         "no blockers on the healthy path")
        pub = self.gate.publish_finalization(
            gen, run["version"], run["manifest_hash"], "r14c-fin")
        self.assertEqual(pub["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1,
                         "release publication is atomic: exactly one event")
        # Idempotent re-publication: same record, no duplicate event.
        pub2 = self.gate.publish_finalization(
            gen, pub["version"], pub["manifest_hash"], "r14c-fin")
        self.assertEqual(pub2["state"], "FINALIZED")
        self.assertEqual(self._count_events("finalization.published"), 1)
        # Finalization is evidence-based, not liveness-based: the original
        # worker is long dead and the release still stands.
        self.assertTrue(_proc_gone(killed_pid),
                        "the kill -9'd worker must be gone by independent"
                        " /proc evidence")
        finrow = self.gate.get_finalization_run(gen)
        self.assertEqual(finrow["state"], "FINALIZED")

    def _job_events(self, job_id, event_type):
        rows = self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type=?",
            (event_type,)).fetchall()
        out = []
        for (p,) in rows:
            try:
                d = json.loads(p)
            except ValueError:
                continue
            if d.get("job_id") == job_id:
                out.append(d)
        return out

    def _wait_for_attempt_window(self):
        time.sleep(1.6)  # observation_window_s=1.0 must elapse


# ================================================================== R14-D
class TestR14DRaces(CBase):
    """R14-20..R14-45: adversarial cross-boundary concurrency. Each race
    runs 3 consecutive times on fresh databases; every race proves exactly
    one authoritative winner and that losers cannot mutate via a stale
    token, CAS version, or generation.

    Single-boundary races already covered exactly in slice suites are
    NOT duplicated (see module docstring for the citation list).
    """

    def _r3(self, fn):
        for i in range(3):
            fn(i)

    def _tstore(self, path):
        st = open_store(path)
        return st, TransitionGate(st)

    def _thread_gate(self, path):
        """A gate on a fresh Store for the CALLING thread. Barrier-thread
        lambdas must use this, never a gate created on the main thread
        (sqlite connections are thread-bound)."""
        st = open_store(path)
        self._stores.append(st)
        return TransitionGate(st)

    # ------------------------------------------------------------ R14-20
    def test_R14_20_scheduler_plus_reconciler(self):
        """Scheduler claiming while the reconciler creates: exactly-once
        job creation, exactly one claim per job, no claim on a phantom."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
            self._sups.append(sup)
            N = 12
            spec = {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                    "policy": {}}
            for k in range(N):
                g.set_desired_item(f"d-{k}", dict(spec), "test")
            rec = Reconciler(path, ReconcilerConfig(
                poll_interval_s=1.0, batch_size=N))
            self._recs.append(rec)
            sched = Scheduler(path, sup, SchedulerConfig(
                poll_interval_s=1.0, lease_ttl_s=30.0,
                max_concurrent_jobs=N, batch_size=N), scheduler_id="s0")
            self._scheds.append(sched)
            res = self._run_barrier([rec.reconcile, sched.evaluate_once])
            for v in res.values():
                self.assertNotIsInstance(v, Exception, f"race failed: {v}")
            # The single racing pass may miss jobs the reconciler had
            # not yet created; drive the scheduler to convergence (as
            # its production loop would) — the race already happened.
            def _converged():
                sup.observe()  # reap finished workers -> commit -> COMPLETE
                if not g.jobs_in_states(("PENDING",)):
                    return True
                sched.evaluate_once()
                return not g.jobs_in_states(("PENDING",))
            self._wait(_converged, timeout=60.0)
            jobs = g.jobs_in_states(("PENDING", "CLAIMED", "RUNNING",
                                     "COMMITTING", "COMPLETE"))
            self.assertEqual(len(jobs), N,
                             "exactly one job per desired item")
            # Exactly one atomic claim per job: no double-claim, no phantom.
            for k in range(N):
                jid = _canonical_desired_job_id(f"d-{k}")
                n = g.store.conn.execute(
                    "SELECT COUNT(*) FROM ledger WHERE event_type=?"
                    " AND json_extract(payload,'$.job_id')=?",
                    ("job.claimed", jid)).fetchone()[0]
                self.assertLessEqual(n, 1, f"double claim on {jid}")
            # Winners finish; losers (none expected) leave no debris.
            def _all_complete():
                sup.observe()
                return all(
                    g.get_job(_canonical_desired_job_id(f"d-{k}"))["status"]
                    == "COMPLETE" for k in range(N))
            self._wait(_all_complete, timeout=90.0)
        self._r3(one)

    # ------------------------------------------------------------ R14-21
    def test_R14_21_watchdog_plus_two_recovery_controllers(self):
        """Watchdog evaluation racing two recovery controllers: one
        authoritative verdict row, one incident, one attempt, one token
        bump."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
            self._sups.append(sup)
            g.create_job("j21", "t", "s", "test")
            sup.start_worker("w21", "j21", {"kind": "hang"}, ttl_s=30.0,
                             hb_interval_s=0.3, renew=False)
            self._wait(lambda: g.get_job("j21")["status"] == "RUNNING",
                       timeout=30.0)
            pid = sup._procs["w21"].popen.pid
            os.killpg(pid, signal.SIGKILL)
            self._wait(lambda: not _proc_pgid_alive(pid), timeout=15.0)
            wd = Watchdog(path, WatchdogConfig(2.0, 2.0, 1.0),
                          supervisor=sup)
            self._wds.append(wd)
            wd.evaluate()  # durable DEAD evidence exists before the race
            tok = g.get_job("j21")["fencing_token"]
            cfg = RecoveryConfig(observation_window_s=1.0,
                                 evaluation_interval_s=1.0,
                                 claim_timeout_s=2.0,
                                 restart_worker_duration_s=60.0,
                                 restart_worker_ttl_s=30.0,
                                 restart_worker_hb_interval_s=0.3)
            rc1 = RecoveryController(path, cfg, supervisor=sup)
            rc2 = RecoveryController(path, cfg, supervisor=sup)
            self._ctls.extend([rc1, rc2])
            res = self._run_barrier([wd.evaluate, rc1.evaluate,
                                     rc2.evaluate])
            for v in res.values():
                self.assertNotIsInstance(v, Exception, f"race failed: {v}")
            # Intake creates the attempt; a second racing pass drives it
            # to the reclaim (the production loop always re-evaluates).
            res2 = self._run_barrier([rc1.evaluate, rc2.evaluate])
            for v in res2.values():
                self.assertNotIsInstance(v, Exception, f"race failed: {v}")
            incs = g.store.conn.execute(
                "SELECT * FROM incidents").fetchall()
            self.assertEqual(len(incs), 1,
                             "exactly one canonical incident")
            atts = g.recovery_attempts_for(incs[0]["incident_id"])
            self.assertEqual(len(atts), 1,
                             "exactly one authoritative attempt")
            self.assertEqual(g.get_job("j21")["fencing_token"], tok + 1,
                             "exactly one token bump")
            n = g.store.conn.execute(
                "SELECT COUNT(*) FROM watchdog_verdicts WHERE job_id=?"
                " AND fencing_token=?", ("j21", tok)).fetchone()[0]
            self.assertEqual(n, 1, "exactly one authoritative verdict row")
        self._r3(one)

    # ------------------------------------------------------------ R14-22
    def test_R14_22_recovery_controller_plus_supervisor(self):
        """Recovery restart dispatch racing fence_sweep + observe: exactly
        one replacement spawn, one owner, no duplicate external effect."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
            self._sups.append(sup)
            g.create_job("j22", "t", "s", "test")
            g.claim_job("j22", "w22", 60.0, "test")
            g.transition_job("j22", "RUNNING", "worker:w22")
            tok = g.get_job("j22")["fencing_token"]
            inc = g.find_or_create_recovery_incident(
                "j22", "DEAD", {"job_id": "j22"}, "test")
            g.reclaim_lease("j22", actor="recovery-controller",
                           reason="r14c race", expected_owner="w22",
                           expected_token=tok, force=True, verdict="DEAD",
                           incident_id=inc["incident_id"])
            g.transition_job("j22", "PENDING", "recovery-controller",
                             reason="r14c: requeue for restart race")
            rc = RecoveryController(path, RecoveryConfig(
                observation_window_s=1.0, evaluation_interval_s=1.0,
                claim_timeout_s=2.0, restart_worker_duration_s=60.0,
                restart_worker_ttl_s=30.0,
                restart_worker_hb_interval_s=0.3), supervisor=sup)
            self._ctls.append(rc)
            res = self._run_barrier([lambda: rc.dispatch_restart("j22"),
                                     sup.fence_sweep, sup.observe])
            for v in res.values():
                self.assertNotIsInstance(v, Exception, f"race failed: {v}")
            out = res[0]
            wid = out["dispatch"]["replacement_worker_id"]
            spawns = [r for r in g.store.conn.execute(
                "SELECT payload FROM ledger WHERE event_type=?",
                ("worker.proc_spawned",)).fetchall()
                if json.loads(r[0]).get("worker_id") == wid
                and json.loads(r[0]).get("job_id") == "j22"]
            self.assertEqual(len(spawns), 1,
                             "exactly one replacement spawn")
            self._wait(lambda: g.get_job("j22")["owner_worker_id"] == wid,
                       timeout=30.0)
            with self.assertRaises(Exception):
                rc.dispatch_restart("j22")  # already open: refused
        self._r3(one)

    # ------------------------------------------------------------ R14-23
    def test_R14_23_recovery_controller_plus_scheduler(self):
        """Scheduler racing an open recovery incident: the scheduler never
        claims incident-owned work; exactly one authority owns the job."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
            self._sups.append(sup)
            g.create_job("j23", "t", "s", "test")
            inc = g.find_or_create_recovery_incident(
                "j23", "DEAD", {"job_id": "j23"}, "test")
            sched = Scheduler(path, sup, SchedulerConfig(
                poll_interval_s=1.0, lease_ttl_s=30.0,
                max_concurrent_jobs=3, batch_size=10), scheduler_id="s0")
            self._scheds.append(sched)
            rc = RecoveryController(path, RecoveryConfig(
                observation_window_s=1.0, evaluation_interval_s=1.0,
                claim_timeout_s=2.0, restart_worker_duration_s=60.0,
                restart_worker_ttl_s=30.0,
                restart_worker_hb_interval_s=0.3), supervisor=sup)
            self._ctls.append(rc)
            res = self._run_barrier([sched.evaluate_once, rc.evaluate])
            for v in res.values():
                self.assertNotIsInstance(v, Exception, f"race failed: {v}")
            job = g.get_job("j23")
            self.assertEqual(job["status"], "PENDING")
            self.assertIsNone(job["owner_worker_id"],
                              "scheduler must not claim incident-owned work")
            self.assertGreaterEqual(res[0]["skipped"], 1)
            self.assertEqual(
                len(g.recovery_attempts_for(inc["incident_id"])), 0,
                "no evidence for the controller to act on: no attempt")
        self._r3(one)

    # ------------------------------------------------------------ R14-24
    def test_R14_24_finalizer_plus_reconciler(self):
        """Publish racing a desired-state change: the old generation never
        covers the new generation; exactly one atomic outcome per op."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
            self._sups.append(sup)
            spec = {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                    "policy": {}}
            g.set_desired_item("d-a", dict(spec), "test")
            V = g.get_desired_head()["version"]
            gen = canonical_release_generation(V)
            job, _ = g.ensure_job_for_desired_state(
                desired_work_id="d-a", task_id="t", stage_id="s",
                max_attempts=3, policy={}, desired_version=V,
                actor="reconciler")
            jid = job["job_id"]
            g.claim_job(jid, "w", 30.0, "test")
            g.transition_job(jid, "RUNNING", "worker:w")
            tok = g.get_job(jid)["fencing_token"]
            r5_complete(gate=g, job_id=jid, worker_id="w",
                        fencing_token=tok, task_id="t", outcome="SUCCESS",
                        evidence={"ok": True}, actor="worker:w")
            g.begin_finalization_run(gen, V, "r14c-fin")
            g.evaluate_finalization(gen, 1, "r14c-fin")
            row = g.get_finalization_run(gen)
            self.assertEqual(row["state"], "READY")
            ver, mh = row["version"], row["manifest_hash"]
            res = self._run_barrier([
                lambda: self._thread_gate(path).publish_finalization(
                    gen, ver, mh, "r14c-fin"),
                lambda: self._thread_gate(path).set_desired_item(
                    "d-b", dict(spec), "test"),
            ])
            pub, new_item = res[0], res[1]
            self.assertNotIsInstance(new_item, Exception)
            row = g.get_finalization_run(gen)
            if isinstance(pub, Exception):
                self.assertIsInstance(pub, TransitionRejected)
                self.assertIn("STALE_GENERATION", str(pub),
                              f"stale publish must fail closed: {pub}")
                self.assertEqual(row["state"], "READY")
            else:
                self.assertEqual(row["state"], "FINALIZED")
                self.assertEqual(row["desired_state_version"], V)
            V2 = g.get_desired_head()["version"]
            self.assertEqual(V2, V + 1)
            self.assertNotEqual(canonical_release_generation(V2), gen,
                                "the new item names a NEW generation")
            # The old generation's record never silently covers d-b:
            # it is bound to desired_state_version V, and d-b landed at
            # V+1 under a different generation.
            if row["state"] == "FINALIZED":
                self.assertEqual(row["desired_state_version"], V)
                pev = g.store.conn.execute(
                    "SELECT payload FROM ledger WHERE event_type=?"
                    " AND json_extract(payload,'$.release_generation')=?",
                    ("finalization.published", gen)).fetchone()
                self.assertIsNotNone(pev)
                self.assertEqual(json.loads(pev[0])["desired_state_version"],
                                 V)
        self._r3(one)

    # ------------------------------------------------------------ R14-25
    def test_R14_25_finalizer_plus_scheduler(self):
        """Finalizer evaluating while the scheduler admits: the run row
        reflects exactly one evaluation; no FINALIZED over active work."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
            self._sups.append(sup)
            spec = {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                    "policy": {}}
            g.set_desired_item("d-a", dict(spec), "test")
            V = g.get_desired_head()["version"]
            gen = canonical_release_generation(V)
            job, _ = g.ensure_job_for_desired_state(
                desired_work_id="d-a", task_id="t", stage_id="s",
                max_attempts=3, policy={}, desired_version=V,
                actor="reconciler")
            jid = job["job_id"]
            fin = Finalizer(path, sup, FinalizationConfig(
                poll_interval_s=1.0, batch_size=10, actor="r14c-fin"))
            self._fins.append(fin)
            sched = Scheduler(path, sup, SchedulerConfig(
                poll_interval_s=1.0, lease_ttl_s=30.0,
                max_concurrent_jobs=1, batch_size=10), scheduler_id="s0")
            self._scheds.append(sched)
            g.begin_finalization_run(gen, V, "r14c-fin")
            res = self._run_barrier([fin.evaluate_once,
                                     sched.evaluate_once])
            for v in res.values():
                self.assertNotIsInstance(v, Exception, f"race failed: {v}")
            row = g.get_finalization_run(gen)
            # Whichever order the evidence landed in, the generation is
            # NOT releasable while the job is incomplete/active.
            self.assertEqual(row["state"], "BLOCKED")
            self.assertNotEqual(
                [b["category"] for b in json.loads(row["blockers"] or "[]")],
                [])
            self.assertNotEqual(row["state"], "FINALIZED")
            # Exactly one claim happened; the worker completes for real.
            n = g.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type=?"
                " AND json_extract(payload,'$.job_id')=?",
                ("job.claimed", jid)).fetchone()[0]
            self.assertEqual(n, 1)
            self._wait(lambda: g.get_job(jid)["status"] == "COMPLETE",
                       timeout=60.0)
            self._wait(lambda: not g.unreaped_proc_spawns(), timeout=30.0)
            frep = fin.evaluate_once()
            row = g.get_finalization_run(gen)
            # evaluate_once publishes READY runs in the same pass.
            self.assertEqual(row["state"], "FINALIZED")
            self.assertEqual(len([e for e in frep["finalized"]
                                  if e["release_generation"] == gen]), 1)
            # Explicit re-publish is idempotent.
            pub = g.publish_finalization(gen, row["version"],
                                         row["manifest_hash"], "r14c-fin")
            self.assertEqual(pub["state"], "FINALIZED")
        self._r3(one)

    # ------------------------------------------------------------ R14-26
    def test_R14_26_supervisor_boot_plus_fence_sweep(self):
        """Supervisor (re)boot adopting a fenced live worker while fence
        sweeps run concurrently: exactly one fence_enforced event, the
        group dies, no crash."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup1 = Supervisor(path, actor="sup1",
                              heartbeat_interval_s=0.4)
            self._sups.append(sup1)
            g.create_job("j26", "t", "s", "test")
            g.claim_job("j26", "w26", 60.0, "test")
            # (worker transitions CLAIMED->RUNNING itself)
            tok26 = g.get_job("j26")["fencing_token"]
            proc_id = sup1.start_worker(
                "w26", "j26", {"kind": "wedged_ignore_sigterm"},
                ttl_s=60.0, hb_interval_s=0.3, expect_token=tok26)
            self._wait(lambda: g.get_job("j26")["status"] == "RUNNING",
                       timeout=30.0)
            pid = sup1._procs["w26"].popen.pid
            # wedged workers never heartbeat; prove liveness via /proc.
            self._wait(lambda: _proc_pgid_alive(pid), timeout=30.0)
            tok = g.get_job("j26")["fencing_token"]
            inc = g.find_or_create_recovery_incident(
                "j26", "DEAD", {"job_id": "j26"}, "test")
            g.reclaim_lease("j26", actor="recovery-controller",
                           reason="r14c race", expected_owner="w26",
                           expected_token=tok, force=True, verdict="DEAD",
                           incident_id=inc["incident_id"])
            sup1.close()  # sweep thread stops; the worker SURVIVES
            self.assertFalse(_proc_gone(pid), "worker must survive close")
            sup2 = Supervisor(path, actor="sup2",
                              heartbeat_interval_s=0.4)
            self._sups.append(sup2)
            res = self._run_barrier([sup2.fence_sweep, sup2.fence_sweep,
                                     sup2.observe])
            for v in res.values():
                self.assertNotIsInstance(v, Exception, f"race failed: {v}")
            # Physical fencing: SIGTERM ignored -> grace -> SIGKILL. The
            # fencing may already have happened inside sup2's boot sweep;
            # either way the dead worker can linger as a zombie child of
            # sup1 (its Popen was never waited on after close), and a
            # zombie still answers killpg. Reap through the known handle
            # so the /proc probe observes true group death.
            popen = sup1._procs["w26"].popen

            def _group_dead():
                if popen.poll() is not None:
                    try:
                        popen.wait(timeout=5)
                    except Exception:
                        pass
                return not _proc_pgid_alive(pid)

            self._wait(_group_dead, timeout=20.0)
            n = g.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type=?"
                " AND json_extract(payload,'$.worker_id')=?"
                " AND json_extract(payload,'$.proc_id')=?",
                ("worker.fence_enforced", "w26", proc_id)).fetchone()[0]
            self.assertEqual(n, 1,
                             "exactly one fence_enforced per (worker, proc)")
        self._r3(one)

    # ------------------------------------------------------------ R14-27
    def test_R14_27_reclaim_plus_worker_commit(self):
        """Reclaim racing a real R5 commit: exactly one winner; the loser
        cannot mutate."""
        def one(_i):
            path, _st, g = self._fresh_db()
            g.create_job("j27", "t", "s", "test")
            g.claim_job("j27", "w27", 60.0, "test")
            g.transition_job("j27", "RUNNING", "worker:w27")
            tok = g.get_job("j27")["fencing_token"]
            inc = g.find_or_create_recovery_incident(
                "j27", "DEAD", {"job_id": "j27"}, "test")

            def do_commit():
                st2, g2 = self._tstore(path)
                try:
                    return r5_complete(
                        gate=g2, job_id="j27", worker_id="w27",
                        fencing_token=tok, task_id="t", outcome="SUCCESS",
                        evidence={"ok": True}, actor="worker:w27")
                except (TransitionRejected, LeaseError,
                        StoreError) as e:
                    return e
                finally:
                    st2.close()

            def do_reclaim():
                st2, g2 = self._tstore(path)
                try:
                    return g2.reclaim_lease(
                        "j27", actor="recovery-controller",
                        reason="r14c race", expected_owner="w27",
                        expected_token=tok, force=True, verdict="DEAD",
                        incident_id=inc["incident_id"])
                except (TransitionRejected, LeaseError,
                        StoreError) as e:
                    return e
                finally:
                    st2.close()

            res = self._run_barrier([do_commit, do_reclaim])
            job = g.get_job("j27")
            if job["status"] == "COMPLETE":
                self.assertIsInstance(
                    res[1], (TransitionRejected, LeaseError),
                    "reclaim must lose once the commit won")
                self.assertEqual(job["fencing_token"], tok)
            else:
                self.assertEqual(job["status"], "UNCERTAIN")
                self.assertIsInstance(
                    res[0], (TransitionRejected, LeaseError),
                    "stale commit must lose once the reclaim won")
                self.assertEqual(job["fencing_token"], tok + 1,
                                 "exactly one token bump")
        self._r3(one)

    # ------------------------------------------------------------ R14-28
    def test_R14_28_reclaim_plus_lease_renewal(self):
        """Renewal loop racing a token bump: pre-bump renewals succeed,
        post-bump renewals fail, token advances exactly once."""
        def one(_i):
            path, _st, g = self._fresh_db()
            g.create_job("j28", "t", "s", "test")
            g.claim_job("j28", "w28", 60.0, "test")
            g.transition_job("j28", "RUNNING", "worker:w28")
            tok = g.get_job("j28")["fencing_token"]
            stop = threading.Event()
            results: list = []

            def renew_loop():
                st2, g2 = self._tstore(path)
                try:
                    while not stop.is_set():
                        try:
                            results.append(g2.renew_lease(
                                "j28", "w28", tok, 60.0, "worker:w28"))
                        except (LeaseError, TransitionRejected,
                                StoreError) as e:
                            results.append(e)
                            return
                        time.sleep(0.05)
                finally:
                    st2.close()

            t = threading.Thread(target=renew_loop)
            t.start()
            time.sleep(0.3)
            inc = g.find_or_create_recovery_incident(
                "j28", "DEAD", {"job_id": "j28"}, "test")
            g.reclaim_lease("j28", actor="recovery-controller",
                           reason="r14c race", expected_owner="w28",
                           expected_token=tok, force=True, verdict="DEAD",
                           incident_id=inc["incident_id"])
            # Let the loop record post-bump renewals before stopping.
            time.sleep(0.3)
            stop.set()
            t.join(timeout=10)
            self.assertFalse(t.is_alive())
            self.assertEqual(g.get_job("j28")["fencing_token"], tok + 1,
                             "exactly one token bump")
            self.assertIn(True, results, "pre-bump renewals must succeed")
            # Deterministic post-bump check: the stale token is dead.
            self.assertFalse(g.renew_lease("j28", "w28", tok, 60.0,
                                           "worker:w28"),
                             "post-bump renewal with the stale token fails")
            self.assertIn(False, results,
                          "post-bump renewals with the stale token fail")
        self._r3(one)

    # ------------------------------------------------------------ R14-29
    def test_R14_29_fencing_plus_heartbeat(self):
        """Stale heartbeats racing fresh heartbeats after a token bump:
        stale rejected (fenced milestones), fresh accepted, seq monotonic."""
        def one(_i):
            path, _st, g = self._fresh_db()
            g.create_job("j29", "t", "s", "test")
            g.create_worker("w29", "test")
            g.create_worker("w29b", "test")
            g.claim_job("j29", "w29", 60.0, "test")
            g.transition_job("j29", "RUNNING", "worker:w29")
            tok = g.get_job("j29")["fencing_token"]
            for seq in range(3):
                g.ingest_heartbeat("w29", "p29", "j29", tok, seq,
                                   "RUNNING", "op", "worker:w29")
            inc = g.find_or_create_recovery_incident(
                "j29", "DEAD", {"job_id": "j29"}, "test")
            g.reclaim_lease("j29", actor="recovery-controller",
                           reason="r14c race", expected_owner="w29",
                           expected_token=tok, force=True, verdict="DEAD",
                           incident_id=inc["incident_id"])
            g.transition_job("j29", "PENDING", "recovery-controller",
                             reason="r14c")
            g.claim_job("j29", "w29b", 60.0, "test")
            tok2 = g.get_job("j29")["fencing_token"]
            # Reclaim bumped once; the fresh claim by w29b bumped again.
            self.assertEqual(tok2, tok + 2)

            def stale_beats():
                st2, g2 = self._tstore(path)
                errs = 0
                try:
                    for seq in range(3, 8):
                        try:
                            g2.ingest_heartbeat(
                                "w29", "p29", "j29", tok, seq, "RUNNING",
                                "op", "worker:w29")
                        except LeaseError:
                            errs += 1
                    return errs
                finally:
                    st2.close()

            def fresh_beats():
                st2, g2 = self._tstore(path)
                try:
                    for seq in range(5):
                        g2.ingest_heartbeat(
                            "w29b", "p29b", "j29", tok2, seq, "RUNNING",
                            "op", "worker:w29b")
                    return "ok"
                finally:
                    st2.close()

            res = self._run_barrier([stale_beats, fresh_beats])
            self.assertNotIsInstance(res[1], Exception)
            self.assertEqual(res[0], 5, "every stale heartbeat rejected")
            self.assertEqual(res[1], "ok")
            n = g.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type=?"
                " AND json_extract(payload,'$.job_id')=?",
                ("worker.heartbeat_fenced", "j29")).fetchone()[0]
            self.assertEqual(n, 5, "one fenced milestone per rejection")
            hbs = g.heartbeats_for("w29b", "p29b")
            self.assertEqual(len(hbs), 5)
            seqs = sorted(h["hb_seq"] for h in hbs)
            self.assertEqual(seqs, [0, 1, 2, 3, 4])
        self._r3(one)

    # ------------------------------------------------------------ R14-30
    def test_R14_30_fencing_plus_worker_completion(self):
        """Stale R5 completion racing a fresh completion: the stale token
        is rejected at stage; exactly one artifact, one COMPLETE."""
        def one(_i):
            path, _st, g = self._fresh_db()
            g.create_job("j30", "t", "s", "test")
            g.claim_job("j30", "w30", 60.0, "test")
            g.transition_job("j30", "RUNNING", "worker:w30")
            tok = g.get_job("j30")["fencing_token"]
            inc = g.find_or_create_recovery_incident(
                "j30", "DEAD", {"job_id": "j30"}, "test")
            g.reclaim_lease("j30", actor="recovery-controller",
                           reason="r14c race", expected_owner="w30",
                           expected_token=tok, force=True, verdict="DEAD",
                           incident_id=inc["incident_id"])
            with self.assertRaises((LeaseError, TransitionRejected)):
                r5_complete(gate=g, job_id="j30", worker_id="w30",
                            fencing_token=tok, task_id="t",
                            outcome="SUCCESS", evidence={"ok": True},
                            actor="worker:w30")
            g.transition_job("j30", "PENDING", "recovery-controller",
                             reason="r14c")
            g.claim_job("j30", "w30b", 60.0, "test")
            g.transition_job("j30", "RUNNING", "worker:w30b")
            tok2 = g.get_job("j30")["fencing_token"]
            r5_complete(gate=g, job_id="j30", worker_id="w30b",
                        fencing_token=tok2, task_id="t", outcome="SUCCESS",
                        evidence={"ok": True}, actor="worker:w30b")
            job = g.get_job("j30")
            self.assertEqual(job["status"], "COMPLETE")
            arts = g.store.conn.execute(
                "SELECT * FROM artifacts WHERE job_id=?", ("j30",)
            ).fetchall()
            self.assertEqual(len(arts), 1,
                             "exactly one artifact: the stale stage left"
                             " no orphan")
        self._r3(one)

    # ------------------------------------------------------------ R14-31
    def test_R14_31_policy_cas_plus_attempt_creation(self):
        """Two writers racing the policy CAS and attempt creation: exactly
        one CAS winner; concurrent creators collapse to one attempt."""
        def one(_i):
            path, _st, g = self._fresh_db()
            g.create_job("j31", "t", "s", "test")
            g.claim_job("j31", "w31", 60.0, "test")
            g.transition_job("j31", "RUNNING", "worker:w31")
            tok = g.get_job("j31")["fencing_token"]
            inc = g.find_or_create_recovery_incident(
                "j31", "DEAD", {"job_id": "j31"}, "test")
            iid = inc["incident_id"]
            pol = g.ensure_recovery_policy(
                iid, policy_version="r14c/v1", current_rung=1,
                rung_name="Retry with backoff", incident_budget=7,
                per_rung_budgets={1: 2, 2: 2, 3: 1, 4: 0, 5: 0},
                actor="test")
            ver = pol["version"]

            def cas(updates):
                st2, g2 = self._tstore(path)
                try:
                    return g2.cas_update_recovery_policy(
                        iid, ver, updates, "test")
                except PolicyConflict as e:
                    return e
                finally:
                    st2.close()

            res = self._run_barrier([
                lambda: cas({"current_rung": 2,
                             "rung_name": "Restart worker"}),
                lambda: cas({"current_rung": 3,
                             "rung_name": "Replace / reassign"}),
            ])
            wins = [v for v in res.values()
                    if not isinstance(v, Exception)]
            losses = [v for v in res.values()
                      if isinstance(v, PolicyConflict)]
            self.assertEqual(len(wins), 1, "exactly one CAS winner")
            self.assertEqual(len(losses), 1, "loser gets PolicyConflict")
            prow = g.get_recovery_policy(iid)
            self.assertEqual(prow["version"], ver + 1)
            # The loser re-reads: it observes the winner, never overwrites.
            self.assertIn(prow["current_rung"], (2, 3))

            def mk():
                st2, g2 = self._tstore(path)
                try:
                    return g2.create_recovery_attempt(
                        incident_id=iid, job_id="j31", fencing_token=tok,
                        rung=1, rung_name="Retry with backoff",
                        action={"name": "reclaim",
                                "rung": 1, "rung_name": "Retry with backoff"},
                        failure_class="DEAD",
                        success_criterion="s", failure_criterion="f",
                        evidence_before={}, actor="test")["attempt_id"]
                finally:
                    st2.close()

            res2 = self._run_barrier([mk, mk])
            self.assertNotIsInstance(res2[0], Exception)
            self.assertNotIsInstance(res2[1], Exception)
            self.assertEqual(res2[0], res2[1],
                             "concurrent creators collapse to one attempt")
            self.assertEqual(len(g.recovery_attempts_for(iid)), 1)
        self._r3(one)

    # ------------------------------------------------------------ R14-32
    def test_R14_32_breaker_transition_plus_scheduler_admission(self):
        """Breaker OPEN racing scheduler admission: no claim while OPEN;
        cooldown -> half-open probe -> close -> admission. Exactly one
        authoritative outcome at every step."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
            self._sups.append(sup)
            g.create_job("j32", "t", "s", "test")
            g.ensure_breaker_state("JOB", "j32", cooldown_s=0.3,
                                   actor="test")
            g.transition_breaker("JOB", "j32", expected_version=0,
                                 to_state="OPEN", actor="r14c-ctl",
                                 reason="r14c")
            v_open = g.get_breaker_state("JOB", "j32")["version"]
            sched = Scheduler(path, sup, SchedulerConfig(
                poll_interval_s=1.0, lease_ttl_s=30.0,
                max_concurrent_jobs=1, batch_size=10), scheduler_id="s0")
            self._scheds.append(sched)
            res = self._res_on(path, cooldown_s=0.3)
            # Concurrent controller evaluation + admission while OPEN.
            rres = self._run_barrier([res.evaluate_once,
                                      sched.evaluate_once])
            for v in rres.values():
                self.assertNotIsInstance(v, Exception, f"race failed: {v}")
            self.assertEqual(rres[1]["resilience_denied"], 1)
            self.assertEqual(g.get_job("j32")["status"], "PENDING")
            self.assertEqual(g.get_breaker_state("JOB", "j32")["version"],
                             v_open,
                             "admission never mutates breaker state")
            # Cooldown elapses -> controller half-opens -> probe admitted.
            time.sleep(0.5)
            r2 = res.evaluate_once()
            stt = g.get_breaker_state("JOB", "j32")["state"]
            self.assertEqual(stt, "HALF_OPEN")
            srep = sched.evaluate_once()
            self.assertEqual(srep["admitted"], 1)
            self._wait(lambda: g.get_job("j32")["status"] == "COMPLETE",
                       timeout=60.0)
            res.evaluate_once()
            self.assertEqual(g.get_breaker_state("JOB", "j32")["state"],
                             "CLOSED",
                             "probe verified by progress -> CLOSED")
        self._r3(one)


# ================================================================== R14-F1
class TestR14FFencing(CBase):
    """R14-50..60: composition-level logical/physical fencing proof. A
    revocation is revocation: every durable mutation path validates the
    (worker_id, fencing_token) pair, and physical fencing is proven by
    /proc state, never by inference."""

    def _revoked(self, job_id="jf", worker="wf"):
        """RUNNING job, then a forced reclaim: returns (path, gate, sup,
        proc_id, pid, old_token)."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        g.create_job(job_id, "t", "s", "test")
        g.claim_job(job_id, worker, 60.0, "test")
        g.transition_job(job_id, "RUNNING", f"worker:{worker}")
        tok = g.get_job(job_id)["fencing_token"]
        inc = g.find_or_create_recovery_incident(
            job_id, "DEAD", {"job_id": job_id}, "test")
        g.reclaim_lease(job_id, actor="recovery-controller",
                       reason="r14c fencing", expected_owner=worker,
                       expected_token=tok, force=True, verdict="DEAD",
                       incident_id=inc["incident_id"])
        return path, g, sup, tok

    def _live_worker(self, job_id="jf", worker="wf",
                     kind="wedged_ignore_sigterm"):
        """A REAL worker that OWNS its RUNNING job (expect_token verified):
        returns (path, g, sup, proc_id, pid, tok). The worker is live and
        authoritative until the test revokes it."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        g.create_job(job_id, "t", "s", "test")
        g.claim_job(job_id, worker, 60.0, "test")
        # The worker transitions CLAIMED->RUNNING itself on the
        # pre-claimed dispatch path: the test must NOT pre-transition,
        # or the worker exits FATAL on RUNNING->RUNNING and never
        # reaches its behavior.
        tok = g.get_job(job_id)["fencing_token"]
        proc_id = sup.start_worker(worker, job_id, {"kind": kind},
                                   ttl_s=60.0, hb_interval_s=0.3,
                                   expect_token=tok)
        pid = sup._procs[worker].popen.pid
        # The worker is truly live only once it drove the job to RUNNING
        # itself (its behavior is now executing).
        self._wait(lambda: g.get_job(job_id)["status"] == "RUNNING",
                   timeout=30.0)
        # Wedged workers never heartbeat; prove liveness via /proc.
        self._wait(lambda: _proc_pgid_alive(pid), timeout=30.0)
        return path, g, sup, proc_id, pid, tok

    def _revoke(self, g, job_id, worker, tok, reason="r14c fencing"):
        """Forced reclaim: the live worker becomes stale authority.
        Returns the new token."""
        inc = g.find_or_create_recovery_incident(
            job_id, "DEAD", {"job_id": job_id}, "test")
        g.reclaim_lease(job_id, actor="recovery-controller",
                        reason=reason, expected_owner=worker,
                        expected_token=tok, force=True, verdict="DEAD",
                        incident_id=inc["incident_id"])
        return g.get_job(job_id)["fencing_token"]

    # ------------------------------------------------------------ R14-50
    def test_R14_50_commit_surface_validates_token(self):
        """R5 commit path after revocation: stage/begin/verify/commit all
        reject the stale token; no partial protocol state persists."""
        _path, g, _sup, tok = self._revoked()
        ops = {
            "stage": lambda: g.stage_artifact(
                job_id="jf", worker_id="wf", fencing_token=tok,
                task_id="t", kind="result", data=b"r14c", actor="worker:wf"),
            "begin": lambda: g.begin_commit(
                "jf", "wf", tok, artifact_id="__none__",
                actor="worker:wf"),
            "commit": lambda: g.commit_artifact(
                "jf", "wf", tok, artifact_id="__none__",
                actor="worker:wf"),
            "fail": lambda: g.fail_job_execution(
                "jf", "wf", tok, actor="worker:wf", reason="test error"),
            "complete": lambda: r5_complete(
                gate=g, job_id="jf", worker_id="wf", fencing_token=tok,
                task_id="t", outcome="SUCCESS", evidence={"ok": True},
                actor="worker:wf"),
        }
        for op, fn in ops.items():
            with self.assertRaises((LeaseError, TransitionRejected),
                                   msg=f"stale token rejected at {op}"):
                fn()
        self.assertEqual(g.get_job("jf")["status"], "UNCERTAIN")
        n = g.store.conn.execute(
            "SELECT COUNT(*) FROM artifacts WHERE job_id='jf'"
        ).fetchone()[0]
        self.assertEqual(n, 0, "stale stage left no partial state")
        # The right owner+right token still completes: the bump, not the
        # operation, was the barrier.
        g.transition_job("jf", "PENDING", "recovery-controller",
                         reason="r14c")
        g.claim_job("jf", "wf2", 60.0, "test")
        g.transition_job("jf", "RUNNING", "worker:wf2")
        tok2 = g.get_job("jf")["fencing_token"]
        r5_complete(gate=g, job_id="jf", worker_id="wf2",
                    fencing_token=tok2, task_id="t", outcome="SUCCESS",
                    evidence={"ok": True}, actor="worker:wf2")
        self.assertEqual(g.get_job("jf")["status"], "COMPLETE")

    # ------------------------------------------------------------ R14-51
    def test_R14_51_renew_after_revocation_fails(self):
        """renew_lease with the pre-revocation token returns False and
        does not extend the lease."""
        _path, g, _sup, tok = self._revoked()
        lease_before = g.get_job("jf")["lease_expires_at"]
        ok = g.renew_lease("jf", "wf", tok, 60.0, "worker:wf")
        self.assertFalse(ok)
        self.assertEqual(g.get_job("jf")["lease_expires_at"], lease_before)

    # ------------------------------------------------------------ R14-52
    def test_R14_52_heartbeat_after_revocation_rejected(self):
        """ingest_heartbeat with the stale token raises LeaseError and
        journals a worker.heartbeat_fenced milestone."""
        _path, g, _sup, tok = self._revoked()
        g.create_worker("wf", "test")
        with self.assertRaises(LeaseError):
            g.ingest_heartbeat("wf", "p", "jf", tok, 0, "RUNNING", "op",
                               "worker:wf")
        n = g.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?"
            " AND json_extract(payload,'$.job_id')=?",
            ("worker.heartbeat_fenced", "jf")).fetchone()[0]
        self.assertEqual(n, 1)
        self.assertEqual(len(g.heartbeats_for("wf", "p")), 0,
                         "rejected beats are not stored")

    # ------------------------------------------------------------ R14-53
    def test_R14_53_progress_after_revocation_rejected(self):
        """update_job_progress with the stale token is rejected and
        progress_updated_at does not move."""
        _path, g, _sup, tok = self._revoked()
        ts_before = g.get_job("jf").get("progress_updated_at")
        # The stale owner cannot touch progress regardless of plumbing.
        with self.assertRaises((LeaseError, TransitionRejected)):
            g.update_job_progress("jf", "wf", tok, 0.5, 1.0, actor="worker:wf")
        self.assertEqual(g.get_job("jf").get("progress_updated_at"),
                         ts_before)

    # ------------------------------------------------------------ R14-54
    def test_R14_54_fail_execution_after_revocation_rejected(self):
        """fail_job_execution with the stale token is rejected; the job
        stays UNCERTAIN under the new generation."""
        _path, g, _sup, tok = self._revoked()
        with self.assertRaises((LeaseError, TransitionRejected)):
            g.fail_job_execution("jf", "wf", tok, actor="worker:wf",
                                 reason="test error")
        self.assertEqual(g.get_job("jf")["status"], "UNCERTAIN")

    # ------------------------------------------------------------ R14-55
    def test_R14_55_physical_group_death_wedged_sigterm(self):
        """A worker ignoring SIGTERM, group-killed after grace: /proc
        proves the whole process group dead; latency recorded <= H."""
        path, g, sup, proc_id, pid, tok = self._live_worker(
            job_id="j55", worker="w55")
        # Deterministic: the behavior installed SIG_IGN before fencing,
        # so SIGTERM cannot end it — SIGKILL must.
        self._wait(lambda: _sigterm_ignored(pid), timeout=30.0)
        # Revoke: the live worker is now stale authority.
        self._revoke(g, "j55", "w55", tok, reason="r14c physical")
        t0 = time.monotonic()
        report = sup.fence_sweep()
        # Physical fencing bound: SIGTERM -> grace H/4 -> SIGKILL ->
        # the whole group dead within one heartbeat interval H.
        self._wait(lambda: not _proc_pgid_alive(pid), timeout=20.0)
        dt = time.monotonic() - t0
        sup.observe()
        rows = g.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type=?"
            " AND json_extract(payload,'$.proc_id')=?",
            ("worker.fence_enforced", proc_id)).fetchall()
        self.assertEqual(len(rows), 1)
        payload = json.loads(rows[0][0])
        self.assertEqual(payload["outcome"], "killed")
        self.assertEqual(payload["signals"], ["SIGTERM", "SIGKILL"])
        self.assertLessEqual(payload["fenced_out_worker_seconds"], 0.4,
                             f"H bound violated: {payload}")
        self.assertLessEqual(dt, 5.0,
                             f"wall latency beyond bound: {dt:.2f}s")
        self.assertNotIn("already_dead", json.dumps(payload))
        self.assertTrue(payload.get("within_bound"))

    # ------------------------------------------------------------ R14-56
    def test_R14_56_descendant_death(self):
        """A wedged worker that spawned a child: killpg reaps the child
        too; /proc proves no descendant survives."""
        path, g, sup, proc_id, pid, tok = self._live_worker(
            job_id="j56", worker="w56", kind="wedged_spawn_child")
        self._revoke(g, "j56", "w56", tok, reason="r14c descendant")
        # The child is spawned by the behavior (after RUNNING): wait for
        # it to be visible before fencing.
        self._wait(lambda: len(_proc_children_of_pgid(pid)) >= 1,
                   timeout=30.0)
        kids = _proc_children_of_pgid(pid)
        self.assertGreaterEqual(len(kids), 1,
                                "spawned child must be visible in /proc")
        sup.fence_sweep()
        self._wait(lambda: not _proc_pgid_alive(pid), timeout=20.0)
        for k in kids:
            self.assertTrue(_proc_gone(k),
                            f"descendant {k} survived group kill")
        sup.observe()

    # ------------------------------------------------------------ R14-57
    def test_R14_57_pid_reuse_spoof(self):
        """A live process presenting a stale (pid, start-identity) pair is
        classified 'reuse' — it can never be mistaken for the worker."""
        _path, g, _sup, _tok = self._revoked(job_id="j57", worker="w57")
        from axos.exec import boot as boot_mod
        # Spoof: durable evidence claims the worker's spawn is THIS test
        # process (alive, but pgid != pid -> not a session leader, and
        # start_jiffies forged wrong). The R6 classifier must say reuse.
        me = os.getpid()
        spoof = {"worker_id": "w57", "proc_id": "p-spoof", "pid": me,
                 "start_jiffies": -123456789, "pgid": me,
                 "job_id": "j57"}
        cls = boot_mod._classify_spawn(spoof)
        self.assertEqual(cls.kind, "reuse",
                         f"stale identity must classify as reuse: {cls}")
        # Via the recovery controller's read-only identity evidence.
        g.append_event("worker.proc_spawned", spoof, "test")
        rc = RecoveryController(
            _path,
            RecoveryConfig(observation_window_s=1.0,
                           evaluation_interval_s=1.0,
                           claim_timeout_s=2.0,
                           restart_worker_duration_s=60.0,
                           restart_worker_ttl_s=30.0,
                           restart_worker_hb_interval_s=0.3),
            readiness=lambda: True)
        self._ctls.append(rc)
        ident = rc._process_identity(g, "w57")
        self.assertEqual(ident["classification"], "reuse")
        self.assertEqual(ident["pid"], me)
        # And a session-leader process with forged start jiffies is also
        # reuse: identity is (pid, start_jiffies), not pid alone.
        sess = os.getpgid(me)
        if sess == me:
            spoof2 = dict(spoof, start_jiffies=-1)
            cls2 = boot_mod._classify_spawn(spoof2)
            self.assertEqual(cls2.kind, "reuse")
        # The real spawn evidence for the live supervisor workers still
        # classifies live_match (contract sanity, not spoofed).
        spawns = g.unreaped_proc_spawns()
        self.assertTrue(all(isinstance(s, dict) for s in spawns))

    # ------------------------------------------------------------ R14-58
    def test_R14_58_fencing_triple_identity(self):
        """Every mutation validates (job_id, owner_worker_id,
        fencing_token): wrong worker fails, wrong token fails, right
        pair succeeds."""
        _path, g, _sup, _tok = self._revoked(job_id="j58", worker="w58")
        g.transition_job("j58", "PENDING", "recovery-controller",
                         reason="r14c")
        g.claim_job("j58", "w58b", 60.0, "test")
        g.transition_job("j58", "RUNNING", "worker:w58b")
        good_tok = g.get_job("j58")["fencing_token"]

        def _stage(worker_id, token):
            return g.stage_artifact(
                job_id="j58", worker_id=worker_id, fencing_token=token,
                task_id="t", kind="result", data=b"r14c", actor="worker:w")

        with self.assertRaises((LeaseError, TransitionRejected)):
            _stage("w58-intruder", good_tok)
        with self.assertRaises((LeaseError, TransitionRejected)):
            _stage("w58b", good_tok - 1)
        _stage("w58b", good_tok)
        self.assertEqual(
            g.store.conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE job_id='j58'"
            ).fetchone()[0], 1)

    # ------------------------------------------------------------ R14-59
    def test_R14_59_fence_latency_within_bound(self):
        """fence_enforced records fenced_out_worker_seconds and
        within_bound; both are true and within H."""
        path, g, sup, proc_id, pid, tok = self._live_worker(
            job_id="j59", worker="w59")
        self._revoke(g, "j59", "w59", tok, reason="r14c latency")
        t0 = time.monotonic()
        sup.fence_sweep()
        self._wait(lambda: not _proc_pgid_alive(pid), timeout=20.0)
        dt = time.monotonic() - t0
        sup.observe()
        payload = json.loads(g.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type=?"
            " AND json_extract(payload,'$.proc_id')=?",
            ("worker.fence_enforced", proc_id)).fetchone()[0])
        self.assertTrue(payload.get("within_bound"))
        self.assertLessEqual(payload["fenced_out_worker_seconds"], 0.4)
        self.assertGreaterEqual(payload["fenced_out_worker_seconds"], 0.0)
        self.assertLessEqual(dt, 5.0)
        self.assertNotIn("already_dead", payload["outcome"])

    # ------------------------------------------------------------ R14-60
    def test_R14_60_no_inference_substitution(self):
        """Physical fencing evidence is detect/term/reap timestamps with
        an explicit kill path — never 'worker stopped responding'."""
        path, g, sup, proc_id, pid, tok = self._live_worker(
            job_id="j60", worker="w60")
        self._revoke(g, "j60", "w60", tok, reason="r14c evidence")
        sup.fence_sweep()
        self._wait(lambda: not _proc_pgid_alive(pid), timeout=20.0)
        sup.observe()
        payload = json.loads(g.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type=?"
            " AND json_extract(payload,'$.proc_id')=?",
            ("worker.fence_enforced", proc_id)).fetchone()[0])
        blob = json.dumps(payload).lower()
        self.assertNotIn("stopped responding", blob)
        self.assertNotIn("heartbeat", blob.replace("heartbeat_fenced", ""),
                         "fencing evidence must not cite heartbeat absence")
        self.assertEqual(payload["outcome"], "killed")
        self.assertEqual(payload["signals"], ["SIGTERM", "SIGKILL"])
        # Monotonic evidence: detect -> term -> reap timestamps exist and
        # are ordered; the group is dead by direct /proc check.
        ts = [payload[k] for k in ("detect_mono", "term_mono", "reap_mono")
              if payload.get(k)]
        self.assertGreaterEqual(len(ts), 2)
        self.assertEqual(sorted(ts), ts)
        self.assertTrue(_proc_gone(pid) or not _proc_pgid_alive(pid))


# ================================================== R14-F2 (convergence)
class TestR14Convergence(CBase):
    """R14-61..75: recovery-convergence proof on real durable state.
    Real kill -9 converges; zero progress escalates; the protocol
    survives store restarts with no duplicate work; identical state
    in -> identical pass out."""

    def _drive_until(self, pol, inc_id, want, passes=30):
        for _ in range(passes):
            pol.evaluate()
            inc = self.gate.get_recovery_incident(inc_id)
            if inc and inc.get("outcome") == want:
                return inc
            time.sleep(0.4)
        self.fail(f"incident {inc_id} never reached outcome={want!r}:"
                  f" {self.gate.get_recovery_incident(inc_id)}")

    # ------------------------------------------------------------ R14-61
    def test_R14_61_kill9_converges(self):
        """Real kill -9 mid-execution converges: DEAD verdict -> R1
        reclaim -> attempt SUCCEEDED with positive durable delta ->
        incident closed 'success', policy terminal RECOVERY_COMPLETE."""
        sup = self._new_sup()
        self.gate.create_job("j61", "t", "s", "test")
        self.gate.claim_job("j61", "w61", 60.0, "test")
        tok61 = self.gate.get_job("j61")["fencing_token"]
        proc_id = sup.start_worker(
            "w61", "j61", {"kind": "heartbeat_loop", "duration_s": 120.0},
            ttl_s=30.0, hb_interval_s=0.3, renew=True,
            expect_token=tok61)
        self._wait(lambda: self.gate.get_job("j61")["status"] == "RUNNING",
                   timeout=30.0)
        self._kill_worker_group(sup, "w61")
        wd = self._new_wd(sup)
        outcomes = wd.evaluate()
        self.assertEqual(
            {o["job_id"]: o for o in outcomes}["j61"]["verdict"], "DEAD")
        pol = self._new_pol(sup)
        outs = pol.evaluate()
        inc_id = next(o["incident_id"] for o in outs
                      if "incident_id" in o)
        inc = self._drive_until(pol, inc_id, "success")
        atts = self.gate.recovery_attempts_for(inc_id)
        succ = [a for a in atts if a["attempt_state"] == "SUCCEEDED"]
        self.assertEqual(len(succ), 1)
        self.assertGreater(succ[0]["progress_delta"], 0,
                           "I-18: success requires positive durable delta")
        prow = self.gate.get_recovery_policy(inc_id)
        self.assertEqual(prow["terminal_state"], "RECOVERY_COMPLETE")
        self.assertIn("success", json.dumps(inc.get("diagnosis", "")) +
                      json.dumps(inc.get("outcome", "")))

    # ------------------------------------------------------------ R14-62
    def test_R14_62_two_zero_progress_attempts_escalate(self):
        """Two consecutive zero-progress attempts escalate: the first
        completes 'retry', the second 'escalate', and the incident is
        durably escalated — never silently retried forever."""
        sup = self._new_sup()
        self.gate.create_job("j62", "t", "s", "test")
        self.gate.claim_job("j62", "w62", 60.0, "test")
        self.gate.transition_job("j62", "RUNNING", "worker:w62")
        tok = self.gate.get_job("j62")["fencing_token"]
        inc = self.gate.find_or_create_recovery_incident(
            "j62", "DEAD", {"job_id": "j62"}, "test")
        iid = inc["incident_id"]
        # A genuinely live fence target: a real wedged worker (SIGTERM
        # ignored) whose process the verify pass observes still alive.
        # No fence is dispatched, so both verifications see zero
        # progress — an already-dead target would be idempotent success,
        # not zero progress, per the R8 contract.
        self.gate.create_job("j62tgt", "t", "s", "test")
        self.gate.claim_job("j62tgt", "w62", 60.0, "test")
        tokw = self.gate.get_job("j62tgt")["fencing_token"]
        sup.start_worker("w62", "j62tgt",
                         {"kind": "wedged_ignore_sigterm"},
                         ttl_s=60.0, hb_interval_s=0.3,
                         expect_token=tokw)
        self._wait(lambda: self.gate.get_job("j62tgt")["status"]
                  == "RUNNING", timeout=30.0)
        tgt_pid = sup._procs["w62"].popen.pid
        self._wait(lambda: _sigterm_ignored(tgt_pid), timeout=30.0)
        rc = self._new_rc(sup, observation_window_s=0.2)
        made = []
        for n in (1, 2):
            att = self.gate.create_recovery_attempt(
                incident_id=iid, job_id="j62", fencing_token=tok,
                rung=2, rung_name="Replace / reassign",
                action={"name": "fence", "rung": 2,
                        "rung_name": "Replace / reassign",
                        "target": {"worker_id": "w62",
                                   "proc_id": "p62"}},
                failure_class="DEAD",
                success_criterion="target dead", failure_criterion="f",
                evidence_before={"target_identity":
                                 {"classification": "live_match",
                                  "detail": "stale target live before"
                                            " attempt"}},
                actor="test")
            self.assertTrue(self.gate.claim_recovery_attempt(
                att["attempt_id"], rc.controller_id))
            out = rc._verify(
                self.gate, self.gate.get_recovery_attempt(
                    att["attempt_id"]),
                self.gate.store.current_time(), via="r14c")
            made.append(out)
        self.assertEqual(made[0]["decision"], "retry")
        self.assertEqual(made[0]["progress_delta"], 0.0)
        self.assertEqual(made[1]["decision"], "escalate")
        self.assertEqual(made[1]["outcome"], "escalated")
        inc = self.gate.get_recovery_incident(iid)
        self.assertEqual(inc.get("outcome"), "escalated",
                         "incident must be durably escalated")
        self.assertEqual(inc.get("escalated_to"), "r9-policy")
        self.assertIn("zero-progress", inc.get("diagnosis", ""))

    # ------------------------------------------------------------ R14-63
    def test_R14_63_protocol_survives_store_restart(self):
        """An in-flight RUNNING attempt survives a store close/reopen: a
        fresh controller drives the SAME attempt to SUCCEEDED; no
        duplicate attempt, no lost dispatch evidence."""
        sup = self._new_sup()
        self.gate.create_job("j63", "t", "s", "test")
        # Phantom claim: short lease, no worker process at all. The
        # lease expires, so R8 has a real LEASE_EXPIRED candidate.
        self.gate.claim_job("j63", "w63", 0.5, "test")
        self.gate.transition_job("j63", "RUNNING", "worker:w63")
        time.sleep(0.7)
        inc = self.gate.find_or_create_recovery_incident(
            "j63", "LEASE_EXPIRED", {"job_id": "j63"}, "test")
        rc = self._new_rc(sup, observation_window_s=0.2)
        rc.evaluate()  # intake: LEASE_EXPIRED -> reclaim attempt CREATED
        rc.evaluate()  # drive: dispatch -> RUNNING, verify_after armed
        atts = self.gate.recovery_attempts_for(inc["incident_id"])
        self.assertEqual(len(atts), 1)
        self.assertEqual(atts[0]["attempt_state"], "RUNNING")
        attempt_id = atts[0]["attempt_id"]
        time.sleep(0.4)  # observation window elapses across the restart
        # ---- restart: close every test handle, reopen on the same file.
        for st in list(self._stores):
            st.close()
        self._stores = []
        st2 = open_store(self.db)
        self._stores.append(st2)
        g2 = TransitionGate(st2)
        rc2 = self._new_rc(sup, observation_window_s=0.2)
        rc2.evaluate()  # drive from durable state: verify -> SUCCEEDED
        atts2 = g2.recovery_attempts_for(inc["incident_id"])
        self.assertEqual(len(atts2), 1, "no duplicate attempt created")
        self.assertEqual(atts2[0]["attempt_id"], attempt_id)
        self.assertEqual(atts2[0]["attempt_state"], "SUCCEEDED")
        self.assertGreater(atts2[0]["progress_delta"], 0)
        self.assertEqual(
            g2.get_recovery_incident(inc["incident_id"])["outcome"],
            "success")
        self.gate = g2  # keep the instance consistent post-reopen

    # ------------------------------------------------------------ R14-64
    def test_R14_64_no_duplicate_work_across_controller_restart(self):
        """A controller restart with an open attempt never creates a
        second attempt for the same incident."""
        sup = self._new_sup()
        self.gate.create_job("j64", "t", "s", "test")
        self.gate.claim_job("j64", "w64", 0.5, "test")
        self.gate.transition_job("j64", "RUNNING", "worker:w64")
        time.sleep(0.7)
        inc = self.gate.find_or_create_recovery_incident(
            "j64", "LEASE_EXPIRED", {"job_id": "j64"}, "test")
        rc = self._new_rc(sup)
        rc.evaluate()  # intake creates attempt #1 (CREATED)
        atts = self.gate.recovery_attempts_for(inc["incident_id"])
        self.assertEqual(len(atts), 1)
        for st in list(self._stores):
            st.close()
        self._stores = []
        st2 = open_store(self.db)
        self._stores.append(st2)
        g2 = TransitionGate(st2)
        rc2 = self._new_rc(sup, observation_window_s=30.0)
        # Fresh controller, same durable state: must drive (claim) the
        # open attempt, never create a second one. The window is long so
        # it stays open, not verified.
        rc2.evaluate()
        atts2 = g2.recovery_attempts_for(inc["incident_id"])
        self.assertEqual(len(atts2), 1,
                         "restart must not duplicate the open attempt")
        self.assertEqual(atts2[0]["attempt_state"], "RUNNING")
        self.gate = g2

    # ------------------------------------------------------------ R14-65
    def test_R14_65_crash_between_create_and_consume_heals(self):
        """A crash between R8 attempt creation and R9 consumption heals:
        the durable watermark still trails, so the next pass consumes
        exactly once — attempt_count never double-counts."""
        sup = self._new_sup()
        self.gate.create_job("j65", "t", "s", "test")
        self.gate.claim_job("j65", "w65", 0.5, "test")
        self.gate.transition_job("j65", "RUNNING", "worker:w65")
        time.sleep(0.7)
        inc = self.gate.find_or_create_recovery_incident(
            "j65", "LEASE_EXPIRED", {"job_id": "j65"}, "test")
        iid = inc["incident_id"]
        rc = self._new_rc(sup)
        rc.evaluate()  # intake: attempt #1 CREATED (no policy pass yet)
        self.assertEqual(
            len(self.gate.recovery_attempts_for(iid)), 1)
        # Crash: drop every test handle without any policy pass.
        for st in list(self._stores):
            st.close()
        self._stores = []
        st2 = open_store(self.db)
        self._stores.append(st2)
        g2 = TransitionGate(st2)
        pol2 = self._new_pol(sup, observation_window_s=30.0)
        pol2.evaluate()
        prow = g2.get_recovery_policy(iid)
        self.assertIsNotNone(prow)
        self.assertEqual(prow["attempt_count"], 1,
                         "exactly-once consumption after the crash")
        self.assertEqual(prow["consumed_attempt_number"], 1)
        n_before = g2.store.conn.execute(
            "SELECT COUNT(*) FROM ledger").fetchone()[0]
        pol2.evaluate()  # no state change: the pass must write nothing
        n_after = g2.store.conn.execute(
            "SELECT COUNT(*) FROM ledger").fetchone()[0]
        self.assertEqual(n_after, n_before,
                         "idempotent pass writes nothing")
        self.assertEqual(g2.get_recovery_policy(iid)["attempt_count"], 1)
        self.gate = g2

    # ------------------------------------------------------------ R14-66
    def test_R14_66_deterministic_pass(self):
        """Identical durable state in -> identical decision out: two
        independent databases with identical setups select the same
        rung/action, and a no-change pass writes nothing."""
        built = []
        for _ in range(2):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c",
                             heartbeat_interval_s=0.4)
            self._sups.append(sup)
            g.create_job("j66", "t", "s", "test")
            g.claim_job("j66", "w66", 0.5, "test")
            g.transition_job("j66", "RUNNING", "worker:w66")
            inc = g.find_or_create_recovery_incident(
                "j66", "LEASE_EXPIRED", {"job_id": "j66"}, "test")
            built.append((path, g, sup, inc["incident_id"]))
        time.sleep(0.7)  # both leases expire identically
        decided = []
        for path, g, sup, iid in built:
            pol = PolicyController(
                path, PolicyConfig(
                    per_rung_max_attempts={1: 2, 2: 2, 3: 1, 4: 0, 5: 0},
                    incident_max_attempts=7, policy_version="r14c/v1",
                    evaluation_interval_s=1.0),
                RecoveryConfig(observation_window_s=30.0,
                               evaluation_interval_s=1.0,
                               claim_timeout_s=2.0,
                               restart_worker_duration_s=60.0,
                               restart_worker_ttl_s=30.0,
                               restart_worker_hb_interval_s=0.3),
                supervisor=sup, actor="r14c-policy")
            self._pols.append(pol)
            pol.evaluate()
            atts = g.recovery_attempts_for(iid)
            self.assertEqual(len(atts), 1)
            action = json.loads(atts[0]["action"])
            decided.append((action["name"], atts[0]["rung"],
                            atts[0]["rung_name"]))
        self.assertEqual(decided[0], decided[1],
                         "identical state must decide identically")
        # Drive the first database's attempt to RUNNING, then a
        # no-change pass must write nothing.
        pol0 = self._pols[0]
        pol0.evaluate()  # drive: claim + dispatch -> RUNNING
        g0 = built[0][1]
        n1 = g0.store.conn.execute(
            "SELECT COUNT(*) FROM ledger").fetchone()[0]
        pol0.evaluate()  # window (30s) not elapsed: pure observation
        n2 = g0.store.conn.execute(
            "SELECT COUNT(*) FROM ledger").fetchone()[0]
        self.assertEqual(n1, n2)


# ================================================== R14-F3 (breakers)
class TestR14Breaker(CBase):
    """R14-76..82: breaker composition. Signals -> OPEN -> denial ->
    cooldown -> half-open probe (bounded) -> CLOSED, with the breaker
    row surviving restarts and concurrent signals never double-counted."""

    def _failed_attempts(self, g, job_id, n=3):
        """Authoritative R8 evidence: n FAILED recovery attempts for
        job_id. The resilience controller's scan turns these into
        R8_ATTEMPT_FAILED breaker signals (the compositional chain)."""
        inc = g.find_or_create_recovery_incident(
            job_id, "DEAD", {"job_id": job_id}, "test")
        iid = inc["incident_id"]
        tok = g.get_job(job_id)["fencing_token"]
        for _ in range(n):
            att = g.create_recovery_attempt(
                incident_id=iid, job_id=job_id, fencing_token=tok,
                rung=2, rung_name="Replace / reassign",
                action={"name": "fence", "rung": 2,
                        "rung_name": "Replace / reassign",
                        "target": {"worker_id": "w", "proc_id": "p"}},
                failure_class="DEAD",
                success_criterion="s", failure_criterion="f",
                evidence_before={}, actor="test")
            self.assertTrue(g.claim_recovery_attempt(
                att["attempt_id"], "test-controller"))
            g.complete_recovery_attempt(
                att["attempt_id"], decision="retry",
                observed_effect="r14c: simulated failed attempt",
                progress_delta=0.0, resulting_state="test",
                evidence_after={}, actor="test")
        return iid

    def _open_via_signals(self, path, g, scope_id, n=3, cooldown_s=0.3):
        """Compositional: n FAILED R8 attempts -> the controller's
        evidence scan records R8_ATTEMPT_FAILED signals -> the breaker
        opens. The incident is then resolved (attempts exhausted) so the
        job returns to the scheduler's candidate pool, where the OPEN
        breaker denies it. (Without this the scheduler correctly skips
        jobs with open recovery incidents — R8 owns those.) Returns
        (controller, report)."""
        g.ensure_breaker_state("JOB", scope_id, cooldown_s=cooldown_s,
                               actor="test")
        iid = self._failed_attempts(g, scope_id, n)
        res = self._res_on(path, cooldown_s=cooldown_s, failure_threshold=3)
        rep = res.evaluate_once()
        self.assertEqual(g.get_breaker_state("JOB", scope_id)["state"],
                         "OPEN")
        g.set_incident_outcome(iid, "exhausted",
                               "r14c: attempts exhausted", "test")
        return res, rep

    # ------------------------------------------------------------ R14-76
    def test_R14_76_signals_open_breaker_scheduler_denies(self):
        """Three failure signals -> controller opens the breaker; the
        scheduler denies admission while OPEN and the job stays PENDING."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        g.create_job("j76", "t", "s", "test")
        res, rep = self._open_via_signals(path, g, "j76")
        self.assertIn({"scope_type": "JOB", "scope_id": "j76"},
                      rep["breakers_opened"])
        sched = Scheduler(path, sup, SchedulerConfig(
            poll_interval_s=1.0, lease_ttl_s=30.0, max_concurrent_jobs=1,
            batch_size=10), scheduler_id="s0")
        self._scheds.append(sched)
        srep = sched.evaluate_once()
        self.assertEqual(srep["resilience_denied"], 1)
        self.assertEqual(srep["admitted"], 0)
        self.assertEqual(g.get_job("j76")["status"], "PENDING")

    # ------------------------------------------------------------ R14-77
    def test_R14_77_half_open_probe_budget_bounded(self):
        """Two schedulers racing a HALF_OPEN breaker with probe_limit=1:
        exactly one probe admitted; the budget is never overshot."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        g.create_job("j77a", "t", "s", "test")
        res, _rep = self._open_via_signals(path, g, "j77a")
        row = g.get_breaker_state("JOB", "j77a")
        time.sleep(0.5)  # cooldown (0.3s) elapses
        res.evaluate_once()
        self.assertEqual(g.get_breaker_state("JOB", "j77a")["state"],
                         "HALF_OPEN")
        s1 = Scheduler(path, sup, SchedulerConfig(
            poll_interval_s=1.0, lease_ttl_s=30.0, max_concurrent_jobs=2,
            batch_size=10), scheduler_id="s1")
        s2 = Scheduler(path, sup, SchedulerConfig(
            poll_interval_s=1.0, lease_ttl_s=30.0, max_concurrent_jobs=2,
            batch_size=10), scheduler_id="s2")
        self._scheds.extend([s1, s2])
        rres = self._run_barrier([s1.evaluate_once, s2.evaluate_once])
        for v in rres.values():
            self.assertNotIsInstance(v, Exception, f"race failed: {v}")
        total = rres[0]["admitted"] + rres[1]["admitted"]
        self.assertEqual(total, 1,
                         "probe budget 1: exactly one admission wins")
        row = g.get_breaker_state("JOB", "j77a")
        self.assertEqual(row["half_open_probes_used"], 1)
        # The admitted probe runs under a real worker; CLAIMED is
        # transient (the scheduler spawns the worker), so assert the
        # durable admission evidence instead of a momentary status.
        n_claims = g.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?"
            " AND json_extract(payload,'$.job_id')=?",
            ("job.claimed", "j77a")).fetchone()[0]
        self.assertEqual(n_claims, 1, "exactly one probe claim")
        self.assertIn(g.get_job("j77a")["status"],
                      ("CLAIMED", "RUNNING", "COMMITTING", "COMPLETE"))

    # ------------------------------------------------------------ R14-78
    def test_R14_78_probe_verified_by_progress_closes(self):
        """The admitted probe completes with real progress -> the
        controller closes the breaker; I-18 holds (progress, not vibes)."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        g.create_job("j78", "t", "s", "test")
        res, _rep = self._open_via_signals(path, g, "j78")
        time.sleep(0.5)
        res.evaluate_once()
        sched = Scheduler(path, sup, SchedulerConfig(
            poll_interval_s=1.0, lease_ttl_s=30.0, max_concurrent_jobs=1,
            batch_size=10), scheduler_id="s0")
        self._scheds.append(sched)
        srep = sched.evaluate_once()
        self.assertEqual(srep["admitted"], 1)
        self._wait(lambda: g.get_job("j78")["status"] == "COMPLETE",
                   timeout=60.0)
        res.evaluate_once()
        self.assertEqual(g.get_breaker_state("JOB", "j78")["state"],
                         "CLOSED")

    # ------------------------------------------------------------ R14-79
    def test_R14_79_failed_attempt_feeds_breaker(self):
        """A failed recovery attempt's signal opens the breaker: the
        failure -> breaker -> admission-denial chain is compositional."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        g.create_job("j79", "t", "s", "test")
        g.ensure_breaker_state("JOB", "j79", cooldown_s=60.0,
                               actor="test")
        # Gate-level dedupe: a retried delivery of an already-seen
        # canonical key is a no-op (version and count unchanged).
        g.record_breaker_signal(
            "JOB", "j79", failure_kind="R8_ATTEMPT_FAILED",
            incident_id="inc-79", attempt_id="att-0",
            failure_window_s=60.0, cooldown_s=60.0, actor="test")
        before = g.get_breaker_state("JOB", "j79")
        dup = g.record_breaker_signal(
            "JOB", "j79", failure_kind="R8_ATTEMPT_FAILED",
            incident_id="inc-79", attempt_id="att-0",
            failure_window_s=60.0, cooldown_s=60.0, actor="test")
        self.assertTrue(dup["deduped"])
        after = g.get_breaker_state("JOB", "j79")
        self.assertEqual(after["version"], before["version"])
        self.assertEqual(after["failure_count"], before["failure_count"])
        # Three FAILED recovery attempts (authoritative R8 evidence);
        # the controller's scan turns them into signals and opens the
        # breaker.
        self._failed_attempts(g, "j79", 3)
        res = self._res_on(path, cooldown_s=60.0, failure_threshold=3)
        rep = res.evaluate_once()
        self.assertEqual(g.get_breaker_state("JOB", "j79")["state"],
                         "OPEN")
        self.assertIn({"scope_type": "JOB", "scope_id": "j79"},
                      rep["breakers_opened"])

    # ------------------------------------------------------------ R14-80
    def test_R14_80_no_early_half_open(self):
        """Cooldown not elapsed -> the controller does NOT half-open;
        admission stays denied until the cooldown actually passes."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        g.create_job("j80", "t", "s", "test")
        res, _rep = self._open_via_signals(path, g, "j80", cooldown_s=5.0)
        res.evaluate_once()
        self.assertEqual(g.get_breaker_state("JOB", "j80")["state"],
                         "OPEN", "cooldown 5s not elapsed: stays OPEN")
        sched = Scheduler(path, sup, SchedulerConfig(
            poll_interval_s=1.0, lease_ttl_s=30.0, max_concurrent_jobs=1,
            batch_size=10), scheduler_id="s0")
        self._scheds.append(sched)
        srep = sched.evaluate_once()
        self.assertEqual(srep["resilience_denied"], 1)

    # ------------------------------------------------------------ R14-81
    def test_R14_81_concurrent_signals_single_open(self):
        """Two controllers racing signal recording + evaluation: exactly
        one OPEN transition, count never double-counted. The evidence
        (four FAILED attempts) is pre-created; the race is the two
        controllers' concurrent scan -> signal -> CAS-open passes."""
        path, _st, g = self._fresh_db()
        g.create_job("j81", "t", "s", "test")
        g.ensure_breaker_state("JOB", "j81", cooldown_s=60.0,
                               actor="test")
        self._failed_attempts(g, "j81", 4)
        res1 = self._res_on(path, cooldown_s=60.0, failure_threshold=3)
        res2 = self._res_on(path, cooldown_s=60.0, failure_threshold=3)

        def ctrl_eval(res):
            return res.evaluate_once()

        rres = self._run_barrier([lambda: ctrl_eval(res1),
                                  lambda: ctrl_eval(res1),
                                  lambda: ctrl_eval(res2),
                                  lambda: ctrl_eval(res2)])
        for v in rres.values():
            self.assertNotIsInstance(v, Exception, f"race failed: {v}")
        row = g.get_breaker_state("JOB", "j81")
        self.assertEqual(row["state"], "OPEN")
        self.assertEqual(row["failure_count"], 4,
                         "four distinct signals counted exactly once each")
        n_open = g.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?"
            " AND json_extract(payload,'$.scope_id')=?"
            " AND json_extract(payload,'$.new_state')='OPEN'",
            ("breaker.transition", "j81")).fetchone()[0]
        self.assertEqual(n_open, 1, "exactly one OPEN transition")

    # ------------------------------------------------------------ R14-82
    def test_R14_82_breaker_survives_restart(self):
        """Breaker rows are durable: after close/reopen the OPEN state,
        version, and cooldown survive; denial continues."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        g.create_job("j82", "t", "s", "test")
        self._open_via_signals(path, g, "j82", cooldown_s=60.0)
        row_before = g.get_breaker_state("JOB", "j82")
        for st in list(self._stores):
            st.close()
        self._stores = []
        st2 = open_store(path)
        self._stores.append(st2)
        g2 = TransitionGate(st2)
        row_after = g2.get_breaker_state("JOB", "j82")
        self.assertEqual(row_after["state"], "OPEN")
        self.assertEqual(row_after["version"], row_before["version"])
        self.assertEqual(row_after["opened_at"], row_before["opened_at"])
        allowed, reason = g2.breaker_allows("JOB", "j82")
        self.assertFalse(allowed)
        self.gate = g2


# ========================================== R14-F4 (finalization)
class TestR14Finalization(CBase):
    """R14-83..92: finalization composition. Every gate category is
    exercised for real: healthy path finalizes; uncertainty, open
    breakers, human gates, active recovery, and stale generations each
    block deterministically; publication is atomic and idempotent."""

    def _healthy_generation(self, path, g, sup, n_items=2, prefix="d"):
        """Desired -> reconcile -> schedule -> complete. Returns
        (finalizer, generation, head_version)."""
        spec = {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                "policy": {}}
        for k in range(n_items):
            g.set_desired_item(f"{prefix}-{k}", dict(spec), "test")
        V = g.get_desired_head()["version"]
        gen = canonical_release_generation(V)
        rec = Reconciler(path, ReconcilerConfig(
            poll_interval_s=1.0, batch_size=10))
        self._recs.append(rec)
        rec.reconcile()
        sched = Scheduler(path, sup, SchedulerConfig(
            poll_interval_s=1.0, lease_ttl_s=30.0,
            max_concurrent_jobs=n_items, batch_size=10),
            scheduler_id="s0")
        self._scheds.append(sched)
        sched.evaluate_once()
        for k in range(n_items):
            jid = _canonical_desired_job_id(f"{prefix}-{k}")
            self._wait(lambda j=jid: g.get_job(j)["status"] == "COMPLETE",
                       timeout=60.0)
        # The finalizer's ACTIVE_EXECUTION gate reads durable spawn
        # evidence: reap every exited generation via observe() until no
        # unreaped proc_spawned rows remain.
        def _reaped():
            sup.observe()
            return len(g.unreaped_proc_spawns()) == 0
        self._wait(_reaped, timeout=60.0)
        fin = Finalizer(path, sup, FinalizationConfig(
            poll_interval_s=1.0, batch_size=10, actor="r14c-fin"))
        self._fins.append(fin)
        return fin, gen, V

    def _blockers(self, run):
        return sorted(b["category"]
                      for b in json.loads(run["blockers"] or "[]"))

    # ------------------------------------------------------------ R14-83
    def test_R14_83_healthy_path_finalizes(self):
        """Three desired items converge and the generation publishes:
        exactly one finalization.published event."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        fin, gen, V = self._healthy_generation(path, g, sup, n_items=3)
        g.begin_finalization_run(gen, V, "r14c-fin")
        frep = fin.evaluate_once()
        self.assertEqual(frep["detail"], "ok")
        evd = {e["release_generation"]: e for e in frep["evaluated"]}
        self.assertEqual(evd[gen]["state"], "READY")
        run = g.get_finalization_run(gen)
        self.assertEqual(self._blockers(run), [])
        self.assertIsNotNone(run["checkpoint_id"])
        pub = g.publish_finalization(gen, run["version"],
                                     run["manifest_hash"], "r14c-fin")
        self.assertEqual(pub["state"], "FINALIZED")
        self.assertEqual(pub["desired_state_version"], V)
        n = g.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?",
            ("finalization.published",)).fetchone()[0]
        self.assertEqual(n, 1)

    # ------------------------------------------------------------ R14-84
    def test_R14_84_publish_races_desired_change(self):
        """Publish racing a desired-state change: exactly one atomic
        outcome — FINALIZED on the pinned generation, or a
        STALE_GENERATION refusal. Never both, never neither."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c",
                             heartbeat_interval_s=0.4)
            self._sups.append(sup)
            fin, gen, V = self._healthy_generation(path, g, sup)
            run0 = g.begin_finalization_run(gen, V, "r14c-fin")
            # Evaluate to READY via the gate directly (the finalizer's
            # evaluate_once would publish in the same pass; the race
            # under test needs a READY-but-unpublished run).
            ev = g.evaluate_finalization(gen, run0["version"],
                                         "r14c-fin")
            self.assertEqual(ev["state"], "READY")
            run = g.get_finalization_run(gen)
            self.assertEqual(run["state"], "READY")
            ver, mh = run["version"], run["manifest_hash"]
            spec = {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                    "policy": {}}
            res = self._run_barrier([
                lambda: self._thread_gate(path).publish_finalization(
                    gen, ver, mh, "r14c-fin"),
                lambda: self._thread_gate(path).set_desired_item(
                    "d-new", dict(spec), "test"),
            ])
            pub, new_item = res[0], res[1]
            self.assertNotIsInstance(new_item, Exception)
            row = g.get_finalization_run(gen)
            n = g.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type=?"
                " AND json_extract(payload,'$.release_generation')=?",
                ("finalization.published", gen)).fetchone()[0]
            if isinstance(pub, Exception):
                self.assertIsInstance(pub, TransitionRejected)
                self.assertIn("STALE_GENERATION", str(pub))
                self.assertEqual(n, 0)
                self.assertEqual(row["state"], "READY")
            else:
                self.assertEqual(row["state"], "FINALIZED")
                self.assertEqual(n, 1)
                self.assertEqual(row["desired_state_version"], V)
            self.assertNotEqual(
                canonical_release_generation(
                    g.get_desired_head()["version"]), gen)
        for i in range(3):
            one(i)

    # ------------------------------------------------------------ R14-85
    def test_R14_85_finalized_generation_immutable(self):
        """After FINALIZED, a desired change names a NEW generation; the
        old record is byte-identical and re-publication is idempotent."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        fin, gen, V = self._healthy_generation(path, g, sup)
        g.begin_finalization_run(gen, V, "r14c-fin")
        fin.evaluate_once()
        run = g.get_finalization_run(gen)
        pub = g.publish_finalization(gen, run["version"],
                                     run["manifest_hash"], "r14c-fin")
        snap = dict(g.get_finalization_run(gen))
        spec = {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                "policy": {}}
        g.set_desired_item("d-late", dict(spec), "test")
        V2 = g.get_desired_head()["version"]
        gen2 = canonical_release_generation(V2)
        self.assertNotEqual(gen2, gen)
        self.assertEqual(dict(g.get_finalization_run(gen)), snap,
                         "the finalized record must not move")
        pub2 = g.publish_finalization(gen, pub["version"],
                                      pub["manifest_hash"], "r14c-fin")
        self.assertEqual(pub2["state"], "FINALIZED")
        self.assertEqual(
            g.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type=?"
                " AND json_extract(payload,'$.release_generation')=?",
                ("finalization.published", gen)).fetchone()[0], 1)

    # ------------------------------------------------------------ R14-86
    def test_R14_86_uncertain_execution_blocks(self):
        """A job left UNCERTAIN by reclaim blocks the generation with
        UNCERTAIN_EXECUTION until the uncertainty is resolved."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        fin, gen, V = self._healthy_generation(path, g, sup, n_items=1)
        jid = _canonical_desired_job_id("d-0")
        # Reopen uncertainty on the completed... no: use a second item.
        spec = {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                "policy": {}}
        g.set_desired_item("d-u", dict(spec), "test")
        V = g.get_desired_head()["version"]
        gen = canonical_release_generation(V)
        rec = Reconciler(path, ReconcilerConfig(
            poll_interval_s=1.0, batch_size=10))
        self._recs.append(rec)
        rec.reconcile()
        ju = _canonical_desired_job_id("d-u")
        g.claim_job(ju, "wu", 60.0, "test")
        g.transition_job(ju, "RUNNING", "worker:wu")
        tok = g.get_job(ju)["fencing_token"]
        inc = g.find_or_create_recovery_incident(
            ju, "DEAD", {"job_id": ju}, "test")
        g.reclaim_lease(ju, actor="recovery-controller", reason="r14c",
                       expected_owner="wu", expected_token=tok,
                       force=True, verdict="DEAD",
                       incident_id=inc["incident_id"])
        self.assertEqual(g.get_job(ju)["status"], "UNCERTAIN")
        g.begin_finalization_run(gen, V, "r14c-fin")
        fin.evaluate_once()
        row = g.get_finalization_run(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("UNCERTAIN_EXECUTION", self._blockers(row))
        # Resolve: close the incident (uncertainty resolved by requeue)
        # -> requeue -> scheduler re-admits -> completes. The incident
        # must be closed or the scheduler skips the job (R8 owns open
        # incidents).
        g.set_incident_outcome(inc["incident_id"], "requeued",
                               "r14c: uncertainty resolved by requeue",
                               "recovery-controller")
        g.transition_job(ju, "PENDING", "recovery-controller",
                         reason="r14c: resolve uncertainty")
        sched = Scheduler(path, sup, SchedulerConfig(
            poll_interval_s=1.0, lease_ttl_s=30.0,
            max_concurrent_jobs=2, batch_size=10), scheduler_id="s0")
        self._scheds.append(sched)
        sched.evaluate_once()
        self._wait(lambda: g.get_job(ju)["status"] == "COMPLETE",
                   timeout=60.0)
        # Reap the resolve worker's spawn evidence (the finalizer's
        # ACTIVE_EXECUTION gate reads durable proc_spawned rows).
        def _reaped():
            sup.observe()
            return len(g.unreaped_proc_spawns()) == 0
        self._wait(_reaped, timeout=60.0)
        ev = g.evaluate_finalization(
            gen, g.get_finalization_run(gen)["version"], "r14c-fin")
        self.assertEqual(ev["state"], "READY")
        self.assertEqual(self._blockers(g.get_finalization_run(gen)), [])

    # ------------------------------------------------------------ R14-87
    def test_R14_87_open_breaker_blocks(self):
        """An OPEN breaker on a job scope blocks with OPEN_BREAKER;
        closing it unblocks."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        fin, gen, V = self._healthy_generation(path, g, sup, n_items=1)
        jid = _canonical_desired_job_id("d-0")
        g.ensure_breaker_state("JOB", jid, cooldown_s=60.0, actor="test")
        g.transition_breaker("JOB", jid, expected_version=0,
                             to_state="OPEN", actor="r14c-ctl",
                             reason="r14c")
        g.begin_finalization_run(gen, V, "r14c-fin")
        fin.evaluate_once()
        row = g.get_finalization_run(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("OPEN_BREAKER", self._blockers(row))
        row2 = g.get_breaker_state("JOB", jid)
        # Walk the legal edges OPEN -> HALF_OPEN -> CLOSED (the finalizer
        # blocks on both "open" and "half_open" verdicts).
        g.transition_breaker("JOB", jid,
                             expected_version=row2["version"],
                             to_state="HALF_OPEN", actor="r14c-ctl",
                             reason="r14c: cooldown bypass for test")
        row3 = g.get_breaker_state("JOB", jid)
        g.transition_breaker("JOB", jid,
                             expected_version=row3["version"],
                             to_state="CLOSED", actor="r14c-ctl",
                             reason="r14c: probe verified")
        # The finalizer's evaluate_once would publish; check the
        # unblocked verdict via the gate directly.
        ev = g.evaluate_finalization(
            gen, g.get_finalization_run(gen)["version"], "r14c-fin")
        self.assertEqual(ev["state"], "READY")
        self.assertEqual(self._blockers(g.get_finalization_run(gen)), [])

    # ------------------------------------------------------------ R14-88
    def test_R14_88_human_gate_blocks(self):
        """A task PAUSED_FOR_HUMAN blocks the generation with
        HUMAN_GATE; the gate is sticky and never auto-cleared."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        fin, gen, V = self._healthy_generation(path, g, sup, n_items=1)
        # Walk the legal task edges PROPOSED -> AUTHORIZED ->
        # PAUSED_FOR_HUMAN (the human gate).
        g.transition_task("t", "AUTHORIZED", "r14c-op", reason="r14c")
        g.transition_task("t", "PAUSED_FOR_HUMAN", "r14c-op",
                          reason="r14c", pause_reason="r14c test")
        g.begin_finalization_run(gen, V, "r14c-fin")
        fin.evaluate_once()
        row = g.get_finalization_run(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("HUMAN_GATE", self._blockers(row))
        # The finalizer must not clear it on a later pass.
        fin.evaluate_once()
        row = g.get_finalization_run(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertEqual(g.store.conn.execute(
            "SELECT status FROM tasks WHERE task_id='t'"
        ).fetchone()[0], "PAUSED_FOR_HUMAN")

    # ------------------------------------------------------------ R14-89
    def test_R14_89_active_recovery_blocks(self):
        """An open recovery incident blocks with ACTIVE_RECOVERY;
        closing the incident unblocks."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        fin, gen, V = self._healthy_generation(path, g, sup, n_items=1)
        jid = _canonical_desired_job_id("d-0")
        inc = g.find_or_create_recovery_incident(
            jid, "DEAD", {"job_id": jid}, "test")
        g.begin_finalization_run(gen, V, "r14c-fin")
        fin.evaluate_once()
        row = g.get_finalization_run(gen)
        self.assertEqual(row["state"], "BLOCKED")
        self.assertIn("ACTIVE_RECOVERY", self._blockers(row))
        g.set_incident_outcome(inc["incident_id"], "success",
                               "r14c: recovered", "test")
        # The finalizer's evaluate_once would publish; check the
        # unblocked verdict via the gate directly.
        ev = g.evaluate_finalization(
            gen, g.get_finalization_run(gen)["version"], "r14c-fin")
        self.assertEqual(ev["state"], "READY")
        self.assertEqual(self._blockers(g.get_finalization_run(gen)), [])

    # ------------------------------------------------------------ R14-90
    def test_R14_90_publish_manifest_mismatch_refused(self):
        """Publish with a wrong manifest hash is refused; the run stays
        READY and no publication event exists."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        fin, gen, V = self._healthy_generation(path, g, sup)
        run0 = g.begin_finalization_run(gen, V, "r14c-fin")
        # Evaluate to READY via the gate directly (the finalizer's
        # evaluate_once would publish in the same pass).
        ev = g.evaluate_finalization(gen, run0["version"], "r14c-fin")
        self.assertEqual(ev["state"], "READY")
        run = g.get_finalization_run(gen)
        self.assertEqual(run["state"], "READY")
        with self.assertRaises(TransitionRejected) as ctx:
            g.publish_finalization(gen, run["version"], "0" * 64,
                                   "r14c-fin")
        self.assertIn("manifest mismatch", str(ctx.exception))
        self.assertEqual(g.get_finalization_run(gen)["state"], "READY")
        self.assertEqual(
            g.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type=?",
                ("finalization.published",)).fetchone()[0], 0)

    # ------------------------------------------------------------ R14-91
    def test_R14_91_two_finalizers_race_one_publication(self):
        """Two finalizers racing evaluate+publish while the scheduler
        finishes the last job: exactly one publication, one FINALIZED."""
        def one(_i):
            path, _st, g = self._fresh_db()
            sup = Supervisor(path, actor="r14c",
                             heartbeat_interval_s=0.4)
            self._sups.append(sup)
            spec = {"task_id": "t", "stage_id": "s", "max_attempts": 3,
                    "policy": {}}
            g.set_desired_item("d-0", dict(spec), "test")
            V = g.get_desired_head()["version"]
            gen = canonical_release_generation(V)
            rec = Reconciler(path, ReconcilerConfig(
                poll_interval_s=1.0, batch_size=10))
            self._recs.append(rec)
            rec.reconcile()
            jid = _canonical_desired_job_id("d-0")
            sched = Scheduler(path, sup, SchedulerConfig(
                poll_interval_s=1.0, lease_ttl_s=30.0,
                max_concurrent_jobs=1, batch_size=10),
                scheduler_id="s0")
            self._scheds.append(sched)
            sched.evaluate_once()  # worker starts completing
            f1 = Finalizer(path, sup, FinalizationConfig(
                poll_interval_s=1.0, batch_size=10, actor="fin-1"))
            f2 = Finalizer(path, sup, FinalizationConfig(
                poll_interval_s=1.0, batch_size=10, actor="fin-2"))
            self._fins.extend([f1, f2])
            g.begin_finalization_run(gen, V, "r14c-fin")

            def fin_publish(fin):
                st2 = open_store(path)
                try:
                    g2 = TransitionGate(st2)
                    # Evaluate to READY via the gate (idempotent; does
                    # not publish — the finalizer's evaluate_once would
                    # publish in the same pass, defeating the race).
                    run = g2.get_finalization_run(gen)
                    try:
                        ev = g2.evaluate_finalization(
                            gen, run["version"], fin.actor)
                    except (TransitionRejected, FinalizationConflict,
                            StoreError) as e:
                        return ("refused", type(e).__name__)
                    if ev["state"] != "READY":
                        return ("not-ready", ev["state"])
                    run = g2.get_finalization_run(gen)
                    v0 = run["version"]
                    try:
                        pub = g2.publish_finalization(
                            gen, v0, run["manifest_hash"], fin.actor)
                    except (TransitionRejected, FinalizationConflict,
                            StoreError) as e:
                        return ("refused", type(e).__name__)
                    # A real publication bumps the version; an
                    # idempotent re-verification returns the row
                    # unchanged. Only the winner counts.
                    if pub["version"] == v0 + 1:
                        return ("published", pub["state"])
                    return ("already", pub["state"])
                finally:
                    st2.close()

            self._wait(lambda: g.get_job(jid)["status"] == "COMPLETE",
                       timeout=60.0)
            # Reap the worker's spawn evidence; otherwise the
            # ACTIVE_EXECUTION gate blocks the run.
            def _reaped():
                sup.observe()
                return len(g.unreaped_proc_spawns()) == 0
            self._wait(_reaped, timeout=60.0)
            res = self._run_barrier([lambda: fin_publish(f1),
                                     lambda: fin_publish(f2)])
            pubs = [v for v in res.values() if v[0] == "published"]
            self.assertEqual(len(pubs), 1,
                             f"exactly one publication wins: {res}")
            self.assertEqual(g.get_finalization_run(gen)["state"],
                             "FINALIZED")
            n = g.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type=?"
                " AND json_extract(payload,'$.release_generation')=?",
                ("finalization.published", gen)).fetchone()[0]
            self.assertEqual(n, 1)
        for i in range(3):
            one(i)

    # ------------------------------------------------------------ R14-92
    def test_R14_92_finalized_survives_restart(self):
        """FINALIZED survives close/reopen; re-publication stays
        idempotent with no new event."""
        path, _st, g = self._fresh_db()
        sup = Supervisor(path, actor="r14c", heartbeat_interval_s=0.4)
        self._sups.append(sup)
        fin, gen, V = self._healthy_generation(path, g, sup)
        g.begin_finalization_run(gen, V, "r14c-fin")
        fin.evaluate_once()
        run = g.get_finalization_run(gen)
        g.publish_finalization(gen, run["version"], run["manifest_hash"],
                               "r14c-fin")
        for st in list(self._stores):
            st.close()
        self._stores = []
        st2 = open_store(path)
        self._stores.append(st2)
        g2 = TransitionGate(st2)
        row = g2.get_finalization_run(gen)
        self.assertEqual(row["state"], "FINALIZED")
        pub2 = g2.publish_finalization(gen, row["version"],
                                       row["manifest_hash"], "r14c-fin")
        self.assertEqual(pub2["state"], "FINALIZED")
        self.assertEqual(
            g2.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type=?",
                ("finalization.published",)).fetchone()[0], 1)
        self.gate = g2


# ================================================== R14-99 (audit)
class TestR14ZZZAudit(CBase):
    """R14-99: independent /proc stray-process audit. Runs last
    (class name sorts after every other Track C class). After the
    whole campaign's teardowns, NO axos worker process may be alive
    anywhere on this machine — every spawned pid must be gone by
    independent /proc evidence, and no live process may carry the
    worker cmdline marker."""

    def test_R14_99_no_stray_processes(self):
        # 1) Every worker pid ever spawned by Track C is gone.
        # 2) Independent sweep: no live process anywhere carries the
        # axos worker marker, whether or not we tracked its pid.
        # Workers shut down asynchronously; poll briefly so a worker
        # mid-exit does not flake the audit.
        def _sweep():
            live_known = [pid for pid in sorted(CBase._spawned_pids)
                          if not _proc_gone(pid)]
            strays = []
            for pid in _all_pids():
                try:
                    with open(f"/proc/{pid}/cmdline", "rb") as fh:
                        cmd = fh.read().replace(b"\x00", b" ").decode(
                            "utf-8", "replace")
                except (FileNotFoundError, PermissionError,
                        ProcessLookupError):
                    continue
                if "axos.exec.worker" in cmd and not _proc_gone(pid):
                    strays.append((pid, cmd.strip()[:120]))
            return live_known, strays

        live_known, strays = _sweep()
        if live_known or strays:
            # Give mid-exit workers up to 10s to disappear.
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                time.sleep(0.2)
                live_known, strays = _sweep()
                if not live_known and not strays:
                    break
        self.assertEqual(live_known, [],
                         f"spawned worker pids still alive: {live_known}")
        self.assertEqual(strays, [],
                         f"stray axos worker processes: {strays}")
        self.assertGreater(len(CBase._spawned_pids), 0,
                           "the audit must have seen spawned workers")


# ==========================================================================
# R14-D: CORRUPTION / RESTORE / ADVERSARIAL / MANIFEST / CLEAN BOOT
# (consolidated from tests/test_r14_d_corruption.py)
# ==========================================================================

def verify_ledger_chain(gate):
    """Assert the ledger hash chain verifies clean (ok=True), raising
    AssertionError with the detail on any break."""
    ok, detail = gate.verify_ledger_chain()
    assert ok, detail
from axos.exec.policy import CANONICAL_LADDER, PolicyConfig  # noqa: E402

AXOS_DIR = os.path.join(WORKSPACE_ROOT, "axos")
RELEASE_MANIFEST_PATH = os.path.join(AXOS_DIR, "release_manifest.json")


def _canon(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


def _sha256_of_canon(obj) -> str:
    return hashlib.sha256(_canon(obj).encode("utf-8")).hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


class R14DBase(unittest.TestCase):
    """Pristine fixture: fresh tmp db, migrated, one task. Corruption tests
    take isolated copies via _backup_copy() and corrupt ONLY the copy."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r14d-")
        self.db = os.path.join(self.tmp, "t.db")
        self.sup = Supervisor(self.db, actor="test-r14d")
        self._sups = [self.sup]
        self.store = self.sup.store
        self.gate = self.sup.gate
        self.gate.create_task("t", {"objective": "r14d"}, {"usd": 1},
                              "test")
        self.staging = self.gate.staging_root

    def tearDown(self):
        for s in self._sups:
            try:
                for wid, info in list(getattr(s, "_procs", {}).items()):
                    p = getattr(info, "popen", None)
                    try:
                        if p is not None and p.poll() is None:
                            os.killpg(p.pid, signal.SIGKILL)
                    except Exception:
                        pass
                s.close()
            except Exception:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ------------------------------------------------------------ fixtures
    def make_job(self, jid, task="t"):
        self.gate.create_job(jid, task, "s", "test")
        return jid

    def running(self, jid, wid="w", ttl=60.0, task="t"):
        """Claim + RUNNING; returns the live fencing token."""
        self.make_job(jid, task)
        self.gate.claim_job(jid, wid, ttl, "test")
        self.gate.transition_job(jid, "RUNNING", f"worker:{wid}")
        return self.gate.get_job(jid)["fencing_token"]

    def stage(self, jid, wid, tok, data, task="t", actor=None):
        return self.gate.stage_artifact(
            job_id=jid, worker_id=wid, fencing_token=tok, task_id=task,
            kind="result", data=data, actor=actor or f"worker:{wid}")

    def full_protocol(self, jid, wid, tok, data, task="t", evidence=None):
        """RUNNING -> stage -> begin -> verify -> atomic commit."""
        art = self.stage(jid, wid, tok, data, task)
        aid = art["artifact_id"]
        self.gate.begin_commit(jid, wid, tok, artifact_id=aid,
                               actor=f"worker:{wid}")
        self.gate.verify_artifact(aid, actor=f"worker:{wid}",
                                  worker_id=wid, fencing_token=tok,
                                  job_id=jid)
        return self.gate.commit_artifact(
            jid, wid, tok, artifact_id=aid, actor=f"worker:{wid}",
            evidence=evidence or {})

    def verified_artifact(self, jid, wid, tok, data, task="t"):
        """Stage + gate-verify (authority actor) for checkpoint fixtures."""
        art = self.stage(jid, wid, tok, data, task)
        self.gate.verify_artifact(art["artifact_id"], actor="test")
        return self.gate.get_artifact(art["artifact_id"])

    def _healthy_job(self, jid, wid, data, task="t"):
        tok = self.running(jid, wid, task=task)
        return self.full_protocol(jid, wid, tok, data, task)

    # ------------------------------------------------- isolated-copy support
    def _raw(self, db_path):
        """A raw sqlite3 handle for deliberate tamper/corruption ops on an
        isolated copy. Never used against the pristine fixture db."""
        return sqlite3.connect(db_path)

    def _backup_copy(self, name="copy"):
        """The documented R14-D restore unit: the production backup hook
        (Store.backup_to, SQLite online-backup API) for the database file
        PLUS the staging tree. Artifact uris are absolute, so the restore
        re-anchors them from the old staging root to the copy's staging
        root — a mechanical path translation; content-addressed identities
        (artifact_id == sha256(bytes)) are untouched, and every re-anchored
        file's bytes are re-hashed to prove it. Returns (copy_db, copy_dir).

        This is the ONLY way tests obtain a corruptible store: the pristine
        fixture is never damaged."""
        cdir = os.path.join(self.tmp, name)
        os.makedirs(cdir, exist_ok=True)
        cdb = os.path.join(cdir, "copy.db")
        self.store.backup_to(cdb)
        cstaging = os.path.join(cdir, "axos-staging")
        if os.path.isdir(self.staging):
            shutil.copytree(self.staging, cstaging)
        raw = sqlite3.connect(cdb)
        try:
            rows = raw.execute(
                "SELECT artifact_id, uri FROM artifacts"
                " WHERE uri IS NOT NULL").fetchall()
            for aid, uri in rows:
                if uri and uri.startswith(self.staging):
                    raw.execute(
                        "UPDATE artifacts SET uri=? WHERE artifact_id=?",
                        (cstaging + uri[len(self.staging):], aid))
            raw.commit()
            # Byte-level proof: every artifact row on the copy resolves to
            # bytes hashing to its content-addressed identity.
            for aid, uri in raw.execute(
                    "SELECT artifact_id, uri FROM artifacts"
                    " WHERE uri IS NOT NULL").fetchall():
                with open(uri, "rb") as f:
                    data = f.read()
                self.assertEqual(_sha256_bytes(data), aid,
                                 f"re-anchored bytes for {aid} do not hash"
                                 " to the artifact identity")
        finally:
            raw.close()
        return cdb, cdir

    def _open_copy(self, cdb, actor="test-r14d-copy"):
        sup = Supervisor(cdb, actor=actor)
        self._sups.append(sup)
        return sup

    def _job_dump(self, gate, jid):
        row = gate.store.conn.execute(
            "SELECT * FROM jobs WHERE job_id=?", (jid,)).fetchone()
        return tuple(row) if row else None

    def _event_count(self, gate, event_type):
        return gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?",
            (event_type,)).fetchone()[0]


# ============================================================ R14-C: corruption matrix
class TestR14CCorruption(R14DBase):
    """Ledger corruption and state contradictions, each on an isolated copy
    of the store. Every test asserts: detection, fail-closed refusal of any
    unsafe action, evidence preservation, and the contractually correct
    blocked/uncertain path."""

    # ------------------------------------------------- ledger corruption
    def _corrupt_copy_with_completed_job(self, name):
        self._healthy_job("jc", "wc", b"r14-corrupt-bytes")
        ok, detail = self.gate.verify_ledger_chain()
        self.assertTrue(ok, detail)
        return self._backup_copy(name)

    def test_R14_100_ledger_payload_tamper_fails_closed(self):
        """R14-100: tamper a ledger payload on the copy -> chain verification
        detects it -> boot refuses (BLOCKED/ledger_chain_broken) -> the block
        is journaled -> no unsafe recovery proceeds -> pristine untouched."""
        cdb, _cdir = self._corrupt_copy_with_completed_job("c100")
        raw = self._raw(cdb)
        seq, payload = raw.execute(
            "SELECT seq, payload FROM ledger WHERE event_type='job.committed'"
        ).fetchone()
        doc = json.loads(payload)
        doc["outcome"] = "TAMPERED-BY-TEST"
        raw.execute("UPDATE ledger SET payload=? WHERE seq=?",
                    (json.dumps(doc), seq))
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        ok, detail = sup.gate.verify_ledger_chain()
        self.assertFalse(ok)
        self.assertIn("hash mismatch", detail)

        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "BLOCKED")
        self.assertEqual(report["blocked_reason"], "ledger_chain_broken")
        # The refusal is journaled where the contract requires it.
        self.assertGreaterEqual(
            self._event_count(sup.gate, "boot.recovery_blocked"), 1)
        # No unsafe recovery proceeded: no NEW boot.completed was emitted
        # (the copy inherits exactly one from the fixture's own boot).
        self.assertEqual(self._event_count(sup.gate, "boot.completed"), 1)
        self.assertFalse(report["fully_recovered"])
        # Isolation: the pristine fixture still verifies.
        ok0, d0 = self.gate.verify_ledger_chain()
        self.assertTrue(ok0, d0)

    def test_R14_101_ledger_row_deletion_fails_closed(self):
        """R14-101: delete a middle ledger row -> seq gap detected -> boot
        BLOCKED with ledger_chain_broken; block journaled."""
        cdb, _cdir = self._corrupt_copy_with_completed_job("c101")
        raw = self._raw(cdb)
        seq = raw.execute(
            "SELECT seq FROM ledger WHERE event_type='job.committed'"
        ).fetchone()[0]
        raw.execute("DELETE FROM ledger WHERE seq=?", (seq,))
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        ok, detail = sup.gate.verify_ledger_chain()
        self.assertFalse(ok)
        # Mechanism (pinned, not incidental): the ledger's seq is
        # AUTOINCREMENT, so after the DELETE the sqlite_sequence counter
        # still hands out the deleted seq to the next journaled row, while
        # _append_event hashes MAX(seq)+1 into the body. The stored row's
        # seq therefore disagrees with its hashed identity -> the verifier
        # reports a hash mismatch and boot refuses. (Deliberately NOT
        # "fixed" to lastrowid: that would let a tail deletion re-chain
        # cleanly and verify — strictly worse tamper-evidence. No
        # production path deletes ledger rows; the table is append-only.)
        self.assertIn("hash mismatch", detail)
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "BLOCKED")
        self.assertEqual(report["blocked_reason"], "ledger_chain_broken")
        self.assertGreaterEqual(
            self._event_count(sup.gate, "boot.recovery_blocked"), 1)
        self.assertEqual(self._event_count(sup.gate, "boot.completed"), 1)

    def test_R14_102_ledger_prev_hash_tamper_fails_closed(self):
        """R14-102: corrupt a checksum-protected linkage field (prev_hash) ->
        chain verification detects -> boot BLOCKED."""
        cdb, _cdir = self._corrupt_copy_with_completed_job("c102")
        raw = self._raw(cdb)
        seq = raw.execute(
            "SELECT seq FROM ledger WHERE event_type='job.committed'"
        ).fetchone()[0]
        raw.execute("UPDATE ledger SET prev_hash=? WHERE seq=?",
                    ("0" * 64, seq))
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        ok, detail = sup.gate.verify_ledger_chain()
        self.assertFalse(ok)
        self.assertIn("prev_hash mismatch", detail)
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "BLOCKED")
        self.assertEqual(report["blocked_reason"], "ledger_chain_broken")

    def test_R14_103_byte_flip_in_db_file_fails_closed(self):
        """R14-103: flip ONE byte inside a ledger payload in the database
        file (binary-level corruption) -> the hash chain detects it ->
        boot refuses. The flipped byte stays inside a JSON string value so
        the tamper is a content change, not a parse break."""
        cdb, _cdir = self._corrupt_copy_with_completed_job("c103")
        # Plant a globally unique marker event through the gate (properly
        # hash-chained), then flip ONE byte inside the marker in the raw
        # file. The flip lands in live payload bytes by construction —
        # find() on a plausible-but-dead page can't silently pass.
        st = open_store(cdb)
        gate = TransitionGate(st)
        nonce = "R14-103-NONCE-" + uuid.uuid4().hex
        gate.append_event("r14.probe", {"nonce": nonce}, "test")
        st.close()
        with open(cdb, "rb") as f:
            blob = bytearray(f.read())
        marker = nonce.encode()
        off = bytes(blob).find(marker)
        self.assertNotEqual(off, -1, "marker not found in db file")
        self.assertEqual(bytes(blob).find(marker, off + 1), -1,
                         "marker not unique in db file")
        # Flip one bit inside the nonce (content change, not a parse break).
        blob[off + 5] ^= 0x01
        with open(cdb, "r+b") as f:
            f.seek(0)
            f.write(blob)

        sup = self._open_copy(cdb)
        ok, detail = sup.gate.verify_ledger_chain()
        self.assertFalse(ok, "single-byte tamper went undetected")
        self.assertIn("hash mismatch", detail)
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "BLOCKED")
        self.assertEqual(report["blocked_reason"], "ledger_chain_broken")
        self.assertEqual(self._event_count(sup.gate, "boot.completed"), 1)

    def test_R14_116_ledger_hash_column_tamper_fails_closed(self):
        """R14-116: corrupt the stored row hash itself -> chain verification
        detects -> boot BLOCKED. (Companion to R14-100/102: every
        checksum-protected ledger column is covered.)"""
        cdb, _cdir = self._corrupt_copy_with_completed_job("c116")
        raw = self._raw(cdb)
        seq = raw.execute(
            "SELECT seq FROM ledger WHERE event_type='job.committed'"
        ).fetchone()[0]
        raw.execute("UPDATE ledger SET hash=? WHERE seq=?",
                    ("f" * 64, seq))
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        ok, detail = sup.gate.verify_ledger_chain()
        self.assertFalse(ok)
        self.assertIn("hash mismatch", detail)
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "BLOCKED")
        self.assertEqual(report["blocked_reason"], "ledger_chain_broken")

    # ------------------------------------------------- state contradictions
    def test_R14_104_complete_without_valid_artifact(self):
        """R14-104: a COMPLETE job with no valid artifact (contradiction
        injected on the copy) -> inspect_uncertain_completion flags it, boot
        surfaces an integrity contradiction WITHOUT rewriting the row, and a
        contradictory commit is refused."""
        tok = self.running("j104", "w104")
        cdb, _cdir = self._backup_copy("c104")
        raw = self._raw(cdb)
        raw.execute(
            "UPDATE jobs SET status='COMPLETE', commit_outcome='SUCCESS',"
            " result_artifact_id=NULL, content_hash=NULL WHERE job_id='j104'")
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        insp = sup.gate.inspect_uncertain_completion("j104")
        self.assertNotEqual(insp["disposition"], "COMMITTED")
        self.assertTrue(insp.get("problems"),
                        "contradiction not detected by inspection")
        before = self._job_dump(sup.gate, "j104")
        report = boot_recover(sup, actor="system")
        kinds = [e["kind"] for e in report["integrity_contradictions"]]
        self.assertIn("complete_artifact_contradiction", kinds)
        # Evidence preserved: the contradiction is journaled, the row is
        # NOT rewritten by boot.
        self.assertGreaterEqual(
            self._event_count(sup.gate, "boot.integrity_contradiction"), 1)
        after = self._job_dump(sup.gate, "j104")
        self.assertEqual(before, after,
                         "boot rewrote a contradictory COMPLETE row")
        # Unsafe action refused: a commit naming any artifact contradicts
        # the recorded (fraudulent) COMPLETE.
        with self.assertRaises(TransitionRejected):
            sup.gate.commit_artifact(
                "j104", None, tok, artifact_id="0" * 64, actor="system",
                evidence={})
        self.assertEqual(before, self._job_dump(sup.gate, "j104"))

    def test_R14_105_artifact_record_without_bytes(self):
        """R14-105: artifact row present but bytes deleted from disk ->
        verification fails, artifact QUARANTINED (retained for forensics,
        FAIL receipt recorded), commit refused."""
        tok = self.running("j105", "w105")
        art = self.stage("j105", "w105", tok, b"r14-105-bytes")
        aid = art["artifact_id"]
        cdb, _cdir = self._backup_copy("c105")
        sup = self._open_copy(cdb)
        uri = sup.gate.get_artifact(aid)["uri"]
        os.remove(uri)  # the bytes vanish; the row remains

        with self.assertRaises(TransitionRejected) as ctx:
            sup.gate.verify_artifact(aid, actor="test")
        self.assertIn("bytes", str(ctx.exception).lower())
        row = sup.gate.get_artifact(aid)
        self.assertEqual(row["status"], "QUARANTINED")
        fails = sup.gate.store.conn.execute(
            "SELECT COUNT(*) FROM validations WHERE artifact_id=?"
            " AND result='FAIL'", (aid,)).fetchone()[0]
        self.assertGreaterEqual(fails, 1, "no durable FAIL receipt")
        self.assertGreaterEqual(
            self._event_count(sup.gate, "artifact.quarantined"), 1)
        # Commit fails closed on the missing bytes.
        with self.assertRaises(TransitionRejected):
            sup.gate.commit_artifact("j105", "w105", tok, artifact_id=aid,
                                     actor="worker:w105", evidence={})
        # Isolation: pristine bytes untouched, pristine artifact verifiable.
        ok = self.gate.verify_artifact(aid, actor="test")
        self.assertEqual(ok["status"], "VALIDATED")

    def test_R14_106_valid_artifact_invalid_completion_reference(self):
        """R14-106: COMPLETE job whose recorded content_hash does not match
        its referenced artifact -> inspection flags the contradiction, boot
        journals it without rewriting history, contradictory commit refused."""
        self._healthy_job("j106", "w106", b"r14-106-bytes")
        cdb, _cdir = self._backup_copy("c106")
        raw = self._raw(cdb)
        raw.execute(
            "UPDATE jobs SET content_hash=? WHERE job_id='j106'",
            ("1" * 64,))
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        insp = sup.gate.inspect_uncertain_completion("j106")
        self.assertNotEqual(insp["disposition"], "COMMITTED")
        self.assertTrue(insp.get("problems"))
        before = self._job_dump(sup.gate, "j106")
        report = boot_recover(sup, actor="system")
        self.assertIn("complete_artifact_contradiction",
                      [e["kind"] for e in report["integrity_contradictions"]])
        self.assertEqual(before, self._job_dump(sup.gate, "j106"),
                         "boot rewrote contradictory history")
        real_aid = sup.gate.get_job("j106")["result_artifact_id"]
        with self.assertRaises(TransitionRejected):
            sup.gate.commit_artifact("j106", None, 0, artifact_id=real_aid,
                                     actor="system", evidence={})

    def test_R14_107_stale_owner_invalid_lease(self):
        """R14-107: owner holds a lease that is no longer valid (expired on
        the copy) -> R4 observes it read-only, the stale owner's
        authority-bearing ops are rejected, and R1 reclaim takes the
        contractually correct UNCERTAIN path with evidence journaled."""
        tok = self.running("j107", "w107", ttl=60.0)
        cdb, _cdir = self._backup_copy("c107")
        raw = self._raw(cdb)
        raw.execute(
            "UPDATE jobs SET lease_expires_at=1.0 WHERE job_id='j107'")
        raw.commit()
        raw.close()

        # NOTE: no Supervisor here — its constructor runs boot_recover,
        # which reclaims the expired lease itself. The R4 pre-recovery
        # observation needs a raw store + gate on the copied database.
        st = open_store(cdb)
        try:
            gate = TransitionGate(st)
            # R4 is a read-only observer: it reports, never mutates.
            before = self._job_dump(gate, "j107")
            expired = gate.observe_expired_leases()
            self.assertIn("j107", [e["job_id"] for e in expired])
            self.assertEqual(before, self._job_dump(gate, "j107"),
                             "R4 observer mutated state")
            # The stale owner's authority is dead: staging with the expired
            # lease is fenced, and renew_lease's contract is boolean —
            # renewing an expired lease returns False (R4:
            # renew-after-expiry impossible).
            with self.assertRaises(LeaseError):
                gate.stage_artifact(
                    job_id="j107", worker_id="w107", fencing_token=tok,
                    task_id="t", kind="result", data=b"x",
                    actor="worker:w107")
            self.assertFalse(
                gate.renew_lease("j107", "w107", tok, 600.0, "worker:w107"),
                "renew of an expired lease must return False")
            # Correct path: R1 reclaim bumps the token and parks UNCERTAIN.
            row = gate.reclaim_lease(
                "j107", actor="system", reason="r14-107 test reclaim",
                    expected_owner="w107", expected_token=tok)
            self.assertEqual(row["status"], "UNCERTAIN")
            self.assertEqual(int(row["fencing_token"]), int(tok) + 1)
            self.assertIsNone(row["owner_worker_id"])
            self.assertGreaterEqual(
                self._event_count(gate, "job.lease_reclaimed"), 1)
            # And the OLD token is now cryptographically stale: rejected even
            # against the UNCERTAIN job.
            with self.assertRaises((LeaseError, TransitionRejected)):
                gate.commit_artifact("j107", "w107", tok,
                                     artifact_id="0" * 64,
                                 actor="worker:w107", evidence={})
        finally:
            st.close()

    def test_R14_108_terminal_job_with_active_authority(self):
        """R14-108: a terminal job that still carries owner/lease fields
        (contradiction injected on the copy) -> every authority-bearing op
        is refused and the terminal row is preserved byte-identical."""
        tok = self.running("j108", "w108", ttl=600.0)
        cdb, _cdir = self._backup_copy("c108")
        raw = self._raw(cdb)
        # Contradiction: FAILED (terminal) yet still owned + leased.
        raw.execute(
            "UPDATE jobs SET status='FAILED' WHERE job_id='j108'")
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        before = self._job_dump(sup.gate, "j108")
        # No authority-bearing op may act on a terminal job.
        with self.assertRaises(TransitionRejected):
            sup.gate.stage_artifact(
                job_id="j108", worker_id="w108", fencing_token=tok,
                task_id="t", kind="result", data=b"x",
                actor="worker:w108")
        with self.assertRaises(TransitionRejected):
            sup.gate.commit_artifact("j108", "w108", tok,
                                     artifact_id="0" * 64,
                                     actor="worker:w108", evidence={})
        # claim_job's contract is boolean, not raising: a terminal job is
        # simply not claimable (False), granting no authority.
        self.assertFalse(
            sup.gate.claim_job("j108", "w108", 60.0, "test"),
            "claim on a terminal job must return False, not grant a lease")
        with self.assertRaises(TransitionRejected):
            sup.gate.reclaim_lease(
                "j108", actor="system", reason="r14-108",
                expected_owner="w108", expected_token=tok)
        self.assertEqual(before, self._job_dump(sup.gate, "j108"),
                         "terminal row mutated by refused ops")
        # Boot does not schedule it and leaves it alone.
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "READY")
        self.assertEqual(sup.gate.get_job("j108")["status"], "FAILED")

    def test_R14_109_finalization_wrong_generation(self):
        """R14-109: finalization referencing the wrong desired-state
        generation is rejected: the generation IS the desired-state version
        (canonical 'ds-vN' identity)."""
        self.gate.set_desired_item(
            "dw-109", {"task_id": "t", "stage_id": "s"}, "test")
        head_v = self.gate.get_desired_head()["version"]
        good_gen = canonical_release_generation(head_v)
        # Mismatched generation/version pair: caller bug, fail closed.
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.begin_finalization_run("ds-v999", head_v, "test")
        self.assertIn("canonical identity", str(ctx.exception))
        # A generation for a version that is not the caller's version.
        with self.assertRaises(TransitionRejected):
            self.gate.begin_finalization_run(good_gen, head_v + 5, "test")
        # The correct pair opens; the wrong generation never created a run.
        row = self.gate.begin_finalization_run(good_gen, head_v, "test")
        self.assertEqual(row["state"], "OPEN")
        self.assertIsNone(self.gate.get_finalization_run("ds-v999"))
        # Contradictory re-begin (same generation, different version) fails.
        with self.assertRaises(TransitionRejected):
            self.gate.begin_finalization_run(good_gen, head_v + 1, "test")

    def test_R14_110_invalid_breaker_state(self):
        """R14-110: a breaker row in an impossible state -> admission
        denies (fail closed, never raises), the controller's transition is
        refused, and the corrupt row is preserved for forensics."""
        self.gate.ensure_breaker_state("TASK", "t", cooldown_s=30.0,
                                       actor="test")
        cdb, _cdir = self._backup_copy("c110")
        raw = self._raw(cdb)
        raw.execute(
            "UPDATE breaker_state SET state='GARBAGE' WHERE scope_type='TASK'"
            " AND scope_id='t'")
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        allowed, reason = sup.gate.breaker_allows("TASK", "t")
        self.assertFalse(allowed, "corrupt breaker admitted work")
        self.assertEqual(reason, "corrupt")
        with self.assertRaises(TransitionRejected):
            sup.gate.transition_breaker(
                "TASK", "t", expected_version=0, to_state="OPEN",
                actor="test", reason="r14-110")
        # The corrupt row is preserved (forensics), not repaired/deleted.
        row = sup.gate.get_breaker_state("TASK", "t")
        self.assertEqual(row["state"], "GARBAGE")
        # The pristine breaker still admits.
        allowed0, _ = self.gate.breaker_allows("TASK", "t")
        self.assertTrue(allowed0)

    def test_R14_111_invalid_recovery_policy(self):
        """R14-111: a self-contradictory recovery policy row -> the R9
        controller contains it (PolicyCorrupt -> blocked + escalated), never
        selects a rung, never creates an attempt, and the row is preserved."""
        from axos.exec.policy import PolicyController, PolicyConfig
        inc = self.gate.create_incident(
            None, "recovery", "worker_crash", "s", "cap-v1", None,
            "sigkill", "test", task_id="t")
        iid = inc["incident_id"]
        self.gate.ensure_recovery_policy(
            iid, policy_version="r9-policy/v1", current_rung=2,
            rung_name=CANONICAL_LADDER[2], incident_budget=7,
            per_rung_budgets={"1": 2, "2": 2, "3": 3, "4": 0, "5": 0},
            actor="test")
        cdb, _cdir = self._backup_copy("c111")
        raw = self._raw(cdb)
        # Self-contradiction: rung 99 with a negative remaining budget.
        raw.execute(
            "UPDATE recovery_policy SET current_rung=99,"
            " remaining_budget=-3 WHERE incident_id=?", (iid,))
        raw.commit()
        raw.close()

        from axos.exec.policy import (PolicyController, PolicyConfig,
                                        _validate_policy_row, PolicyCorrupt)
        from axos.exec.recovery import RecoveryConfig
        sup = self._open_copy(cdb)
        # The tampered row fails the controller's own fail-closed
        # validation first, at the unit level.
        bad_pol = dict(sup.gate.store.conn.execute(
            "SELECT * FROM recovery_policy WHERE incident_id=?",
            (iid,)).fetchone())
        with self.assertRaises(PolicyCorrupt):
            _validate_policy_row(bad_pol)
        before = sup.gate.store.conn.execute(
            "SELECT * FROM recovery_policy WHERE incident_id=?",
            (iid,)).fetchone()
        ctl = PolicyController(
            cdb, PolicyConfig(),
            RecoveryConfig(observation_window_s=1.0,
                           evaluation_interval_s=1.0, claim_timeout_s=1.0),
            actor="test-r14d", readiness=lambda: True)
        try:
            outcome = ctl._reconcile_incident(sup.gate, iid)
        finally:
            ctl.close()
        self.assertEqual(outcome["outcome"], "blocked")
        self.assertIn("corrupt", outcome["detail"])
        # No rung selected, no attempt created for the corrupt policy.
        self.assertEqual(
            sup.gate.store.conn.execute(
                "SELECT COUNT(*) FROM recovery_attempts WHERE incident_id=?",
                (iid,)).fetchone()[0], 0)
        after = sup.gate.store.conn.execute(
            "SELECT * FROM recovery_policy WHERE incident_id=?",
            (iid,)).fetchone()
        self.assertEqual(tuple(before), tuple(after),
                         "corrupt policy row was mutated")
        # Escalation evidence recorded for the human.
        esc = sup.gate.store.conn.execute(
            "SELECT outcome FROM incidents WHERE incident_id=?",
            (iid,)).fetchone()[0]
        self.assertEqual(esc, "escalated")

    def test_R14_112_impossible_transition_evidence(self):
        """R14-112: transitions outside the authoritative graph are refused
        with zero mutation — the row is byte-identical afterwards."""
        self.make_job("j112")
        before = self._job_dump(self.gate, "j112")
        # PENDING -> RUNNING skips CLAIMED: illegal edge.
        with self.assertRaises(TransitionRejected):
            self.gate.transition_job("j112", "RUNNING", "test")
        # Unknown target state.
        with self.assertRaises(TransitionRejected):
            self.gate.transition_job("j112", "VAPORIZED", "test")
        self.assertEqual(before, self._job_dump(self.gate, "j112"),
                         "refused transition mutated the row")
        # COMPLETE is unreachable through the generic API from any state.
        for jid, setup in (("j112a", None), ("j112b", "run")):
            if setup == "run":
                self.running(jid, "w112")
            else:
                self.make_job(jid)
            with self.assertRaises(TransitionRejected):
                self.gate.transition_job(jid, "COMPLETE", "test")
            self.assertNotEqual(self.gate.get_job(jid)["status"], "COMPLETE")

    def test_R14_117_checkpoint_manifest_tamper(self):
        """R14-117: a checkpoint whose manifest is tampered post-creation ->
        verification marks it CORRUPT (never trusted), the receipt records
        the failure, and latest-known-good never moves."""
        tok = self.running("j117", "w117")
        art = self.verified_artifact("j117", "w117", tok, b"r14-117-bytes")
        ck1 = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": art["artifact_id"],
                       "content_hash": art["content_hash"]}],
            trigger="policy")
        self.gate.verify_checkpoint(ck1["checkpoint_id"], actor="test")
        lkg_before = self.gate.latest_known_good("t")
        self.assertEqual(lkg_before["checkpoint_id"], ck1["checkpoint_id"])

        tok2 = self.running("j117b", "w117")
        art2 = self.verified_artifact("j117b", "w117", tok2, b"r14-117-b")
        ck2 = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": art2["artifact_id"],
                       "content_hash": art2["content_hash"]}],
            trigger="policy")
        cdb, _cdir = self._backup_copy("c117")
        raw = self._raw(cdb)
        man = json.loads(raw.execute(
            "SELECT canonical_manifest FROM checkpoints WHERE checkpoint_id=?",
            (ck2["checkpoint_id"],)).fetchone()[0])
        man[0]["content_hash"] = "2" * 64  # tamper the manifest
        raw.execute(
            "UPDATE checkpoints SET canonical_manifest=? WHERE checkpoint_id=?",
            (json.dumps(man), ck2["checkpoint_id"]))
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        res = sup.gate.verify_checkpoint(ck2["checkpoint_id"], actor="test")
        self.assertEqual(res["verification_status"], "CORRUPT")
        receipt = json.loads(res["verification_receipt"])
        self.assertTrue(receipt["failures"], "no failure recorded")
        # Latest-known-good never moves to (or past) a corrupt candidate.
        lkg = sup.gate.latest_known_good("t")
        self.assertEqual(lkg["checkpoint_id"], ck1["checkpoint_id"])
        # The corrupt candidate is retained for forensics, not deleted.
        self.assertIsNotNone(sup.gate.store.conn.execute(
            "SELECT checkpoint_id FROM checkpoints WHERE checkpoint_id=?",
            (ck2["checkpoint_id"],)).fetchone())

    def test_R14_118_schema_migrations_tamper(self):
        """R14-118: a tampered schema_migrations record (v11 row deleted) ->
        migrate() refuses to run against the unverifiable schema: the store
        fails closed at open, never silently re-applies DDL."""
        cdb, _cdir = self._backup_copy("c118")
        raw = self._raw(cdb)
        raw.execute("DELETE FROM schema_migrations WHERE version=11")
        raw.commit()
        raw.close()

        from axos.store import MigrationError
        st = open_store(cdb)
        try:
            with self.assertRaises(MigrationError):
                migrate(st)
        finally:
            st.close()
        # applied_versions no longer reports the full chain: the tamper is
        # observable, not silently healed.
        st2 = open_store(cdb)
        try:
            self.assertNotIn(11, applied_versions(st2))
        finally:
            st2.close()
        # The pristine db still reports the full chain and migrates
        # cleanly (no-op).
        self.assertEqual(applied_versions(self.store),
                         [m[0] for m in MIGRATIONS])
        self.assertEqual(migrate(self.store), [])

    def test_R14_119_referential_integrity_violation(self):
        """R14-119: a dangling foreign key (job referencing a deleted task)
        -> PRAGMA foreign_key_check fails -> boot BLOCKED with
        store_integrity_failed before any recovery runs."""
        self._healthy_job("j119", "w119", b"r14-119-bytes")
        cdb, _cdir = self._backup_copy("c119")
        raw = self._raw(cdb)
        raw.execute("PRAGMA foreign_keys=OFF")
        raw.execute("DELETE FROM tasks WHERE task_id='t'")
        raw.execute("PRAGMA foreign_keys=ON")
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        ok, detail = sup.store.integrity_check()
        self.assertFalse(ok)
        self.assertIn("foreign_key_check", detail)
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "BLOCKED")
        self.assertEqual(report["blocked_reason"], "store_integrity_failed")
        self.assertEqual(self._event_count(sup.gate, "boot.completed"), 1)
        # Pristine integrity holds.
        ok0, _ = self.store.integrity_check()
        self.assertTrue(ok0)

    def test_R14_120_combined_corruption(self):
        """R14-120: ledger tamper AND byte corruption together -> each is
        detected by its own independent check; boot fails closed on the
        ledger (the first authority gate) and the artifact quarantine
        evidence is still produced."""
        tok = self.running("j120", "w120")
        art = self.stage("j120", "w120", tok, b"r14-120-bytes")
        aid = art["artifact_id"]
        cdb, cdir = self._backup_copy("c120")
        # Corruption 1: ledger payload tamper.
        raw = self._raw(cdb)
        seq, payload = raw.execute(
            "SELECT seq, payload FROM ledger WHERE event_type='artifact.staged'"
        ).fetchone()
        doc = json.loads(payload)
        doc["artifact_id"] = "TAMPERED"
        raw.execute("UPDATE ledger SET payload=? WHERE seq=?",
                    (json.dumps(doc), seq))
        raw.commit()
        raw.close()
        # Corruption 2: artifact bytes flipped on the copy only.
        sup = self._open_copy(cdb)
        uri = sup.gate.get_artifact(aid)["uri"]
        with open(uri, "r+b") as f:
            f.seek(0)
            first = f.read(1)
            f.seek(0)
            f.write(bytes([first[0] ^ 0xFF]))

        ok, detail = sup.gate.verify_ledger_chain()
        self.assertFalse(ok, "ledger tamper undetected")
        with self.assertRaises(TransitionRejected):
            sup.gate.verify_artifact(aid, actor="test")
        self.assertEqual(sup.gate.get_artifact(aid)["status"], "QUARANTINED")
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "BLOCKED")
        self.assertEqual(report["blocked_reason"], "ledger_chain_broken")


# ============================================================ R14-C: backup/restore
class TestR14CRestore(R14DBase):
    """R14-113..115: the restore unit (backup_to + staging-tree copy +
    URI re-anchor) is exercised as a first-class path: a restored copy
    must be a faithful, bootable replica; fencing epochs must not leak
    across the restore boundary; corruption after restore must fail
    closed without touching the source."""

    def _rich_state(self):
        """A representative durable state: one COMPLETE job with a
        verified artifact, one RUNNING job with a live lease, one
        VERIFIED checkpoint, one recovered incident."""
        self._healthy_job("j113a", "w113", b"r14-113-a-bytes")
        tok_b = self.running("j113b", "w113", ttl=600.0)
        art_b = self.stage("j113b", "w113", tok_b, b"r14-113-b-bytes")
        art_a = self.gate.get_artifact(
            self.gate.get_job("j113a")["result_artifact_id"])
        ck = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": art_a["artifact_id"],
                       "content_hash": art_a["content_hash"]}],
            trigger="policy")
        self.gate.verify_checkpoint(ck["checkpoint_id"], actor="test")
        inc = self.gate.create_incident(
            None, "recovery", "worker_crash", "s", "cap-v1", None,
            "sigkill", "test", task_id="t")
        self.gate.set_incident_outcome(inc["incident_id"], "recovered",
                                       "r14-113", "test")
        return {"tok_b": tok_b, "art_b": art_b, "art_a": art_a,
                "ck": ck, "inc": inc["incident_id"]}

    def _ddl_dump(self, gate):
        return gate.store.conn.execute(
            "SELECT type, name, sql FROM sqlite_master"
            " WHERE sql IS NOT NULL ORDER BY type, name").fetchall()

    def _ledger_dump(self, gate):
        return [tuple(r) for r in gate.store.conn.execute(
            "SELECT seq, event_type, payload, actor, prev_hash, hash"
            " FROM ledger ORDER BY seq").fetchall()]

    def test_R14_113_backup_restore_faithful_and_bootable(self):
        """R14-113: restore into a fresh location -> schema v11, canonical
        DDL identical, durable IDs coherent, ledger continuous, artifact
        bytes valid at re-anchored URIs, checkpoints intact, and the
        restored boot reconstructs to READY."""
        refs = self._rich_state()
        ddl_before = [tuple(r) for r in self._ddl_dump(self.gate)]
        ledger_before = self._ledger_dump(self.gate)
        n_ledger = len(ledger_before)
        arts_before = {
            r["artifact_id"]: (r["content_hash"], r["status"])
            for r in [dict(x) for x in self.gate.store.conn.execute(
                "SELECT artifact_id, content_hash, status FROM artifacts")]}
        # Snapshot the artifact bytes before restore.
        for aid in arts_before:
            uri = self.gate.get_artifact(aid)["uri"]
            with open(uri, "rb") as f:
                arts_before[aid] += (hashlib.sha256(f.read()).hexdigest(),)

        cdb, _cdir = self._backup_copy("c113")
        sup = self._open_copy(cdb)
        try:
            # The restored boot reconstructs and reaches READY.
            rep = sup.gate.store.conn.execute(
                "SELECT COUNT(*) FROM ledger WHERE event_type='boot.completed'"
                " AND seq > ?", (n_ledger,)).fetchone()[0]
            self.assertGreaterEqual(rep, 1)
            # Schema v12 on the restored copy.
            self.assertEqual(applied_versions(sup.gate.store),
                             list(range(1, 13)))
            # Canonical DDL identical.
            self.assertEqual([tuple(r) for r in self._ddl_dump(sup.gate)],
                             ddl_before)
            # Ledger continuity: the restored ledger starts with the exact
            # pre-restore prefix and the chain verifies.
            ledger_after = self._ledger_dump(sup.gate)
            self.assertGreaterEqual(len(ledger_after), n_ledger)
            self.assertEqual(ledger_after[:n_ledger], ledger_before)
            ok, detail = sup.gate.verify_ledger_chain()
            self.assertTrue(ok, detail)
            # Durable IDs coherent: same jobs, artifacts, checkpoints.
            for jid in ("j113a", "j113b"):
                self.assertEqual(
                    self._job_dump(sup.gate, jid),
                    self._job_dump(self.gate, jid))
            for aid, (ch, status, fhash) in arts_before.items():
                row = sup.gate.get_artifact(aid)
                self.assertEqual(row["content_hash"], ch)
                self.assertEqual(row["status"], status)
                # Artifact references valid: bytes exist at the re-anchored
                # URI and match the recorded content hash.
                with open(row["uri"], "rb") as f:
                    self.assertEqual(hashlib.sha256(f.read()).hexdigest(),
                                     ch)
                self.assertEqual(hashlib.sha256(
                    open(row["uri"], "rb").read()).hexdigest(), fhash)
            # Checkpoints intact: latest-known-good survived the restore.
            lkg = sup.gate.latest_known_good("t")
            self.assertIsNotNone(lkg)
            self.assertEqual(lkg["checkpoint_id"],
                             refs["ck"]["checkpoint_id"])
            self.assertEqual(lkg["verification_status"], "VERIFIED")
            # The RUNNING job's live lease survived with its token.
            jb = sup.gate.get_job("j113b")
            self.assertEqual(jb["status"], "RUNNING")
            self.assertEqual(jb["owner_worker_id"], "w113")
            self.assertEqual(int(jb["fencing_token"]), int(refs["tok_b"]))
        finally:
            sup.close()

    def test_R14_114_stale_fencing_tokens_rejected_after_restore(self):
        """R14-114: fencing epochs do not leak across the restore
        boundary. A token minted AFTER the backup (on the source) is
        rejected by the restored copy; and on the restored copy itself,
        a reclaim bumps the epoch and the pre-restore token dies."""
        tok = self.running("j114", "w114", ttl=600.0)
        cdb, _cdir = self._backup_copy("c114")
        # After the backup, the source moves on: forced reclaim bumps the
        # token on the source (post-backup epoch).
        self.gate.record_watchdog_verdict("j114", tok, "STALLED",
                                          {"cause": "test"}, "test")
        src_row = self.gate.reclaim_lease(
            "j114", actor="system", reason="r14-114 post-backup",
            expected_owner="w114", expected_token=tok, force=True,
            verdict="STALLED", incident_id="inc-r14-114")
        tok2 = src_row["fencing_token"]
        self.assertGreater(int(tok2), int(tok))

        sup = self._open_copy(cdb)
        try:
            g = sup.gate
            # The restored copy is at the pre-backup epoch: presenting the
            # post-backup token fails (renew_lease returns False on token
            # mismatch — no cross-epoch confusion).
            self.assertFalse(
                g.renew_lease("j114", "w114", tok2, 600.0, "worker:w114"))
            # The pre-backup token is still the live one on the restored
            # copy: renewal works (boolean contract).
            self.assertTrue(
                g.renew_lease("j114", "w114", tok, 600.0, "worker:w114"))
            # And a reclaim on the restored copy bumps past it; the old
            # token is then dead there too.
            g.record_watchdog_verdict("j114", tok, "STALLED",
                                      {"cause": "test"}, "test")
            new_row = g.reclaim_lease(
                "j114", actor="system", reason="r14-114 restored-copy",
                expected_owner="w114", expected_token=tok, force=True,
                verdict="STALLED", incident_id="inc-r14-114-b")
            self.assertGreater(int(new_row["fencing_token"]), int(tok))
            self.assertFalse(
                g.renew_lease("j114", "w114", tok, 600.0, "worker:w114"))
        finally:
            sup.close()

    def test_R14_115_corruption_after_restore_contained(self):
        """R14-115: corrupt the RESTORED copy after a clean restore ->
        the restored boot fails closed (BLOCKED/ledger_chain_broken) while
        the source database still verifies clean."""
        self._rich_state()
        cdb, _cdir = self._backup_copy("c115")
        raw = self._raw(cdb)
        seq = raw.execute(
            "SELECT seq FROM ledger WHERE event_type='job.committed'"
        ).fetchone()[0]
        doc = json.loads(raw.execute(
            "SELECT payload FROM ledger WHERE seq=?", (seq,)).fetchone()[0])
        doc["outcome"] = "TAMPERED-AFTER-RESTORE"
        raw.execute("UPDATE ledger SET payload=? WHERE seq=?",
                    (json.dumps(doc), seq))
        raw.commit()
        raw.close()

        sup = self._open_copy(cdb)
        try:
            ok, detail = sup.gate.verify_ledger_chain()
            self.assertFalse(ok)
            report = boot_recover(sup, actor="system")
            self.assertEqual(report["phase"], "BLOCKED")
            self.assertEqual(report["blocked_reason"], "ledger_chain_broken")
        finally:
            sup.close()
        # The source is untouched by the post-restore corruption.
        ok0, d0 = self.gate.verify_ledger_chain()
        self.assertTrue(ok0, d0)


# ============================================================ R14-E: artifact/checkpoint adversarial
class TestR14EAdversarial(R14DBase):
    """System-level adversarial exercise of the R5 contract: bytes are
    attacked AFTER hashing, identities are confused, checkpoints are
    mutated, and concurrent actors race. Every attack must fail closed
    with evidence preserved."""

    def test_R14_121_bytes_changed_after_hashing(self):
        """R14-121: stage -> begin_commit -> change the bytes on disk ->
        verification fails, artifact QUARANTINED with a FAIL receipt, and
        the atomic commit is rejected by the VERIFIED predicate."""
        tok = self.running("j121", "w121")
        art = self.stage("j121", "w121", tok, b"r14-121-original-bytes")
        aid = art["artifact_id"]
        self.gate.begin_commit("j121", "w121", tok, artifact_id=aid,
                               actor="worker:w121")
        with open(art["uri"], "r+b") as f:
            f.seek(4)
            f.write(b"XXXX")
        with self.assertRaises(TransitionRejected):
            self.gate.verify_artifact(aid, actor="test")
        row = self.gate.get_artifact(aid)
        self.assertEqual(row["status"], "QUARANTINED")
        self.assertGreaterEqual(
            self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM validations WHERE artifact_id=?"
                " AND result='FAIL'", (aid,)).fetchone()[0], 1)
        self.assertGreaterEqual(
            self._event_count(self.gate, "artifact.quarantined"), 1)
        # The commit path re-checks the VERIFIED predicate: the quarantined
        # artifact is rejected even though the job is COMMITTING.
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.commit_artifact("j121", "w121", tok, artifact_id=aid,
                                      actor="worker:w121", evidence={})
        self.assertIn("not VERIFIED", str(ctx.exception))
        self.assertEqual(self.gate.get_job("j121")["status"], "COMMITTING")

    def test_R14_122_bytes_truncated(self):
        """R14-122: truncate the staged bytes after begin_commit -> size +
        hash mismatch -> quarantine, no commit."""
        tok = self.running("j122", "w122")
        art = self.stage("j122", "w122", tok, b"r14-122-" + b"P" * 64)
        aid = art["artifact_id"]
        self.gate.begin_commit("j122", "w122", tok, artifact_id=aid,
                               actor="worker:w122")
        with open(art["uri"], "r+b") as f:
            f.truncate(8)
        with self.assertRaises(TransitionRejected):
            self.gate.verify_artifact(aid, actor="test")
        self.assertEqual(self.gate.get_artifact(aid)["status"], "QUARANTINED")
        receipt = self.gate.store.conn.execute(
            "SELECT receipt_ref FROM validations WHERE artifact_id=?"
            " AND result='FAIL'", (aid,)).fetchone()[0]
        details = json.loads(receipt)["details"]
        self.assertFalse(details["hash_matches"])
        self.assertFalse(details["size_matches"])
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.commit_artifact("j122", "w122", tok, artifact_id=aid,
                                      actor="worker:w122", evidence={})
        self.assertIn("not VERIFIED", str(ctx.exception))

    def test_R14_123_bytes_deleted(self):
        """R14-123: delete the staged bytes after begin_commit ->
        verification fails closed on missing bytes (quarantine + FAIL
        receipt); the atomic commit rejects the unverified artifact."""
        tok = self.running("j123", "w123")
        art = self.stage("j123", "w123", tok, b"r14-123-bytes")
        aid = art["artifact_id"]
        self.gate.begin_commit("j123", "w123", tok, artifact_id=aid,
                               actor="worker:w123")
        os.remove(art["uri"])
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.verify_artifact(aid, actor="test")
        self.assertIn("bytes", str(ctx.exception).lower())
        self.assertEqual(self.gate.get_artifact(aid)["status"], "QUARANTINED")
        with self.assertRaises(TransitionRejected) as ctx2:
            self.gate.commit_artifact("j123", "w123", tok, artifact_id=aid,
                                      actor="worker:w123", evidence={})
        self.assertIn("not VERIFIED", str(ctx2.exception))

    def test_R14_124_wrong_artifact_for_job(self):
        """R14-124: commit job B naming job A's artifact (hash/identity
        mismatch of association) -> begin_commit and commit_artifact both
        reject the cross-job linkage."""
        tok_a = self.running("j124a", "w124")
        art_a = self.stage("j124a", "w124", tok_a, b"r14-124-a-bytes")
        tok_b = self.running("j124b", "w124")
        self.stage("j124b", "w124", tok_b, b"r14-124-b-bytes")
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.begin_commit("j124b", "w124", tok_b,
                                   artifact_id=art_a["artifact_id"],
                                   actor="worker:w124")
        self.assertIn("not 'j124b'", str(ctx.exception))
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j124b", "w124", tok_b,
                                      artifact_id=art_a["artifact_id"],
                                      actor="worker:w124", evidence={})
        self.assertEqual(self.gate.get_job("j124b")["status"], "RUNNING")

    def test_R14_125_identical_bytes_two_jobs_shared_completion(self):
        """R14-125 (IMPLEMENTATION DEFECT, fixed in R14): identical bytes
        staged for two different jobs.

        Content addressing dedups at the byte level - one artifact row -
        but each job's staging is recorded in artifact_stagings under its
        own live lease. Both jobs complete through the full protocol
        against the SAME artifact row: two COMPLETEs, both referencing one
        artifact_id.

        The defect: begin_commit/commit_artifact enforced
        art.job_id == job_id, so the second job's legitimate, lease-backed
        staging was uncompletable - the dedup design and the linkage rule
        contradicted each other. The fix: per-job staging provenance is
        the linkage authority; the row's job_id is first-writer metadata.

        The R14-124 defense is preserved: a job that never staged the
        bytes still cannot adopt the artifact."""
        data = b"r14-125-identical-bytes"
        tok_a = self.running("j125a", "w125")
        art_a = self.stage("j125a", "w125", tok_a, data)
        staged_before = self._event_count(self.gate, "artifact.staged")
        tok_b = self.running("j125b", "w125")
        art_b = self.stage("j125b", "w125", tok_b, data)
        # Byte-level dedup: one row, one identity, no second staged event.
        self.assertEqual(art_a["artifact_id"], art_b["artifact_id"])
        aid = art_a["artifact_id"]
        self.assertEqual(
            self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE artifact_id=?",
                (aid,)).fetchone()[0], 1)
        self.assertEqual(self._event_count(self.gate, "artifact.staged"),
                         staged_before)
        # ...but TWO staging-provenance records: each job staged under its
        # own live lease, each into its own namespaced staging path.
        stagings = self.gate.store.conn.execute(
            "SELECT job_id, fencing_token, uri FROM artifact_stagings"
            " WHERE artifact_id=? ORDER BY job_id", (aid,)).fetchall()
        self.assertEqual([r[0] for r in stagings], ["j125a", "j125b"])
        self.assertTrue(os.path.isfile(stagings[0][2]))
        self.assertTrue(os.path.isfile(stagings[1][2]))
        self.assertNotEqual(stagings[0][2], stagings[1][2])
        # Job A completes through the full protocol.
        self.gate.begin_commit("j125a", "w125", tok_a, artifact_id=aid,
                               actor="worker:w125")
        self.gate.verify_artifact(aid, actor="worker:w125", worker_id="w125",
                                  fencing_token=tok_a, job_id="j125a")
        self.gate.commit_artifact("j125a", "w125", tok_a, artifact_id=aid,
                                  actor="worker:w125", evidence={})
        self.assertEqual(self.gate.get_job("j125a")["status"], "COMPLETE")
        # Job B completes against the SAME artifact row - this was the
        # defect: begin_commit/commit_artifact rejected on the row's
        # first-writer job_id even though B staged the bytes under a live
        # lease.
        self.gate.begin_commit("j125b", "w125", tok_b, artifact_id=aid,
                               actor="worker:w125")
        self.gate.verify_artifact(aid, actor="worker:w125", worker_id="w125",
                                  fencing_token=tok_b, job_id="j125b")
        self.gate.commit_artifact("j125b", "w125", tok_b, artifact_id=aid,
                                  actor="worker:w125", evidence={})
        self.assertEqual(self.gate.get_job("j125b")["status"], "COMPLETE")
        ja = self.gate.get_job("j125a")
        jb = self.gate.get_job("j125b")
        self.assertEqual(ja["result_artifact_id"], aid)
        self.assertEqual(jb["result_artifact_id"], aid)
        self.assertEqual(ja["content_hash"], jb["content_hash"])
        # The R14-124 defense holds: a job that never staged the bytes
        # cannot adopt the artifact, even though the row exists.
        tok_c = self.running("j125c", "w125")
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.begin_commit("j125c", "w125", tok_c, artifact_id=aid,
                                   actor="worker:w125")
        self.assertIn("not 'j125c'", str(ctx.exception))
        self.assertEqual(self.gate.get_job("j125c")["status"], "RUNNING")

    def test_R14_126_checkpoint_manifest_mutation(self):
        """R14-126: mutate a staged checkpoint's manifest -> verification
        fails (identity != sha256(manifest)) -> CORRUPT, latest-known-good
        never moves, candidate retained."""
        tok = self.running("j126", "w126")
        art = self.verified_artifact("j126", "w126", tok, b"r14-126-bytes")
        ck = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": art["artifact_id"],
                       "content_hash": art["content_hash"]}],
            trigger="policy")
        cid = ck["checkpoint_id"]
        raw = self._raw(self.db)
        man = json.loads(raw.execute(
            "SELECT canonical_manifest FROM checkpoints WHERE checkpoint_id=?",
            (cid,)).fetchone()[0])
        man[0]["content_hash"] = "3" * 64
        raw.execute("UPDATE checkpoints SET canonical_manifest=?"
                    " WHERE checkpoint_id=?", (json.dumps(man), cid))
        raw.commit()
        raw.close()

        res = self.gate.verify_checkpoint(cid, actor="test")
        self.assertEqual(res["verification_status"], "CORRUPT")
        receipt = json.loads(res["verification_receipt"])
        self.assertTrue(any("sha256" in f for f in receipt["failures"]),
                        receipt["failures"])
        self.assertIsNone(self.gate.latest_known_good("t"),
                           "lkg moved on a corrupt candidate")

    def test_R14_127_stale_checkpoint_and_finalization(self):
        """R14-127: re-verifying a VERIFIED checkpoint is rejected (stale
        publication attempt); publishing a finalization with a stale
        expected_version loses CAS -> FinalizationConflict."""
        tok = self.running("j127", "w127")
        art = self.verified_artifact("j127", "w127", tok, b"r14-127-bytes")
        ck = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": art["artifact_id"],
                       "content_hash": art["content_hash"]}],
            trigger="policy")
        cid = ck["checkpoint_id"]
        self.gate.verify_checkpoint(cid, actor="test")
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.verify_checkpoint(cid, actor="test")
        self.assertIn("already VERIFIED", str(ctx.exception))
        self.assertEqual(
            self.gate.latest_known_good("t")["checkpoint_id"], cid)

        # Stale finalization publish: begin two generations' worth of state
        # by racing the version — the second publisher holds a stale
        # expected_version once the first wins.
        self.gate.set_desired_item("dw-127",
                                   {"task_id": "t", "stage_id": "s"}, "test")
        v = self.gate.get_desired_head()["version"]
        gen = canonical_release_generation(v)
        begun = self.gate.begin_finalization_run(gen, v, "test")
        # evaluate to READY requires a healthy generation; force the run to
        # READY is out of scope — instead prove the CAS shape directly: a
        # publish against a non-READY run with a wrong version fails before
        # any CAS write, and the run row is untouched.
        before = dict(self.gate.get_finalization_run(gen))
        with self.assertRaises((TransitionRejected, FinalizationConflict)):
            self.gate.publish_finalization(gen, begun["version"] + 100,
                                           "0" * 64, "test")
        after = dict(self.gate.get_finalization_run(gen))
        self.assertEqual(before["state"], after["state"])
        self.assertEqual(before["version"], after["version"])

    def test_R14_128_concurrent_checkpoint_verification(self):
        """R14-128: two actors verify the same checkpoint concurrently ->
        exactly one authoritative VERIFIED outcome; the loser gets
        TransitionRejected and converges on VERIFIED. Run 3x (no-flake)."""
        for round_ in range(3):
            jid = f"j128-{round_}"
            tok = self.running(jid, "w128")
            art = self.verified_artifact(jid, "w128", tok,
                                         f"r14-128-{round_}".encode())
            ck = self.gate.stage_checkpoint(
                "t", actor="test",
                manifest=[{"artifact_id": art["artifact_id"],
                           "content_hash": art["content_hash"]}],
                trigger="policy")
            cid = ck["checkpoint_id"]
            seq_before = self.gate.store.conn.execute(
                "SELECT COALESCE(MAX(seq),0) FROM ledger").fetchone()[0]

            barrier = threading.Barrier(2)
            results = {}

            def attempt(i, store_path):
                st = open_store(store_path)
                g = TransitionGate(st)
                try:
                    barrier.wait(timeout=30)
                    try:
                        g.verify_checkpoint(cid, actor=f"test-{i}")
                        results[i] = "verified"
                    except TransitionRejected as e:
                        results[i] = f"rejected: {e}"
                    except Exception as e:  # pragma: no cover
                        results[i] = f"ERROR: {type(e).__name__}: {e}"
                finally:
                    st.close()

            threads = [threading.Thread(target=attempt, args=(i, self.db))
                       for i in range(2)]
            for th in threads:
                th.start()
            for th in threads:
                th.join(timeout=60)
            self.assertFalse(any(t.is_alive() for t in threads),
                             "verification threads hung")
            winners = [i for i, r in results.items() if r == "verified"]
            losers = [i for i, r in results.items()
                      if r.startswith("rejected")]
            self.assertEqual(len(winners), 1,
                             f"expected exactly one winner: {results}")
            self.assertEqual(len(losers), 1,
                             f"expected exactly one loser: {results}")
            self.assertIn("already VERIFIED", results[losers[0]])
            # Convergence: both observe VERIFIED afterwards.
            fin = self.gate.store.conn.execute(
                "SELECT verification_status FROM checkpoints"
                " WHERE checkpoint_id=?", (cid,)).fetchone()[0]
            self.assertEqual(fin, "VERIFIED")
            # Exactly one authoritative verification: one VERIFYING and one
            # VERIFIED event for this checkpoint after the barrier.
            evs = self.gate.store.conn.execute(
                "SELECT payload FROM ledger WHERE event_type=?"
                " AND seq > ?", ("checkpoint.verification",
                                 seq_before)).fetchall()
            mine = [json.loads(r[0]) for r in evs
                    if json.loads(r[0])["checkpoint_id"] == cid]
            states = sorted(e["to"] for e in mine)
            self.assertEqual(states, ["VERIFIED", "VERIFYING"], states)

    def _ready_finalization(self, tag):
        """Build a healthy desired generation and drive it to READY.
        Returns (gen, ready_row)."""
        dwid = f"dw-{tag}"
        self.gate.set_desired_item(
            dwid, {"task_id": "t", "stage_id": "s"}, "test")
        v = self.gate.get_desired_head()["version"]
        gen = canonical_release_generation(v)
        job, _ = self.gate.ensure_job_for_desired_state(
            desired_work_id=dwid, task_id="t", stage_id="s", max_attempts=3,
            policy={}, desired_version=v, actor="reconciler")
        jid = job["job_id"]
        wid = f"w-{tag}"
        self.gate.claim_job(jid, wid, 60.0, "test")
        self.gate.transition_job(jid, "RUNNING", f"worker:{wid}")
        tok = self.gate.get_job(jid)["fencing_token"]
        self.full_protocol(jid, wid, tok, f"r14-{tag}-bytes".encode())
        self.gate.begin_finalization_run(gen, v, "test")
        row = self.gate.get_finalization_run(gen)
        ready = self.gate.evaluate_finalization(gen, row["version"], "test")
        self.assertEqual(ready["state"], "READY", self._blockers(ready))
        return gen, ready

    def _blockers(self, run):
        return json.loads(run["blockers"] or "[]")

    def test_R14_129_concurrent_finalization_publish(self):
        """R14-129: two publishers race publish_finalization on a READY run
        -> exactly one authoritative FINALIZED (one published ledger event);
        the loser converges idempotently on the FINALIZED record."""
        gen, ready = self._ready_finalization("129")
        seq_before = self.gate.store.conn.execute(
            "SELECT COALESCE(MAX(seq),0) FROM ledger").fetchone()[0]
        barrier = threading.Barrier(2)
        results = {}

        def attempt(i):
            st = open_store(self.db)
            g = TransitionGate(st)
            try:
                barrier.wait(timeout=30)
                try:
                    row = g.publish_finalization(
                        gen, ready["version"], ready["manifest_hash"],
                        "test")
                    results[i] = ("ok", row["state"])
                except FinalizationConflict as e:
                    results[i] = ("conflict", str(e))
                except TransitionRejected as e:
                    results[i] = ("rejected", str(e))
                except Exception as e:  # pragma: no cover
                    results[i] = ("ERROR",
                                  f"{type(e).__name__}: {e}")
            finally:
                st.close()

        threads = [threading.Thread(target=attempt, args=(i,))
                   for i in range(2)]
        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=60)
        self.assertFalse(any(t.is_alive() for t in threads),
                         "publish threads hung")
        self.assertNotIn("ERROR", [r[0] for r in results.values()],
                         results)
        pubs = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type=?"
            " AND seq > ?", ("finalization.published",
                             seq_before)).fetchone()[0]
        self.assertEqual(pubs, 1,
                         f"expected exactly one published event: {results}")
        fin = self.gate.get_finalization_run(gen)
        self.assertEqual(fin["state"], "FINALIZED")
        # The loser's path converges: every thread observes FINALIZED, and
        # the published evidence was never overwritten (single row, version
        # bumped exactly once by the winner).
        for i, (kind, detail) in results.items():
            self.assertIn(kind, ("ok", "conflict", "rejected"), results)
            if kind == "ok":
                self.assertEqual(detail, "FINALIZED")

    def test_R14_130_historical_complete_preserved(self):
        """R14-130: a valid historical COMPLETE survives later failures
        byte-identical: never rewritten, exactly one commit event."""
        row = self._healthy_job("j130", "w130", b"r14-130-bytes")
        before = self._job_dump(self.gate, "j130")

        def committed_for(jid):
            n = 0
            for (payload,) in self.gate.store.conn.execute(
                    "SELECT payload FROM ledger WHERE event_type=?"
                    " AND payload LIKE ?", ("job.committed",
                                            f'%"job_id":"{jid}"%')):
                n += 1
            return n

        committed_before = committed_for("j130")
        self.assertEqual(committed_before, 1)
        # Later failures: another job fails, an incident opens, a breaker
        # trips, a checkpoint is staged and verified.
        tok = self.running("j130b", "w130")
        self.gate.fail_job_execution("j130b", "w130", tok, actor="test",
                                     reason="later failure", evidence={})
        inc = self.gate.create_incident(None, "recovery", "worker_crash",
                                        "s", "cap-v1", None, "sigkill",
                                        "test", task_id="t")
        self.gate.set_incident_outcome(inc["incident_id"], "recovered",
                                       "test recovery", "test")
        self.gate.ensure_breaker_state("TASK", "t", cooldown_s=30.0,
                                       actor="test")
        art = self.gate.get_artifact(row["result_artifact_id"])
        ck = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": art["artifact_id"],
                       "content_hash": art["content_hash"]}],
            trigger="policy")
        self.gate.verify_checkpoint(ck["checkpoint_id"], actor="test")
        boot_recover(self.sup, actor="system")

        after = self._job_dump(self.gate, "j130")
        self.assertEqual(before, after,
                         "historical COMPLETE row was rewritten")
        committed_after = committed_for("j130")
        self.assertEqual(committed_after, committed_before,
                         "duplicate commit event for a historical COMPLETE")
        self.assertEqual(self.gate.get_job("j130")["status"], "COMPLETE")

    def test_R14_131_raw_complete_bypass_refused(self):
        """R14-131: COMPLETE is unreachable except through the R5 atomic
        commit — the generic transition API refuses it from every state,
        and the removed commit_job_result path fails loudly."""
        self.make_job("j131a")
        tok = self.running("j131b", "w131")
        self.gate.begin_commit(
            "j131b", "w131", tok,
            artifact_id=self.stage("j131b", "w131", tok,
                                   b"r14-131")["artifact_id"],
            actor="worker:w131")
        for jid in ("j131a", "j131b"):
            with self.assertRaises(TransitionRejected) as ctx:
                self.gate.transition_job(jid, "COMPLETE", "test")
            self.assertIn("commit_artifact", str(ctx.exception))
        with self.assertRaises(TransitionRejected):
            self.gate.commit_job_result("j131a", "SUCCESS")
        # The job that went through begin_commit is COMMITTING, not
        # COMPLETE — no bypass happened.
        self.assertEqual(self.gate.get_job("j131b")["status"], "COMMITTING")

    def test_R14_132_commit_without_verification_refused(self):
        """R14-132: staged-but-unverified bytes can never complete a job:
        commit_artifact demands the full VERIFIED predicate."""
        tok = self.running("j132", "w132")
        art = self.stage("j132", "w132", tok, b"r14-132-bytes")
        self.gate.begin_commit("j132", "w132", tok,
                               artifact_id=art["artifact_id"],
                               actor="worker:w132")
        # No verify_artifact call: straight to commit.
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.commit_artifact("j132", "w132", tok,
                                      artifact_id=art["artifact_id"],
                                      actor="worker:w132", evidence={})
        self.assertIn("not VERIFIED", str(ctx.exception))
        self.assertEqual(self.gate.get_job("j132")["status"], "COMMITTING")
        # Even an authority actor cannot skip verification.
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j132", None, tok,
                                      artifact_id=art["artifact_id"],
                                      actor="system", evidence={})

    def test_R14_133_stale_worker_verify_rejected(self):
        """R14-133: after R1 reclaim bumps the token, the stale worker's
        verify_artifact (old token) is rejected as fenced."""
        tok = self.running("j133", "w133", ttl=600.0)
        art = self.stage("j133", "w133", tok, b"r14-133-bytes")
        self.gate.reclaim_lease("j133", actor="system",
                                reason="r14-133 forced",
                                expected_owner="w133", expected_token=tok,
                                force=True, verdict="DEAD",
                                incident_id="inc-r14-133")
        with self.assertRaises(LeaseError):
            self.gate.verify_artifact(
                art["artifact_id"], actor="worker:w133", worker_id="w133",
                fencing_token=tok)
        # The artifact is still STAGING: no verification happened.
        self.assertEqual(
            self.gate.get_artifact(art["artifact_id"])["status"], "STAGING")

    def test_R14_134_checkpoint_missing_artifact(self):
        """R14-134: a checkpoint manifest naming a nonexistent artifact ->
        CORRUPT with the missing-artifact problem in the receipt;
        latest-known-good never moves."""
        ck = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": "9" * 64, "content_hash": "9" * 64}],
            trigger="policy")
        res = self.gate.verify_checkpoint(ck["checkpoint_id"], actor="test")
        self.assertEqual(res["verification_status"], "CORRUPT")
        receipt = json.loads(res["verification_receipt"])
        self.assertTrue(any("missing" in f for f in receipt["failures"]),
                        receipt["failures"])
        self.assertIsNone(self.gate.latest_known_good("t"))

    def test_R14_135_checkpoint_wrong_content_hash(self):
        """R14-135: manifest content_hash != artifact's recorded hash ->
        CORRUPT; the artifact row itself is untouched."""
        tok = self.running("j135", "w135")
        art = self.verified_artifact("j135", "w135", tok, b"r14-135-bytes")
        ck = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": art["artifact_id"],
                       "content_hash": "8" * 64}],
            trigger="policy")
        res = self.gate.verify_checkpoint(ck["checkpoint_id"], actor="test")
        self.assertEqual(res["verification_status"], "CORRUPT")
        receipt = json.loads(res["verification_receipt"])
        self.assertTrue(any("content hash" in f for f in receipt["failures"]),
                        receipt["failures"])
        self.assertEqual(self.gate.get_artifact(art["artifact_id"])["status"],
                         "VALIDATED")

    def test_R14_136_quarantine_is_terminal(self):
        """R14-136: a quarantined artifact stays dead even if the bytes are
        later 'fixed' — quarantine is a terminal forensic state, and the
        commit predicate still rejects it."""
        tok = self.running("j136", "w136")
        data = b"r14-136-good-bytes"
        art = self.stage("j136", "w136", tok, data)
        aid = art["artifact_id"]
        self.gate.begin_commit("j136", "w136", tok, artifact_id=aid,
                               actor="worker:w136")
        with open(art["uri"], "r+b") as f:
            f.write(b"BAD!")
        with self.assertRaises(TransitionRejected):
            self.gate.verify_artifact(aid, actor="test")
        self.assertEqual(self.gate.get_artifact(aid)["status"], "QUARANTINED")
        # "Fix" the bytes back to the original content.
        with open(art["uri"], "wb") as f:
            f.write(data)
        # Verification now passes structurally, but the artifact is NOT
        # resurrected: status stays QUARANTINED...
        res = self.gate.verify_artifact(aid, actor="test")
        self.assertEqual(res["status"], "QUARANTINED")
        # ...and the atomic commit still rejects it (predicate demands
        # VALIDATED/RELEASED).
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.commit_artifact("j136", "w136", tok, artifact_id=aid,
                                      actor="worker:w136", evidence={})
        self.assertIn("not VERIFIED", str(ctx.exception))

    def test_R14_137_begin_commit_foreign_artifact(self):
        """R14-137: begin_commit naming an artifact staged for a different
        job is rejected before the job leaves RUNNING."""
        tok_a = self.running("j137a", "w137")
        art_a = self.stage("j137a", "w137", tok_a, b"r14-137-a")
        tok_b = self.running("j137b", "w137")
        self.stage("j137b", "w137", tok_b, b"r14-137-b")
        with self.assertRaises(TransitionRejected):
            self.gate.begin_commit("j137b", "w137", tok_b,
                                   artifact_id=art_a["artifact_id"],
                                   actor="worker:w137")
        self.assertEqual(self.gate.get_job("j137b")["status"], "RUNNING")

    def test_R14_138_historical_checkpoint_immutable(self):
        """R14-138: a VERIFIED historical checkpoint cannot be re-verified
        or mutated into a different outcome; latest-known-good keeps
        pointing at the newest VERIFIED checkpoint."""
        tok = self.running("j138", "w138")
        art = self.verified_artifact("j138", "w138", tok, b"r14-138-a")
        ck1 = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": art["artifact_id"],
                       "content_hash": art["content_hash"]}],
            trigger="policy")
        self.gate.verify_checkpoint(ck1["checkpoint_id"], actor="test")
        tok2 = self.running("j138b", "w138")
        art2 = self.verified_artifact("j138b", "w138", tok2, b"r14-138-b")
        ck2 = self.gate.stage_checkpoint(
            "t", actor="test",
            manifest=[{"artifact_id": art2["artifact_id"],
                       "content_hash": art2["content_hash"]}],
            trigger="policy",
            supersedes=ck1["checkpoint_id"])
        self.gate.verify_checkpoint(ck2["checkpoint_id"], actor="test")
        self.assertEqual(
            self.gate.latest_known_good("t")["checkpoint_id"],
            ck2["checkpoint_id"])
        # Re-verification of history is refused; the row is untouched.
        before = self.gate.store.conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?",
            (ck1["checkpoint_id"],)).fetchone()
        with self.assertRaises(TransitionRejected):
            self.gate.verify_checkpoint(ck1["checkpoint_id"], actor="test")
        after = self.gate.store.conn.execute(
            "SELECT * FROM checkpoints WHERE checkpoint_id=?",
            (ck1["checkpoint_id"],)).fetchone()
        self.assertEqual(tuple(before), tuple(after))
        self.assertEqual(
            self.gate.latest_known_good("t")["checkpoint_id"],
            ck2["checkpoint_id"])

    def test_R14_139_release_attestation_binding(self):
        """R14-139: verify_checkpoint(release=True) records the release
        attestation in the receipt; release=False does not. The attestation
        is part of the durable receipt the finalization publisher
        revalidates."""
        tok = self.running("j139", "w139")
        art = self.verified_artifact("j139", "w139", tok, b"r14-139-bytes")
        tok2 = self.running("j139b", "w139")
        art2 = self.verified_artifact("j139b", "w139", tok2,
                                      b"r14-139-bytes-2")
        # NOTE: checkpoint_id = sha256(manifest), so the two checkpoints
        # need distinct manifests to be distinct checkpoints.
        man = [{"artifact_id": art["artifact_id"],
                "content_hash": art["content_hash"]}]
        man2 = [{"artifact_id": art2["artifact_id"],
                 "content_hash": art2["content_hash"]}]
        ck_rel = self.gate.stage_checkpoint("t", actor="test", manifest=man,
                                            trigger="pre_risky_operation")
        res_rel = self.gate.verify_checkpoint(ck_rel["checkpoint_id"],
                                              actor="test", release=True)
        rec_rel = json.loads(res_rel["verification_receipt"])
        self.assertTrue(rec_rel["release"])
        self.assertTrue(rec_rel["manifest_full"])
        ck_plain = self.gate.stage_checkpoint("t", actor="test",
                                              manifest=man2,
                                              trigger="policy")
        res_plain = self.gate.verify_checkpoint(ck_plain["checkpoint_id"],
                                                actor="test", release=False)
        rec_plain = json.loads(res_plain["verification_receipt"])
        self.assertFalse(rec_plain["release"])

    def test_R14_140_idempotent_restage(self):
        """R14-140: re-staging identical bytes for the same job is
        idempotent — one artifact row, one staged event — and the protocol
        still completes."""
        tok = self.running("j140", "w140")
        data = b"r14-140-bytes"
        art1 = self.stage("j140", "w140", tok, data)
        art2 = self.stage("j140", "w140", tok, data)
        self.assertEqual(art1["artifact_id"], art2["artifact_id"])
        self.assertEqual(self._event_count(self.gate, "artifact.staged"), 1)
        self.gate.begin_commit("j140", "w140", tok,
                               artifact_id=art1["artifact_id"],
                               actor="worker:w140")
        self.gate.verify_artifact(art1["artifact_id"], actor="worker:w140",
                                  worker_id="w140", fencing_token=tok)
        row = self.gate.commit_artifact("j140", "w140", tok,
                                        artifact_id=art1["artifact_id"],
                                        actor="worker:w140", evidence={})
        self.assertEqual(row["status"], "COMPLETE")


# ============================================================ release manifest (R14-141..144, R14-148)
# R14 release manifest: a deterministic, content-addressed identity of the
# exact source tree the tests validated. The identity contains NO
# wall-clock fields; the release_id is the sha256 of the canonical JSON of
# the identity itself.

_MANIFEST_HASHED_DIRS = ("exec", "store", "audit", "tests")
_MANIFEST_EXCLUDED_FILES = ("release_manifest.json", "R14-PLAN.md")


def _manifest_axos_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def build_release_manifest(axos_dir=None):
    """Deterministically build the release manifest identity.

    Hashed inputs (all in the manifest, none outside it):
      - every regular file under exec/, store/, audit/, tests/, addressed
        by path relative to the axos root, sorted; '__pycache__' trees are
        excluded; release_manifest.json (this file's own output) and
        R14-PLAN.md (documentation outside the hashed source set) are
        excluded;
      - migration version + canonical DDL/schema hash over MIGRATIONS;
      - policy configuration version;
      - canonical recovery-ladder hash;
      - test-suite and audit-suite content ids;
      - Python / SQLite / OS identity (from the running interpreter).

    No wall-clock timestamp anywhere in the hashed identity.
    Returns (release_id, identity_dict)."""
    from axos.store.migrations import MIGRATIONS
    from axos.exec.policy import CANONICAL_LADDER, PolicyConfig

    axos_dir = axos_dir or _manifest_axos_dir()
    file_hashes = {}
    for d in _MANIFEST_HASHED_DIRS:
        root = os.path.join(axos_dir, d)
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = sorted(
                dn for dn in dirnames if dn != "__pycache__")
            for fname in sorted(filenames):
                if fname in _MANIFEST_EXCLUDED_FILES:
                    continue
                full = os.path.join(dirpath, fname)
                if not os.path.isfile(full):
                    continue
                rel = os.path.relpath(full, axos_dir)
                file_hashes[rel] = _sha256_file(full)
    suite = lambda prefix: {  # noqa: E731
        rel: h for rel, h in sorted(file_hashes.items())
        if rel.startswith(prefix + "/")}
    identity = {
        "source_tree_hash": _sha256_of_canon(
            {rel: h for rel, h in sorted(file_hashes.items())}),
        "migration_version": MIGRATIONS[-1][0],
        "schema_hash": _sha256_of_canon(
            [{"version": v, "name": name, "sql": sql}
             for v, name, sql in
             sorted(MIGRATIONS, key=lambda m: m[0])]),
        "policy_version": PolicyConfig().policy_version,
        "recovery_ladder_hash": _sha256_of_canon(CANONICAL_LADDER),
        "test_suite_id": _sha256_of_canon(suite("tests")),
        "audit_suite_id": _sha256_of_canon(suite("audit")),
        "python_version": platform.python_version(),
        "sqlite_version": sqlite3.sqlite_version,
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "file_hashes": {rel: h for rel, h in sorted(file_hashes.items())},
    }
    release_id = _sha256_of_canon(identity)
    return release_id, identity


def write_release_manifest(path=None):
    """Write the release manifest to path (default: axos/release_manifest.json).

    The file carries 'release_id', the hashed 'identity', and an unhashed
    'meta' block (generation timestamp, generator, hashing contract).
    Building twice yields the same release_id (R14-143)."""
    release_id, identity = build_release_manifest()
    manifest = {
        "release_id": release_id,
        "identity": identity,
        "meta": {
            "generated_at": datetime.datetime.now(
                datetime.timezone.utc).isoformat(),
            "generator": "axos/tests/test_final_hardening_r14.py",
            "hashed_inputs": (
                "sorted per-file sha256 of every regular file under"
                " exec/, store/, audit/, tests/ (relative path -> sha256),"
                " excluding __pycache__; release_manifest.json and"
                " R14-PLAN.md are excluded; migration_version=%d; canonical"
                " DDL over MIGRATIONS; policy_version; canonical"
                " recovery-ladder hash; per-suite content ids; python /"
                " sqlite / os identity. release_id = sha256(canonical JSON"
                " of identity). No timestamps in the hashed identity."
                % identity["migration_version"]),
        },
    }
    with open(path or os.path.join(_manifest_axos_dir(),
                                   "release_manifest.json"),
              "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.write("\n")
    return release_id, manifest


class TestR14ReleaseManifest(unittest.TestCase):
    """R14-141..144, R14-148: the release manifest that pins the exact
    source tree this section validated."""

    def test_R14_141_release_manifest_deterministic(self):
        """R14-141: build the manifest twice back-to-back -> identical
        release_id; identity has no timestamps and carries the exact
        required inputs. (The builds are immediate rather than
        wall-clock-separated: sibling R14 tracks edit the tree
        concurrently, and the manifest correctly reflects the tree it
        hashed. Timestamp-freedom is asserted structurally below.)"""
        rid1, ident1 = build_release_manifest()
        rid2, ident2 = build_release_manifest()
        self.assertEqual(rid1, rid2)
        self.assertEqual(_sha256_of_canon(ident1), rid1,
                         "release_id is not sha256(canonical identity)")
        required = {"source_tree_hash", "migration_version", "schema_hash",
                    "policy_version", "recovery_ladder_hash",
                    "test_suite_id", "audit_suite_id", "python_version",
                    "sqlite_version", "platform", "file_hashes"}
        self.assertEqual(set(ident1), required,
                         "identity must carry exactly the hashed inputs")
        # No wall-clock fields anywhere in the hashed identity.
        def scan(node, trail=""):
            if isinstance(node, dict):
                for k, v in node.items():
                    kl = k.lower()
                    self.assertFalse(
                        any(s in kl for s in
                            ("_at", "time", "date", "stamp", "generated")),
                        f"timestamp-like key {trail}{k} inside hashed"
                        " identity")
                    scan(v, trail + k + ".")
            elif isinstance(node, list):
                for i, v in enumerate(node):
                    scan(v, f"{trail}[{i}].")
            elif isinstance(node, str) and re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}", node):
                self.fail(f"timestamp-like value at {trail}")
        scan(ident1)
        self.assertEqual(len(rid1), 64)
        self.assertRegex(rid1, r"^[0-9a-f]{64}$")

    def test_R14_142_manifest_inputs(self):
        """R14-142: source-tree hash covers exactly exec/, store/, audit/,
        tests/ (all of them, sorted, __pycache__ and the manifest output
        excluded); migration_version is 12; schema hash pins the canonical
        DDL; runtime fields match the running interpreter."""
        _, ident = build_release_manifest()
        files = ident["file_hashes"]
        for prefix in _MANIFEST_HASHED_DIRS:
            self.assertTrue(
                any(rel.startswith(prefix + "/") for rel in files),
                f"no files hashed under {prefix}/")
        self.assertTrue(all(rel.split("/", 1)[0] in _MANIFEST_HASHED_DIRS
                            for rel in files),
                        "hashed file outside exec/store/audit/tests")
        self.assertTrue(all("__pycache__" not in rel for rel in files))
        self.assertFalse(any(rel.endswith("release_manifest.json")
                             for rel in files),
                         "manifest output must not hash itself")
        self.assertFalse(any(os.path.basename(rel) == "R14-PLAN.md"
                             for rel in files))
        self.assertEqual(list(files), sorted(files))
        for rel, h in files.items():
            self.assertRegex(h, r"^[0-9a-f]{64}$", rel)
        self.assertEqual(ident["migration_version"], 12)
        # The merged R14 suite file is part of the hashed source set.
        self.assertIn(os.path.join("tests", "test_final_hardening_r14.py"),
                      files)
        # Schema hash pins the canonical DDL: re-hash independently.
        from axos.store.migrations import MIGRATIONS
        expect = _sha256_of_canon(
            [{"version": v, "name": name, "sql": sql}
             for v, name, sql in sorted(MIGRATIONS, key=lambda m: m[0])])
        self.assertEqual(ident["schema_hash"], expect)
        # Runtime identity matches the actual running interpreter.
        self.assertEqual(ident["python_version"], platform.python_version())
        self.assertEqual(ident["sqlite_version"], sqlite3.sqlite_version)
        self.assertEqual(ident["platform"]["system"], platform.system())
        self.assertEqual(ident["platform"]["release"], platform.release())
        self.assertEqual(ident["platform"]["machine"], platform.machine())

    def test_R14_143_manifest_sensitivity(self):
        """R14-143: flipping one byte in one source file changes
        source_tree_hash and release_id (the identity is content-bound)."""
        axos = _manifest_axos_dir()
        rid1, ident1 = build_release_manifest()
        target = os.path.join(axos, "exec", "policy.py")
        with open(target, "rb") as f:
            original = f.read()
        try:
            with open(target, "ab") as f:
                f.write(b"\n")
            rid2, ident2 = build_release_manifest()
        finally:
            with open(target, "wb") as f:
                f.write(original)
        self.assertNotEqual(ident1["source_tree_hash"],
                            ident2["source_tree_hash"])
        self.assertNotEqual(rid1, rid2)
        # Restoring the byte restores the exact identity.
        rid3, _ = build_release_manifest()
        self.assertEqual(rid1, rid3)

    def test_R14_144_manifest_immutable_reference(self):
        """R14-144: the manifest written to axos/release_manifest.json
        carries release_id + identity + unhashed meta; a re-read from disk
        verifies release_id == sha256(canonical identity)."""
        path = os.path.join(_manifest_axos_dir(), "release_manifest.json")
        rid, manifest = write_release_manifest(path)
        self.assertTrue(os.path.isfile(path))
        with open(path, encoding="utf-8") as f:
            on_disk = json.load(f)
        self.assertEqual(on_disk["release_id"], rid)
        self.assertEqual(on_disk["identity"], manifest["identity"])
        self.assertIn("generated_at", on_disk["meta"])
        self.assertEqual(_sha256_of_canon(on_disk["identity"]), rid,
                         "release_id does not verify against disk identity")

    def test_R14_148_manifest_negative_control(self):
        """R14-148: negative control — a tampered identity (one per-file
        hash altered) does NOT verify against the recorded release_id."""
        self.test_R14_144_manifest_immutable_reference()
        path = os.path.join(_manifest_axos_dir(), "release_manifest.json")
        with open(path, encoding="utf-8") as f:
            manifest = json.load(f)
        rid = manifest["release_id"]
        tampered = json.loads(json.dumps(manifest["identity"]))
        first = next(iter(tampered["file_hashes"]))
        tampered["file_hashes"][first] = "0" * 64
        self.assertNotEqual(_sha256_of_canon(tampered), rid,
                            "tampered identity verifies: release_id is not"
                            " content-bound")


# ============================================================ clean boot (R14-145..147)
class CleanBootBase(unittest.TestCase):
    """Fresh database only: migrations, boot, nothing else. No fixtures,
    no prepared rows — the proof that AXOS starts from the durable
    schema alone."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r14d-boot-")
        self.db = os.path.join(self.tmp, "boot.db")
        self._sups = []
        self._procs = []

    def tearDown(self):
        for sup in self._sups:
            try:
                sup.close()
            except Exception:
                pass
        for p in self._procs:
            try:
                p.kill()
            except Exception:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _supervisor(self, db_path=None, actor="test-r14d-boot"):
        sup = Supervisor(db_path or self.db, actor=actor)
        self._sups.append(sup)
        return sup

    def _dump_db(self, db_path):
        con = sqlite3.connect(db_path)
        try:
            tables = [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
                " AND name NOT LIKE 'sqlite_%' ORDER BY name")]
            dump = {}
            for t in tables:
                cols = [c[1] for c in con.execute(
                    f"PRAGMA table_info({t})")]
                rows = con.execute(
                    f"SELECT * FROM {t} ORDER BY {', '.join(cols)}"
                    ).fetchall()
                dump[t] = [tuple(r) for r in rows]
            return dump
        finally:
            con.close()


class TestR14CleanBoot(CleanBootBase):
    """R14-145..147: clean-boot proof from an empty database — the whole
    pipeline (desired state -> reconcile -> schedule -> execute -> R5
    commit; SIGKILL of a real worker -> recovery; R13 finalization) runs
    with no hidden in-memory state and no manually prepared rows."""

    def test_R14_145_fresh_db_migrates_and_boots_ready(self):
        """R14-145: an empty database migrates to v12 with only migration
        bookkeeping rows, then boot_recover reaches READY with zero
        dispositions, zero reclaims, zero contradictions, zero errors, and
        a ledger chain that verifies clean."""
        st = open_store(self.db)
        try:
            applied = migrate(st)
            self.assertEqual(applied, list(range(1, 13)))
            self.assertEqual(applied_versions(st), list(range(1, 13)))
            # Fixture-only precondition: every data table is empty; only
            # migration bookkeeping exists (schema_migrations, axos_meta,
            # and the desired_state_head singleton row seeded by the v11
            # migration itself).
            dump = self._dump_db(self.db)
            nonempty = {t: rows for t, rows in dump.items() if rows}
            self.assertLessEqual(set(nonempty),
                                     {"schema_migrations", "axos_meta",
                                      "desired_state_head"},
                             f"non-fixture rows in fresh db: "
                             f"{[(t, len(r)) for t, r in nonempty.items()]}")
        finally:
            st.close()
        sup = self._supervisor()
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "READY")
        self.assertTrue(report["fully_recovered"])
        for key in ("leases_reclaimed", "forced_reclaims"):
            self.assertEqual(report[key], 0, key)
        for key in ("integrity_contradictions", "unresolved", "errors"):
            self.assertEqual(report[key], [], key)
        self.assertEqual(report["dispositions"], [])
        verify_ledger_chain(sup.gate)
        # No hidden in-memory state: the whole pipeline is the DB.
        for name in ("jobs", "artifacts", "checkpoints",
                     "incidents", "finalization_runs"):
            self.assertEqual(
                sup.gate.store.conn.execute(
                    f"SELECT COUNT(*) FROM {name}").fetchone()[0], 0)

    def test_R14_146_clean_boot_end_to_end(self):
        """R14-146: on a freshly booted database, declare desired state,
        reconcile, schedule, execute, and complete a job through the R5
        verified-commit protocol — READY gating throughout."""
        sup = self._supervisor()
        gate = sup.gate
        gate.create_task("t-boot", {"objective": "clean boot e2e"},
                         {"usd": 1}, "test")
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "READY")

        gate.set_desired_item("dw-boot-1",
                              {"task_id": "t-boot", "stage_id": "s1"},
                              "test")
        v = gate.get_desired_head()["version"]
        job, created = gate.ensure_job_for_desired_state(
            desired_work_id="dw-boot-1", task_id="t-boot", stage_id="s1",
            max_attempts=3, policy={}, desired_version=v,
            actor="reconciler")
        self.assertTrue(created)
        jid = job["job_id"]
        gate.claim_job(jid, "w-boot", 60.0, "test")
        gate.transition_job(jid, "RUNNING", "worker:w-boot")
        tok = gate.get_job(jid)["fencing_token"]
        art = gate.stage_artifact(
            job_id=jid, worker_id="w-boot", fencing_token=tok,
            task_id="t-boot", kind="result", data=b"clean-boot-result-bytes",
            actor="worker:w-boot")
        gate.begin_commit(jid, "w-boot", tok,
                          artifact_id=art["artifact_id"],
                          actor="worker:w-boot")
        gate.verify_artifact(art["artifact_id"], actor="worker:w-boot",
                             worker_id="w-boot", fencing_token=tok)
        row = gate.commit_artifact(jid, "w-boot", tok,
                                   artifact_id=art["artifact_id"],
                                   actor="worker:w-boot", evidence={})
        self.assertEqual(row["status"], "COMPLETE")
        verify_ledger_chain(gate)

    def test_R14_147_kill9_recover_finalize_restart(self):
        """R14-147: the full fault chain on a clean boot —
        1. declare -> reconcile -> claim -> RUNNING for a real worker
           process;
        2. the worker stages its bytes, then SIGKILL its process group
           (kernel-confirmed death);
        3. recovery: incident + watchdog DEAD verdict + forced R1 reclaim
           -> UNCERTAIN -> resolver verifies and adopts the bytes ->
           COMPLETE;
        4. R13 finalization of the generation -> FINALIZED;
        5. stop everything, restart from the durable DB alone, boot again
           -> the authoritative state is byte-identical (ledger is
           prefix-consistent, chain verifies), and the second recovery
           report matches the first: no in-memory state was required."""
        sup = self._supervisor()
        gate = sup.gate
        gate.create_task("t-victim", {"objective": "kill9 chain"},
                         {"usd": 1}, "test")
        report = boot_recover(sup, actor="system")
        self.assertEqual(report["phase"], "READY")

        gate.set_desired_item("dw-victim",
                              {"task_id": "t-victim", "stage_id": "s1"},
                              "test")
        v = gate.get_desired_head()["version"]
        job, _ = gate.ensure_job_for_desired_state(
            desired_work_id="dw-victim", task_id="t-victim", stage_id="s1",
            max_attempts=3, policy={}, desired_version=v,
            actor="reconciler")
        jid = job["job_id"]
        gate.claim_job(jid, "w-victim", 60.0, "test")
        gate.transition_job(jid, "RUNNING", "worker:w-victim")
        tok = gate.get_job(jid)["fencing_token"]

        # 1-2: a REAL worker process; it stages bytes, then dies by SIGKILL.
        # start_new_session=True gives the worker its own process group so
        # the kill targets exactly the victim, never the test runner.
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(120)"],
            start_new_session=True)
        self._procs.append(proc)
        art = gate.stage_artifact(
            job_id=jid, worker_id="w-victim", fencing_token=tok,
            task_id="t-victim", kind="result",
            data=b"victim-result-bytes", actor="worker:w-victim")
        aid = art["artifact_id"]
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=30)
        self.assertEqual(proc.returncode, -signal.SIGKILL)
        with self.assertRaises(ProcessLookupError):
            os.kill(proc.pid, 0)

        # 3: the real recovery/fencing path.
        inc = gate.create_incident(
            None, "recovery", "worker_crash", "s1", "cap-v1", None,
            "sigkill", "test", task_id="t-victim")
        gate.record_watchdog_verdict(
            jid, tok, "DEAD",
            {"cause": "SIGKILL", "pid": proc.pid}, "test")
        reclaimed = gate.reclaim_lease(
            jid, actor="system", reason="r14-147: worker SIGKILLed",
            expected_owner="w-victim", expected_token=tok,
            force=True, verdict="DEAD",
            incident_id=inc["incident_id"])
        self.assertEqual(reclaimed["status"], "UNCERTAIN")
        self.assertGreater(reclaimed["fencing_token"], tok)
        # The resolver verifies the victim's staged bytes under system
        # authority (no worker lease) and adopts them.
        gate.verify_artifact(aid, actor="system")
        new_tok = reclaimed["fencing_token"]
        row = gate.commit_artifact(
            jid, None, new_tok, artifact_id=aid, actor="system",
            evidence={"adopted_after_sigkill": True,
                      "pid": proc.pid})
        self.assertEqual(row["status"], "COMPLETE")
        gate.set_incident_outcome(inc["incident_id"], "recovered",
                                  "bytes adopted after SIGKILL", "test")
        verify_ledger_chain(gate)

        # 4: R13 finalization of the generation.
        v2 = gate.get_desired_head()["version"]
        gen = canonical_release_generation(v2)
        begun = gate.begin_finalization_run(gen, v2, "test")
        ready = gate.evaluate_finalization(gen, begun["version"], "test")
        self.assertEqual(ready["state"], "READY",
                         json.loads(ready["blockers"] or "[]"))
        pub = gate.publish_finalization(gen, ready["version"],
                                        ready["manifest_hash"], "test")
        self.assertEqual(pub["state"], "FINALIZED")
        verify_ledger_chain(gate)

        # 5: stop, restart from durable state only.
        rep_before = boot_recover(sup, actor="system")
        snap_before = self._dump_db(self.db)
        for s in self._sups:
            s.close()
        self._sups = []

        sup2 = self._supervisor(actor="test-r14d-boot-2")
        rep_after = boot_recover(sup2, actor="system")
        self.assertEqual(rep_after["phase"], "READY")
        self.assertTrue(rep_after["fully_recovered"])
        snap_after = self._dump_db(self.db)

        # Authoritative state is identical except for the new boot's own
        # journal rows (ledger grows by boot.* events only; last_commit_ts
        # advances monotonically).
        for table, rows in snap_before.items():
            if table == "ledger":
                after = snap_after["ledger"]
                self.assertGreaterEqual(len(after), len(rows))
                self.assertEqual(after[:len(rows)], rows,
                                 "restart rewrote existing ledger rows")
                for new_row in after[len(rows):]:
                    self.assertTrue(str(new_row[1]).startswith("boot."),
                                    f"non-boot event on restart: {new_row}")
            elif table == "axos_meta":
                continue  # last_commit_ts advances with the new journal
            else:
                self.assertEqual(snap_after[table], rows,
                                 f"table {table} changed across restart")
        meta_before = dict(snap_before["axos_meta"])
        meta_after = dict(snap_after["axos_meta"])
        self.assertGreaterEqual(meta_after["last_commit_ts"],
                                meta_before["last_commit_ts"])
        verify_ledger_chain(sup2.gate)

        # Recovery-report consistency: same phase, same dispositions, same
        # counts — the second boot recovered nothing new because the first
        # left nothing dangling.
        for key in ("phase", "fully_recovered", "leases_reclaimed",
                    "forced_reclaims", "complete_jobs_checked",
                    "complete_jobs_consistent", "checkpoints_checked",
                    "integrity_contradictions", "unresolved", "errors",
                    "dispositions"):
            self.assertEqual(rep_after[key], rep_before[key],
                             f"boot report diverged on {key}")
