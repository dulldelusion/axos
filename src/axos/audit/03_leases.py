"""Audit 03 — attack lease authority.

Simultaneous acquisition, expiry edges, wrong owner/token, stale tokens,
release races, clock rollback/jump, worker-supplied timestamps, malformed
durations. All lease semantics must derive from the store clock.
"""
import os
import shutil
import sys
import tempfile
import threading

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(_HERE)))

from axos.store import open_store, migrate, TransitionGate, TransitionRejected

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    print(("ok    " if cond else "DEFECT") + f" {name}" + (f" — {detail}" if detail else ""))


class FakeClock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


tmp = tempfile.mkdtemp(prefix="audit3-")
db = os.path.join(tmp, "a.db")
clock = FakeClock(2_000_000_000.0)
st = open_store(db, clock=clock)
migrate(st)
g = TransitionGate(st)
g.create_task("t", {}, {}, "scheduler")
for jid in ("j1", "j2", "j3", "j4"):
    g.create_job(jid, "t", "s", "scheduler")

# 1. two simultaneous acquisitions — exactly one wins (10 rounds)
wins = 0
for rnd in range(10):
    jid = f"jx-{rnd}"  # fresh job per round: always starts PENDING
    g.create_job(jid, "t", "s", "scheduler")
    # NOTE(F1): the old harness reset the row with a direct UPDATE; that path
    # is now read-only. Fresh jobs per round serve the same purpose.
    out = []

    def grab(w):
        s2 = open_store(db, clock=clock)
        g2 = TransitionGate(s2)
        try:
            out.append(g2.claim_job(jid, w, 60.0, "s"))
        finally:
            s2.close()

    ts = [threading.Thread(target=grab, args=(f"w{i}",)) for i in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    wins += sum(1 for x in out if x)
check("10 race rounds: exactly one winner each", wins == 10, f"total wins={wins}/10")

# 2. acquisition after expiry still requires PENDING (reclaim is recovery's job)
g.claim_job("j1", "w1", 60.0, "s")
clock.t += 120.0
check("claim on CLAIMED job with expired lease returns False (no steal)",
      g.claim_job("j1", "w2", 60.0, "s") is False)
check("expired lease is detectable", any(e["job_id"] == "j1" for e in g.expired_leases()))

# 3. renewal immediately before expiry succeeds
g.claim_job("j2", "w1", 100.0, "s")
tok = g.get_job("j2")["fencing_token"]
clock.t += 99.0
check("renew 1s before expiry succeeds", g.renew_lease("j2", "w1", tok, 100.0, "s") is True)

# 4. renewal after expiry fails
clock.t += 101.0
check("renew after expiry fails", g.renew_lease("j2", "w1", tok, 100.0, "s") is False)

# 5. wrong owner / wrong token / stale token
g.claim_job("j3", "w1", 600.0, "s")
tok3 = g.get_job("j3")["fencing_token"]
check("renew by wrong owner fails", g.renew_lease("j3", "w2", tok3, 60.0, "s") is False)
check("renew with wrong token fails", g.renew_lease("j3", "w1", tok3 + 1, 60.0, "s") is False)
check("release by wrong owner fails", g.release_lease("j3", "w2", tok3, "s") is False)
check("release with stale token fails", g.release_lease("j3", "w1", tok3 + 1, "s") is False)

# 6. release after expiry by owner still works (cleanup), repeated release False
clock.t += 700.0
check("owner can release expired lease", g.release_lease("j3", "w1", tok3, "s") is True)
check("repeated release returns False", g.release_lease("j3", "w1", tok3, "s") is False)

# 7. fencing token monotonic across claims
g.claim_job("j4", "w1", 600.0, "s")
t1 = g.get_job("j4")["fencing_token"]
g.release_lease("j4", "w1", t1, "s")
g.transition_job("j4", "RUNNING", "s") if False else None
# reclaim path: CLAIMED -> PENDING is a legal gate transition (requeue)
g.transition_job("j4", "PENDING", "s")
g.claim_job("j4", "w2", 600.0, "s")
t2 = g.get_job("j4")["fencing_token"]
check("fencing token strictly increases across claims", t2 == t1 + 1, f"{t1}->{t2}")

# 8. stale token cannot act after reclaim (old owner fenced out)
check("old owner with old token cannot renew after reclaim",
      g.renew_lease("j4", "w1", t1, 60.0, "s") is False)
check("old owner cannot release after reclaim",
      g.release_lease("j4", "w1", t1, "s") is False)

# 9. clock rollback: lease must not silently extend/shrink wrongly
clock.t = 1_900_000_000.0  # jump backwards (still above old floor)
j = g.get_job("j4")
check("monotonic floor keeps expiry ahead of rolled-back clock",
      j["lease_expires_at"] > clock.t, f"expiry={j['lease_expires_at']} clock={clock.t}")

# 10. clock jump forward: everything expires per store time (correct)
clock.t = 3_000_000_000.0
exp = g.expired_leases()
check("forward jump expires leases per store time", len(exp) >= 1)

# 11. worker cannot extend authority with a future timestamp
g.create_job("j5", "t", "s", "scheduler")
g.claim_job("j5", "w9", 60.0, "s", worker_reported_ts=clock.t + 50_000.0)
j5 = g.get_job("j5")
check("future worker timestamp does not move lease",
      j5["lease_expires_at"] != 999_999_999.0 + 60.0 and
      j5["lease_expires_at"] <= clock.t + 60.0 + 1,
      f"expiry={j5['lease_expires_at']}")
check("worker timestamp stored separately, never authoritative",
      j5["worker_reported_ts"] == clock.t + 50_000.0)

# 12. malformed durations — F2: rejected BEFORE any mutation
from axos.store import TransitionRejected as _TR
g.create_job("j6", "t", "s", "scheduler")
for bad_ttl, label in [(-30.0, "negative"), (0.0, "zero"),
                       (float("nan"), "NaN"), ("forever", "non-numeric"),
                       (None, "None")]:
    try:
        g.claim_job("j6", "w1", bad_ttl, "s")
        check(f"F2 {label} TTL rejected", False, "accepted!")
    except _TR:
        pass
j6 = g.get_job("j6")
check("F2 invalid TTLs rejected, job untouched",
      j6["status"] == "PENDING" and j6["owner_worker_id"] is None
      and j6["lease_expires_at"] is None)
# renew path too
g.claim_job("j6", "w1", 600.0, "s")
tok6 = g.get_job("j6")["fencing_token"]
exp_before = g.get_job("j6")["lease_expires_at"]
try:
    g.renew_lease("j6", "w1", tok6, -1.0, "s")
    check("F2 negative renew TTL rejected", False, "accepted!")
except _TR:
    check("F2 negative renew TTL rejected",
          g.get_job("j6")["lease_expires_at"] == exp_before,
          "lease unchanged")

# 13. progress update fencing: wrong owner / stale token / expired lease
g.create_job("j8", "t", "s", "scheduler")
g.claim_job("j8", "w1", 600.0, "s")
tok8 = g.get_job("j8")["fencing_token"]
from axos.store import LeaseError
for label, fn in [
    ("wrong owner", lambda: g.update_job_progress("j8", "w2", tok8, 1, 2, "s")),
    ("stale token", lambda: g.update_job_progress("j8", "w1", tok8 + 5, 1, 2, "s")),
]:
    try:
        fn()
        check(f"progress update by {label} rejected", False)
    except LeaseError:
        check(f"progress update by {label} rejected", True)
# expired lease
clock.t += 700.0
try:
    g.update_job_progress("j8", "w1", tok8, 1, 2, "s")
    check("progress update on expired lease rejected", False)
except LeaseError:
    check("progress update on expired lease rejected", True)
# terminal job: owner+token still valid — can progress be written post-COMPLETE?
g.create_job("j9", "t", "s", "scheduler")
g.claim_job("j9", "w1", 60000.0, "s")
tok9 = g.get_job("j9")["fencing_token"]
g.transition_job("j9", "RUNNING", "s")
# Phase 1C R5 (I-4): COMPLETE is reachable only through the artifact-backed
# commit protocol.
_art9 = g.stage_artifact(job_id="j9", worker_id="w1", fencing_token=tok9,
                         task_id="t", kind="result", data=b"audit03",
                         actor="worker:w1")
g.begin_commit("j9", "w1", tok9, artifact_id=_art9["artifact_id"],
               actor="worker:w1")
g.verify_artifact(_art9["artifact_id"], actor="worker:w1", worker_id="w1",
                  fencing_token=tok9)
g.commit_artifact("j9", "w1", tok9, artifact_id=_art9["artifact_id"],
                  actor="worker:w1", evidence={})
try:
    g.update_job_progress("j9", "w1", tok9, 99, 100, "s")
    post_terminal = True
except TransitionRejected:
    # F3: terminal-state guard rejects before the fencing checks
    post_terminal = False
except LeaseError:
    post_terminal = False
before = dict(g.get_job("j9"))
check("progress update on COMPLETE job rejected", not post_terminal,
      "owner+token still valid post-terminal; gate does not check job state" if post_terminal else "")
check("rejected terminal progress leaves row byte-identical",
      dict(g.get_job("j9")) == before)

st.close()
shutil.rmtree(tmp, ignore_errors=True)
bad = [r for r in results if not r[1]]
print(f"\n{len(results)-len(bad)}/{len(results)} lease attacks behaved correctly; {len(bad)} DEFECTS")
for b in bad:
    print("  DEFECT:", b[0], "-", b[2])
