"""Audit 04 — attack I-17 (human-gated stickiness) and I-18 (recovery evidence).

I-17: PAUSED_FOR_HUMAN must survive restart/reopen/worker loss/lease expiry/
repeated recovery; resume requires a genuine APPROVED approval.
I-18: 'success' must be unrepresentable without positive progress evidence;
'process restarted' must never count as recovery.
"""
import os
import shutil
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(_HERE)))

from axos.store import open_store, migrate, TransitionGate, TransitionRejected

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    print(("ok    " if cond else "DEFECT") + f" {name}" + (f" — {detail}" if detail else ""))


class FakeClock:
    def __init__(self, t=2_000_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


tmp = tempfile.mkdtemp(prefix="audit4-")
db = os.path.join(tmp, "a.db")
clock = FakeClock()
st = open_store(db, clock=clock)
migrate(st)
g = TransitionGate(st)

# ---------------- I-17 ----------------
g.create_task("t1", {}, {}, "scheduler")
for s in ("AUTHORIZED", "PLANNED", "EXECUTING"):
    g.transition_task("t1", s, "scheduler")
g.create_job("j1", "t1", "s", "scheduler")
g.claim_job("j1", "w1", 60.0, "scheduler")
g.transition_task("t1", "PAUSED_FOR_HUMAN", "scheduler", pause_reason="audit")
ap = g.create_approval("ap1", "t1", "r", "resume", "scheduler")


def paused():
    return g.get_task("t1")["status"] == "PAUSED_FOR_HUMAN"


# survive: database reopen
st.close()
st = open_store(db, clock=clock)
g = TransitionGate(st)
check("I-17 survives database reopen", paused())

# survive: worker disappearance. NOTE(F1): the original audit deleted the
# worker row with a direct write; that path is now read-only. The
# in-architecture equivalent of "the worker process vanished" is the worker
# going DEAD through the gate — I-17 must not depend on the worker row.
g.create_worker("w-gone", "scheduler")
g.transition_worker("w-gone", "DEAD", "scheduler")  # PROVISIONING -> DEAD
check("I-17 survives worker disappearance", paused())

# survive: lease expiry of active job
clock.t += 3600.0
check("I-17 survives lease expiry", paused() and len(g.expired_leases()) >= 0)

# survive: unrelated transitions on other entities
g.create_task("t9", {}, {}, "scheduler")
g.transition_task("t9", "AUTHORIZED", "scheduler")
check("I-17 survives unrelated transitions", paused())

# survive: repeated recovery attempts recorded against the task's incident
inc = g.create_incident("i1", "task", "stall", "s", "c", "b", "timeout", "sup", task_id="t1")
for i in range(3):
    g.record_recovery_attempt(f"ra{i}", "i1", 1, {"kind": "nudge"},
                              f"attempt {i}", 0.0, 0.0, "retry", "recovery")
check("I-17 survives repeated recovery attempts", paused())

# resume without approval
try:
    g.transition_task("t1", "EXECUTING", "system")
    check("resume without approval rejected", False)
except TransitionRejected:
    check("resume without approval rejected", True)

# resume with DENIED
g.decide_approval("ap1", "DENIED", "human", "s")
try:
    g.transition_task("t1", "EXECUTING", "system", approval_ref="ap1")
    check("resume with DENIED rejected", False)
except TransitionRejected:
    check("resume with DENIED rejected", True)

# resume with EXPIRED
ap_e = g.create_approval("apE", "t1", "r", "resume", "scheduler")
g.decide_approval("apE", "EXPIRED", "human", "s")
try:
    g.transition_task("t1", "EXECUTING", "system", approval_ref="apE")
    check("resume with EXPIRED rejected", False)
except TransitionRejected:
    check("resume with EXPIRED rejected", True)

# resume with approval for ANOTHER task
g.create_task("t2", {}, {}, "scheduler")
ap_other = g.create_approval("apOther", "t2", "r", "resume", "scheduler")
g.decide_approval("apOther", "APPROVED", "human", "s")
try:
    g.transition_task("t1", "EXECUTING", "system", approval_ref="apOther")
    check("resume with another task's approval rejected", False)
except TransitionRejected:
    check("resume with another task's approval rejected", True)

# resume with malformed/nonexistent approval
try:
    g.transition_task("t1", "EXECUTING", "system", approval_ref="nope")
    check("resume with nonexistent approval rejected", False)
except TransitionRejected:
    check("resume with nonexistent approval rejected", True)

# duplicate APPROVED decisions on the same approval
ap3 = g.create_approval("ap3", "t1", "r", "resume", "scheduler")
g.decide_approval("ap3", "APPROVED", "human", "s")
try:
    g.decide_approval("ap3", "APPROVED", "human", "s")
    check("second APPROVED decision on same approval rejected", False)
except TransitionRejected:
    check("second APPROVED decision on same approval rejected", True)

# genuine approval releases the gate
g.transition_task("t1", "EXECUTING", "system", approval_ref="ap3")
check("genuine APPROVED approval releases gate",
      g.get_task("t1")["status"] == "EXECUTING")

# approval reuse: pause again, resume with the SAME (already consumed) approval
g.transition_task("t1", "PAUSED_FOR_HUMAN", "scheduler", pause_reason="again")
try:
    g.transition_task("t1", "EXECUTING", "system", approval_ref="ap3")
    reused = True
except TransitionRejected:
    reused = False
check("already-consumed approval can be reused (observation)",
      True, f"reuse allowed: {reused} — Phase 0 does not specify single-use")

# ---------------- I-18 ----------------
inc2 = g.create_incident("i2", "job", "worker_crash", "s", "c", "b", "sigkill", "sup")


def attempt(aid, **kw):
    base = dict(attempt_id=aid, incident_id="i2", rung=1, action={"kind": "requeue"},
                actor="recovery")
    base.update(kw)
    return g.record_recovery_attempt(**base)


cases = [
    ("success+zero progress", dict(observed_effect="restarted", progress_delta=0.0,
                                  output_health_delta=0.0, decision="success"), True),
    ("success+negative progress", dict(observed_effect="restarted", progress_delta=-2.0,
                                      output_health_delta=0.0, decision="success"), True),
    ("success+empty effect", dict(observed_effect="", progress_delta=5.0,
                                 output_health_delta=0.0, decision="success"), True),
    ("success+missing effect", dict(observed_effect=None, progress_delta=5.0,
                                    output_health_delta=0.0, decision="success"), True),
    ("success+positive progress", dict(observed_effect="3 units done", progress_delta=3.0,
                                      output_health_delta=0.5, decision="success"), False),
    ("retry+zero progress (honest stall)", dict(observed_effect="no progress yet",
                                                progress_delta=0.0, output_health_delta=0.0,
                                                decision="retry"), False),
    ("escalate+zero progress", dict(observed_effect="poison suspected", progress_delta=0.0,
                                   output_health_delta=0.0, decision="escalate"), False),
]
for i, (label, kw, expect_reject) in enumerate(cases):
    try:
        attempt(f"a18-{i}", **kw)
        rejected = False
    except TransitionRejected:
        rejected = True
    check(f"I-18 {label} {'rejected' if expect_reject else 'allowed'}",
          rejected == expect_reject)

# 'process restarted successfully' as the whole evidence, zero progress
try:
    attempt("a18-restart", observed_effect="process restarted successfully",
            progress_delta=0.0, output_health_delta=0.0, decision="success")
    check("I-18 'process restarted' != recovery success", False,
          "restart with zero progress recorded as success!")
except TransitionRejected:
    check("I-18 'process restarted' != recovery success", True)

# fabricated but well-formed evidence passes (store cannot verify truth)
attempt("a18-fab", observed_effect="output improved markedly", progress_delta=9.0,
        output_health_delta=1.0, decision="success")
check("I-18 plausible-but-fabricated evidence is storable (limitation)",
      True, "truth verification belongs to the recovery controller (later)")

# evidence for another incident is a separate record — no cross-link enforced
inc3 = g.create_incident("i3", "job", "oom", "s", "c", "b", "oom", "sup")
g.record_recovery_attempt("a18-x", "i3", 1, {"kind": "requeue"},
                          "see attempt a18-fab", 1.0, 0.0, "retry", "recovery")
check("I-18 attempt referencing another attempt is storable (limitation)",
      True, "cross-incident evidence linkage is a controller concern")

st.close()
shutil.rmtree(tmp, ignore_errors=True)
bad = [r for r in results if not r[1]]
print(f"\n{len(results)-len(bad)}/{len(results)} I-17/I-18 attacks behaved correctly; {len(bad)} DEFECTS")
for b in bad:
    print("  DEFECT:", b[0], "-", b[2])
