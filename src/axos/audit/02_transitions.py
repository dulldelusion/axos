"""Audit 02 — attack the transition graph.

For every entity: enumerate legal states/transitions, attempt every obvious
illegal transition (via the gate), terminal-state escape, replay, and
malformed evidence. Verify invalid transitions leave state unchanged.
"""
import os
import shutil
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(_HERE)))

from axos.store import open_store, migrate, TransitionGate, TransitionRejected
from axos.store import transitions as T

tmp = tempfile.mkdtemp(prefix="audit2-")
db = os.path.join(tmp, "a.db")
st = open_store(db)
migrate(st)
g = TransitionGate(st)

results = []


def attack(name, fn, expect_reject=True, state_check=None):
    before = state_check() if state_check else None
    try:
        fn()
        rejected = False
    except TransitionRejected:
        rejected = True
    except Exception as e:
        results.append((name, "ERROR", f"unexpected {type(e).__name__}: {e}"))
        print(f"ERROR {name}: unexpected {type(e).__name__}: {e}")
        return
    after = state_check() if state_check else None
    if expect_reject:
        ok = rejected and (before == after)
        detail = "" if ok else (
            f"NOT rejected!" if not rejected else f"state changed {before}->{after}")
    else:
        ok = not rejected
        detail = "" if ok else "wrongly rejected"
    results.append((name, "ok" if ok else "DEFECT", detail))
    print(("ok    " if ok else "DEFECT") + f" {name} {detail}")


# ---- build fixtures reaching interesting states
g.create_task("t", {"o": 1}, {"usd": 1}, "scheduler")
g.create_job("j", "t", "s", "scheduler")
g.create_worker("w", "scheduler")
g.create_task("t2", {}, {}, "scheduler")
g.create_artifact("a" * 64, "t", "test")
ap = g.create_approval("ap1", "t", "r", "resume", "scheduler")

STATES = {
    "task": ("t", g.get_task, g.transition_task, T.TASK_TRANSITIONS),
    "job": ("j", g.get_job, g.transition_job, T.JOB_TRANSITIONS),
    "worker": ("w", g.get_worker if hasattr(g, "get_worker") else None, None, None),
}
# gate has no get_worker; use direct read for state checks
def worker_state():
    return st.conn.execute("SELECT status FROM workers WHERE worker_id='w'").fetchone()[0]

# drive entities through legal paths, attacking at each step
# TASK: PROPOSED -> AUTHORIZED -> PLANNED -> EXECUTING -> PAUSED_FOR_HUMAN
g.transition_task("t", "AUTHORIZED", "s")
attack("task PROPOSED->FINALIZED (skip)", lambda: g.transition_task("t2", "FINALIZED", "s"),
       state_check=lambda: g.get_task("t2")["status"])
attack("task AUTHORIZED->EXECUTING (skip PLANNED)", lambda: g.transition_task("t", "EXECUTING", "s"),
       state_check=lambda: g.get_task("t")["status"])
attack("task AUTHORIZED->AUTHORIZED (self)", lambda: g.transition_task("t", "AUTHORIZED", "s"),
       state_check=lambda: g.get_task("t")["status"])
attack("task AUTHORIZED->BOGUS", lambda: g.transition_task("t", "BOGUS", "s"),
       state_check=lambda: g.get_task("t")["status"])
g.transition_task("t", "PLANNED", "s")
g.transition_task("t", "EXECUTING", "s")
attack("task EXECUTING->PROPOSED (backward)", lambda: g.transition_task("t", "PROPOSED", "s"),
       state_check=lambda: g.get_task("t")["status"])
attack("task EXECUTING->FINALIZED (skip VERIFYING)", lambda: g.transition_task("t", "FINALIZED", "s"),
       state_check=lambda: g.get_task("t")["status"])
# terminal states: drive t2 to CANCELLED via pause, t to FINALIZED
g.transition_task("t2", "AUTHORIZED", "s"); g.transition_task("t2", "PLANNED", "s")
g.transition_task("t2", "EXECUTING", "s")
g.transition_task("t2", "PAUSED_FOR_HUMAN", "s", pause_reason="x")
ap2 = g.create_approval("ap2", "t2", "r", "cancel", "s")
g.decide_approval("ap2", "APPROVED", "human", "s")
g.transition_task("t2", "CANCELLED", "s", approval_ref="ap2")
attack("task CANCELLED->EXECUTING (terminal escape)", lambda: g.transition_task("t2", "EXECUTING", "s", approval_ref="ap2"),
       state_check=lambda: g.get_task("t2")["status"])
g.transition_task("t", "VERIFYING", "s")
g.transition_task("t", "FINALIZED", "s")
attack("task FINALIZED->EXECUTING (terminal escape)", lambda: g.transition_task("t", "EXECUTING", "s"),
       state_check=lambda: g.get_task("t")["status"])
attack("task on missing id", lambda: g.transition_task("nope", "EXECUTING", "s"))

# JOB: PENDING -> CLAIMED -> RUNNING -> COMMITTING -> COMPLETE
attack("job PENDING->RUNNING (skip)", lambda: g.transition_job("j", "RUNNING", "s"),
       state_check=lambda: g.get_job("j")["status"])
attack("job PENDING->COMPLETE (skip)", lambda: g.transition_job("j", "COMPLETE", "s"),
       state_check=lambda: g.get_job("j")["status"])
attack("job PENDING->PENDING (self)", lambda: g.transition_job("j", "PENDING", "s"),
       state_check=lambda: g.get_job("j")["status"])
g.claim_job("j", "w", 60.0, "s")
attack("job CLAIMED->COMPLETE (skip)", lambda: g.transition_job("j", "COMPLETE", "s"),
       state_check=lambda: g.get_job("j")["status"])
g.transition_job("j", "RUNNING", "s")
attack("job RUNNING->CLAIMED (backward)", lambda: g.transition_job("j", "CLAIMED", "s"),
       state_check=lambda: g.get_job("j")["status"])
# Phase 1C R5 (I-4): COMPLETE is reachable only through the artifact-backed
# commit protocol. The audit fixture drives it; the "->COMPLETE (skip)"
# attacks above now exercise the I-4 refusal.
_tok = g.get_job("j")["fencing_token"]
_art = g.stage_artifact(job_id="j", worker_id="w", fencing_token=_tok,
                        task_id="t", kind="result", data=b"audit02",
                        actor="worker:w")
g.begin_commit("j", "w", _tok, artifact_id=_art["artifact_id"],
               actor="worker:w")
attack("job COMMITTING->RUNNING (backward)", lambda: g.transition_job("j", "RUNNING", "s"),
       state_check=lambda: g.get_job("j")["status"])
g.verify_artifact(_art["artifact_id"], actor="worker:w", worker_id="w",
                  fencing_token=_tok)
g.commit_artifact("j", "w", _tok, artifact_id=_art["artifact_id"],
                  actor="worker:w", evidence={})
attack("job COMPLETE->PENDING (terminal escape / replay)", lambda: g.transition_job("j", "PENDING", "s"),
       state_check=lambda: g.get_job("j")["status"])
attack("job COMPLETE->FAILED (terminal escape)", lambda: g.transition_job("j", "FAILED", "s"),
       state_check=lambda: g.get_job("j")["status"])
# FAILED -> QUARANTINED -> PENDING requeue path, then QUARANTINED terminal-ish
g.create_job("j2", "t", "s", "scheduler")
g.claim_job("j2", "w", 60.0, "s")
g.transition_job("j2", "RUNNING", "s")
g.transition_job("j2", "FAILED", "s")
g.transition_job("j2", "QUARANTINED", "s")
attack("job QUARANTINED->COMPLETE", lambda: g.transition_job("j2", "COMPLETE", "s"),
       state_check=lambda: g.get_job("j2")["status"])
attack("job QUARANTINED->FAILED (backward)", lambda: g.transition_job("j2", "FAILED", "s"),
       state_check=lambda: g.get_job("j2")["status"])
g.transition_job("j2", "PENDING", "s")  # legal requeue
attack("job PENDING->QUARANTINED (skip)", lambda: g.transition_job("j2", "QUARANTINED", "s"),
       state_check=lambda: g.get_job("j2")["status"])

# WORKER lifecycle
attack("worker PROVISIONING->RUNNING (skip)", lambda: g.transition_worker("w", "RUNNING", "s"),
       state_check=worker_state)
g.transition_worker("w", "IDLE", "s")
attack("worker IDLE->RUNNING (skip ASSIGNED)", lambda: g.transition_worker("w", "RUNNING", "s"),
       state_check=worker_state)
g.transition_worker("w", "ASSIGNED", "s")
g.transition_worker("w", "SUSPECT", "s")
attack("worker SUSPECT->IDLE", lambda: g.transition_worker("w", "IDLE", "s"),
       state_check=worker_state)
g.transition_worker("w", "DEAD", "s")
attack("worker DEAD->RUNNING (terminal escape)", lambda: g.transition_worker("w", "RUNNING", "s"),
       state_check=worker_state)
attack("worker DEAD->IDLE (terminal escape)", lambda: g.transition_worker("w", "IDLE", "s"),
       state_check=worker_state)

# ARTIFACT
attack("artifact STAGING->RELEASED (skip)", lambda: g.transition_artifact("a" * 64, "RELEASED", "s"),
       state_check=lambda: st.conn.execute("SELECT status FROM artifacts WHERE artifact_id=?", ("a" * 64,)).fetchone()[0])
g.transition_artifact("a" * 64, "VALIDATED", "s")
g.transition_artifact("a" * 64, "RELEASED", "s")
attack("artifact RELEASED->VALIDATED (backward)", lambda: g.transition_artifact("a" * 64, "VALIDATED", "s"),
       state_check=lambda: st.conn.execute("SELECT status FROM artifacts WHERE artifact_id=?", ("a" * 64,)).fetchone()[0])
attack("artifact RELEASED->STAGING (backward)", lambda: g.transition_artifact("a" * 64, "STAGING", "s"),
       state_check=lambda: st.conn.execute("SELECT status FROM artifacts WHERE artifact_id=?", ("a" * 64,)).fetchone()[0])

# APPROVAL double-decision + terminal
g.decide_approval("ap1", "APPROVED", "human", "s")
attack("approval APPROVED->DENIED (re-decide)", lambda: g.decide_approval("ap1", "DENIED", "human", "s"),
       state_check=lambda: st.conn.execute("SELECT status FROM approvals WHERE approval_id='ap1'").fetchone()[0])
attack("approval bad decision value", lambda: g.decide_approval("ap2", "MAYBE", "human", "s"))

# CHECKPOINT
ck = g.create_checkpoint("ck1", "t", "s")
attack("checkpoint UNVERIFIED->VERIFIED (skip VERIFYING)",
       lambda: g.set_checkpoint_verification("ck1", "VERIFIED", "s",
              receipt={"manifest_full": True, "ledger_chain_verified": True}),
       state_check=lambda: st.conn.execute("SELECT verification_status FROM checkpoints WHERE checkpoint_id='ck1'").fetchone()[0])
g.set_checkpoint_verification("ck1", "VERIFYING", "s")
g.set_checkpoint_verification("ck1", "VERIFIED", "s",
    receipt={"manifest_full": True, "ledger_chain_verified": True, "method": "full"})
attack("checkpoint VERIFIED->VERIFYING (terminal escape)",
       lambda: g.set_checkpoint_verification("ck1", "VERIFYING", "s"),
       state_check=lambda: st.conn.execute("SELECT verification_status FROM checkpoints WHERE checkpoint_id='ck1'").fetchone()[0])
# VERIFIED with manifest_full but chain NOT verified -> must reject
ck2 = g.create_checkpoint("ck2", "t", "s")
g.set_checkpoint_verification("ck2", "VERIFYING", "s")
attack("checkpoint VERIFIED without chain proof",
       lambda: g.set_checkpoint_verification("ck2", "VERIFIED", "s",
              receipt={"manifest_full": True, "ledger_chain_verified": False}),
       state_check=lambda: st.conn.execute("SELECT verification_status FROM checkpoints WHERE checkpoint_id='ck2'").fetchone()[0])

# ledger chain still verifies after all rejected transitions
ok, detail = g.verify_ledger_chain()
results.append(("ledger chain verifies after attack run", "ok" if ok else "DEFECT", detail))
print(("ok    " if ok else "DEFECT") + f" ledger chain verifies after attack run {detail}")

st.close()
shutil.rmtree(tmp, ignore_errors=True)
bad = [r for r in results if r[1] != "ok"]
print(f"\n{len(results)-len(bad)}/{len(results)} transition attacks correctly rejected; {len(bad)} DEFECTS")
for b in bad:
    print("  DEFECT:", b)
