"""Audit 01 — single-write-path claim.

Question: can authoritative state be mutated without going through
TransitionGate? Uses only the public Store API (conn, write_txn) — i.e. what
any ordinary in-process caller (a future supervisor, worker, or bug) has.

HISTORICAL NOTE (F1 remediation, 2026-09-23): this script is the pre-fix
evidence — 11/12 bypasses succeeded. Post-fix, Store.conn is read-only, so
the conn-based attempts now raise sqlite3.OperationalError; see audit
08_retest.py for the replay. The write_txn-based attempts remain possible
BY DESIGN: write_txn is the gate-owned transaction capability, and calling
it directly is the equivalent of opening the DB file with sqlite3 — i.e.
filesystem-level access, outside the application-level boundary.
"""
import os
import shutil
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(_HERE)))  # ~/workspace

from axos.store import open_store, migrate, TransitionGate, TransitionRejected

tmp = tempfile.mkdtemp(prefix="audit1-")
db = os.path.join(tmp, "a.db")
st = open_store(db)
migrate(st)
g = TransitionGate(st)
g.create_task("t1", {"objective": "x"}, {"usd": 1}, "scheduler")
g.create_job("j1", "t1", "s", "scheduler")

findings = []


def check(name, cond, detail=""):
    findings.append((name, bool(cond), detail))
    print(("PASS " if cond else "FAIL ") + name + (f" — {detail}" if detail else ""))


# 1. direct INSERT of a task, bypassing the gate
with st.write_txn() as (conn, now):
    conn.execute(
        "INSERT INTO tasks(task_id,status,objective,budgets,created_at,updated_at)"
        " VALUES('evil-task','EXECUTING','{}','{}',?,?)", (now, now))
row = st.conn.execute("SELECT status FROM tasks WHERE task_id='evil-task'").fetchone()
n_ledger = st.conn.execute(
    "SELECT COUNT(*) FROM ledger WHERE payload LIKE '%evil-task%'").fetchone()[0]
check("direct INSERT task succeeds", row is not None and row[0] == "EXECUTING")
check("direct INSERT leaves no ledger trace", n_ledger == 0,
      f"ledger events mentioning evil-task: {n_ledger}")
ok, _ = g.verify_ledger_chain()
check("ledger chain still 'verifies' after bypass insert", ok,
      "chain cannot see what was never recorded")

# 2. direct UPDATE: PENDING -> COMPLETE, skipping the whole lifecycle
with st.write_txn() as (conn, now):
    conn.execute("UPDATE jobs SET status='COMPLETE', updated_at=? WHERE job_id='j1'",
                 (now,))
check("direct UPDATE jumps job PENDING->COMPLETE",
      g.get_job("j1")["status"] == "COMPLETE")

# 3. alter a lease: steal ownership + forge fencing token
with st.write_txn() as (conn, now):
    conn.execute("UPDATE jobs SET owner_worker_id='mallory', fencing_token=999,"
                 " lease_expires_at=?, updated_at=? WHERE job_id='j1'",
                 (now + 3600, now))
j = g.get_job("j1")
check("direct lease steal succeeds",
      j["owner_worker_id"] == "mallory" and j["fencing_token"] == 999)

# 4. release another worker's lease via direct write (no token check)
with st.write_txn() as (conn, now):
    conn.execute("UPDATE jobs SET owner_worker_id=NULL, lease_expires_at=NULL,"
                 " updated_at=? WHERE job_id='j1'", (now,))
check("direct lease release bypasses token/owner checks",
      g.get_job("j1")["owner_worker_id"] is None)

# 5. insert an APPROVED approval directly, then use the gate to resume a
#    human-gated task — I-17's enforcement trusts the approvals table.
g2t = g.create_task("t2", {}, {}, "scheduler")
for s in ("AUTHORIZED", "PLANNED", "EXECUTING"):
    g.transition_task("t2", s, "scheduler")
g.transition_task("t2", "PAUSED_FOR_HUMAN", "scheduler", pause_reason="audit")
with st.write_txn() as (conn, now):
    conn.execute(
        "INSERT INTO approvals(approval_id,task_id,reason,requested_action,status,"
        " decided_by,decided_at,created_at,updated_at)"
        " VALUES('forged-ap','t2','x','resume','APPROVED','mallory',?, ?, ?)",
        (now, now, now))
try:
    g.transition_task("t2", "EXECUTING", "mallory", approval_ref="forged-ap")
    resumed = g.get_task("t2")["status"] == "EXECUTING"
except TransitionRejected:
    resumed = False
check("forged APPROVED approval resumes human-gated task", resumed,
      "I-17 holds only if the approvals table itself is gate-written")

# 6. flip a PENDING approval to APPROVED directly, then resume
g.transition_task("t2", "PAUSED_FOR_HUMAN", "scheduler", pause_reason="again")
ap = g.create_approval("ap-real", "t2", "r", "resume", "scheduler")
with st.write_txn() as (conn, now):
    conn.execute("UPDATE approvals SET status='APPROVED', decided_by='mallory',"
                 " decided_at=?, updated_at=? WHERE approval_id='ap-real'",
                 (now, now))
try:
    g.transition_task("t2", "EXECUTING", "mallory", approval_ref="ap-real")
    resumed2 = True
except TransitionRejected:
    resumed2 = False
check("direct approval-status flip resumes human-gated task", resumed2)

# 7. modify recovery evidence directly — note: the I-18 CHECK constraint
# fires on UPDATE as well as INSERT, so this *should* fail.
import sqlite3 as _sq
inc = g.create_incident("i9", "job", "worker_crash", "s", "c", "b", "sigkill", "sup")
g.record_recovery_attempt("ra9", "i9", 1, {"kind": "requeue"}, "restarted",
                          5.0, 0.0, "retry", "recovery")
try:
    with st.write_txn() as (conn, now):
        conn.execute("UPDATE recovery_attempts SET progress_delta=0, decision='success'"
                     " WHERE attempt_id='ra9'")
    r = st.conn.execute("SELECT decision, progress_delta FROM recovery_attempts"
                        " WHERE attempt_id='ra9'").fetchone()
    check("direct edit fabricates 'success' with zero progress",
          r["decision"] == "success" and r["progress_delta"] == 0)
except _sq.IntegrityError as e:
    check("direct edit fabricates 'success' with zero progress", False,
          f"blocked by CHECK constraint: {e}")
# but a *consistent* lie (success + positive delta) is writable:
with st.write_txn() as (conn, now):
    conn.execute("UPDATE recovery_attempts SET progress_delta=99.0, decision='success',"
                 " observed_effect='totally really recovered' WHERE attempt_id='ra9'")
r = st.conn.execute("SELECT decision, progress_delta, observed_effect FROM recovery_attempts"
                    " WHERE attempt_id='ra9'").fetchone()
check("direct edit fabricates 'success' with plausible evidence",
      r["decision"] == "success" and r["progress_delta"] == 99.0,
      "store cannot verify evidence truth, only shape")

# 8. direct DELETE of an incident (delete child row first to satisfy FK)
with st.write_txn() as (conn, now):
    conn.execute("DELETE FROM recovery_attempts WHERE incident_id='i9'")
    conn.execute("DELETE FROM incidents WHERE incident_id='i9'")
check("direct DELETE of incident (+child) succeeds silently",
      st.conn.execute("SELECT * FROM incidents WHERE incident_id='i9'").fetchone() is None)

# 9. migration path as mutation: migrate() accepts an arbitrary list
with st.write_txn() as (conn, now):
    pass
from axos.store import migrate as mig
mig(st, [(99, "evil", "CREATE TABLE IF NOT EXISTS evil_pwned(x TEXT)")])
check("migrate() runs caller-supplied SQL",
      st.conn.execute("SELECT name FROM sqlite_master WHERE name='evil_pwned'").fetchone() is not None)

st.close()
shutil.rmtree(tmp, ignore_errors=True)
print(f"\n{sum(1 for _, c, _ in findings if c)}/{len(findings)} bypasses confirmed possible")
