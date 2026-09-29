"""Audit 08 — RE-TEST of the 11/12 bypasses after F1 remediation.

Each bypass from audit 01 is replayed through every publicly reachable
application interface a non-gate component could hold:
  (a) Store.conn            (now PRAGMA query_only=ON)
  (b) Store.read_only()     (ReadOnlyStore: no write capability at all)
  (c) open_readonly_store() (same, independently opened)

Expected: sqlite3.OperationalError on every attempt, state unchanged,
ledger chain still verifies.

Deliberately NOT re-tested here: calling open_store().write_txn() directly.
That is the gate-owned transaction capability; reaching for it from
non-gate code is the equivalent of opening the DB file with sqlite3
directly — filesystem-level access, explicitly outside the
application-level boundary (see db.py module docstring).
"""
import os
import shutil
import sqlite3
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(_HERE)))

from axos.store import (open_store, open_readonly_store, migrate,
                        TransitionGate, TransitionRejected)

tmp = tempfile.mkdtemp(prefix="audit8-")
db = os.path.join(tmp, "a.db")
st = open_store(db)
migrate(st)
g = TransitionGate(st)
g.create_task("t1", {"objective": "x"}, {"usd": 1}, "scheduler")
g.create_job("j1", "t1", "s", "scheduler")
g.create_task("t2", {}, {}, "scheduler")
for s in ("AUTHORIZED", "PLANNED", "EXECUTING"):
    g.transition_task("t2", s, "scheduler")
g.transition_task("t2", "PAUSED_FOR_HUMAN", "scheduler", pause_reason="audit")
g.create_incident("i9", "job", "worker_crash", "s", "c", "b", "sigkill", "sup")
g.record_recovery_attempt("ra9", "i9", 1, {"kind": "requeue"}, "restarted",
                          5.0, 0.0, "retry", "recovery")

NOW = 1700000000.0
BYPASSES = [
    ("silent task INSERT",
     "INSERT INTO tasks(task_id,status,objective,budgets,created_at,updated_at)"
     f" VALUES('evil-task','EXECUTING','{{}}','{{}}',{NOW},{NOW})"),
    ("illegal state jump PENDING->COMPLETE",
     f"UPDATE jobs SET status='COMPLETE', updated_at={NOW} WHERE job_id='j1'"),
    ("lease theft (owner+token)",
     "UPDATE jobs SET owner_worker_id='mallory', fencing_token=999,"
     f" lease_expires_at={NOW + 3600}, updated_at={NOW} WHERE job_id='j1'"),
    ("silent lease release",
     f"UPDATE jobs SET owner_worker_id=NULL, lease_expires_at=NULL,"
     f" updated_at={NOW} WHERE job_id='j1'"),
    ("forged APPROVED approval (I-17 attack)",
     "INSERT INTO approvals(approval_id,task_id,reason,requested_action,status,"
     " decided_by,decided_at,created_at,updated_at)"
     f" VALUES('forged-ap','t2','x','resume','APPROVED','mallory',{NOW},{NOW},{NOW})"),
    ("approval status flip PENDING->APPROVED",
     "INSERT INTO approvals(approval_id,task_id,reason,requested_action,status,"
     f" created_at,updated_at) VALUES('ap9','t2','r','resume','PENDING',{NOW},{NOW})"),
    ("recovery evidence fabrication",
     "UPDATE recovery_attempts SET progress_delta=99.0, decision='success',"
     " observed_effect='totally really recovered' WHERE attempt_id='ra9'"),
    ("incident DELETE",
     "DELETE FROM recovery_attempts WHERE incident_id='i9'"),
    ("arbitrary ledger append",
     "INSERT INTO ledger(event_type,payload,actor,ts,prev_hash,hash)"
     f" VALUES('forged','{{}}','mallory',{NOW},'x','y')"),
    ("DDL",
     "CREATE TABLE evil_pwned(x TEXT)"),
]

INTERFACES = {
    "Store.conn": lambda: st.conn,
    "Store.read_only()": lambda: st.read_only(),
    "open_readonly_store()": lambda: open_readonly_store(db),
}

results = []
for name, sql in BYPASSES:
    for iface_name, opener in INTERFACES.items():
        h = opener()
        try:
            if hasattr(h, "execute"):
                h.execute(sql)
            else:
                h.execute(sql)  # both expose .execute
            results.append((name, iface_name, False, "MUTATION SUCCEEDED"))
            print(f"DEFECT {name} via {iface_name}: MUTATION SUCCEEDED")
        except sqlite3.OperationalError as e:
            results.append((name, iface_name, True, str(e)[:60]))
            print(f"ok     {name} via {iface_name}: blocked ({str(e)[:50]})")
        except Exception as e:
            results.append((name, iface_name, False,
                            f"wrong exception {type(e).__name__}"))
            print(f"DEFECT {name} via {iface_name}: wrong exc {type(e).__name__}")
        finally:
            if iface_name != "Store.conn":
                h.close()

# flip-then-use variant: the second half of the I-17 attack must also be dead
ro = st.read_only()
try:
    ro.execute("UPDATE approvals SET status='APPROVED' WHERE approval_id='ap9'")
    print("DEFECT approval flip via read_only: succeeded")
    results.append(("approval flip", "Store.read_only()", False, "succeeded"))
except sqlite3.OperationalError:
    print("ok     approval flip via read_only: blocked")
    results.append(("approval flip", "Store.read_only()", True, "blocked"))
ro.close()

# state must be unchanged after all blocked attempts
checks = [
    ("no evil task", st.conn.execute(
        "SELECT COUNT(*) FROM tasks WHERE task_id='evil-task'").fetchone()[0] == 0),
    ("job still PENDING", g.get_job("j1")["status"] == "PENDING"),
    ("no forged approval", st.conn.execute(
        "SELECT COUNT(*) FROM approvals WHERE approval_id LIKE 'forged%'").fetchone()[0] == 0),
    ("task still paused", g.get_task("t2")["status"] == "PAUSED_FOR_HUMAN"),
    ("incident intact", st.conn.execute(
        "SELECT COUNT(*) FROM incidents WHERE incident_id='i9'").fetchone()[0] == 1),
    ("no evil table", st.conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE name='evil_pwned'").fetchone()[0] == 0),
]
for name, ok_ in checks:
    results.append((name, "state", ok_, ""))
    print(("ok     " if ok_ else "DEFECT") + f" {name}")

ok, detail = g.verify_ledger_chain()
results.append(("ledger chain verifies", "ledger", ok, detail))
print(("ok     " if ok else "DEFECT") + f" ledger chain verifies ({detail})")

# the sanctioned path still works: gate mutates, I-17 releases via real approval
ap = g.create_approval("ap-legit", "t2", "r", "resume", "scheduler")
g.decide_approval("ap-legit", "APPROVED", "human", "scheduler")
g.transition_task("t2", "EXECUTING", "mallory", approval_ref="ap-legit")
results.append(("legit approval releases I-17", "gate",
                g.get_task("t2")["status"] == "EXECUTING", ""))
print("ok     legit approval releases I-17 via gate")

st.close()
shutil.rmtree(tmp, ignore_errors=True)
bad = [r for r in results if not r[2]]
print(f"\n{len(results)-len(bad)}/{len(results)} re-test checks blocked/correct; "
      f"{len(bad)} STILL POSSIBLE")
for b in bad:
    print("  STILL POSSIBLE:", b)
sys.exit(1 if bad else 0)
