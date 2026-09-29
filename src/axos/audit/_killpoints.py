"""Kill-point injector: argv = db_path point.
Points: before_txn | after_begin | after_entity | after_ledger |
        before_commit | after_commit.
Each does: open, migrate, BEGIN IMMEDIATE, insert task 'kp-<point>',
insert ledger row manually, then SIGKILL at the requested point.
after_commit commits first, then SIGKILLs before any checkpoint.
"""
import os, signal, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from axos.store import open_store, migrate
db, point = sys.argv[1], sys.argv[2]
st = open_store(db); migrate(st)
# NOTE(F1): this injector stands in for the gate's OWN write path — it must
# die mid-transaction, which write_txn()'s context manager cannot express.
# It therefore uses the Store's private writable connection, exactly as the
# gate's write_txn does internally. No public API offers this.
c = st._conn
name = f"kp-{point}"
if point == "before_txn":
    os.kill(os.getpid(), signal.SIGKILL)
c.execute("BEGIN IMMEDIATE")
if point == "after_begin":
    os.kill(os.getpid(), signal.SIGKILL)
row = c.execute("SELECT v FROM axos_meta WHERE k='last_commit_ts'").fetchone()
now = float(row[0]) + 0.001 if row else 1.0
c.execute("INSERT INTO tasks(task_id,status,objective,budgets,created_at,updated_at)"
          " VALUES(?, 'PROPOSED', '{}','{}',?,?)", (name, now, now))
if point == "after_entity":
    os.kill(os.getpid(), signal.SIGKILL)
c.execute("INSERT INTO ledger(event_type,payload,actor,ts,prev_hash,hash)"
          " VALUES('kp', '{}','kp',?, 'x','y')", (now,))
if point == "after_ledger":
    os.kill(os.getpid(), signal.SIGKILL)
if point == "before_commit":
    # force WAL frames out, then die before COMMIT
    c.execute("SELECT 1")
    os.kill(os.getpid(), signal.SIGKILL)
c.execute("COMMIT")
if point == "after_commit":
    os.kill(os.getpid(), signal.SIGKILL)
