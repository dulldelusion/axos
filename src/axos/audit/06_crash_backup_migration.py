"""Audit 06 — crash consistency at dangerous points, backup under write load,
interrupted migration. All kills are real SIGKILLs of real subprocesses.
"""
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(_HERE)))

from axos.store import open_store, migrate, TransitionGate
from axos.store.migrations import MIGRATIONS

ALL_VERSIONS = sorted(m[0] for m in MIGRATIONS)

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    print(("ok    " if cond else "DEFECT") + f" {name}" + (f" — {detail}" if detail else ""))


KILLER = os.path.join(_HERE, "_killpoints.py")
with open(KILLER, "w") as f:
    f.write('''"""Kill-point injector: argv = db_path point.
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
''')


def kill_at(db, point):
    env = dict(os.environ, PYTHONPATH=os.path.dirname(os.path.dirname(_HERE)))
    r = subprocess.run([sys.executable, KILLER, db, point], env=env,
                       capture_output=True, text=True, timeout=60)
    return r.returncode


tmp = tempfile.mkdtemp(prefix="audit6-")

# crash at each dangerous point
for point in ("before_txn", "after_begin", "after_entity", "after_ledger",
              "before_commit", "after_commit"):
    db = os.path.join(tmp, f"kp-{point}.db")
    st0 = open_store(db)
    migrate(st0)
    st0.close()
    rc = kill_at(db, point)
    assert rc == -signal.SIGKILL, (point, rc)
    st = open_store(db)
    ok, detail = st.integrity_check()
    g = TransitionGate(st)
    lok, ldetail = g.verify_ledger_chain()
    row = st.conn.execute(
        "SELECT task_id FROM tasks WHERE task_id=?", (f"kp-{point}",)).fetchone()
    if point == "after_commit":
        check(f"kill {point}: committed row durable", row is not None)
        # the injector deliberately wrote a bogus ledger row (prev_hash='x');
        # it must be preserved EXACTLY as committed — atomicity, not validity.
        kp_ledger = st.conn.execute(
            "SELECT COUNT(*) FROM ledger WHERE event_type='kp'").fetchone()[0]
        check(f"kill {point}: committed txn preserved exactly (incl. bogus row)",
              kp_ledger == 1,
              "chain invalidity here is the injector's deliberate garbage")
    else:
        check(f"kill {point}: uncommitted row absent", row is None,
              f"row present: {row is not None}")
        check(f"kill {point}: ledger chain verifies", lok, ldetail)
    check(f"kill {point}: integrity ok", ok, detail)
    st.close()

# kill during concurrent access: writer loop in parent + killer child
db = os.path.join(tmp, "concurrent.db")
st0 = open_store(db)
migrate(st0)
g0 = TransitionGate(st0)
g0.create_task("ct", {}, {}, "scheduler")
stop = False


def writer():
    i = 0
    while not stop:
        try:
            with st0.write_txn() as (conn, now):
                conn.execute(
                    "INSERT INTO ledger(event_type,payload,actor,ts,prev_hash,hash)"
                    " VALUES('w','{}','w',?, 'x','y')", (now,))
        except Exception:
            pass
        i += 1


wt = threading.Thread(target=writer)
wt.start()
time.sleep(0.2)
rc = kill_at(db, "before_commit")
stop = True
wt.join()
st0.close()
st = open_store(db)
ok, detail = st.integrity_check()
check("kill during concurrent writes: integrity ok", ok, detail)
check("kill during concurrent writes: reopen works",
      st.conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] >= 0)
st.close()

# backup during active write load
db = os.path.join(tmp, "load.db")
st0 = open_store(db)
migrate(st0)
g0 = TransitionGate(st0)
stop = False
errs = []


def loader():
    stL = open_store(db, clock=None)
    gL = TransitionGate(stL)
    i = 0
    while not stop:
        try:
            gL.create_task(f"lt-{i}", {"i": i}, {}, "scheduler")
        except Exception as e:
            errs.append(e)
        i += 1
    stL.close()


lt = threading.Thread(target=loader)
lt.start()
time.sleep(0.3)
bak = os.path.join(tmp, "load-backup.db")
st0.backup_to(bak)
time.sleep(0.3)
stop = True
lt.join()
n_orig = st0.conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
st0.close()
rb = open_store(bak)
ok, detail = rb.integrity_check()
g2 = TransitionGate(rb)
lok, ldetail = g2.verify_ledger_chain()
n_bak = rb.conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
check("backup under write load: integrity ok", ok, detail)
check("backup under write load: ledger chain verifies", lok, ldetail)
check("backup under write load: backup is a prefix of original",
      0 < n_bak <= n_orig, f"backup={n_bak} original={n_orig}")
check("backup under write load: no loader errors", not errs, str(errs[:1]))
rb.close()

# interrupted migration: BEGIN, apply half of v1, SIGKILL, then reopen
db = os.path.join(tmp, "migkill.db")
env = dict(os.environ, PYTHONPATH=os.path.dirname(os.path.dirname(_HERE)))
migkiller = os.path.join(_HERE, "_migkill.py")
with open(migkiller, "w") as f:
    f.write('''import os, signal, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from axos.store import open_store
from axos.store.migrations import _split_statements, V1_SCHEMA, _ensure_migrations_table
st = open_store(sys.argv[1])
_ensure_migrations_table(st)
# NOTE(F1): stands in for the migration's own write path (private conn).
c = st._conn
c.execute("BEGIN IMMEDIATE")
stmts = _split_statements(V1_SCHEMA)
for s in stmts[:len(stmts)//2]:
    c.execute(s)
os.kill(os.getpid(), signal.SIGKILL)
''')
r = subprocess.run([sys.executable, migkiller, db], env=env,
                   capture_output=True, text=True, timeout=60)
check("migration killer died by SIGKILL", r.returncode == -signal.SIGKILL, str(r.returncode))
st = open_store(db)
ok, detail = st.integrity_check()
from axos.store import applied_versions
vers = applied_versions(st)
n_tables = st.conn.execute(
    "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
    " AND name NOT LIKE 'sqlite_%'").fetchone()[0]
check("interrupted migration: no version recorded", vers == [], str(vers))
check("interrupted migration: no partial tables", n_tables == 1,  # only schema_migrations
      f"tables={n_tables}")
check("interrupted migration: integrity ok", ok, detail)
# recovery: migrate cleanly afterwards (expect ALL migration versions)
n_applied = migrate(st)
check("interrupted migration: clean migrate after kill",
      n_applied == ALL_VERSIONS and applied_versions(st) == ALL_VERSIONS)
st.close()

shutil.rmtree(tmp, ignore_errors=True)
bad = [r for r in results if not r[1]]
print(f"\n{len(results)-len(bad)}/{len(results)} crash/backup/migration attacks behaved correctly; {len(bad)} DEFECTS")
for b in bad:
    print("  DEFECT:", b[0], "-", b[2])
