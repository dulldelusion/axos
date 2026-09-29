import os, signal, sys
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
