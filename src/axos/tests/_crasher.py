"""Crash-injection helper for Phase 1A tests.

Usage: _crasher.py <db_path> <mode>

mode 'uncommitted': opens the store, begins a write transaction, inserts a
    task row, then SIGKILLs itself WITHOUT committing — the WAL is left with
    uncommitted frames and the next open must recover cleanly.

mode 'committed': commits one task, then begins a second transaction,
    inserts another row, and SIGKILLs itself mid-transaction — the committed
    row must survive, the uncommitted one must not.
"""
import os
import signal
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from axos.store import open_store, migrate  # noqa: E402


def main() -> None:
    db_path, mode = sys.argv[1], sys.argv[2]
    store = open_store(db_path)
    migrate(store)  # no-op on an already-migrated db
    if mode == "uncommitted":
        with store.write_txn() as (conn, now):
            conn.execute(
                "INSERT INTO tasks(task_id,status,objective,budgets,"
                "created_at,updated_at) VALUES('doomed-task','PROPOSED',"
                "'{}','{}',?,?)", (now, now))
            conn.execute("SELECT 1")  # make sure the write hit the WAL
            os.kill(os.getpid(), signal.SIGKILL)
    elif mode == "committed":
        with store.write_txn() as (conn, now):
            conn.execute(
                "INSERT INTO tasks(task_id,status,objective,budgets,"
                "created_at,updated_at) VALUES('committed-task','PROPOSED',"
                "'{}','{}',?,?)", (now, now))
        with store.write_txn() as (conn, now):
            conn.execute(
                "INSERT INTO tasks(task_id,status,objective,budgets,"
                "created_at,updated_at) VALUES('doomed-task2','PROPOSED',"
                "'{}','{}',?,?)", (now, now))
            conn.execute("SELECT 1")
            os.kill(os.getpid(), signal.SIGKILL)
    else:
        raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    main()
