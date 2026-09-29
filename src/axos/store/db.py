"""Phase 1A — authoritative store handle.

Phase 0 decision (Q1 / 02-architecture 2.4 / ADR-001): SQLite in WAL mode is
the authoritative state store for the v1 single-VM deployment. No Redis, no
Postgres, no distributed store.

Phase 0 decision (Q8 / ADR-008): store transaction time is authoritative.
Worker clocks are never trusted. Every write transaction stamps its rows with
a single monotonic store time captured at transaction start; a worker-supplied
timestamp is stored only as informational metadata and is never used for
lease, ordering, or fencing semantics.

This module owns: connection setup with verified pragmas, the monotonic
store clock (persisted, so it survives restarts), write transactions
(BEGIN IMMEDIATE so concurrent writers serialize), consistent backup via the
SQLite backup API, and integrity verification.

AUTHORITY BOUNDARY (F1 remediation, enforced by construction):
- `Store` is the writable capability. Only TransitionGate (plus schema
  migration / setup code, which holds a Store transiently) may hold one.
  Its `write_txn()` is the gate-owned transaction capability — the sole
  sanctioned mutation path.
- `Store.conn` is deliberately READ-ONLY (PRAGMA query_only=ON). Readers,
  the gate's own read paths, and tests use it for SELECTs; any write through
  it raises sqlite3.OperationalError.
- Non-gate components must be given a `ReadOnlyStore` (via
  `Store.read_only()` or `open_readonly_store()`), which exposes no writable
  connection and no transaction capability at all — it cannot be escalated.

This boundary is application-level: a programmer who deliberately calls
`open_store()` or `sqlite3.connect()` on the file has the equivalent of
filesystem access, which is outside the boundary by definition. What the
boundary guarantees is that components wired the intended way cannot
accidentally (or via the normal API) mutate authoritative state outside the
gate.
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from typing import Callable, Iterator, Tuple


class StoreError(Exception):
    """Base error for store-layer failures."""


class TransitionRejected(StoreError):
    """A state transition or creation was refused by the gate."""


class LeaseError(StoreError):
    """A lease operation was refused (conflict, stale token, not owner)."""


class MigrationError(StoreError):
    """A database migration failed; the schema was left untouched."""


class PolicyConflict(StoreError):
    """An R9 recovery-policy compare-and-swap lost: the policy row changed
    (or the budget guard failed) between read and write. The loser must
    re-read the authoritative row and reconcile from durable state; it must
    never assume its stale view won."""


class DesiredStateConflict(StoreError):
    """An R11 desired-state canonical-identity collision: the deterministic
    job_id for a desired_work_id exists but its map row is absent or bound
    to a different desired identity, a map row exists without its job row,
    or the desired spec drifted under an existing mapping. Fail closed:
    the gate never adopts a foreign job, never deletes, never recreates —
    creation is idempotent only when identity AND spec both match. The
    contradictory actual state must be resolved by the operator."""


class BreakerConflict(StoreError):
    """An R12 circuit-breaker compare-and-swap lost: the breaker row changed
    between the caller's read and its conditional write (a concurrent
    controller transitioned it, or its version moved). The loser must
    re-read the authoritative row and reconcile from durable state; it must
    never assume its stale view won. (A lost half-open probe-claim race
    inside admission is NOT this: it surfaces as a clean claim denial,
    never as a half-allocated probe.)"""


class FinalizationConflict(StoreError):
    """An R13 finalization compare-and-swap lost: the finalization_runs row
    changed between the caller's read and its conditional write (a
    concurrent finalizer evaluated or published it, or its version moved).
    The loser must re-read the authoritative row and reconcile from durable
    state; it must never assume its stale view won. (A run that became
    FINALIZED under the loser is NOT this: publish/evaluate return the
    verified finalized record idempotently instead of conflicting.)"""


#: The raw storage-engine error type. The gate raises StoreError
#: subclasses for domain failures, but when the underlying database
#: itself is unreadable (missing tables, storage I/O faults) the
#: engine's own errors escape the gate untranslated. Components that
#: must fail closed on unreadable state (e.g. the R11 reconciler) catch
#: this alongside StoreError. Naming the type grants no read or write
#: capability — it is purely for fail-closed error classification, so it
#: is safe to import wherever the store's error surface is already
#: imported.
StorageEngineError = sqlite3.Error


Clock = Callable[[], float]


def _open_read_connection(path: str) -> sqlite3.Connection:
    """A connection that physically cannot write (PRAGMA query_only=ON).

    Any INSERT/UPDATE/DELETE/DDL through it raises sqlite3.OperationalError.
    """
    c = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA query_only=ON")
    c.execute("PRAGMA busy_timeout=5000")
    return c


class ReadOnlyStore:
    """Read-only view of an AXOS database: the interface non-gate components
    receive.

    Exposes no writable connection and no transaction capability. There is no
    method on this class that yields one, so a component holding only a
    ReadOnlyStore cannot mutate authoritative state through the normal API —
    not tasks, jobs, leases, approvals, incidents, recovery evidence,
    checkpoints, validators, or ledger events.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._conn = _open_read_connection(path)

    @property
    def path(self) -> str:
        return self._path

    def execute(self, sql: str, params=()):
        """Run a read query. Anything that would mutate the database raises
        sqlite3.OperationalError."""
        return self._conn.execute(sql, params)

    def close(self) -> None:
        self._conn.close()


class Store:
    """Handle to one authoritative SQLite/WAL database file.

    WRITABLE CAPABILITY — only TransitionGate (and transient schema
    migration / setup code) may hold a Store. Every other component must be
    given a ReadOnlyStore instead; see the module docstring.
    """

    def __init__(self, path: str, clock: Clock | None = None) -> None:
        self.path = path
        self._clock: Clock = clock or time.time
        # Autocommit mode: we manage BEGIN/COMMIT explicitly so every write
        # transaction is a real, single atomic unit.
        self._conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._apply_pragmas()
        # Read-only companion: all casual reads (gate read paths, tests,
        # debugging) go through here so a stray write is impossible.
        self._ro_conn = _open_read_connection(path)

    def read_only(self) -> ReadOnlyStore:
        """Return a ReadOnlyStore over the same database file: the handle to
        give to every non-gate component."""
        return ReadOnlyStore(self.path)

    # ------------------------------------------------------------------ setup
    def _apply_pragmas(self) -> None:
        c = self._conn
        c.execute("PRAGMA journal_mode=WAL")
        mode = c.execute("PRAGMA journal_mode").fetchone()[0]
        if mode.lower() != "wal":
            raise StoreError(f"journal_mode is {mode!r}, expected WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        sync = c.execute("PRAGMA synchronous").fetchone()[0]
        if int(sync) != 1:  # 1 == NORMAL
            raise StoreError(f"synchronous is {sync!r}, expected NORMAL(1)")
        c.execute("PRAGMA foreign_keys=ON")
        fk = c.execute("PRAGMA foreign_keys").fetchone()[0]
        if int(fk) != 1:
            raise StoreError("foreign_keys could not be enabled")
        c.execute("PRAGMA busy_timeout=5000")
        # Small, documented page size for a state database; default cache.
        c.execute("PRAGMA page_size=4096")

    def pragma_report(self) -> dict:
        """Return the actual, verified SQLite configuration."""
        c = self._conn
        return {
            "journal_mode": c.execute("PRAGMA journal_mode").fetchone()[0],
            "synchronous": c.execute("PRAGMA synchronous").fetchone()[0],
            "foreign_keys": c.execute("PRAGMA foreign_keys").fetchone()[0],
            "busy_timeout": c.execute("PRAGMA busy_timeout").fetchone()[0],
            "page_size": c.execute("PRAGMA page_size").fetchone()[0],
            "sqlite_version": sqlite3.sqlite_version,
        }

    # ------------------------------------------------------- authoritative time
    def current_time(self) -> float:
        """Authoritative time without starting a transaction.

        Monotonic with respect to every previously committed transaction,
        because it is floored at the persisted last-commit timestamp.
        """
        row = self._conn.execute(
            "SELECT v FROM axos_meta WHERE k='last_commit_ts'"
        ).fetchone()
        last = float(row[0]) if row else 0.0
        return max(self._clock(), last)

    @contextmanager
    def write_txn(self) -> Iterator[Tuple[sqlite3.Connection, float]]:
        """One atomic write transaction — the GATE-OWNED transaction capability.

        Yields (connection, now) where `now` is the single authoritative
        timestamp for everything this transaction stamps. It is monotonic
        across restarts because it is derived from the persisted
        last-commit timestamp. BEGIN IMMEDIATE serializes concurrent writers
        on a single VM.

        Only TransitionGate (and schema migration / setup) may call this.
        Non-gate components never receive a Store, so they can never reach it.
        """
        c = self._conn
        if c.in_transaction:
            raise StoreError("nested write transactions are not allowed")
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute(
                "SELECT v FROM axos_meta WHERE k='last_commit_ts'"
            ).fetchone()
            last = float(row[0]) if row else 0.0
            now = self._clock()
            if now <= last:
                now = last + 0.001
            c.execute(
                "INSERT INTO axos_meta(k, v) VALUES('last_commit_ts', ?) "
                "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (repr(now),),
            )
            yield c, now
            c.execute("COMMIT")
        except BaseException:
            c.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------- maintenance
    def backup_to(self, dest_path: str) -> None:
        """Consistent snapshot via the SQLite online-backup API.

        The backup is a valid, independent database file: it can be opened
        directly as a restored copy. This is the hook the hourly-snapshot
        backup design (Phase 0, Q1) builds on.
        """
        dest = sqlite3.connect(dest_path, isolation_level=None)
        try:
            with dest:
                self._conn.backup(dest)
        finally:
            dest.close()

    def integrity_check(self) -> Tuple[bool, str]:
        """PRAGMA integrity_check + foreign-key check. Returns (ok, detail)."""
        rows = self._conn.execute("PRAGMA integrity_check").fetchall()
        if len(rows) != 1 or rows[0][0] != "ok":
            return False, "; ".join(r[0] for r in rows)
        fk = self._conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk:
            return False, f"foreign_key_check violations: {fk}"
        return True, "ok"

    def close(self) -> None:
        self._conn.close()
        self._ro_conn.close()

    @property
    def conn(self) -> sqlite3.Connection:
        """Read-only connection (PRAGMA query_only=ON).

        Used by the gate's own read paths, tests, and debugging. Any
        INSERT/UPDATE/DELETE/DDL through it raises sqlite3.OperationalError.
        The only write capability on a Store is write_txn(), owned by the
        TransitionGate.
        """
        return self._ro_conn


def open_store(path: str, clock: Clock | None = None) -> Store:
    """Open (creating if needed) the authoritative store at `path`.

    Returns the WRITABLE capability: only hand this to TransitionGate (or
    transient migration/setup code). Every other component gets
    open_readonly_store() / Store.read_only().
    """
    return Store(path, clock=clock)


def open_readonly_store(path: str) -> ReadOnlyStore:
    """Open a read-only view of the database at `path`.

    This is the store interface for every non-gate component. It cannot be
    escalated to a writable handle.
    """
    return ReadOnlyStore(path)
