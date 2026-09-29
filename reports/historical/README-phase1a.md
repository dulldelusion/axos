# AXOS — Phase 1A: authoritative store layer

The boring, deterministic foundation everything later depends on.
Standard library only (sqlite3, hashlib, json, threading) — no frameworks,
no external services.

## Layout

- `store/db.py` — `Store` handle: SQLite/WAL with verified pragmas, monotonic
  DB-backed store clock, atomic write transactions (`BEGIN IMMEDIATE`),
  consistent backup via the SQLite backup API, integrity checks.
- `store/migrations.py` — versioned, deterministic, atomic migrations.
  Fresh DB from zero; re-running is a no-op; a failed migration rolls back
  fully and records nothing.
- `store/transitions.py` — the exact state-transition graphs from the
  corrected Phase 0 documents (05/06/07/13, 04-domain-model, ADR-005).
- `store/gate.py` — `TransitionGate`: the single narrow API for all
  authoritative mutations. Validates transitions, verifies preconditions
  inside the transaction, stamps authoritative store time, appends
  hash-chained ledger events, commits. Invalid transitions are rejected
  transactionally; state is left unchanged.
- `tests/test_store.py` — 25 tests against real database files, including
  real SIGKILL crash injection, real backup/restore, and executable
  invariant tests (I-16, I-17, I-18).
- `tests/test_remediation.py` — 18 regression tests for the F1/F2/F3
  remediation (authority boundary, TTL validation, terminal progress guard).
- `tests/_crasher.py` — crash-injection helper (SIGKILLs itself
  mid-transaction).

## Run

    cd ~/workspace/axos
    python3 -m unittest discover -s tests -v

## Key laws enforced in code, not comments

- Worker-supplied timestamps are stored as informational metadata only and
  are never used for lease, expiry, ordering, or fencing semantics.
- `PAUSED_FOR_HUMAN` cannot be exited without a recorded `APPROVED`
  approval — across restarts, unconditionally.
- A recovery attempt recorded as `success` with zero progress delta is
  rejected by the gate *and* by a DB CHECK constraint.
- Workers cannot create tasks or jobs (actor check at creation).
- Checkpoint `VERIFIED` requires a receipt with full manifest +
  ledger-chain verification. No receipt, no trust.
- The lease is the `(owner_worker_id, lease_expires_at, fencing_token)`
  triple on the job row (Phase 0 04), mutated only by atomic conditional
  updates; all lease timestamps are store time.
- Authority boundary (F1): the gate holds the writable `Store`;
  `Store.conn` is read-only (`PRAGMA query_only=ON`); non-gate components
  receive a `ReadOnlyStore` (via `Store.read_only()` /
  `open_readonly_store()`), which exposes no writable connection and no
  transaction capability. `Store.write_txn()` is the gate-owned transaction
  capability. This is an application-level boundary: direct `sqlite3` access
  to the DB file is filesystem-level access and outside it by definition.
- Lease TTLs must be positive numbers of seconds (F2): zero, negative, NaN,
  and non-numeric TTLs are rejected before any mutation.
- Job progress can only be recorded while the job is actively executing
  (`CLAIMED`/`RUNNING`); terminal-state progress mutations are rejected and
  leave the row byte-identical (F3).
- Ledger integrity = tamper-evident against ordinary application-level
  mutation (naive edits are detected), NOT protection against an attacker
  with unrestricted database-file authority (full chain rewrite / tail
  truncation can verify cleanly). No cryptographic key management in v1 —
  recorded as a known property, not solved here.

## Explicitly NOT in Phase 1A

Supervisor, worker processes, recovery controller, fencing enforcement,
synthetic executor, planner, LLM agents, heartbeats ingestion, cron jobs,
multi-node, multi-tenancy. Those are later milestones.
