# AXOS Phase 1A — Implementation Report

Date: 2026-09-23
Scope: authoritative SQLite/WAL store layer + transition-gate API only.
No supervisor, no workers, no recovery logic, no agents — per the brief.

## What was built

`~/workspace/axos/` — stdlib-only Python (sqlite3, hashlib, json):

| File | Contents |
|---|---|
| `store/db.py` | `Store`: WAL + verified pragmas, monotonic DB-backed store clock, `BEGIN IMMEDIATE` write transactions, backup via SQLite backup API, integrity checks |
| `store/migrations.py` | Versioned, ordered, atomic migrations; state recorded in `schema_migrations`; fresh-from-zero and deterministic upgrade |
| `store/transitions.py` | Exact transition graphs from corrected Phase 0 docs (05/06/07/13, 04-domain-model, ADR-005) |
| `store/gate.py` | `TransitionGate`: create/transition/lease/incident/recovery/checkpoint/artifact/validation/ledger operations — the only write path |
| `tests/test_store.py` | 25 tests on real database files |
| `tests/_crasher.py` | Real SIGKILL crash injection mid-transaction |

Verified pragmas on every open: `journal_mode=WAL`, `synchronous=NORMAL`,
`foreign_keys=ON`, `busy_timeout=5000` (SQLite 3.45.1).

## Test results: 25/25 pass

- Fresh init, migration from empty, idempotent re-migrate, atomic migration failure (bad migration rolls back fully, version unrecorded, retry succeeds)
- State creation, valid transitions (task/job/artifact lifecycles), invalid transitions rejected with state unchanged, transaction rollback
- Concurrent claim: two threads, two connections — exactly one wins; ledger chain still verifies
- Lease triple acquire/renew/release; conflicting acquire rejected; stale-token and wrong-owner operations rejected; expiry detected from store time
- Worker-submitted timestamps stored informationally only; lease/expiry derive solely from store time; store time monotonic across wall-clock skew (DB-backed floor)
- I-17: `PAUSED_FOR_HUMAN` survives close/reopen; resume without `APPROVED` approval rejected; `DENIED` approval does not release; `APPROVED` releases
- Canonical incident signatures (same causal shape collapses; raw messages excluded)
- I-18: recovery attempt requires observed effect + progress evidence; `success` with zero progress delta rejected by gate *and* DB CHECK; raw INSERT also rejected
- Checkpoint receipts: `VERIFIED` requires manifest-full + chain-verified receipt; `CORRUPT` terminal and retained
- Validator provenance: identity + version + result + receipt preserved; quarantine marks all verdicts of a version without destroying records
- Real SIGKILL mid-transaction: uncommitted work absent, integrity ok, ledger chain verifies
- Real SIGKILL after commit: committed work durable, uncommitted sibling absent
- Reopen consistency; backup → independent restore (chain verifies, row counts match, later writes to original don't leak)
- Ledger tamper-evidence: direct payload mutation detected by chain verification
- I-16: `worker:*` actors cannot create tasks or jobs

A representative database was also built by hand through the gate and
inspected: 15 hash-chained ledger events verify, all entity tables populated,
lease triple correct, worker timestamp isolated as informational.

## Status (per the Phase 1A reporting law)

- SQLite/WAL authoritative store — IMPLEMENTED + VERIFIED
- Migration system — IMPLEMENTED + VERIFIED
- Transition-gate API — IMPLEMENTED + VERIFIED
- Authoritative store timestamps — IMPLEMENTED + VERIFIED
- Lease primitives (acquire/renew/release/expiry/conflict) — IMPLEMENTED + VERIFIED
- Human-gated stickiness (I-17) — IMPLEMENTED + VERIFIED
- Incident + recovery-attempt records (I-18 evidence model) — IMPLEMENTED + VERIFIED
- Checkpoint + verification-receipt model — IMPLEMENTED + VERIFIED
- Validator provenance + quarantine — IMPLEMENTED + VERIFIED
- Hash-chained ledger + tamper detection — IMPLEMENTED + VERIFIED
- Crash consistency (kill/reopen/WAL recovery) — IMPLEMENTED + VERIFIED
- Backup/restore hooks + correctness — IMPLEMENTED + VERIFIED
- I-16 no-nested-work creation guard — IMPLEMENTED + VERIFIED
- Store-level invariant tests — IMPLEMENTED + VERIFIED
- Supervisor / worker processes — NOT IMPLEMENTED
- Recovery controller / fencing enforcement — NOT IMPLEMENTED
- Heartbeat ingestion / health monitoring — NOT IMPLEMENTED
- Planner / LLM agents / synthetic executor — NOT IMPLEMENTED
- Production backup service (hourly schedule, off-site, weekly drill) — NOT IMPLEMENTED (hooks only; Phase 0 requires the drill before production)
- Multi-node / multi-tenancy — NOT IMPLEMENTED (out of scope for v1)

## Not claimed

The store is not claimed "production-ready." What is established: the
durable state foundation behaves as specified under the tested conditions,
including real process termination. Self-healing, recovery behavior, and
production readiness remain unproven — they belong to later milestones and
the 12 failure-injection tests.

## Stop condition

Phase 1A complete. No Phase 1B work started. Awaiting explicit authorization
before building the supervisor, executor, recovery controller, workers, or
agents.
