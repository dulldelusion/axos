# Failure handling

How AXOS survives the failures it is designed for: process crashes,
supervisor loss, full restarts, corrupted checkpoints, interrupted
migrations, and the ambiguous middle of completion. The governing
principle is the same throughout: **crash first, reconcile from durable
state** — every layer is exercised against real failures, not simulated
ones, and recovery never trusts a pre-crash claim without re-verifying
it.

## Crash injection (SIGKILL)

Crash behavior is tested with real process kills, not mocks.

- **Store-level crashes.** `src/axos/tests/_crasher.py` SIGKILLs itself
  at dangerous points — before the transaction, after `BEGIN`, after
  entity insert, after ledger insert, immediately before `COMMIT`, and
  during concurrent writes. `test_19_wal_recovery_after_process_kill`
  proves uncommitted work is gone atomically; `test_20_committed_data_survives_kill`
  proves committed rows are durable and exact; integrity checks pass
  after every kill (`src/axos/tests/test_store.py`).
- **Worker crash mid-execution.** FI-01: a worker is SIGKILLed
  mid-execution; heartbeats stop, the lease expires, the watchdog
  classifies, R1 reclaims (`RUNNING → UNCERTAIN`), and R8/R9 drive
  verified recovery.
- **Crash after staging, before commit.** FI-02: partial artifacts are
  classified as staging orphans (`classify_staging_orphans`); no
  unclassified orphans may survive a scenario.
- **Supervisor kill.** FI-03: `kill -9` on the supervisor leaves orphaned
  workers running; a fresh supervisor on the same database re-adopts
  durable state and continues.
- **Full restart.** FI-04: `kill -9` on all AXOS processes, then boot
  from disk — `exec/boot.py` runs the recovery pass (phases
  `STARTING → RECOVERING → READY | BLOCKED`), validates SQLite integrity
  and the ledger chain, observes expired leases (R4), and force-reclaims
  live leases whose owner process is definitively dead. No new worker
  starts until the boot report phase is `READY`.

**R14 honesty note.** The internal R14 fault-injection campaign is
complete: FI-01…FI-12, each executed twice, on a real VM
(`reports/historical/R14-EVIDENCE.md`). What remains blocked is the
*external* real-VM lifecycle proof: genuine hypervisor-level power-cycle
for FI-04 and the FI-12 VM-restart variant cannot be executed because the
environment provides no hypervisor-level hard power-off capability — and
simulated evidence was not substituted and declared success. See
`reports/historical/R14-EXTERNAL-VALIDATION.md` for the frozen release
identity and the handoff specification for a future external execution.

## Backup / restore

`Store.backup_to()` (`src/axos/store/db.py`) takes a consistent backup
via the SQLite backup API — a strict, consistent prefix of the original
database, even under active write load; later writes do not leak into it.
The restored copy is verified independently: integrity check passes and
the ledger chain verifies (`src/axos/tests/test_store.py::test_22_backup_restore_correctness`;
also attacked in the Phase 1A audit, audit 06:
`reports/historical/AUDIT-REPORT.md` §A.8 — backup taken during a
sustained write load restored as a strict prefix, 3839 of 4705 tasks).

## Migration failure atomicity

Migrations are versioned, deterministic, and atomic
(`src/axos/store/migrations.py`): a fresh database builds from zero,
re-running is a no-op, and a failed migration rolls back fully and
records nothing. The audit proved this with a real SIGKILL halfway
through applying DDL: no version row, no partial tables (only
`schema_migrations`), integrity ok, and a subsequent `migrate()` applied
cleanly (`reports/historical/AUDIT-REPORT.md` §A.9). Migration v5→v6
additionally preserves legacy `recovery_attempts` rows byte-identical
while the I-18 `CHECK` keeps rejecting success-without-progress afterward
(`src/axos/tests/test_recovery_r8.py::test_R8_M1_v5_to_v6_migration`).

## Uncertain completion → reconciliation

The crash-after-stage case is the canonical uncertain completion:
artifact bytes are staged and verified, but the worker dies before
`commit_artifact`. The lease expires; R4 observes; R1 moves
`COMMITTING → UNCERTAIN` (never a blind requeue — the bytes may already
be the completed result).

Resolution is the UNCERTAIN resolver path through `commit_artifact()`,
available only to reclaim-authority actors and requiring artifact token
lineage that predates the reclaim:

- **Adopt** when the staged bytes re-verify: the gate re-reads the bytes
  and recomputes the full `VERIFIED` predicate inside the completion
  transaction — it never trusts the pre-crash verification alone.
- **Requeue** when they do not (the `corrupt_staged_bytes` injection
  forces the requeue path).

(`src/axos/tests/test_final_hardening_r14.py`, FI-02; the recovery
architecture export §7/§14.2.)

General reconciliation follows the same discipline everywhere:

- **R8 crash reconciliation** checks durable action effects; it never
  blindly redispatches another controller's action. Safe ambiguity ends
  in `BLOCKED`, `UNCERTAIN`, or escalation — never guessing.
- **R10 orphan redispatch** re-dispatches `CLAIMED` jobs owned by a dead
  scheduler **under the same claim identity** (`expect_token` carries
  the durable token) — never re-claimed; expired-lease orphans are left
  to R4/R1.
- **R11 reconciler** converges actual → desired with zero mutation on
  refusal: contradictory desired state refuses with no writes at all.
- **R12 probe evaluation** reads durable evidence only (job `COMPLETE`
  or positive `progress_done` delta); heartbeats and process existence
  are never consulted.
- **R13 finalization** is the read-only consumer: the gate re-validates
  everything authoritatively inside one `write_txn` and flips to
  `FINALIZED` atomically, or refuses.

## Where failure provably cannot propagate

Each claim below is grounded in an observed authority boundary (see the
recovery architecture export §13):

- A wedged or hostile worker cannot take down the control plane — no
  call path exists from any worker into supervisor, scheduler, watchdog,
  recovery, or policy code; the worst it can do is burn its own lease
  epoch, which expires (R4) and is reclaimed (R1) without cooperation.
- A dead scheduler cannot strand work — beats plus orphan redispatch
  under the same claim identity; expired-lease orphans recycled by
  R4/R1.
- A failed watchdog degrades classification, not revocation — R4 expiry
  observation continues independently.
- R8 failure cannot corrupt budgets or rungs — budgets live in R9's
  policy rows; a crashed attempt stays open and is accounted on the next
  reconcile pass.
- R9 failure cannot execute anything — R8's autonomous loop and orphan
  redispatch continue; escalation *to* R9 is R8's rule.
- A dead breaker controller fails closed, never open — `OPEN` breakers
  keep denying; they never admit work they should not.

The store is the single point of failure, and the design admits it:
`BEGIN IMMEDIATE` serializes writers, one authoritative timestamp per
transaction, hash-chained ledger (tamper-evident, not tamper-proof),
atomic migrations. No replication, failover, backup automation, or
tamper-proofing — known boundaries, not oversights.

## Related

- [Recovery model](recovery-model.md) — the incident lifecycle.
- [Escalation](escalation.md) — the rung ladder and terminal pause.
- [Invariants](../invariants/invariants.md) — I-4, I-16, I-17, I-18.
- [Operations](../operations/README.md) — verification and
  troubleshooting.
