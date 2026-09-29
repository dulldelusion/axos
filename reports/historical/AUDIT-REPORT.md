# AXOS Phase 1A — Adversarial Audit Report

Date: 2026-09-23
Scope: `~/workspace/axos/` — store layer only (Phase 1A). No Phase 1B work started.
Method: independent attack scripts in `~/workspace/axos/audit/` (`01_write_path.py`
through `07_race.py`). All attacks use only the public `Store`/`TransitionGate` API
plus real `SIGKILL`s. The existing 25-test suite was NOT treated as verification;
every claim below was re-proven or re-attacked independently.
The implementation was not modified during this audit.

## Claims matrix

| Claim | Implementation location | Existing test | Independent verification | Status |
|---|---|---|---|---|
| SQLite/WAL authoritative store | `store/db.py` `Store._apply_pragmas` | test_01 | `pragma_report` on reopened DBs after kills; integrity checks in audit 06 | VERIFIED |
| Store-authoritative timestamps | `store/db.py` `write_txn`/`current_time` (+DB-backed floor) | test_12, test_13 | audit 03: future worker ts ignored; rollback/jump handled; `worker_reported_ts` never read for decisions (grep) | VERIFIED |
| Single write path (gate is the enforcement boundary) | `store/gate.py` docstring (convention only) | none | audit 01: 11/12 bypasses succeed via public `Store.conn`/`write_txn` | **BROKEN** |
| Transition enforcement | `store/gate.py` `_transition`, `store/transitions.py` | test_05, test_06 | audit 02: 34/34 illegal transitions rejected, state byte-identical after | VERIFIED (through gate) |
| Lease semantics | `store/gate.py` claim/renew/release/`expired_leases` | test_08–11 | audit 03: 23/24; audit 07: renew/release race serializes cleanly | PARTIALLY VERIFIED |
| I-16 (no nested work creation) | `store/gate.py` `_check_work_creator` | test_24 | audit 01 + 02: `worker:*`/`loader`-style actors rejected on create paths | VERIFIED (through gate) |
| I-17 (human-gated stickiness) | `store/gate.py` `transition_task` gate clause | test_14 | audit 04: 12/12 — restart, reopen, worker loss, lease expiry, recovery attempts, wrong/stale/foreign/duplicate approvals all handled | VERIFIED (through gate); **BROKEN if approvals table written directly** (depends on single-write-path) |
| I-18 (recovery verified by progress) | `store/gate.py` `record_recovery_attempt` + DB `CHECK` | test_16 | audit 04: 11/11 — zero/negative/empty success rejected at gate AND by CHECK (even on direct UPDATE); 'process restarted' ≠ success | PARTIALLY VERIFIED (structural only — truth of evidence is a recovery-controller concern) |
| Ledger integrity | `store/gate.py` `_append_event`/`verify_ledger_chain` | test_23 | audit 05: naive tamper/missing/reorder detected; **full rewrite and tail truncation undetectable** | PARTIALLY VERIFIED (tamper-evident, not tamper-proof — matches Phase 0's actual requirement) |
| Crash consistency | `store/db.py` WAL + `write_txn` | test_19, test_20 | audit 06: real SIGKILL at 6 dangerous points + kill during concurrent writes; no partial state, integrity ok | VERIFIED |
| Backup/restore | `store/db.py` `backup_to` | test_22 | audit 06: backup under active write load is a consistent prefix; independent restore verifies, no leakage | VERIFIED |
| Validator provenance | `store/gate.py` `record_validation`/`quarantine_validator` | test_18 | audit 02/04: records survive quarantine; quarantine is terminal; receipt required | VERIFIED (through gate) |
| Checkpoint verification | `store/gate.py` `set_checkpoint_verification` | test_17 | audit 02: UNVERIFIED→VERIFIED skip rejected; VERIFIED without chain proof rejected; VERIFIED terminal | VERIFIED (through gate) |

## A. VERIFIED CLAIMS

Independently confirmed by execution, not by test names:

1. **SQLite/WAL store with the specified pragmas.** `journal_mode=WAL`,
   `synchronous=NORMAL`, `foreign_keys=ON`, `busy_timeout=5000` observed on real
   files, including files reopened after `SIGKILL`. `integrity_check` returns ok
   after every kill scenario.
2. **Store-authoritative, monotonic timestamps.** Every committed write stamps
   `now` from `write_txn`; the floor `max(clock(), last_commit_ts)` is persisted
   in `axos_meta` and survives reopen. A worker-supplied timestamp 50,000s in the
   future did not move a lease by one second. Clock rollback cannot move store
   time backwards. Grep confirms `worker_reported_ts` is write-only —
   no decision path reads it.
3. **Transition-graph enforcement through the gate.** 34 attacks across tasks,
   jobs, workers, artifacts, approvals, checkpoints: skips, backward moves,
   self-transitions, bogus states, terminal-state escapes (`FINALIZED`,
   `CANCELLED`, `COMPLETE`, `DEAD`, `VERIFIED`, `RELEASED`), replay after
   terminal, missing entities — all rejected with `TransitionRejected`, and the
   pre/post state comparison proved the database was left unchanged in every case.
   Rejected transitions append no ledger event.
4. **Lease authority through the gate.** 10/10 two-thread claim races had exactly
   one winner. Renew-before-expiry succeeds; renew-after-expiry, wrong owner,
   wrong token, stale token, double-release all fail. Fencing tokens strictly
   increase across claims, so a fenced-out owner's stale token is useless after
   reclaim. Renew-vs-release races serialize cleanly (single conditional UPDATEs
   under `BEGIN IMMEDIATE`; both-True orderings are valid sequential histories).
   Non-numeric TTL raises `TypeError` with the job left `PENDING`.
5. **I-17 stickiness and approval discipline through the gate.**
   `PAUSED_FOR_HUMAN` survived close/reopen, worker-row deletion, lease expiry,
   unrelated transitions, and repeated recovery attempts. Resume was rejected
   without approval, with `DENIED`, with `EXPIRED`, with another task's approval,
   and with a nonexistent approval. Double `APPROVED` on one approval rejected.
   A genuine `APPROVED` released the gate.
6. **I-18 structural enforcement.** `success` with zero/negative progress or
   empty/missing effect is rejected by the gate AND by the database `CHECK`
   constraint — including on direct `UPDATE`, which SQLite also polices.
   "Process restarted successfully" with zero progress cannot be recorded as
   success. Honest `retry`/`escalate` with zero progress remain representable.
7. **Crash consistency.** Real `SIGKILL` before txn, after `BEGIN`, after entity
   insert, after ledger insert, immediately before `COMMIT`, during concurrent
   writes: uncommitted work absent, integrity ok, ledger chain verifies.
   `SIGKILL` immediately after `COMMIT`: the row is durable, preserved exactly.
8. **Backup/restore.** Backup taken during a sustained write load (loader thread
   creating tasks) restored independently: integrity ok, ledger chain verifies,
   backup is a strict consistent prefix of the original (3839 of 4705 tasks),
   later writes did not leak in, schema version and FK/WAL behavior intact.
9. **Interrupted migration.** `SIGKILL` halfway through applying v1 DDL left no
   version row, no partial tables (only `schema_migrations`), integrity ok, and a
   subsequent `migrate()` applied cleanly.

## B. PARTIALLY VERIFIED CLAIMS

1. **Lease semantics.** Two gaps (see E: D2, D3). The fencing/ownership/expiry
   core is solid; input validation and terminal-state hygiene are not.
2. **I-18.** The store enforces *shape* (evidence present, positive delta for
   success). It cannot enforce *truth*: a plausible-but-fabricated
   `observed_effect` with a positive delta is storable, and cross-incident
   evidence linkage is unchecked. This is correctly a recovery-controller
   responsibility (later milestone), but "recovery is verified by progress" is
   therefore only structurally — not semantically — enforced at this layer.
3. **Ledger integrity.** Naive tampering (payload edit, middle deletion,
   reorder) is detected with the exact offending `seq`. But a full chain rewrite
   with recomputed hashes verifies cleanly, and tail truncation (deleting the
   newest events) is undetectable by `verify_ledger_chain` — there is no external
   anchor. The hash is unkeyed SHA-256: tamper-*evident*, not tamper-*proof*.
   This matches Phase 0's actual requirement (doc 20 says tamper-evident), and
   checkpoints bind `ledger_tip_seq`, which gives an auditor a cross-check
   against truncation — but nothing enforces that cross-check today.

## C. BROKEN CLAIMS

1. **"TransitionGate is the enforcement boundary" / single write path — BROKEN
   as a mechanism.** `Store.conn` is a public property (`store/db.py:168`) and
   `Store.write_txn` is public. Any ordinary in-process caller can bypass the
   gate entirely. Demonstrated (audit 01, all on real DBs):
   - direct `INSERT` of a task in `EXECUTING` — succeeds, **zero ledger trace**,
     and the ledger chain still "verifies" (it cannot see what was never recorded);
   - direct `UPDATE` jumping a job `PENDING → COMPLETE`;
   - lease theft: direct `UPDATE` of `owner_worker_id`/`fencing_token`, and
     silent lease release with no token check;
   - **I-17 defeated**: a directly-inserted `APPROVED` approval, or a direct
     `status='APPROVED'` flip on a pending approval, lets the gate resume a
     `PAUSED_FOR_HUMAN` task — the gate trusts the `approvals` table, which is
     not gate-protected;
   - fabricated recovery `success` with plausible evidence via direct `UPDATE`;
   - silent `DELETE` of incidents (after child rows);
   - `migrate()` executes caller-supplied SQL lists.
   
   The gate is the *only documented* write path, but nothing in the code makes
   it the *only possible* one. Every "VERIFIED (through gate)" claim above is
   therefore conditional: the guarantees hold for writers that use the gate, and
   evaporate for writers that don't. For Phase 1B this matters directly — the
   supervisor will be a new, large writer.

## D. UNTESTED ATTACK SURFACE

- Disk-full (`ENOSPC`) during commit — behavior unknown (write_txn rolls back on
  exception, but SQLite error paths under ENOSPC were not exercised).
- WAL-file deletion or corruption while the DB is open.
- `synchronous=NORMAL` durability across *OS* crash / power loss (all kill tests
  were process-level; NORMAL may lose the last transaction on OS crash — Phase 0
  doc 24's durability bar was not tested at this level).
- `busy_timeout=5000` exhaustion under extreme write contention.
- Non-UTF8 or pathological-size payloads in ledger/tasks.
- Approval single-use: a consumed `APPROVED` approval can be reused for a later
   pause→resume cycle (allowed; Phase 0 does not specify single-use — recorded
   as an open semantic question, not a defect).
- No automated cross-check that checkpoint `ledger_tip_seq` values are consistent
  with the actual ledger tip (the mitigation for tail truncation exists as data,
  not as enforcement).

## E. PHASE 1A DEFECTS

**D1. Write path is convention, not mechanism.**
File: `store/db.py` (`Store.conn` property, line 168; public `write_txn`).
Failure mechanism: any in-process caller obtains the raw connection and issues
arbitrary SQL; no authorizer, no read-only handle, no capability separation.
Impact: bypasses transitions, leases, approvals (defeats I-17), ledger
completeness, and silent deletes — the entire invariant base is conditional on
writer discipline.
Reproduction: `audit/01_write_path.py` — 11/12 bypasses succeed.

**D2. No validation of lease durations.**
File: `store/gate.py`, `claim_job` (line 237).
Failure mechanism: `ttl_s <= 0` is accepted; a negative TTL mints an
immediately-expired lease (`lease_expires_at < lease_acquired_at`).
Impact: low — sloppy input becomes a nonsense lease; a supervisor bug passing a
bad TTL would silently get "acquired" leases that are already dead.
Reproduction: `audit/03_leases.py` — negative TTL check.

**D3. Progress writable on terminal jobs.**
File: `store/gate.py`, `update_job_progress` (line 210).
Failure mechanism: ownership/token/expiry are checked, but job `status` is not;
a former owner with a still-valid token can write `progress_done` after
`COMPLETE`.
Impact: low-medium — post-terminal mutation of supposedly-final state.
Reproduction: `audit/03_leases.py` — "progress update on COMPLETE job".

**D4. `mark_worker_seen` silently no-ops on unknown workers.**
File: `store/gate.py`, line 333.
Failure mechanism: `UPDATE ... WHERE worker_id=?` matches zero rows; no error.
Impact: negligible — heartbeat for a nonexistent worker vanishes silently.
Reproduction: call with an unknown id; no exception, no row.

**D5 (limitation, not vs-spec). Ledger rewrite/truncation undetectable.**
File: `store/gate.py`, `verify_ledger_chain`.
Failure mechanism: unkeyed hash chain; attacker with DB write access recomputes
hashes or truncates the tail and verification passes.
Impact: medium if oversold; within Phase 0's tamper-evident requirement, but the
report must not imply tamper-proofness.
Reproduction: `audit/05_ledger.py` — full-rewrite and truncation checks.

## F. REQUIRED FIXES BEFORE PHASE 1B

Only what is actually necessary — all are small and in 1A scope:

- **F1.** Make the write path a mechanism, not a convention: add
  `Store.read_only()` returning a handle with `PRAGMA query_only=ON` (or
  `mode=ro`), and establish the rule that every non-gate component receives only
  the read-only handle while the gate owns the sole write connection. Honest
  scoping: in-process Python can never stop *deliberate* bypass; this stops
  *accidental* bypass, which is the realistic threat from our own supervisor
  code. Without F1, the claim "the gate is the enforcement boundary" must be
  struck from the report.
- **F2.** Reject `ttl_s <= 0` in `claim_job`/`renew_lease` with
  `TransitionRejected`.
- **F3.** Reject `update_job_progress` unless job status is `CLAIMED` or
  `RUNNING`.
- (Optional, trivial) **F4.** `mark_worker_seen` on an unknown worker should
  raise rather than silently no-op.

D5 needs no code fix; it needs the report to stop implying tamper-proofness, and
a later milestone should enforce the checkpoint `ledger_tip_seq` cross-check.

## G. PHASE 1B GATE

**BLOCKED**

The state machine itself earned high marks under fire: 34/34 transition
attacks, 23/24 lease attacks, 23/23 I-17/I-18 attacks, and 29/29
crash/backup/migration attacks behaved correctly through the gate, and no
authority-confusion was found (worker time, memory, filesystem, logs, and cache
are never treated as authoritative — verified by grep and execution). But the
load-bearing architectural claim — that the gate is the *enforcement boundary*
for authoritative state — is false as implemented: it is a documented convention
around a publicly writable database, and I demonstrated forging an approval to
defeat the human gate. A supervisor built on this store today would inherit
guarantees that hold only while every future writer behaves. Fix F1–F3 (small,
1A-scope), re-run this audit's scripts, then re-open the gate.

PHASE 1A ADVERSARIAL GATE: BLOCKED
