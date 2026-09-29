# AXOS Test Architecture

Read-only source export, 2026-09-28. Grounded in `~/workspace/axos/tests/`,
`~/workspace/axos/audit/`, and root evidence docs (`R14-EVIDENCE.md`,
`R14-EXTERNAL-VALIDATION.md` / `.json`, `AUDIT-REPORT.md`,
`PHASE1A-REPORT.md`, `PHASE-1C-FINAL-STATUS.md`).

The suite standard, stated in nearly every module docstring: **real
SQLite/WAL files, real subprocesses, real SIGKILL/SIGTERM, real
monotonic timing — no mocks for the critical guarantees** (FakeClock only
for deterministic timing assertions; injected in-transaction failures only
to prove atomic rollback). Test counts below are `def test_` counts from
this worker's own grep of the tree.

## 1. Phase 1A — store layer

| Test file | Tests | Purpose |
|---|---|---|
| `tests/test_store.py` | 25 | Authoritative store + gate API: WAL/pragma enforcement, store-authoritative monotonic timestamps, transition graphs, leases, I-16/I-17/I-18, ledger chain, crash injection (real SIGKILL via `tests/_crasher.py`), backup/restore |
| `tests/test_remediation.py` | 18 | Regression suite for the Phase 1A F1/F2/F3 remediation: F1 authority boundary (`Store.conn` read-only, `ReadOnlyStore`, gate-owned `write_txn`), F2 positive-numeric TTL validation, F3 terminal-state progress guard |

**Major invariants exercised (Phase 1A):** SQLite/WAL with verified
pragmas; every committed write stamps store time (worker timestamps are
informational only and never decide leases/expiry/ordering/fencing);
34 illegal transition-graph attacks rejected with byte-identical state;
fencing tokens strictly increase; `PAUSED_FOR_HUMAN` exits only via a
recorded `APPROVED` approval; `success` recovery attempts with zero
progress delta rejected by gate *and* DB CHECK constraint; workers cannot
create tasks/jobs (actor check, I-16); checkpoint `VERIFIED` requires a
receipt with full manifest + ledger-chain verification.

**Independent audit:** `AUDIT-REPORT.md` (2026-09-23) re-proved claims
without trusting test names. Verdicts: SQLite/WAL, store timestamps,
transition enforcement, lease semantics, I-16, I-17, I-18, crash
consistency, backup/restore, validator provenance, checkpoint
verification — VERIFIED; ledger integrity PARTIALLY VERIFIED
(tamper-evident, not tamper-proof — matches Phase 0); single write path —
**BROKEN** (11/12 bypasses through public `Store.conn`/`write_txn`
succeeded) → this drove the F1 remediation; audit 08 re-tested all
bypasses post-F1 (39/39 blocked/correct). Gate: "Phase 1A complete"
(`PHASE1A-REPORT.md`).

## 2. Phase 1B — execution substrate

| Test file | Tests | Purpose |
|---|---|---|
| `tests/test_exec.py` | 36 | Execution substrate: fencing scenarios (gate + worker level), concurrent commit/claim races (stale token never wins, exactly one claim winner), lease renewal, heartbeat ingestion, idempotency under duplicate delivery, real process lifecycle (exit codes, signals, restart), worker/supervisor crash fault injection (brief §12: CRASH-1..10), supervisor SIGKILL with reconstruction from store, observability, identity model |

## 3. Phase 1C — R1..R14

| Gate | Test file | Tests | Purpose / major invariants | Faults injected |
|---|---|---|---|---|
| R1 lease-reclaim primitive | `tests/test_reclaim.py` | 23 | Expired reclaim from CLAIMED/RUNNING/COMMITTING; non-expired/stale-token/wrong-owner rejected with zero mutation; concurrent reclaim → exactly one winner, one token increment; old owner fenced from commit/renew/heartbeat/progress; token monotonicity; injected in-txn failure → no partial reclaim; store-time authority (worker ts never decides expiry) | real concurrent writers; injected failure inside the reclaim transaction |
| R2 enforced process-group self-fencing | `tests/test_fence_enforce.py` | 18 | Authorized R1 reclaim → automatic sweep enforcement; process-group death (parent/child/grandchild); graceful SIGTERM (exit 143, no SIGKILL); ignored SIGTERM → SIGKILL escalation; **fence→death latency < H, measured 0.1075–0.1274s vs H=0.4s**; sweep every ~H/2; owner/token divergence; already-dead → evidence only; exactly-once enforcement; supervisor restart adopts from durable evidence | real subprocesses, real SIGTERM/SIGKILL via killpg, real supervisor restart on same db |
| R3 durable heartbeat/progress | `tests/test_heartbeat_r3.py` | 22 | Heartbeat → durable row with store-time ts, worker ts informational; seq monotonicity (N+1→N rejected, zero mutation); duplicates rejected; stale-token heartbeat rejected + journaled; owner-cleared heartbeat restores nothing; highest valid seq wins concurrently; progress writes stamp store time; **heartbeat alone never advances progress**; races never let stale win | real worker subprocesses; threads with one Store per thread (as in production); injected rollback failures |
| R4 lease-expiry observation | `tests/test_expiry_r4.py` | 19 | `observe_expired_leases()`: read-only, deterministic observer — **advisory evidence, never authorization**; exact boundary (`lease_expires_at == now` is expired); future worker ts cannot prevent expiry; heartbeat without renewal leaves lease expired; legitimate renewal prevents false expiry; renewal/reclaim races never falsely reclaim; idempotent observation, no duplicate mutations | real store clock (FakeClock only for determinism); real worker subprocesses via supervisor |
| R5 verified artifacts & integrity | `tests/test_artifacts_r5.py` | 37 | **I-4: no artifact-free path to COMPLETE** (explicit R5-32 regression); content addressing (sha256, gate-computed); worker-claimed checksum mismatch rejected; real staging (fsync'd bytes); validator provenance (receipt identity + hash binding); real corruption → quarantine + durable FAIL; VERIFIED requires validators (no setter/bypass); stale worker cannot mutate; atomic completion (single txn: COMPLETE + artifact + hash + ledger); missing artifact → completion rejected; checkpoint VERIFIED requires receipt; per-job provenance via v12 `artifact_stagings` | real bytes, real sha256, real on-disk byte corruption, real fsync |
| R6 boot recovery | `tests/test_boot_r6.py` | 27 | Clean boot → READY with boot journal milestones; authoritative worker adoption (live worker, current authority); stale worker fenced via R2; already-dead → ALREADY_DEAD (boot not blocked); PID reuse → PID_REUSE, unrelated process survives; real SIGKILL of supervisor/driver; reconstruction purely from durable state | real SIGKILL of supervisor/driver processes; real supervisor boot/restart on same db (R6-05's PID-reuse observation is injected at the observation layer — documented in that test, the one controlled exception) |
| R7 watchdog | `tests/test_watchdog_r7.py` | 34 | Deterministic HEALTHY/STALLED/DEAD verdicts; progress stale + heartbeat fresh → STALLED; fresh progress prevents STALLED; definitively dead worker → DEAD; missing heartbeat alone ≠ DEAD; expired lease composes with R4 read-only; **watchdog cannot reclaim through an R1 bypass** (behavioral + static); consumes durable `worker.proc_reaped` evidence via read-only `latest_spawn_generation` (R14 fix) | real subprocesses/process groups; real signals; real supervisor boot/restart |
| R8 recovery controller | `tests/test_recovery_r8.py` | 39 | Controller refuses before READY; HEALTHY → no incident/attempt/reclaim/fence/restart; no verdict → no incident; STALLED/DEAD + live lease → incident + attempt#1 (reclaim), full Recovery Contract fields, canonical rung 2; reclaim verify → durable token bump/owner clear → success with measured delta; zero progress twice → escalate; no blind third retry; recovery-loop breaker | real subprocesses/process groups for fence/restart; gate-level fixtures elsewhere |
| R9 recovery ladder/budgets/escalation | `tests/test_recovery_r9.py` | 55 | Policy layer above R8: **chooses** the rung from the canonical 5-rung ladder, **bounds** recovery (durable per-rung and per-incident attempt budgets via CAS UPDATEs), **decides** terminal escalation; never executes or verifies an action; deterministic migration v6→v7 expectation (see §5) | deterministic seeding through real R8 authority APIs; real components, no store/gate/policy mocks |
| R10 scheduler/admission | `tests/test_scheduler_r10.py` | 47 | PENDING discovered → admitted (CLAIMED, proc spawned, `proc_spawned` evidence); non-PENDING (CLAIMED/RUNNING/BLOCKED/FAILED/COMPLETE/UNCERTAIN) never scheduled; claim atomic → exactly one winner with full lease triple + ledger event; fencing token bumped; worker receives expected token | real subprocesses for dispatch/worker lifecycle; claim races across threads and across two Scheduler instances |
| R11 desired-state reconciliation | `tests/test_reconciliation_r11.py` | 56 | Durable desired-state snapshot (head version bumps, hash changes); deterministic snapshot identity (same spec → same head hash; tampered spec → FAILED, zero mutation); canonical job_id stable across restarts (`axos-desired-job:v1`); diff detects missing items; idempotent creation | deterministic fault injection (wrapped gate methods); one real-process restart; real worker subprocesses for dispatch/completion legs |
| R12 circuit breakers | `tests/test_resilience_r12.py` | 74 | Default CLOSED; double-recorded failure counted once; scope fan-out (JOB/TASK/GLOBAL); threshold → OPEN via CAS with ledger event; OPEN → claim path denies retries (no storm); half-open probe → CLOSE on success; scheduler's resilient admission path | FakeClock for deterministic windows; real worker crashes via supervisor; real watchdog; three real worker crashes driving the TASK breaker (FI-07) |
| R13 finalization/release checkpoint | `tests/test_finalization_r13.py` | 82 | Generation begin → OPEN, version 1, idempotent re-begin; durable run identity (`fin-` id stable across restart); deterministic snapshot (manifest `desired_state_hash` == head hash); desired-satisfaction gate: items with no job or PENDING/CLAIMED/RUNNING/COMMITTING jobs → BLOCKED (`DESIRED_WORK_UNSATISFIED`); `latest_known_good` fail-closed + `invalidate_checkpoint` (R14 fix); release checkpoint entries | deterministic fault injection (wrapped gate methods, documented raw-SQL tamper — the gate itself can never produce such rows); real worker subprocesses where composition needs them |
| R14 final hardening & release proof | `tests/test_final_hardening_r14.py` | 123 | **Track B** (25): fault-injection campaign FI-01..FI-12, each ×2 consecutive, + environment record. **Track C** (49): full-system composition, concurrency/races, fencing, recovery convergence, breaker/finalization composition, stray-process audit. **Track D** (49): corruption/restore, artifact/checkpoint adversarial battery, release manifest generation, clean boot. Earlier section files (`test_r14_b_injection.py`, `test_r14_c_composition.py`, `test_r14_d_corruption.py`) were merged into this single file and removed | see §4 |

## 4. R14 fault-injection campaign — exactness assessment

From `R14-EVIDENCE.md` (authoritative; FI source: `05-failure-injection-plan.md`
Test 1..12). Every FI ran **twice consecutively** on the real VM, PASS/PASS.

| FI | Scenario | Assessment |
|---|---|---|
| FI-01 | worker crash | EXACT — real SIGKILL, real watchdog DEAD, real reclaim; stale token rejected on renew + stage |
| FI-02 | kill after stage | EXACT — adopt branch and corrupt-bytes requeue branch |
| FI-03 | supervisor kill | EXACT injection + behavioral criteria; **LIMIT:** `service_restart` ledger event journaled by the harness (standing in for the bootstrap monitor), not emitted by production |
| FI-04 | full VM restart | **HONEST-EQUIVALENT — the gate-blocking gap.** Real kill -9 of all AXOS processes then boot-from-disk recovery by a fresh Supervisor; 8/8 jobs COMPLETE, no rescheduling, checkpoint intact, UNCERTAIN adopted. **NOT EXECUTED:** genuine hypervisor/OS hard power-off (page-cache loss, wall-clock jumps). `vm_recovery_complete` journaled by the harness with payload `"honest_equivalent"` stating exactly what was done |
| FI-05 | checkpoint corruption | EXACT injection (real on-disk byte corruption post-verification); **LIMIT:** corruption detection has no production re-validation API (test helper); C1 fallback assembled via read-only SQL + production APIs |
| FI-06 | stall | EXACT — scripted `stall_progress`; L2 STALLED verdict before any recovery; measured deltas; never reported healthy |
| FI-07 | dependency failure | HONEST-ANALOG — Phase 1C has no external-service breaker; the real TASK breaker driven with three real worker crashes: OPEN → deny retries → half-open probe → CLOSE |
| FI-08 | repeated recovery failure | EXACT — two crashing replacements, zero progress delta twice → escalated per R9; no third blind retry; recovery-loop breaker opens; spend within budget |
| FI-09 | wrong validator | EXACT — inverted-threshold validator quarantined; bad-window artifacts revalidated independently; zero FINALIZED artifacts carrying only the bad verdict |
| FI-10 | partition flap / fencing | CORE PROPERTY PROVEN with real mechanisms — three wedged workers, real forced-reclaim fencing transactions, fence→death latency ≤ one heartbeat on a real monotonic clock; stale-token rejection on renew/stage/begin_commit/verify. **LIMIT:** the 5-flap A/B token-churn choreography not replicated step-for-step (no network to partition on a single-VM SQLite architecture; covered by the R14-20…32 race group instead) |
| FI-11 | checkpoint sampling | EXACT — 60 artifacts, 5% seeded sample with stated detection bound, ~3% post-verification corruption, release 100% revalidation catches all, no FINALIZE until repaired |
| FI-12 | pause drill (SEV-2) | HONEST-EQUIVALENT — operator pause mid-flight; pause survives supervisor restart (I-17 stickiness, no auto-resume); APPROVED approval resumes; remaining jobs complete. **LIMIT:** canonical "VM restarted mid-pause" executed as supervisor restart |

Suite rules asserted per FI: ledger chain verifies; no orphaned leases; no
running fenced-out workers; no unclassified staging orphans; every
recovery action has a contract row with a measured progress delta.

## 5. Status — from actual evidence only

Gate matrix source: `PHASE-1C-FINAL-STATUS.md` (2026-09-24).

- **R1–R13: COMPLETE.** Each cites a slice campaign plus the full-suite
  ×3 campaign on the final tree: **735/735 + 10 subtests, three
  consecutive full-green runs** (~366s each). R5 37/37 after the v12
  fix; R9 55/55 after the fix below.
- **Test defect found and fixed in evidence:** the pre-fix tree ran
  734/735 ×3 with one deterministic failure —
  `test_recovery_r9.py::TestR9M1::test_R9_M1_v6_to_v7_migration` —
  classified TEST DEFECT (stale hardcoded migration list `[7, 8, 9, 10,
  11]`; production v12 correct). Fixed by updating the expectation to
  `[7, 8, 9, 10, 11, 12]`; the manifest was regenerated after this fix.
- **R14: BLOCKED — REAL-VM PROOF INCOMPLETE.** The internal campaign is
  VERIFIED (R14 suite 123/123 ×3; full suite ×3; audits 01–09 + A20 all
  green — audit 09: 177/177; 12/12 FI ×2; 0 stray processes), but the
  R14 contract prescribes BLOCKED when any exact canonical scenario is
  missing — FI-04's genuine hypervisor power-off was not executed.
- **External validation: BLOCKED — EXTERNAL VM POWER-CYCLE UNAVAILABLE**
  (`R14-EXTERNAL-VALIDATION.md` / `.json`, 2026-09-24 attempt). Recon:
  no libvirt socket, no hypervisor tooling (`virsh`, `qemu-*`,
  `VBoxManage`), no metadata service, no reachable platform control API,
  no external observer host. FI-04 ×2 and the FI-12 VM-restart variant
  ×2 never ran. **No substitute simulation was executed and no mocked
  evidence was created** (per mission §13). The validation spec freezes
  the release identity, observer contract, procedures, and acceptance
  rules for a future external run; promotion to R14_COMPLETE requires
  FI-04 ×2 and FI-12 VM-restart ×2 under those rules with zero manual
  healing.
- **Phase 1C: BLOCKED PENDING EXTERNAL VALIDATION** — all software gates
  complete; the only open gate is the external power-cycle proof.

## 6. audit/ scripts — roles

Standalone scripts (top-level code; run `python3 audit/<name>.py`), using
only the public `Store`/`TransitionGate` API plus real SIGKILLs unless
noted. They are adversarial, not unit tests.

| Script | Role |
|---|---|
| `audit/01_write_path.py` | Attack the single-write-path claim — 11/12 bypasses via public `Store.conn`/`write_txn` confirmed possible (historical baseline; this was the Phase 1A "BROKEN" claim that drove F1) |
| `audit/02_transitions.py` | Attack the transition graph — 34/34 illegal transitions rejected, state byte-identical after |
| `audit/03_leases.py` | Attack lease authority — 25/25 correct |
| `audit/04_i17_i18.py` | Attack I-17 human-gated stickiness and I-18 recovery-evidence truth — 23/23 correct |
| `audit/05_ledger.py` | Attack the ledger — 10/10 as expected: naive tamper/missing/reorder detected; full chain rewrite and tail truncation undetectable (tamper-evident, not tamper-proof — recorded as known property) |
| `audit/06_crash_backup_migration.py` | Crash consistency at 6 dangerous kill points (via `audit/_killpoints.py`: before_txn / after_begin / after_entity / after_ledger / before_commit / after_commit) + kill mid-concurrent-writes + kill mid-migration (via `audit/_migkill.py`); backup under active write load is a consistent prefix — 29/29 correct |
| `audit/07_race.py` | Claim/renew/release races — 0/20 inconsistent races |
| `audit/08_retest.py` | Re-test of the 11/12 audit-01 bypasses **after the F1 remediation** — 39/39 blocked/correct |
| `audit/09_authority_audit.py` | Authority-boundary audit of the execution layer (the F1 enforcement): A1 exec/ never calls `write_txn()`; A2 exec/ never opens its own sqlite3 connections; A3 no SQL write statements in exec/; A4 every `store.conn` touch in exec/ is read-only reconstruct SELECTs; A5 `worker_reported_ts` never used for authority decisions; A6 every authoritative mutation through `TransitionGate.write_txn` with fencing rejections inside the txn (verified structurally via `ast`); A7 supervisor caches no lease/token/job authority; … + A20 (no private `Store._conn`/`_ro_conn` access from exec/) — **177/177 PASS** on the final tree |
| `audit/stray_process_audit.py` | Independent stray-process/resource audit for R14 — 0 stray AXOS processes after the final campaign |
| `audit/_killpoints.py`, `audit/_migkill.py` | Kill-point injector helpers for audit 06 (not audits themselves) |

`tests/_crasher.py` is the crash-injection helper (SIGKILLs itself
mid-transaction) used by `test_store.py`; `tests/_supdrv.py` is a
supervisor driver helper; `tests/r5_helpers.py` holds the `r5_complete`
completion helper used across R5/R14 suites.

## 7. Root evidence docs

| Doc | Role |
|---|---|
| `R14-EVIDENCE.md` | Immutable historical record of the internal R14 campaign (test inventory, ×3 campaign, implementation changes, FI-01..12 exactness, gate, external-proof attempt) |
| `R14-EXTERNAL-VALIDATION.md` / `.json` | Frozen handoff for the future external hypervisor-proof run: release identity, observer contract, FI-04/FI-12 procedures, evidence package, acceptance rules; status: BLOCKED |
| `AUDIT-REPORT.md` | Phase 1A adversarial audit: claims matrix (VERIFIED / PARTIALLY VERIFIED / BROKEN), defects, required fixes |
| `PHASE1A-REPORT.md` | Phase 1A completion report (store layer + gate API) |
| `PHASE-1C-FINAL-STATUS.md` | Phase 1C final status: R1–R14 gate matrix, durable-state summary, external-validation blocker |
| `R14-PLAN.md`, `R14-CONTRACT-AMENDMENT-PROPOSAL.md` | Planning and contract documents (context, not test evidence) |
