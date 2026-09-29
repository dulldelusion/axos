# R14 — Final Hardening & Release Proof: Implementation Plan

Written 2026-09-24 before any R14 code change (mission §1).

## Baseline (recorded 2026-09-24)

- Full suite: **612 passed + 10 subtests** in 147.85s
- Audits: 01 → 11/12 bypasses (historical F1 baseline, intentional); 02 → 34/34; 03 → 25/25; 04 → 23/23; 05 → 10/10; 06 → 29/29; 07 → 0/20 inconsistent; 08 → 39/39; 09 → all checks pass (176 [PASS] incl. A19a–A19l)

## Canonical scenario source

`~/workspace/your_files/axos-phase-0-review/05-failure-injection-plan.md` — Tests 1–12 used verbatim. Mapping to FI-01..FI-12:

- FI-01 (Test 1, kill worker mid-job): synthetic `crash` behavior, real `kill -9` from the test; expected: R7 DEAD → R1 reclaim/token bump → job UNCERTAIN → R11/R8 requeue → replacement worker → exactly one COMPLETE; stale token commit rejected.
- FI-02 (Test 2, kill after artifact stage): new synthetic behavior `crash_after_stage` (stage bytes, fsync, then `os._exit(3)` before commit — harness-only addition to `exec/synthetic.py`, whose documented purpose is fault injection); expected: UNCERTAIN → orphan adoption path → one artifact row, one COMPLETE, `uncertain_resolved(adopted)` ledgered; corrupt-bytes variant takes the requeue path.
- FI-03 (Test 3, kill supervisor): reuse `tests/_supdrv.py` real-subprocess supervisor driver; `drv.kill()` (SIGKILL); expected: workers keep heartbeating; new Supervisor reconstructs from store; zero duplicate workers; `service_restart` + one reconciler pass ledgered.
- FI-04 (Test 4, abrupt VM restart): environment cannot hard power-off the VM; strongest honest equivalent = real `kill -9` of every AXOS process (workers, supervisor, controllers) mid-flight with a task ~50% complete, an engineered UNCERTAIN job, and a verified checkpoint — then full boot-from-disk recovery (integrity check → ledger chain verify → DEAD(`vm_restart`) → reclaim/token bump → artifact reconciliation → latest verified checkpoint adopted → desired-state reconstruction → resume → `vm_recovery_complete`). Report explicitly as abrupt-halt-with-total-process-loss; do NOT claim a power-off.
- FI-05 (Test 5, corrupt latest checkpoint): corrupt C2 manifest bytes on disk; expected: VERIFY fails → C2 CORRUPT (never trusted) → fallback C1 latest-known-good → ledger replay → resume → C3 created and verified with receipt.
- FI-06 (Test 6, heartbeat continues, progress stops): `heartbeat_loop` behavior → R7 STALLED → R8/R9 recovery contract rows with measured deltas → two zero-deltas escalate; never reported "healthy" while stalled.
- FI-07 (Test 7, external dependency repeatedly fails): Phase 1C has no external-service breaker; harness models a scripted failing external service behind a bounded-retry policy (3 attempts, exp backoff) + R12 recovery-loop breaker → open → jobs pause (no retry storm, ≤ budgeted calls) → half-open probe → on recovery, resume → complete. Assert call count within budget, breaker open/half-open/close transitions ledgered.
- FI-08 (Test 8, recovery itself repeatedly fails): poisoned batch with varied errors → canonical incident signature → zero-delta-twice → escalate → R12 breaker opens → safe stop with incident report. Assert: recovery actions ≤ per-incident budget; breaker opened; no infinite cycling.
- FI-09 (Test 9, wrong validator): `verify_checkpoint` already supports `sampled` receipts and `validations` carries `validator_id`/`validator_version`. Harness deploys a bad validator version (inverted threshold on part of range); gold-set probes fail → bad version quarantined (VALIDATION_FAILURE) → affected artifacts identified by provenance scope → revalidated by the independent second validator → bad verdicts overturned → incident disclosed. Assert: zero FINALIZED artifacts carry only the bad validator's verdict.
- FI-10 (Test 10, partition flap A/B): worker A; SIGSTOP A's process (real signal — A can't heartbeat) → real lease expiry via store time → R7 DEAD verdict → B reclaims with token bump (5 times) → A's late commit attempts rejected as fenced → fence sweep terminates A's group within one heartbeat interval each time → flap damping holds job BLOCKED(`lease_instability`) until settle → exactly one commit ever accepted.
- FI-11 (Test 11, checkpoint sampling under corruption): N artifacts (scaled for test time), interim checkpoint verified with `sampled` receipt stating its detection bound → corrupt ~3% of bytes post-verification → release gate's 100% revalidation catches all → affected units re-executed → no FINALIZE until clean.
- FI-12 (Test 12, pause drill): scripted SEV-2 PAUSED_FOR_HUMAN → diagnostic package → real `kill -9` of all processes mid-pause → restart → pause holds, auto-recovery stood down for the scope → human approves → resume from verified checkpoint with fresh health baselining. Assert: no auto-resume across restart; no recovery touched the paused scope.

Suite rules (canonical): ledger chain verifies after every test; no orphan leases; no running fenced-out workers; no unclassified staging orphans; every recovery action has a contract row with a measured progress delta; real signals/restarts, never mocks of the component under test.

## Track structure (parallel, non-overlapping)

- **Track B — R14-B fault-injection campaign.** All 12 FI tests ×2 consecutive in `tests/test_final_hardening_r14.py`. May add harness-only `crash_after_stage` to `exec/synthetic.py` (documented FI purpose). Owns: suite rules, twice-consecutive proof, environment recording (§15).
- **Track C — R14-A composition + R14-D races + fencing/convergence/breaker/finalization composition + stray-process audit.** Owns: end-to-end desired→FINALIZED surviving one injected failure; all §10 race combinations proving exactly one authoritative winner + losers can't mutate via stale token/CAS/generation; fencing proof (logical + physical, measured latency vs R2 bound); recovery convergence proof; breaker composition; finalization composition with concurrent activity; stray-process/resource audit (zero, checked independently).
- **Track D — R14-C corruption/restore + R14-E artifact adversarial + release manifest + clean boot.** Owns: isolated-copy corruption matrix (ledger corruption → fail closed; state contradictions → blocked/uncertain path; backup/restore with schema-version + ledger-continuity checks; pre-restore workers can't regain authority); artifact/checkpoint adversarial battery; release manifest (`release_id = sha256(canonical_release_manifest)`, no wall-clock in identity); clean-boot proof from the release artifact; audit expansion in `audit/09_authority_audit.py` ONLY where R14 discovers an uncovered boundary (no duplicates).

## Rules for all tracks

- R14 = tests, harnesses, audit checks, release tooling. Production-code changes only when justified by a concrete R14 finding; a finding reopens the relevant R-slice (fix, re-run affected proof + relevant suite).
- Never weaken an assertion. Classify every failure: implementation / test / harness / infrastructure-environment / expected-race.
- No-flake: critical scenarios 3× in dev; FI ×2 consecutive; races 3 consecutive; full suite 3 consecutive.
- If the environment genuinely cannot satisfy a real-VM requirement (power-off for FI-04), implement the strongest honest equivalent and report the gap precisely — never substitute mocked evidence and declare success.

## Deliverable

Exactly the mission §24 structure, one gate: R14_COMPLETE or R14_BLOCKED — REAL-VM PROOF INCOMPLETE. Coordinator (me) assembles from the three tracks' reports.
