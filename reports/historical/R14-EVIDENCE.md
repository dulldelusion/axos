# R14 Final Hardening & Release Proof — Evidence Record

Date: 2026-09-23/24. Repo: `~/workspace/axos` (not a git repo; identity by source-tree hash).
Release manifest: `~/workspace/axos/release_manifest.json`
Release ID: `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
(final regeneration after the R9-M1 test-defect fix; supersedes `0340c513…`).
Migration version: 12. Schema/manifest generated AFTER the final source change.

## Test inventory (final tree)

- `tests/test_final_hardening_r14.py`: 123 tests (merged Track B 25 + Track C 49 + Track D 49).
- Full repository: 735 tests (612 R1–R13 + 123 R14) + 10 subtests (baseline; final counts below).
- Section files `test_r14_b_injection.py` / `test_r14_c_composition.py` /
  `test_r14_d_corruption.py` removed (recoverable trash, 30 days) after merge;
  full-suite collection shows no duplication.

## Consecutive-run campaign (final stable tree)

- Merged R14 suite: run 1: 123/123 (218.01s, pre-cleanup tree — evidence only);
  run 2: 123/123 (217.43s, final tree); run 3: 123/123 (217.59s, final tree).
  Three more final-tree passes come from the full-repo ×3 below (each embeds
  the merged file).
- Full repository ×3 (final tree): run 1: 735/735 + 10 subtests (368.34s);
  run 2: 735/735 + 10 subtests (366.43s); run 3: 735/735 + 10 subtests
  (366.51s). Three consecutive full-green runs on the final stable tree.
  Prior runs on the pre-fix tree: 734/735 + 10 subtests ×3 with one
  deterministic failure — `test_recovery_r9.py::TestR9M1::
  test_R9_M1_v6_to_v7_migration`, classified TEST DEFECT (stale hardcoded
  migration list `[7, 8, 9, 10, 11]`; production v12 correct). Fixed by
  updating the expectation to `[7, 8, 9, 10, 11, 12]`; R9 file 55/55 after fix.
- Audits 01–09 + A20 on final tree: ALL GREEN.
  01: 11/12 historical bypasses confirmed possible (baseline);
  02: 34/34 rejected, 0 defects; 03: 25/25 correct; 04: 23/23 correct;
  05: 10/10 as expected; 06: 29/29 correct; 07: 0/20 inconsistent races;
  08: 39/39 blocked/correct; 09: 177/177 PASS including A20
  (no private Store._conn/_ro_conn access from exec/).
- Stray-process audit after final campaign: ZERO stray AXOS processes.

## Implementation changes in R14 (narrow fixes only)

1. `latest_known_good()` fail-closed + new `invalidate_checkpoint(...)` (Track B finding:
   corrupt latest-known-good could be returned).
2. Watchdog consumes durable `worker.proc_reaped` evidence via new read-only
   `latest_spawn_generation(worker_id)` in `store/gate.py` (Track C finding:
   reaped worker could appear HEALTHY). Audit allowlist updated.
3. Migration v12 `artifact_stagings` (one row per `(artifact_id, job_id)`):
   fixes cross-job identical-byte artifact liveness gap (Track D finding).
   `stage_artifact` records per-job provenance; `begin_commit`/`commit_artifact`
   use per-job provenance; `verify_artifact` accepts optional `job_id`;
   resolver token-lineage uses job-specific staging token. R5 suite 37/37 after fix.

No duplicate scheduler/reconciler/recovery-controller/checkpoint-authority/
finalizer/fencing/policy engine. No direct SQL mutation outside TransitionGate.
No weakened assertions. No test-only production bypass.

## FI-01…FI-12 exactness assessment (canonical: 05-failure-injection-plan.md)

- FI-01 worker crash: EXACT. Real SIGKILL, real watchdog DEAD, real reclaim; stale
  token rejected on renew + stage.
- FI-02 kill after stage: EXACT. Adopt branch (one artifact row, one COMPLETE,
  `uncertain_resolved(adopted)`) and corrupt-bytes requeue branch.
- FI-03 supervisor kill: injection + behavioral criteria EXACT (real SIGKILL of
  control plane, workers survive, boot recovery, one reconciler pass, zero
  duplicates, task completes). LIMIT: `service_restart` ledger event is
  journaled by the harness (standing in for the bootstrap monitor), not emitted
  by production.
- FI-04 full VM restart: HONEST-EQUIVALENT — real kill -9 of ALL AXOS processes
  (worker groups + driver supervisor) then boot-from-disk recovery by a fresh
  Supervisor; 8/8 jobs COMPLETE, no rescheduling, checkpoint intact, UNCERTAIN
  adopted. NOT EXECUTED: genuine hypervisor/OS hard power-off (page-cache loss,
  wall-clock jumps). Cannot be orchestrated from inside this VM (no hypervisor
  access; crashing the VM would destroy the observing harness). Store uses
  SQLite WAL + synchronous=NORMAL: power loss cannot corrupt the DB but may roll
  back the most recent committed transactions; staged bytes are explicitly
  fsync'd. `vm_recovery_complete` is journaled by the harness AFTER all genuine
  recovery stages succeed, with payload `"honest_equivalent"` stating exactly
  what was done. — THIS IS THE GATE-BLOCKING GAP.
- FI-05 checkpoint corruption: injection EXACT (real on-disk byte corruption of
  C2 post-verification). Production: detection of corruption has NO production
  re-validation API (test helper `_revalidate_checkpoint_files`); invalidation
  via production `invalidate_checkpoint`; C2 never trusted again via hardened
  `latest_known_good`. LIMIT: no production API enumerates a task's checkpoints
  or performs the C1 fallback — the test assembles C1 + ledger replay via
  read-only SQL and stages/verifies C3 through production APIs. Pass criteria
  met (no resumption from C2; C1 receipt intact; incident journaled with
  corruption evidence).
- FI-06 stall: EXACT. Scripted `stall_progress`; L2 STALLED verdict before any
  recovery; contract rows carry measured deltas; never reported healthy.
- FI-07 dependency failure: HONEST-ANALOG. Phase 1C has no external-service
  breaker; the real TASK breaker (R12) was driven with three real worker
  crashes: OPEN → claim path denies retries (no storm) → half-open probe →
  CLOSE on success. The breaker property is proven; the external-dependency
  failure source is not present in Phase 1C to test against.
- FI-08 repeated recovery failure: EXACT. Two crashing replacements, zero
  progress delta twice → escalated per R9; no third blind retry; recovery-loop
  breaker opens; spend within budget.
- FI-09 wrong validator: EXACT (corrected this round from poison-message
  substitute). Inverted-threshold validator incl. score 50; quarantined;
  bad-window artifacts revalidated by independent validator; zero FINALIZED
  artifacts carrying only the bad verdict.
- FI-10 partition flap / fencing: CORE PROPERTY PROVEN with real mechanisms —
  three wedged workers, real forced-reclaim fencing transactions, fence→death
  latency ≤ one heartbeat on a real monotonic clock (R14-55 0.1274s, R14-59
  0.1075s vs H=0.4s); stale-token rejection proven on renew/stage/begin_commit/
  verify surfaces. LIMIT: the canonical 5-flap A/B token-churn choreography
  (lease-expiry-driven reclaim, flap damping to BLOCKED, alert firing) was not
  replicated step-for-step. Rationale: on the single-VM SQLite architecture
  (Phase 0: no premature distribution) there is no network connectivity to
  flap — "A's store connectivity" cannot be literally partitioned; the
  fencing safety properties the scenario exists to prove (late commits
  rejected, fenced process groups dead within one heartbeat) are proven by
  mechanism with real processes, and the A/B race shape is covered by the
  R14-20…R14-32 race group (exactly one authoritative winner; losers cannot
  mutate via stale token/CAS/generation).
- FI-11 checkpoint sampling: EXACT (corrected this round from split-brain
  substitute). 60 artifacts, 5% seeded sample receipt stating its detection
  bound, ~3% post-verification corruption, release 100% revalidation catches
  all, no FINALIZE until repaired.
- FI-12 pause drill: HONEST-EQUIVALENT. Operator pause mid-flight; in-flight
  worker drains; claim path refuses while paused; pause survives SUPERVISOR
  restart (I-17 stickiness, no auto-resume); APPROVED approval resumes;
  remaining jobs complete. LIMIT: canonical "VM restarted mid-pause" executed
  as supervisor restart; resume-from-verified-checkpoint composition not
  separately exercised (covered by FI-04 boot × FI-12 pause composition).

Every FI ran twice consecutively on the real VM (htch-runtime): PASS/PASS.
Suite rules asserted per FI: ledger chain verifies; no orphaned leases; no
running fenced-out workers; no unclassified staging orphans; every recovery
action has a contract row with a measured progress delta.

## Gate

Per the R14 contract — "If real-VM proof cannot be executed, the required gate
is R14_BLOCKED — REAL-VM PROOF INCOMPLETE" and "do not report R14 complete if
any exact canonical scenario is missing" — FI-04's canonical injection (hard
VM power-off) was not executed and cannot be orchestrated from inside this VM.

Gate: **R14_BLOCKED — REAL-VM PROOF INCOMPLETE**

This is not a code-failure verdict: 123/123 R14 tests pass repeatedly, every
executable canonical injection passed twice, and the full-suite ×3 campaign
(final tree) results are recorded above. It is the contractually prescribed
gate when a mandatory proof cannot be executed.
To clear it: run FI-04 (and FI-12's VM-restart variant) as genuine
hypervisor-level power cycles with observed boot recovery, twice, from outside
this VM.

---

## External Hypervisor Proof (attempt 2026-09-24)

Release: 551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192
Release identity binding: verified — source tree byte-identical to
release_manifest.json (50/50 files present, 0 changed since generation).
Migration: v12. No source or migration changes since the internal R14
campaign; evidence remains bound to the same release identity.

VM: guest VM (this runtime). Hypervisor: Cloud Hypervisor (KVM-based),
from /sys/class/dmi/id/product_name ("cloud-hypervisor") and sys_vendor
("Cloud Hypervisor").

FI-04 Run 1: BLOCKED — EXTERNAL VM POWER-CYCLE UNAVAILABLE
FI-04 Run 2: BLOCKED — EXTERNAL VM POWER-CYCLE UNAVAILABLE
FI-12 VM Restart: BLOCKED — EXTERNAL VM POWER-CYCLE UNAVAILABLE

Actual hypervisor power-off evidence: none executed. Reconnaissance found
no executable path to a genuine hypervisor-level abrupt power-off:

- No libvirt socket (/var/run/libvirt* absent).
- No hypervisor control tooling (no virsh, qemu-img, qemu-system-x86_64,
  VBoxManage anywhere on PATH).
- No instance metadata service (http://169.254.169.254/ returns empty).
- The HATCH_API_SOCKET env var points to /run/hatch/daemon/http-api.sock,
  which does not exist — no platform control API is reachable from the
  guest. Host-side control scripts (runtime-cell/control-daemon.sh,
  control-execd.sh) live on the host and are not accessible.
- No external observer host exists in this environment: the only paired
  device is the user's smartphone; no cloud credentials; no second VM.
  The target VM is necessarily this VM (repo + release live here), and
  any power-off would kill the only available runtime with no surviving
  observer — an interrupted run the contract forbids counting as PASS.

Per mission §10 ("If the hypervisor environment itself cannot perform the
required power-cycle, the correct result remains: R14_BLOCKED —
EXTERNAL VM POWER-CYCLE UNAVAILABLE") and §13 ("Do not use FAILED merely
because the external environment cannot execute the test"), no
substitute simulation was executed and no mocked evidence was created.

Boot/reconstruction evidence: n/a (no power-cycle executed).
Fencing evidence: n/a. Recovery evidence: n/a.
Artifact/checkpoint evidence: n/a.

Manual intervention: NONE (no run attempted; no state touched).

External evidence hash: n/a — no external run exists to hash. The
reconnaissance above is the complete external-proof record.

Final determination: **R14_BLOCKED — REAL-VM PROOF INCOMPLETE**
(real-VM proof remains incomplete; external power-cycle unavailable).

Historical internal R14 evidence (above) is unchanged and remains valid.
