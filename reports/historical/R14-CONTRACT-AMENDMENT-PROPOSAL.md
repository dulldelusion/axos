# R14 Contract Amendment Proposal

**Verdict: AMENDMENT NOT JUSTIFIED** (see §Decision Required)

Read-only analysis. No production code, contract document, evidence
file, test, manifest, migration, or verdict was modified during this
task.

## Current Status

- R1–R13: COMPLETE
- R14 software/composition campaign: VERIFIED
  (R14 suite 123/123 ×3; full repository 735/735 + 10 subtests ×3;
  audit 09 177/177 incl. A20; canonical FI-01…FI-12 executed twice;
  0 stray processes; migration v12; release `551d559c…`)
- R14: **BLOCKED — REAL-VM PROOF INCOMPLETE**
- Phase 1C: **BLOCKED**
- Reason: the genuine FI-04 power-cycle injection cannot be executed
  from inside the Muse managed execution environment.

## Canonical Contract Requirements

Sources (all under `~/workspace/your_files/`):

- S1: `axos-phase-0-review/05-failure-injection-plan.md` — the
  canonical 12 scenarios and suite rules.
- S2: `axos-phase-1c-contract/PHASE-1C-IMPLEMENTATION-CONTRACT.md`
  §H (Failure-Injection Contract, ~line 436) and R14 section (~line 630).
- S3: `axos-phase-0-review/06-phase-1-implementation-plan.md`,
  Milestone 4 (the proof gate).
- S4: `axos-phase-0-review/04-recovery-kernel-spec.md` §4.5
  (acceptance criteria).
- S5: the user-authorized R14 mission (operative phase contract),
  §24 final report format.

### FI-04

S1, Test 4 — Abrupt VM restart (verbatim, minimum necessary):

- **Inject:** "Hard power-off (not graceful shutdown)."
- **Expected:** "Boot → store integrity check → ledger chain verify →
  all non-retired workers marked DEAD (`vm_restart`) → leases reclaimed
  with token bumps → UNCERTAIN jobs reconciled → **artifact
  reconciliation before checkpoint adoption** → latest verified
  checkpoint adopted → desired-state reconstruction → workers
  reprovisioned → jobs reassigned → scheduler resumes → recovery
  verified … → `vm_recovery_complete` ledgered."

What the contract actually requires:

- (a) a literal hypervisor power-off? **No.** The word "hypervisor"
  appears nowhere in S1–S4. "Hypervisor-level" is the R14 mission's
  wording (S5), not the canonical contract's. The contract requires
  "hard power-off (not graceful shutdown)" — the abruptness is
  specified, the layer that performs it is not.
- (b) merely an abrupt VM restart? The inject is the power-off; the
  expected outcome is the full boot→recovery chain. Both halves are
  required.
- (c) an external observer? **No.** The phrase "external observer"
  appears nowhere in S1–S4 or the architecture suite. It is an
  R14-mission (S5) requirement.
- (d) a specific infrastructure? **No.** No hypervisor, cloud, or
  host API is named anywhere in the canonical contract.
- (e) other exact conditions: S1 suite rules — "injections use real
  signals (`kill -9`, power-off, real clock skew), **not mocks of the
  component under test**"; every test asserts ledger-chain
  verification, no orphaned leases, no running fenced-out workers, no
  unclassified staging orphans, and a recovery-contract row with a
  *measured* progress delta for every recovery action.

### FI-12

S1, Test 12 — Pause drill (verbatim, minimum necessary):

- **Inject:** "Trigger the pause."
- **Expected:** "… → VM restarted mid-pause → pause holds,
  auto-recovery stood down for the scope → human taps the default →
  task resumes from the verified checkpoint with fresh health
  baselining."
- **Pass criteria:** "No auto-resume across the restart; no recovery
  action touched the paused scope; resume reconciliation ran before new
  claims."

The VM-level operation that is mandatory is "VM restarted mid-pause"
— again, the layer performing the restart is unspecified. S1 states no
per-test run count; the campaign rule (S2 §H; S3 M4) is that all 12
scenarios run twice (the R14 campaign's "two consecutive runs").

### Real-VM definition

**"Real VM" is not defined in the canonical contract.** Searched S1,
S2, S3, S4, the gap analysis
(`axos-recovery-gap-analysis/AXOS-RECOVERY-KERNEL-GAP-ANALYSIS.md`),
and the architecture suite
(`autonomous-work-machine/28-failure-injection-tests.md`): the phrase
occurs only as "runs on a real VM" / "green on a real VM, twice
(clean + background load)". The contract does not distinguish real OS
process vs. real process group vs. real VM vs. real hypervisor vs.
external observer. The operative distinctions actually present are:
real signals/processes (vs. mocks of the component under test) and
abrupt vs. graceful termination.

### External observer requirement

Absent from the canonical contract (see above). Present only in the
R14 mission (S5), which is the user-authorized operative contract for
this phase.

### Existing escape/exception mechanism

S5 §24 (final report format): "If a mandatory real-VM requirement
cannot be executed in the current environment, report:
**R14_BLOCKED — REAL-VM PROOF INCOMPLETE** rather than substituting
mocked evidence and declaring success." The contract therefore
already prescribes the exact outcome for unavailable infrastructure.
The current BLOCKED verdict is that mechanism operating as designed,
not a gap in the contract.

## Evidence From Current Environment

Established by direct reconnaissance (2026-09-24), not assumed:

- The execution environment is a guest VM: DMI product
...[truncated 9848 chars]