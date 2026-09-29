# R14 External Validation Specification

**Status: BLOCKED — REAL-VM PROOF INCOMPLETE (environmental, not implementation)**

This document is the authoritative handoff for the future external
execution of the R14 mandatory hypervisor-level power-cycle proof. It
freezes the release identity, the procedures, the observer contract, and
the acceptance rules. It introduces no new AXOS mechanism and changes no
production code.

## 1. Release identity (frozen)

| Field | Value |
|---|---|
| Release ID | `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192` |
| Release definition | `release_id = sha256(canonical_release_manifest)` |
| Migration version | v12 |
| Source tree hash | the release ID itself (content-addressed; 50/50 files, 0 changed since generation) |
| Manifest file | `~/workspace/axos/release_manifest.json` |
| Manifest file sha256 | `2fa26d3ef74ee603d69b9d64ca54b7eb7c84f092ee0c283b3138b2672ad911eb` |
| Git commit | n/a — the repository is not a Git repository; identity is the release ID above |
| Internal evidence | `~/workspace/axos/R14-EVIDENCE.md` (immutable historical record) |

Verified 2026-09-24: working tree byte-identical to the release
manifest (50/50 files present, 0 changed). No production changes, no new
migration, no changed release identity since the internal R14 campaign.

## 2. Internal evidence preserved (immutable)

- R14 suite: 123/123 ×3 (merged Track B/C/D)
- Full repository: 735/735 + 10 subtests ×3
- Authority audits 01–09 + A20: all green (audit 09: 177/177)
- Canonical FI-01…FI-12: all executed twice on a real VM; FI-01, 02, 06,
  08, 09, 11 EXACT; FI-03, 05, 07, 10, 12 exact/honest-equivalent with
  documented limits (see R14-EVIDENCE.md)
- Stray processes: 0

Preserve the distinction between **PROVEN** (internal, real-VM,
software-equivalent) and **NOT EXECUTED / BLOCKED** (genuine
hypervisor-level power-cycle: FI-04 genuine injection; FI-12 VM-restart
variant). Do not collapse honest-equivalent results into genuine VM
results.

## 3. Required external environment

The minimum environment that can close this blocker must provide:

1. A VM containing the frozen AXOS release (release ID above, migration
   v12), as the target.
2. Hypervisor-level control of that VM: genuine hard power-off
   capability (the guest must actually lose power; no graceful
   shutdown, no in-guest reboot command).
3. Ability to power the VM back on from the hypervisor layer.
4. An external observation host that is NOT the target VM and that
   remains alive through the entire outage.
5. Persistent storage outside the target VM for the evidence bundle.
6. A network or equivalent observation path to the target VM after
   reboot (to confirm boot, observe AXOS READY, and collect evidence).

Do not assume a specific hypervisor (KVM/QEMU, VMware, Hyper-V,
Cloud Hypervisor, etc.) unless the actual external environment
provides one. The procedures below are hypervisor-agnostic.

## 4. External observer contract

The observer must survive the target VM disappearing. It must never
depend on the target VM remaining alive, and it must not perform any
AXOS state repair — it is an evidence collector, not a recovery
authority.

Observer responsibilities, in order:

```
prepare
  ↓  verify release identity (release ID, migration v12, manifest hash)
verify release
  ↓  establish the FI precondition (per §5 / §6)
verify precondition
  ↓  record the complete durable baseline
record durable baseline
  ↓  trigger hypervisor-level hard power-off (guest is NOT shut down gracefully)
trigger hypervisor power-off
  ↓  record target VM disappearance (power state from the hypervisor layer)
record VM disappearance
  ↓  power the VM back on from the hypervisor layer
power VM on
  ↓  wait for the guest OS to become reachable
wait for boot
  ↓  observe AXOS boot/reconstruction; do not intervene
observe AXOS recovery
  ↓  collect all evidence categories (§7)
collect evidence
  ↓  verify the final state against the canonical expectation
verify final state
  ↓  hash the evidence bundle
hash evidence
```

Any manual repair of the AXOS database, ledger, tokens, jobs, artifacts,
checkpoints, policy, or incidents by the observer or operator voids the
run: record it as `BLOCKED — MANUAL INTERVENTION REQUIRED` with the
exact reason. Never convert such a run to PASS.

## 5. Frozen FI-04 procedure (canonical)

Source: Phase 1C contract, verbatim from the failure-injection plan
(`05-failure-injection-plan.md`, Test 4 — Abrupt VM restart).

Precondition: a task ~47% complete, multiple workers, one job
mid-critical-path, one job engineered UNCERTAIN, one verified checkpoint
exists.

Each run must follow:

```
Run N
  ↓
required precondition established and recorded
  ↓
genuine hypervisor hard power-off (guest NOT shut down gracefully)
  ↓
VM unavailable (confirmed at the hypervisor layer)
  ↓
VM power restored (hypervisor power-on)
  ↓
guest OS boot observed
  ↓
AXOS boot: store integrity check → ledger chain verify →
all non-retired workers marked DEAD (vm_restart) →
leases reclaimed with token bumps → UNCERTAIN jobs reconciled →
artifact reconciliation BEFORE checkpoint adoption →
latest verified checkpoint adopted → desired-state reconstruction →
workers reprovisioned → jobs reassigned → scheduler resumes
  ↓
recovery verified: each repaired diff converges;
vm_recovery_complete ledgered
  ↓
expected convergence verified:
task resumes and completes; no job runs twice to commit;
the recovery report states exactly what was reclaimed, adopted,
requeued; human-gated states (if any) stayed paused
```

**Required runs: 2 consecutive successful runs.** An interrupted or
ambiguous run does not count as PASS.

The following do NOT qualify as the genuine injection:

- SIGKILL or any process termination
- OS reboot / graceful shutdown / supervisor restart
- synthetic power-loss injection
- mocked hypervisor API
- snapshot restore without a genuine abrupt power loss
- killing the current runtime
- any test where the observer dies with the target VM

## 6. Frozen FI-12 procedure (canonical VM-restart variant)

Source: Phase 1C contract (`05-failure-injection-plan.md`, Test 12 —
Pause drill, production gate).

The canonical scenario: a task with a scripted SEV-2 pause (simulated
ambiguous input). Diagnostic package delivered (evidence, options,
one-tap conservative default, SLA). Where the contract requires an
abrupt VM-level restart/power-cycle mid-pause, perform it at the
hypervisor layer — NOT as a process/supervisor restart.

Pass criteria (canonical, unchanged):

- No auto-resume across the restart.
- No recovery action touched the paused scope.
- The pause held and auto-recovery stood down for the scope.
- Human taps the default → task resumes from the verified checkpoint
  with fresh health baselining.
- Resume reconciliation ran before new claims.

Distinguish **FI-12 software/process restart** (supervisor restart —
already proven internally, does not close the blocker) from **FI-12
genuine VM-level restart** (hypervisor power-cycle mid-pause — the
operation that closes the blocker).

**Required runs: 2 consecutive successful runs** (per the R14 campaign
rule that each canonical scenario repeats twice consecutively).

## 7. Evidence package

The external evidence bundle lives OUTSIDE the target VM:

```
R14-external/
├── manifest.json            # release identity + environment record
├── observer.log             # full observer timeline (timestamps, VM identity)
├── hypervisor.log           # hypervisor event/log evidence of actual power-off
├── vm-power-events.log      # power-off → disappearance → power-on → boot events
├── precondition.json        # established precondition per run
├── pre_failure_state.json   # complete durable baseline before injection
├── post_boot_state.json     # durable state after boot/reconstruction
├── recovery_evidence.json   # recovery observations + contract rows
├── fencing_evidence.json    # fencing/token evidence
├── artifact_evidence.json   # artifact integrity evidence
├── checkpoint_evidence.json # checkpoint integrity evidence
├── FI-04-run-1/
├── FI-04-run-2/
├── FI-12/
└── SHA256SUMS               # hashes of every file above
```

Exact filenames may differ if the external infrastructure requires it,
but every evidence category above must remain represented.

## 8. Evidence acceptance rules

An external run is PASS only when the evidence proves ALL of:

1. **Genuine power loss** — the target VM actually lost power at the
   hypervisor layer (hypervisor logs/events, not guest testimony).
2. **Observer survival** — the external observer remained alive during
   the entire outage.
3. **Release identity** — the VM ran the exact frozen release
   (release ID, migration v12, manifest hash).
4. **Durable reconstruction** — AXOS reconstructed from durable state
   after power restoration; no in-memory pre-crash state required.
5. **Authority safety** — no stale pre-power-loss authority survived;
   PID reuse rejected; process-group identity authoritative.
6. **Fencing** — required stale execution was physically fenced; stale
   tokens rejected on all surfaces.
7. **Recovery** — recovery followed the established R1–R9 mechanisms;
   recovery success required positive durable progress (a heartbeat
   without progress does not count; a restart without progress does not
   count).
8. **Integrity** — artifact/checkpoint state remained valid; no
   incomplete artifact published as COMPLETE; latest-known-good
   semantics correct.
9. **Convergence** — the final state matches the canonical FI
   expectation.
10. **No manual healing** — no database/state repair occurred.

Anything less is `BLOCKED / INSUFFICIENT EVIDENCE`, not PASS.

## 9. Frozen work (do not reopen)

The following are frozen and verified; the blocker is environmental and
justifies no software change:

- R1–R13 and all verified R14 implementation work
- R6 boot/reconstruction, R2 fencing, R8 recovery, R9 policy,
  R10 scheduling, R11 reconciliation, R12 resilience, R13 finalization
- No new checkpoint system, persistence layer, or recovery controller

## 10. External runner checklist

Before execution:

- [ ] external observer available and outside the target VM
- [ ] hypervisor control available (genuine hard power-off + power-on)
- [ ] target VM identified
- [ ] frozen release verified (release ID match)
- [ ] migration v12 verified
- [ ] release manifest verified (manifest file hash match)
- [ ] observer storage outside target VM
- [ ] AXOS precondition established and recorded

During execution:

- [ ] genuine hypervisor power-off (not guest shutdown)
- [ ] VM disappearance recorded (hypervisor layer)
- [ ] observer survived the outage
- [ ] VM powered on from hypervisor layer
- [ ] guest boot observed
- [ ] AXOS READY observed
- [ ] recovery observed without intervention

After execution:

- [ ] durable state verified
- [ ] fencing verified
- [ ] recovery verified
- [ ] artifact integrity verified
- [ ] checkpoint integrity verified
- [ ] final state matches canonical expectation
- [ ] no manual healing
- [ ] evidence copied outside the target VM
- [ ] evidence hashed (SHA256SUMS)

Repeat according to the required run count (FI-04 ×2, FI-12 ×2).
