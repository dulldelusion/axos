# Changelog

All notable changes to the AXOS public repository are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [v0.1.0] — 2026-09-29

Initial canonical public release of AXOS (Autonomous Execution OS).

### Source

- Frozen AXOS implementation, release
  `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`,
  migration v12, policy `r9-policy/v1`, under `src/axos/`.
- Python 3.12.3, SQLite 3.45.1, Linux x86_64, stdlib-only, zero
  third-party dependencies, zero environment-variable configuration,
  fully offline-capable.
- Authority core (`src/axos/store/`): SQLite/WAL store, `TransitionGate`
  single enforcement boundary, transition graphs, lease/fencing
  authority, tamper-evident hash-chained ledger.
- Execution substrate and controllers (`src/axos/exec/`): supervisor,
  worker protocol and reference subprocess, synthetic fault-injection
  executor, boot recovery, scheduler (R10), reconciler (R11),
  watchdog (R7), recovery controller (R8), policy ladder/budgets (R9),
  circuit breakers (R12), finalizer (R13).

### Verification evidence (preserved, not rewritten)

- R1–R13 COMPLETE: 735/735 test methods, three consecutive full-green
  runs.
- R14 internal campaign VERIFIED: 123/123 ×3; 12/12 fault injections
  (FI-01..FI-12) ×2, PASS/PASS; adversarial audits 01–09 and the
  stray-process audit green (audit 09: 177/177); 0 stray processes.
- R14 external validation BLOCKED — REAL-VM PROOF INCOMPLETE: the
  genuine hypervisor power-off proof was never executed. Recorded
  honestly; no substitute evidence manufactured.
- Historical R-series reports, audits, and the frozen external-validation
  spec preserved under `reports/historical/`.

### Repository

- Professional layout: `src/`, `tests` (in-tree under `src/axos/`),
  `docs/` (architecture, contracts, invariants, recovery, operations,
  reproducibility, decisions, repository), `contracts/`, `schemas/`,
  `migrations/`, `scripts/`, `config/`, `examples/`, `tools/`,
  `reports/`.
- One-shot bootstrap (`scripts/bootstrap/bootstrap.sh`): validate
  environment → initialize runtime → validate state → self-test (17
  checks); initializes a STOPPED runtime with zero workers and never
  starts production.
- Cross-Muse bootstrap guide at
  `docs/reproducibility/MUSE_BOOTSTRAP.md`.

### Known limitations (carried over honestly)

- R14 real-VM lifecycle proof remains blocked/incomplete (see above).
- Ledger is tamper-evident, not tamper-proof.
- No replication, failover, backup automation, or operator CLI in the
  tree; controller `start()` runs a background thread, not a daemon.
- The workload contract is implicit (enforced by the gate), not a
  formal plugin interface.
- Single test-environment quirk: the R6-19b driver hardcodes
  `~/workspace` as a subprocess import path.

[v0.1.0]: https://github.com/dulldelusion/axos/releases/tag/v0.1.0
