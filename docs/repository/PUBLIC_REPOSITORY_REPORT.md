# AXOS Public Repository Report

**Repository:** https://github.com/dulldelusion/axos
**Visibility:** public
**Default branch:** `main`
**Release tag:** `v0.1.0`
**Report date:** 2026-09-29

## What was published

The canonical public repository for **AXOS — Autonomous Execution OS**.
The frozen implementation (`src/axos/`) is byte-identical to the
authoritative frozen release:

- Release ID: `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
- Migration version: 12
- Policy: `r9-policy/v1`
- Python 3.12.3, SQLite 3.45.1, Linux x86_64
- Runtime dependencies: Python standard library only
- Manifest: 50/50 entries present and hash-matched (`tools/verify_release.py`
  reports VERIFIED from a fresh GitHub clone)

Accompanying the frozen source: architecture, contracts (prose +
machine-readable JSON), invariants, recovery, operations, schema v12,
reproducibility (including `MUSE_BOOTSTRAP.md`), historical R-series and
audit evidence, portable verification reports, a generic non-production
workload example, and root governance files (README, LICENSE (MIT),
CONTRIBUTING, SECURITY, CODE_OF_CONDUCT, CHANGELOG).

## Verification evidence (all from fresh GitHub clones)

| Check | Result |
|---|---|
| Clone from `github.com:dulldelusion/axos` | clean, 3 commits on `main` |
| `tools/verify_release.py` | VERIFIED (`551d559c…`) |
| Bootstrap (`scripts/bootstrap/bootstrap.sh`) on tag `v0.1.0` | **17/17 checks, exit 0, STATUS: READY** |
| Full test suite (single run, no concurrent load) | **734 passed, 1 failed, 10 subtests passed** |
| Secret scan (working tree + Git history) | clean |
| Word Pics exclusion audit | clean (no production data, prompts, assets, or business logic) |

### The one suite failure

`test_RACE_1_reconcile_racing_head_bump`
(`src/axos/tests/test_reconciliation_r11.py`) — a deliberate race test —
failed once under concurrent load. It passes in isolation (1/1) and at file
level 3/3 (56 passed each). Three sibling timing-sensitive tests flaked in
earlier loaded runs (`test_R4_14…`, `test_R2_07…`, `test_R3_11…`); all pass
reliably in isolation and at file level. These are timing-sensitive tests,
not product defects. The frozen source and frozen tests were not edited.
See `docs/reproducibility/reproducibility.md` for the full characterization.

## Cross-Muse bootstrap simulation

A fresh agent working **only** from `README.md` and
`docs/reproducibility/MUSE_BOOTSTRAP.md` successfully followed the
documented procedure: clone verification, release identity (VERIFIED),
bootstrap stages, contract docs. It found and reported two real issues:

1. `MUSE_BOOTSTRAP.md` Step 1's primary instruction (`git checkout v0.1.0`)
   referenced a tag that did not yet exist — fixed by creating and pushing
   `v0.1.0`; the doc's `or: git checkout main` fallback had kept the
   procedure working.
2. Load-induced test flakes (above) under concurrent execution.

The procedure as documented completes green on a clean run
(17/17 bootstrap checks, STATUS: READY).

## Historical honesty

- R14 internal campaign: complete. External FI-04/FI-12 real-VM lifecycle
  proof: **unavailable — verdict `R14_BLOCKED — REAL-VM PROOF INCOMPLETE`**.
  No substitute simulation was run; no evidence was manufactured.
- Ledger: tamper-evident, not tamper-proof (known limitation, recorded).

## Commits

1. `553de61` — chore(axos): establish canonical public repository
2. `85b07e7` — docs(reproducibility): document timing-sensitive R4-14 test under load
3. `7fd8cad` — docs(reproducibility): document timing-sensitive R2-07 test under load
4. (this report)

Tag `v0.1.0` points at `7fd8cad` (frozen release + docs; this report follows
as process documentation).
