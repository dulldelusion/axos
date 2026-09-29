# Contributing to AXOS

AXOS (Autonomous Execution OS) is a **frozen release**:
`551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
(migration v12, policy `r9-policy/v1`). There is no R15. The core under
`src/axos/` is the verified artifact — contributions preserve it, they
do not redesign it.

## What contributions are welcome

- **Documentation**: clarifications, corrections, and expansions of
  `docs/`, provided they describe the actual implementation rather
  than a redesigned one.
- **Tooling**: operator tooling, inspection scripts, and developer
  ergonomics around the frozen core (see `scripts/` and `tools/`).
- **Verification**: additional independent verification of the frozen
  release — reproduction of the test evidence, adversarial probing,
  audit scripts. New failing evidence must be reported honestly, with
  exact counts.
- **Workload adapters**: out-of-tree examples showing how an external
  workload implements the worker protocol against `TransitionGate`
  (see `examples/`). Workload-specific logic must stay out of the
  AXOS core — AXOS is workload-independent.

## What is not accepted

- Redesigns of AXOS architecture "to make the repository cleaner" or
  for any other reason **without evidence**. Architectural changes
  require demonstrated defects in the current design, reproduced
  against the frozen release, with the same rigor as the existing
  R-series evidence.
- Casual modification of frozen invariants. Every invariant in
  `docs/invariants/` is backed by implementation, tests, and audit
  evidence; changing one means re-proving all three.
- Changes that weaken the authority boundary: all authoritative
  mutations go through `TransitionGate`; `src/axos/store/` imports
  nothing from `src/axos/exec/`; non-gate components never hold a
  writable `Store`. Any contribution that bypasses, duplicates, or
  relaxes this boundary will be rejected.
- Changes that weaken fencing semantics, safe stopping
  (`PAUSED_FOR_HUMAN` stickiness, no automatic resume), or the
  artifact-gated completion path (`stage_artifact → begin_commit →
  verify_artifact → commit_artifact`; there is no artifact-free path
  to `COMPLETE`).
- Word Pics or any other production workload corpus, assets, prompts,
  or application-specific business logic. Minimal deterministic test
  fixtures are acceptable only if clearly marked as fixtures.
- Secrets, credentials, or personal data of any kind (see SECURITY.md).

## Development rules

1. **Do not redesign AXOS without evidence.** A change to the core
   must cite a reproduced defect or a blocked proof, not a preference.
2. **Do not modify frozen invariants casually.** See rule above.
3. **Preserve durable-state authority.** The SQLite store (WAL mode)
   is the single source of truth. No caches of authority, no
   second sources of truth.
4. **Preserve fencing semantics.** A worker has authority only while
   it is the current owner with the current fencing token and an
   unexpired lease. `LeaseError` means stop immediately.
5. **Preserve safe stopping.** No automatic worker start, no automatic
   production resume, no clearing human gates.
6. **Run the regression suites before any architectural change.**
   The full suite (`python3 -m unittest discover -s tests` from
   `src/axos/`, or `PYTHONPATH=<repo>/src python3 -m unittest
   discover -s src/axos/tests` from the repo root) plus the adversarial
   audits must pass. Do not remove difficult tests because they are
   inconvenient.
7. **Separate workload-specific logic from the AXOS core.** Payloads
   are opaque to AXOS; workload semantics live outside the tree.
8. **Do not infer correctness from AI reasoning alone.** A claim is
   proven by durable state and executed tests, never by argument.
9. **Preserve historical evidence honestly.** R-series reports and
   audits under `reports/historical/` are immutable records. The R14
   external validation is BLOCKED (real-VM proof incomplete) — do not
   relabel it.

## Workflow

1. Fork and branch from `main`.
2. For docs/tooling changes: keep the existing structure and naming
   (`snake_case` for Python modules, `kebab-case` for documentation
   filenames). Avoid `temp`/`new`/`final`/`misc`/`old`/`backup`
   naming.
3. Verify: run the relevant test suites and, for anything touching
   `src/axos/`, the full suite plus audit 09
   (`src/axos/audit/09_authority_audit.py`, which must exit 0).
4. Keep the tree clean: after running the R14 manifest tests, restore
   `src/axos/release_manifest.json` (`git checkout --
   src/axos/release_manifest.json`); delete `__pycache__` directories
   created by test runs.
5. Open a pull request describing what was changed, what evidence was
   run (exact counts), and what remains blocked or unverified.

## Reporting problems

- Security issues: see SECURITY.md — **do not open a public issue**.
- Test failures on a fresh checkout: include the exact command, the
  Python/SQLite/OS versions, and the full output. Check
  `docs/operations/troubleshooting.md` first — several failures are
  known environmental quirks (e.g. the R6-19b driver path).
- Honest negative results (a blocked proof, a failing audit) are
  valuable contributions. Report them as findings with evidence, not
  as fixed.
