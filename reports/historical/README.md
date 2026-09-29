# Historical reports

Phase reports and R-series evidence from the AXOS development
timeline, preserved verbatim as design history. These documents record
what was true when they were written — they are not re-edited to match
later outcomes. In particular, limitations and blocked proofs are
preserved as blocked.

## Contents

- `AUDIT-REPORT.md` — AXOS Phase 1A adversarial audit report
  (dated 2026-09-23).
- `PHASE1A-REPORT.md` — Phase 1A implementation report: the
  authoritative store layer as built 2026-09-23 (dated).
- `README-phase1a.md` — Phase 1A layout document for the store layer.
- `PHASE-1C-FINAL-STATUS.md` — Phase 1C final status package,
  frozen 2026-09-24; read-only, no implementation work performed.
- `R14-EVIDENCE.md` — R14 final hardening and release proof, evidence
  record (dated 2026-09-23/24; repo identity by source-tree hash).
- `R14-CONTRACT-AMENDMENT-PROPOSAL.md` — the proposal to amend the
  contract for a real-VM lifecycle proof. Preserved as a **proposal**:
  it was never enacted, and the implementation was not changed to
  match it.
- `R14-EXTERNAL-VALIDATION.md` / `R14-EXTERNAL-VALIDATION.json` —
  R14 external validation specification and result. The external
  real-VM proof is **BLOCKED (R14_BLOCKED)** — the environment could
  not execute the real-VM lifecycle proof, so no success is claimed.
  Do not convert this into a pass.
- `axos-source-export-20260927/` — a source export of the AXOS
  implementation dated 2026-09-27 (`AXOS_ARCHITECTURE_AS_IMPLEMENTED.md`,
  `AXOS_DEPENDENCY_GRAPH_AS_IS.md`, `AXOS_FILE_MANIFEST.json`,
  `AXOS_MODULE_INVENTORY.md`, `AXOS_PUBLIC_API.md`): a read-only
  snapshot describing the implementation as it stood then.

## Reading rule

Historical evidence is evidence *of its own date*. For the current
frozen state, see `src/axos/release_manifest.json` (release
`551d559c…`), `tools/verify_release.py`, and `reports/verification/`.
