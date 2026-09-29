# Invariants

This section is the central registry of AXOS invariants — statements that
must hold unconditionally, regardless of which component runs or what
failures occur. Each invariant is enforced by code (the authoritative
store and the `TransitionGate`), not by convention or by comment, and each
is covered by executable tests.

- [Invariants registry](invariants.md) — the full registry: ID, statement,
  implementation, tests, audit evidence, and current status.

## Status vocabulary

| Status | Meaning |
|---|---|
| `ENFORCED_IN_CODE + TESTED` | Mechanism in `store/gate.py` (or a DB constraint) actively
  rejects violations, and executable tests prove it. |
| `TESTED` | Executable tests cover the invariant, but no single
  enforcement point rejects every violation. |
| `AUDITED` | Independently re-attacked or re-verified by a formal audit
  campaign (see `reports/historical/AUDIT-REPORT.md` and
  `reports/historical/R14-EVIDENCE.md`). |
| `STATED_IN_TEST` | No prose statement found outside the tests; the wording is
  taken from the test that enforces it. |

No invariant is marked proven without evidence. Where the evidence has a
known gap, the gap is recorded rather than hidden.

## Reading order

1. [invariants.md](invariants.md) — start here.
2. `store/gate.py` — the enforcement mechanisms cited in the registry.
3. [../recovery/recovery-model.md](../recovery/recovery-model.md) — I-18
   is the recovery contract's core rule.
4. [../recovery/escalation.md](../recovery/escalation.md) — I-17 governs
   the terminal rung of the recovery ladder.
5. [../../reports/historical/AUDIT-REPORT.md](../../reports/historical/AUDIT-REPORT.md)
   — the Phase 1A adversarial audit that re-proved I-16, I-17, I-18 and
   found the authority-boundary defects later fixed in remediation.

## Related

- [Architecture overview](../architecture/overview.md)
- [Contracts](../contracts/README.md)
- [Recovery model](../recovery/recovery-model.md)
