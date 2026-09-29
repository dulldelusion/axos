# Verification

Reproducibility and portability verification reports for the frozen
AXOS release. A report here is evidence that a claim was actually
exercised (a fresh environment, a clean clone, a real run) — not that
the code merely exists.

## Contents

- `AXOS_PORTABLE_BUNDLE_20260927T202655Z_PORTABILITY_REPORT.md` —
  portability report generated 2026-09-27T20:20Z (UTC) for the portable
  runtime bundle `AXOS_PORTABLE_BUNDLE_20260927T202655Z.tar.gz`: the
  bundle's provenance, contents, and the portability checks run against
  it.

Cross-check with `tools/verify_release.py`, which recomputes the
release manifest over `src/axos/` and asserts the release_id
(`551d559c…`) matches the committed manifest.
