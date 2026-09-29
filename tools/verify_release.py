#!/usr/bin/env python3
"""Verify the frozen AXOS release manifest against the source tree.

Recomputes the release manifest over src/axos/ using the project's own
builder (build_release_manifest from
src/axos/tests/test_final_hardening_r14) and asserts the computed
release_id equals the release_id recorded in the committed manifest.

Usage (from the repository root):
    python3 tools/verify_release.py

Exit status 0 on VERIFIED, 1 otherwise.

Provenance:
- Builder: src/axos/tests/test_final_hardening_r14.py::build_release_manifest
- Committed manifest: src/axos/release_manifest.json
- Importing the test module does not execute tests (it returns
  immediately after import; verified by the module exposing
  build_release_manifest with no test output).
"""

import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_ROOT, "src")
sys.path.insert(0, SRC_DIR)

from axos.tests.test_final_hardening_r14 import build_release_manifest  # noqa: E402


def main() -> int:
    manifest_path = os.path.join(SRC_DIR, "axos", "release_manifest.json")
    with open(manifest_path) as f:
        committed = json.load(f)
    committed_id = committed["release_id"]

    computed_id, identity = build_release_manifest()

    if computed_id == committed_id:
        print(f"VERIFIED: release_id {computed_id} matches "
              f"src/axos/release_manifest.json")
        return 0

    print(f"MISMATCH: committed release_id = {committed_id}")
    print(f"          computed release_id  = {computed_id}")
    committed_identity = committed.get("identity", {})
    for key in sorted(set(committed_identity) | set(identity)):
        c, n = committed_identity.get(key), identity.get(key)
        if c != n:
            c_repr = repr(c)[:120]
            n_repr = repr(n)[:120]
            print(f"  identity[{key!r}] differs:")
            print(f"    committed: {c_repr}")
            print(f"    computed:  {n_repr}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
