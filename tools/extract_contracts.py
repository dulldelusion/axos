#!/usr/bin/env python3
"""Extract machine-readable contract JSON from the frozen AXOS source.

Reads the canonical data structures directly out of the implementation
(no hand transcription):
  - src/axos/store/transitions.py  -> state transition graphs, gated sets
  - src/axos/exec/policy.py        -> CANONICAL_LADDER (R9 recovery ladder)
  - src/axos/exec/recovery.py      -> _CANONICAL_RUNGS + R8 recovery constants

Emits (all paths relative to the repository root):
  - contracts/transitions.json
  - contracts/recovery-ladder.json
  - contracts/recovery-rungs.json

Regenerate with:
    python3 tools/extract_contracts.py
(run from the repository root)

Provenance: derived from frozen AXOS release 551d559c (see
src/axos/release_manifest.json). The frozen source is imported, never
edited; every emitted value is the live object from the module.
"""

import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(REPO_ROOT, "src")
RELEASE_ID = "551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192"

sys.path.insert(0, SRC_DIR)

from axos.store import transitions as T          # noqa: E402
from axos.exec import policy as P               # noqa: E402
from axos.exec import recovery as R             # noqa: E402


def _meta(description: str) -> dict:
    return {
        "provenance": {
            "source": "frozen AXOS source in src/axos/",
            "release_id": RELEASE_ID,
            "generated_by": "tools/extract_contracts.py",
            "regenerate": "python3 tools/extract_contracts.py",
        },
        "description": description,
    }


def _transitions_module() -> dict:
    doc = {}
    doc.update(_meta(
        "State transition graphs enforced by the gate. The gate "
        "transactionally rejects any transition not listed here "
        "(src/axos/store/transitions.py module docstring)."))
    doc["graphs"] = {
        name: {frm: list(to) for frm, to in graph.items()}
        for name, graph in (
            ("task", T.TASK_TRANSITIONS),
            ("job", T.JOB_TRANSITIONS),
            ("worker", T.WORKER_TRANSITIONS),
            ("artifact", T.ARTIFACT_TRANSITIONS),
            ("approval", T.APPROVAL_TRANSITIONS),
            ("checkpoint", T.CHECKPOINT_TRANSITIONS),
        )
    }
    doc["sets"] = {
        # I-17: sticky human-gated states.
        "HUMAN_GATED_FROM": sorted(T.HUMAN_GATED_FROM),
        # I-16: actors allowed to create work (workers cannot).
        "WORK_CREATOR_ACTORS": sorted(T.WORK_CREATOR_ACTORS),
        # D1/R1: actors allowed to invoke lease reclaim.
        "RECLAIM_ACTORS": sorted(T.RECLAIM_ACTORS),
    }
    return doc


def _ladder_module() -> dict:
    doc = {}
    doc.update(_meta(
        "Canonical five-rung recovery ladder (contract section E.1), the "
        "single ladder shared by R8 (records rung context) and R9 (owns "
        "ladder policy). From CANONICAL_LADDER in src/axos/exec/policy.py."))
    doc["canonical_ladder"] = {
        str(rung): name for rung, name in sorted(P.CANONICAL_LADDER.items())
    }
    doc["terminal_states"] = sorted(P._TERMINAL_STATES)
    doc["terminal_state_constants"] = {
        "T_RECOVERY_COMPLETE": P.T_RECOVERY_COMPLETE,
        "T_BUDGET_EXHAUSTED": P.T_BUDGET_EXHAUSTED,
        "T_R5_TERMINAL": P.T_R5_TERMINAL,
        "T_BLOCKED_CONTRADICTORY": P.T_BLOCKED_CONTRADICTORY,
        "T_BLOCKED_STATE_UNAVAILABLE": P.T_BLOCKED_STATE_UNAVAILABLE,
        "T_SUPERSEDED": P.T_SUPERSEDED,
    }
    return doc


def _rungs_module() -> dict:
    doc = {}
    doc.update(_meta(
        "R8 recovery controller's canonical rung mapping: each R8 action "
        "records its canonical ladder rung (contract E.1) without "
        "implementing ladder policy. From src/axos/exec/recovery.py."))
    doc["canonical_rungs"] = {
        action: {"rung": rung, "name": name}
        for action, (rung, name) in sorted(R._CANONICAL_RUNGS.items())
    }
    doc["failure_classes"] = list(R.RECOVERY_FAILURE_CLASSES)
    doc["recovery_actions"] = list(R.RECOVERY_ACTIONS)
    doc["escalation"] = {
        "zero_progress_attempt_run_before_escalation":
            R._TWO_ATTEMPT_ESCALATION_RUN,
        "escalation_target": R._ESCALATION_TARGET,
        "rule": "zero authoritative progress across TWO consecutive "
                "recovery attempts is failed recovery -> escalate "
                "(contract E section 4, mission R8 section 11).",
    }
    return doc


def main() -> None:
    out = {
        "transitions.json": _transitions_module(),
        "recovery-ladder.json": _ladder_module(),
        "recovery-rungs.json": _rungs_module(),
    }
    contracts_dir = os.path.join(REPO_ROOT, "contracts")
    os.makedirs(contracts_dir, exist_ok=True)
    for fname, payload in out.items():
        path = os.path.join(contracts_dir, fname)
        with open(path, "w") as f:
            json.dump(payload, f, indent=2, sort_keys=False)
            f.write("\n")
        print(f"wrote {os.path.relpath(path, REPO_ROOT)}")


if __name__ == "__main__":
    main()
