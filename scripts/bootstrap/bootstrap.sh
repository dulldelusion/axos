#!/usr/bin/env bash
# AXOS repository — bootstrap entry point.
#
# Contract:
#   Runs, in order:  validate environment -> initialize runtime ->
#   validate state -> self-test, aborting on the first failure.
#   Prints a readiness report. NEVER starts workers or production.
#
# Repo root resolution: $AXOS_REPO_ROOT (or legacy $AXOS_HOME) if set,
# else two directories up from this scripts/bootstrap/ dir.
# The axos package is resolved via <repo>/src on PYTHONPATH.
# Runtime state defaults to <repo>/.axos-state (gitignored); override
# with $AXOS_STATE_ROOT. No absolute machine paths are ever assumed.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
if [ -n "${AXOS_REPO_ROOT:-}" ]; then
    ROOT="$AXOS_REPO_ROOT"
elif [ -n "${AXOS_HOME:-}" ]; then
    ROOT="$AXOS_HOME"
else
    ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
fi
export AXOS_REPO_ROOT="$ROOT"
export AXOS_STATE_ROOT="${AXOS_STATE_ROOT:-$ROOT/.axos-state}"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

pass() { echo "[BOOTSTRAP] PASS: $1"; }
fail() { echo "[BOOTSTRAP] FAIL: $1" >&2; }

echo "================================================================"
echo "AXOS repository bootstrap"
echo "repo root : $ROOT"
echo "state root: $AXOS_STATE_ROOT"
echo "================================================================"

echo ""
echo "--- step 1/4: validate_environment.py"
if python3 "$SCRIPT_DIR/validate_environment.py"; then pass "environment"; else
    fail "validate_environment.py"; echo "BOOTSTRAP ABORTED"; exit 1; fi

echo ""
echo "--- step 2/4: initialize_runtime.py"
if python3 "$SCRIPT_DIR/initialize_runtime.py"; then pass "initialize"; else
    fail "initialize_runtime.py"; echo "BOOTSTRAP ABORTED"; exit 1; fi

echo ""
echo "--- step 3/4: validate_state.py"
if python3 "$SCRIPT_DIR/validate_state.py"; then pass "state"; else
    fail "validate_state.py"; echo "BOOTSTRAP ABORTED"; exit 1; fi

echo ""
echo "--- step 4/4: run_self_test.py"
if python3 "$SCRIPT_DIR/run_self_test.py"; then pass "self-test"; else
    fail "run_self_test.py"; echo "BOOTSTRAP ABORTED"; exit 1; fi

echo ""
echo "================================================================"
echo "AXOS READINESS REPORT"
echo "  repo root   : $ROOT"
echo "  environment : PASS"
echo "  runtime init: PASS"
echo "  state       : PASS (fresh DB, migration v12, governance STOPPED)"
echo "  self-test   : PASS"
echo "  workers     : NOT STARTED (by design; governance is STOPPED)"
echo "  production  : NOT STARTED"
echo "  STATUS      : READY — operator may now schedule work explicitly."
echo "================================================================"
