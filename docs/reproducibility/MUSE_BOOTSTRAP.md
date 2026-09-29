# MUSE Bootstrap — verifying AXOS from this repository alone

This document is the complete procedure for an independent agent (or
human) to verify and run AXOS starting from **only** this repository.
No access to the original build machine, no private context, no
additional files. If any step fails, the bootstrap fails — report the
failing step exactly.

## Prerequisites

- Linux x86_64 (AXOS reads `/proc` and uses POSIX process control;
  other platforms are untested and unsupported).
- Python 3.12+ (check: `python3 --version`).
- SQLite 3.45+ (check: `python3 -c "import sqlite3; print(sqlite3.sqlite_version)"`).
- `git`. `pytest` recommended but not required (the self-test has
  functional fallbacks).

## Step 1 — clone

```bash
git clone https://github.com/dulldelusion/axos.git
cd axos
git checkout v0.1.0   # or: git checkout main
```

## Step 2 — confirm the release identity

```bash
python3 tools/verify_release.py
```

Expected: `VERIFIED: release_id
551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192
matches src/axos/release_manifest.json`.

This recomputes the release identity from `src/axos/` using the
project's own manifest builder and compares it to the committed
`src/axos/release_manifest.json`. A mismatch means the source tree is
not the frozen release — stop.

## Step 3 — run the bootstrap

```bash
bash scripts/bootstrap/bootstrap.sh
```

This runs four stages and aborts on the first failure:

1. `validate_environment.py` — Python/SQLite floors, importability of
   the `axos` package via `src/` on `sys.path`.
2. `initialize_runtime.py` — creates `<repo>/.axos-state/` (or
   `$AXOS_STATE_ROOT`), initializes the SQLite database through AXOS's
   own `migrate()` path, seeds the empty desired-state head, writes
   `runtime/governance.json`. Refuses to proceed if the database already
   contains workload rows.
3. `validate_state.py` — re-verifies the release identity (all 50
   manifest file hashes), schema migrations 1..12, DB integrity, and
   governance flags (STOPPED, zero workers/tasks/jobs).
4. `run_self_test.py` — the real test suites plus functional probes:
   imports, database open, schema, migrations, TransitionGate,
   transitions, leases, fencing, heartbeat, watchdog, checkpoint,
   artifact, reconciliation, recovery, finalization, governance,
   supervisor.

Expected ending: the `AXOS READINESS REPORT` block with
`STATUS: READY`. **The bootstrap never starts workers or production.**

Note: `run_self_test.py` runs a real checkpoint round-trip that rewrites
only the `meta.generated_at` timestamp in
`src/axos/release_manifest.json`. The release identity (file hashes,
migration version, policy) is unaffected; `tools/verify_release.py`
still reports VERIFIED afterwards. `git status` will show the manifest
as modified — that is expected and documented, not corruption.

## Step 4 — run the full test suite (optional, recommended)

```bash
PYTHONPATH="$PWD/src" python3 -m pytest src/axos/tests/ -q -p no:cacheprovider
```

Record the exact result (pass count / failures). The frozen release's
final gate was green; any failure on a fresh clone is a finding, not
something to fix by editing `src/axos/`.

## Step 5 — read the contracts

With the runtime verified, read in this order:

1. `docs/architecture/overview.md` — what AXOS is.
2. `docs/architecture/state-machine.md` — Task/Job lifecycles.
3. `docs/contracts/lease.md` — the lease triple and fencing.
4. `docs/invariants/invariants.md` — I-3, I-4, I-16, I-17, I-18.
5. `docs/recovery/recovery-model.md` — incidents, attempts, budgets.

## The truths that must survive any re-implementation

- Durable state is authoritative; reasoning is not.
- `TransitionGate` is the narrow authoritative mutation API; the gate
  owns the writable `Store`, everything else reads through
  `ReadOnlyStore`.
- Lease authority is the `(owner_worker_id, lease_expires_at,
  fencing_token)` triple; timestamps are store time.
- Recovery success requires verified positive progress (I-18);
  zero-progress "success" is rejected in code and by DB constraint.
- `PAUSED_FOR_HUMAN` exits only via durable `APPROVED`, across restarts.
- Workers cannot create tasks or jobs.
- Uncertain completion is reconciled, never assumed.

## Honest status

- **Implemented, tested, audited**: the core kernel above.
- **Blocked**: external hypervisor-level power-cycle proof for failure
  injection (R14 FI-04/FI-12) was never obtainable in the original
  environment; see `reports/historical/R14-EXTERNAL-VALIDATION.md`.
  The status is `R14_BLOCKED — REAL-VM PROOF INCOMPLETE`, not complete.
- **Known limitation**: the execution ledger is tamper-evident, not
  tamper-proof.
