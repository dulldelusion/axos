# Bootstrap

`scripts/bootstrap/bootstrap.sh` is the one-shot installer for an AXOS
checkout. It takes a fresh clone to a verified, initialized, STOPPED
runtime — and nothing further. It **never starts workers or
production**.

## Command

From the repository root:

```bash
bash scripts/bootstrap/bootstrap.sh
```

`bootstrap.sh` runs with `set -euo pipefail` and aborts on the first
failed stage. A successful run ends with the `AXOS READINESS REPORT`
block on stdout.

## Environment variables (bootstrap scripts only)

These variables are read by the bootstrap *scripts*, not by AXOS
itself (AXOS reads zero environment variables — see
`config/ENVIRONMENT_CONTRACT.md`).

| Variable | Default | Purpose |
|---|---|---|
| `AXOS_REPO_ROOT` | two directories up from `scripts/bootstrap/` (i.e. the repo root) | Repository root. Legacy alias `AXOS_HOME` is still honored. |
| `AXOS_STATE_ROOT` | `<repo>/.axos-state` (gitignored) | Where runtime state is created: `state/axos.db`, `state/axos-staging/`, `runtime/governance.json`. Point it elsewhere to keep the checkout pristine. |

`bootstrap.sh` additionally exports `PYTHONPATH=<repo>/src` so that
`import axos` resolves to the frozen tree at `src/axos/`.

## The four stages

### 1/4 — validate_environment.py

Verifies the machine can host AXOS. Checks:

1. `python3 >= 3.12` (floor from the release manifest's `python_version`).
2. `src/axos/release_manifest.json` is present, valid JSON, and carries
   a `release_id`.
3. Required modules importable (`sqlite3`, `hashlib`, `json`,
   `subprocess`, `threading`, `tempfile`, `uuid`, `datetime`,
   `platform`), plus the checkout's own `axos.store` and `axos.exec`
   (resolved via `<repo>/src` on `sys.path`).
4. The `sqlite3` runtime version is `>=` the manifest's
   `sqlite_version` (3.45.1).
5. `state/` and `runtime/` are creatable and writable under the state
   root (verified with a write probe).
6. `pytest` availability — **warning only**; `run_self_test.py` falls
   back to functional probes when pytest is absent.

### 2/4 — initialize_runtime.py

Initializes runtime state using AXOS's own initialization code path —
`axos.store.open_store(db_path)` + `axos.store.migrate(store)` —
never invented logic. Steps:

1. Creates `<state-root>/state/`, `<state-root>/state/axos-staging/`,
   and `<state-root>/runtime/`.
2. Loads `<state-root>/runtime/.env` if present (`KEY=VALUE` lines;
   never overrides already-set variables; runtime config only).
3. Opens/creates the store at `<state-root>/state/axos.db` and applies
   migrations. The resulting schema versions must be exactly `1..12`
   (re-running on an initialized DB is a no-op).
4. **Governance gate — a fresh install can never adopt an old
   workload.** If `tasks`, `jobs`, `workers`, or `desired_state` contain
   any rows, or `desired_state_head` is not at the seeded v0,
   initialization refuses to proceed. Delete the state database
   explicitly if a truly fresh install is intended.
5. Writes the safe-default governance file
   `<state-root>/runtime/governance.json`:
   `DESIRED_STATE=STOPPED`, `workers=0`, schema migration version 12.

Idempotent: safe to re-run on an already initialized, empty database.

### 3/4 — validate_state.py

Validates the initialized state without mutating it:

1. `state/axos.db` exists and opens.
2. Schema migrations are exactly `1..12`.
3. The manifest's `migration_version` (12) matches the DB.
4. **Release identity:** every file hash recorded in
   `src/axos/release_manifest.json` matches the source file on disk
   (50 manifest file hashes checked on the frozen tree).
5. `Store.integrity_check()` passes (`PRAGMA integrity_check` +
   `foreign_key_check`).
6. Governance flags: `tasks`, `jobs`, `workers`, `desired_state` all
   empty; `desired_state_head` at seeded v0; `runtime/governance.json`
   records `DESIRED_STATE=STOPPED` and `workers=0`.

### 4/4 — run_self_test.py

Runs 17 real checks against the frozen source — SOURCE_IMPORTS,
DATABASE_OPEN, SCHEMA_VALID, MIGRATIONS_VALID, TRANSITION_GATE,
STATE_TRANSITIONS, LEASES, FENCING, HEARTBEAT, WATCHDOG, CHECKPOINT,
ARTIFACT, RECONCILIATION, RECOVERY, FINALIZATION, GOVERNANCE,
SUPERVISOR. Each check either:

- runs the relevant existing pytest suites from `src/axos/tests/` via
  subprocess (`python3 -m pytest -q -p no:cacheprovider`, 900 s timeout
  per file, working directory = repo root), when pytest is available; or
- runs a direct functional probe against the real AXOS modules,
  labeled `[functional fallback]`, when pytest is absent.

Notes:

- `STATE_TRANSITIONS` always executes the adversarial audit script
  `src/axos/audit/02_transitions.py` for real.
- `CHECKPOINT` and `FINALIZATION` exercise canonical identity
  functions and the gate checkpoint lifecycle directly.
- All checks use fresh throwaway databases in the system temp dir —
  **never** the state database.
- Each pytest invocation runs with `-p no:cacheprovider`, so no
  `.pytest_cache` is written into the checkout.

## What bootstrap guarantees on success

- A fresh store at `<state-root>/state/axos.db`, schema v12, WAL mode,
  empty workload tables, desired-state authority at seeded v0.
- `runtime/governance.json` recording `DESIRED_STATE=STOPPED`,
  `workers=0`.
- The release identity verified against `src/axos/release_manifest.json`.
- The self-test passing against throwaway databases.

## What bootstrap never does

- Never starts workers, controllers, the scheduler, or production.
- Never adopts a pre-existing workload (refuses if the DB is non-empty).
- Never clears a `PAUSED_FOR_HUMAN` gate (there is none on a fresh
  install; the rule holds for every later operation).
- Never modifies anything under `src/axos/` (the frozen source).

After bootstrap, the system is STOPPED with zero workers. That is the
safe default, not a bug. Do not start any controller until you have a
declared task and an explicit instruction to run it.
