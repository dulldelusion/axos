# AXOS Portable Runtime Bundle — Portability Report

**Generated:** 2026-09-27T20:20Z (UTC)
**Bundle:** `AXOS_PORTABLE_BUNDLE_20260927T202655Z.tar.gz`
**Type:** Packaging / portability task. No AXOS source modified, no production touched.

> Structural note: this report ships **alongside** the archive, not inside
> it — it records the archive's own SHA-256, which is circular to embed.
> (Adapted bundle structure, documented per the build brief.)

## Final return fields

```
PORTABLE_BUNDLE=COMPLETE
ARCHIVE_PATH=~/workspace/AXOS_PORTABLE_BUNDLE_20260927T202655Z.tar.gz
ARCHIVE_SIZE_BYTES=571321
ARCHIVE_SHA256=8fcd6307cd5371cbdde4b75421d2c9cedcae40b6862572ce56888ba821822e00

AXOS_SOURCE_INCLUDED=YES          (60 files, exact hierarchy, release 551d559c…, migration v12)
BOOTSTRAP_INCLUDED=YES            (bootstrap.sh + 4 Python stages)
ENVIRONMENT_CONTRACT_INCLUDED=YES (runtime/ENVIRONMENT_CONTRACT.md + .env.example)
DEPENDENCIES_LOCKED=YES           (runtime/requirements.lock — stdlib-only, pinned)
MIGRATIONS_INCLUDED=YES           (in axos/store/migrations.py, v1→v12)
SELF_TEST_INCLUDED=YES            (bootstrap/run_self_test.py, 17 checks)
MUSE_HANDOFF_INCLUDED=YES         (docs/MUSE_HANDOFF.md)

CLEAN_ROOM_BOOT=PASS
DEPENDENCY_REPRODUCTION=PASS
DATABASE_INITIALIZATION=PASS
MIGRATION_VALIDATION=PASS
SELF_TEST=PASS
AXOS_IMPORT=PASS
TRANSITION_GATE=PASS
SAFE_DEFAULT_STATE=PASS

SECRET_SCAN=PASS
ARCHIVE_READBACK=PASS
SOURCE_MUTATION=NO

WORD_PICS_CORE_DEPENDENCY=NO
PORTABILITY_BLOCKER=NONE

HARD_STOP=YES
```

## What was built

A self-contained bundle a fresh Muse account can extract → bootstrap →
validate → self-test → operate, with no reliance on this conversation,
hidden workspace state, or the original account:

- `axos/` — complete source (60 files), caches and `store.db` placeholders excluded
- `bootstrap/` — `bootstrap.sh` entry point; environment validator; runtime
  initializer (uses AXOS's own `open_store` + `migrate`); state validator;
  17-check self-test runner
- `runtime/` — requirements lock, environment contract (0 env vars — AXOS is
  argument-configured), `.env.example` placeholders, runtime requirements doc
- `state/` — intentionally empty skeleton + README (clean-init from migrations
  is the designed boot path; copied DBs carry dead-worker leases/PIDs)
- `docs/` — README quickstart, MUSE_HANDOFF, OPERATING_AXOS (real commands
  only), ARCHITECTURE, STATE_MODEL, RECOVERY, WORKLOAD_CONTRACT
- `workloads/word-pics/` — OPTIONAL toy adapter example (118 lines, verified
  against the real `TransitionGate` API in a sandbox); AXOS runs without it
- `MANIFEST.json`, `FILE_MANIFEST.json` (81 files, SHA-256 each), `SHA256SUMS`

## Key portability findings (evidence-grounded)

1. **Zero third-party runtime dependencies.** Exhaustive import audit: stdlib
   only (`sqlite3`, `hashlib`, `json`, `threading`, …). No network modules —
   fully offline capable. Pinned: Python == 3.12.3, SQLite == 3.45.1.
2. **Zero environment-variable configuration.** All config is
   constructor/argument based (`open_store(path)`, `Supervisor(db_path, …)`).
3. **Linux x86_64 required** — `/proc` reads, `os.killpg`, `start_new_session`.
   No Windows/macOS fallback. Documented, not a blocker for the target.
4. **No hard-coded machine paths.** DB path is always caller-supplied;
   staging root defaults to `<db-dir>/axos-staging`, overridable. Import
   roots are `__file__`-derived. One hard-coded `/tmp` debug log (FHS,
   non-authoritative). One test-only `~/workspace` reference in
   `tests/test_boot_r6.py` (runtime unaffected).
5. **PORTABILITY_BLOCKER=NONE.** Nothing requires a source change.
6. **State ships empty by design.** Clean `migrate()` init is the tested
   fresh-boot path; the in-tree `store.db` files are 0-byte placeholders.
7. **AXOS core has no global DESIRED_STATE/SUPERVISOR_STOP flag** — that flag
   belonged to the Word Pics operational control plane, not AXOS. The bundle
   enforces STOPPED as: empty `desired_state` table, head at seeded v0, zero
   workers/tasks/jobs, plus durable `runtime/governance.json`
   (`DESIRED_STATE=STOPPED`, `workers=0`). Init refuses to run over a
   non-empty DB, so a fresh install can never adopt a foreign workload.

## Clean-room acceptance test (simulated second Muse account)

Extracted the final archive to a fresh temp dir with `PYTHONPATH` and
`AXOS_HOME` unset. `bash bootstrap/bootstrap.sh` exercised the full flow:

- environment PASS (Python 3.12.3, SQLite 3.45.1, stdlib imports, writable fs)
- runtime init PASS (dirs created, migrations 1–12 applied, governance written)
- state PASS (DB opens, versions 1–12, 50/50 manifest file hashes match
  release `551d559c…`, integrity check ok, STOPPED verified)
- self-test: **17/17 checks PASS** on re-run (exit 0)
- Workers: NOT STARTED. Production: NOT STARTED. No reference to the
  original machine's paths in generated state.

Flake note (recorded, not hidden): the first full-suite run on the final
archive showed 1 failure in `test_reconciliation_r11.py::TestR11Races::
test_RACE_1_reconcile_racing_head_bump` (a timing-sensitive race test, VM
under load). It passed 3/3 in isolation immediately after, and the full
self-test re-run passed 17/17. Verdict: flaky race test, not a portability
defect. No code was changed in response.

## Security

Secret scan over the extracted archive: **PASS** — no API keys, tokens,
passwords, private keys, `.env` files, or credentials. (Hits for `token`
are AXOS's own `fencing_token` lease concept.)

## Source immutability

Pre-packaging baseline (154 files incl. caches) vs post-packaging re-hash:
**154/154 intact, 0 mismatched, 0 missing — AXOS_SOURCE_MUTATED=NO.**

## Exclusions (documented)

- `__pycache__`, `.pytest_cache`, `*.pyc` — regenerable caches
- `store.db` — 0-byte placeholders; live state never ships by design
- No Word Pics production state, canonical data, prompts, or worker code
  anywhere in the bundle (the optional example is a toy, ~118 lines)

## Second-account workflow

```
Account A: build/export  →  AXOS_PORTABLE_BUNDLE_<ts>.tar.gz
Account B: tar -xzf <bundle> → cd <dir> → bash bootstrap/bootstrap.sh
           → read AXOS READINESS REPORT (all PASS)
           → confirm runtime/governance.json: DESIRED_STATE=STOPPED
           → operate per docs/OPERATING_AXOS.md
```

Account B needs nothing from Account A beyond the archive. Default state is
STOPPED with zero workers; execution requires explicit operator action.

## Deliverable locations

- Archive: `~/workspace/AXOS_PORTABLE_BUNDLE_20260927T202655Z.tar.gz`
- This report: `~/workspace/AXOS_PORTABLE_BUNDLE_BUILD/PORTABILITY_REPORT.md`
  (also copied next to the archive)
- Staging/build dir: `~/workspace/AXOS_PORTABLE_BUNDLE_BUILD/` (includes
  `_work/` inspection outputs: source baseline, path audit, state analysis)
