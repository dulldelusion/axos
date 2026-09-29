# AXOS Reproducibility

Read-only source export, 2026-09-28. Everything below is verified
against `~/workspace/axos/` (release
`551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`).
Items that could not be verified from the tree are marked as such.

## 1. Language runtime

- **Python 3.12.3** — recorded in `release_manifest.json`
  (`"python_version": "3.12.3"`) as part of the release identity, and
  verified present on this VM (`python3 --version` → `Python 3.12.3`).
- The release manifest also binds the OS identity:
  `"platform": {"system": "Linux", "release": "7.0.0-38-generic",
  "machine": "x86_64"}`. Tests spawn real subprocesses and send real
  signals (`SIGTERM`/`SIGKILL` via `killpg`); a POSIX/Linux environment
  is required. Windows/macOS behavior is **unverifiable from the tree**
  (never exercised in evidence).

## 2. Dependencies — stdlib only

- This worker grepped every import in all 55 `.py` files under
  `store/`, `exec/`, `tests/`, `audit/`. Every import resolves to the
  **Python standard library** (`sqlite3`, `hashlib`, `json`,
  `threading`, `argparse`, `ast`, `datetime`, `math`, `os`, `platform`,
  `random`, `re`, `shutil`, `signal`, `subprocess`, `sys`, `tempfile`,
  `textwrap`, `time`, `unittest`, `uuid`, `dataclasses`, `contextlib`,
  `typing`, `inspect`) or to intra-package modules (`axos.store.*`,
  `axos.exec.*`, `axos.tests.*`).
- **No `requirements.txt`, no `pyproject.toml`, no `setup.py`, no
  `setup.cfg`, no `Pipfile`** — verified by filename search over the
  whole tree (excluding `__pycache__`). Nothing to install; **no
  package manager is needed**.
- No vendored third-party code found.

## 3. Package layout and import mechanics (no install step)

- The tree is used in place; there is no build and no install. The
  workspace root (`~/workspace`) must be on `sys.path` so `import axos`
  resolves (`~/workspace/axos/` is the package; note the sibling
  `~/workspace/axos.py`-style collisions do not exist — verified: no
  top-level `axos.py`).
- Test files set this up themselves, e.g. `tests/test_final_hardening_r14.py`
  inserts `REPO` (`~/workspace`) and `HERE` (`tests/`) into `sys.path`.
- The supervisor, when spawning real worker subprocesses, sets
  `env["PYTHONPATH"] = WORKSPACE_ROOT + os.pathsep + ...`
  (`exec/supervisor.py:332–334`); `exec/worker.py` additionally inserts
  the workspace root itself. So spawned workers resolve `axos` without
  any installation.

## 4. Database requirements

- **SQLite 3.45.1** — recorded in `release_manifest.json`
  (`"sqlite_version": "3.45.1"`) and verified present on this VM
  (`sqlite3.sqlite_version` → `3.45.1`). Required features: **WAL mode**,
  the **SQLite backup API**, and CHECK constraints — all standard in
  3.45.1.
- Enforced pragmas (from `AUDIT-REPORT.md`, verified on real files):
  `journal_mode=WAL`, `synchronous=NORMAL`, `foreign_keys=ON`,
  `busy_timeout=5000`. Applied in `store/db.py` (`Store._apply_pragmas`)
  and verified by tests on reopened files after kills.
- The database is a plain file created by `open_store(path)`; schema is
  built by `migrate(store)` through the versioned `MIGRATIONS` list in
  `store/migrations.py` — **v1 (Phase 1A initial schema) through v12**
  (`"migration_version": 12` in the manifest; `applied_versions(store)`
  reports applied versions; re-running migrations is a no-op; a failed
  migration rolls back fully and records nothing).
- Note: `store.db` in the tree is a **0-byte leftover file** (dated after
  manifest generation) and is **not part of the release identity** — it
  appears in none of `release_manifest.json`'s `file_hashes`. Tests
  always use their own temp DB files. It can be ignored or deleted;
  nothing depends on it (no code references a default `store.db` path —
  verified by grep).
- Operational notes: the store clock is DB-backed and monotonic
  (`axos_meta.last_commit_ts` floor); workers log non-authoritative
  debug output to `/tmp/axos-<proc_id>.log`, so `/tmp` must be writable
  when running the execution-layer tests.

## 5. Environment variables and configuration

- **No required environment variables.** Verified: the only
  `os.environ`/`os.getenv` uses in `store/ exec/ tests/ audit/` are
  (a) `os.environ.copy()` to inherit the ambient environment for
  spawned workers (`exec/supervisor.py:332`, `tests/test_exec.py:111`),
  (b) `dict(os.environ, PYTHONPATH=...)` in test/audit harnesses, and
  (c) the supervisor *setting* `PYTHONPATH` for its child workers.
  No `AXOS_*` or application config variable is read anywhere.
- **No config files.** There is no settings module, no `.ini`/`.yaml`/
  `.toml` config, no `.env` file (verified by filename search).
  Behaviour is driven by constructor arguments (`SchedulerConfig`,
  `WatchdogConfig`, `RecoveryConfig`, `ReconcilerConfig`,
  `ResilienceConfig`, `FinalizationConfig`, `PolicyConfig`) and by
  command-line flags on `axos.exec.worker` (`--db`, `--worker-id`,
  `--proc-id`, `--job-id`, `--ttl-s`, `--behavior`, `--hb-interval-s`,
  `--no-renew`, `--expect-token`).
- **No secrets of any kind** in the tree — verified by filename search
  (`*secret*`, `*.env*`, `*credential*`, `*.pem`, `*.key`): no hits.
  Nothing in this document discloses, or needs, a credential.

## 6. Test commands

- **Documented command** (`README.md`):
  ```
  cd ~/workspace/axos
  python3 -m unittest discover -s tests -v
  ```
  This runs the whole `unittest`-based suite (the R1–R13 files and
  `test_store.py`/`test_remediation.py` are `unittest.TestCase`-based).
- **R14 suite** (`tests/test_final_hardening_r14.py` docstring):
  ```
  python -m pytest tests/test_final_hardening_r14.py
  ```
  pytest is **not a declared dependency** (no requirements file, and
  every test file is `unittest`-compatible), but the R14 file's own
  docstring prescribes pytest. pytest 9.1.1 is installed on this VM
  (verified); its presence on a fresh environment is **not guaranteed
  by the tree** — install it via the environment's own package manager
  if needed. The full R14 suite takes ~218s; the full repository
  ×3 campaign took ~366s per pass in evidence.
- **Audit scripts** (standalone, top-level code, no `__main__` guard):
  ```
  python3 audit/01_write_path.py   # … through 09_authority_audit.py
  python3 audit/stray_process_audit.py
  ```
  Audit 09 exits non-zero on any failing check ("any failure blocks the
  gate"). Several audits take minutes (audit 09 ran 177 checks).

## 7. Build commands

- **There is no build step.** The tree is pure Python, stdlib-only, run
  in place. Nothing to compile, transpile, bundle, or package.
- **Release identity** is computed, not built: `release_id =
  sha256(canonical_release_manifest)`, where the manifest hashes every
  regular file under `exec/ store/ audit/ tests/` (excluding
  `__pycache__`; `release_manifest.json` and `R14-PLAN.md` excluded).
  The generator is `tests/test_final_hardening_r14.py`
  (`TestR14ReleaseManifest`). Regenerating the manifest changes the
  release identity by design.

## 8. Operational prerequisites

- Linux/POSIX with working process groups and signal delivery
  (`SIGTERM`/`SIGKILL`, `killpg`) — the R2/R6/R7/R14 tests rely on it.
- Writable `/tmp` (worker debug logs) and a writable directory for temp
  test databases (tests use `tempfile`; they clean up after themselves
  and assert zero stray processes).
- Sufficient file descriptors / process headroom for concurrent
  subprocess tests (the R14 campaign spawns dozens of real worker and
  supervisor processes).
- No network, no external services, no containers, no cloud credentials
  are needed — the entire suite runs against local SQLite files and
  local processes. (The blocked R14 *external* hypervisor proof is a
  different matter; see `AXOS_TEST_ARCHITECTURE.md` §5 — it requires a
  hypervisor with hard power-off control and an external observer host,
  neither of which this tree provides.)

## 9. What was *not* verified

- This worker did **not** execute the test suite (the task is read-only;
  running pytest would write `.pytest_cache`/`__pycache__` under
  `~/workspace/axos/`). All claims above are static, from the source
  tree and the evidence documents listed in
  `AXOS_TEST_ARCHITECTURE.md` §7.
- pytest availability on a *fresh* environment is not guaranteed by the
  tree (see §6).
- Non-Linux behaviour is unverified (see §1).
