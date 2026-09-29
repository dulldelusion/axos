# AXOS Environment Contract

**Release:** 551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192
(migration v12)

## Environment variables: NONE

AXOS reads **zero** environment variables for configuration. Verified by
exhaustive grep of the source tree (`exec/`, `store/`, `audit/`, `tests/`):

- No `os.getenv`, no `os.environ.get`, no `os.environ[...]` read for any
  configuration key exists anywhere in the source.
- The only `os.environ` touches are pass-through copies when spawning child
  processes, never configuration reads:
  - `exec/supervisor.py:332` — `env = os.environ.copy()` passed to the
    supervised worker's `subprocess.Popen` (worker inherits the parent's
    environment unchanged).
  - `tests/test_store.py:402`, `tests/test_exec.py:111`,
    `audit/06_crash_backup_migration.py:75,196` — same pass-through pattern
    for test/audit subprocesses (one audit script adds `PYTHONPATH` pointing
    at the repo root so the child can `import axos`).

### Environment variable inventory

| NAME | REQUIRED | DEFAULT | TYPE | PURPOSE | SAFE_EXAMPLE | SECRET |
|------|----------|---------|------|---------|--------------|--------|
| *(none)* | — | — | — | AXOS has no env-var configuration. | — | — |

> **Note — repo bootstrap tooling only.** The repository's bootstrap
> scripts under `scripts/bootstrap/` (which are *not* part of the frozen
> AXOS release) honor three optional convenience variables:
> `AXOS_REPO_ROOT` (fallback legacy `AXOS_HOME`), and `AXOS_STATE_ROOT`.
> These affect only where the bootstrap *tooling* looks for the checkout
> and where it writes runtime state (`<repo>/.axos-state` by default).
> The AXOS core (`src/axos/exec/`, `src/axos/store/`, `src/axos/audit/`,
> `src/axos/tests/`) still reads zero environment variables.

### How AXOS is actually configured

Everything is **constructor / function-argument based**. The key entry
points take explicit paths:

- `store/db.py:299` — `open_store(path: str, clock: Clock | None = None)`
- `store/db.py:309` — `open_readonly_store(path: str)`
- `exec/supervisor.py:157` — `Supervisor(db_path: str, actor: str = "supervisor", heartbeat_interval_s: float = 0.5)`
- `TransitionGate(staging_root=...)` — artifact staging directory is passed
  explicitly (gate auto-creates parent dirs: `os.makedirs(..., exist_ok=True)`).

There is no config file format, no dotenv loading, no settings module.

### What a portable launcher must provide instead of env vars

Since there is no env-var surface, a portable bundle launcher supplies
configuration as arguments:

| Concern | How to supply it |
|---|---|
| Database location | `open_store("<path>/store.db")` — any writable path; parent dirs are NOT auto-created by `open_store` (UNKNOWN whether callers guarantee them — pass an existing directory) |
| Artifact staging root | `TransitionGate(..., staging_root="<path>/staging")` — parents auto-created |
| Worker command | passed to the supervisor's spawn path as an argv list; workers are launched as `sys.executable`-based child processes with `start_new_session=True` |
| `PYTHONPATH` for spawned `python -c` children | inherited from the parent environment (pass-through, not read) |

### Notes for operators

- Do **not** invent `AXOS_*` variables: no code reads them, and a future
  reader would assume a contract that does not exist.
- Secrets: AXOS has no secret inputs at all (no keys, tokens, or passwords
  in the codebase; the ledger is tamper-evident via hash chaining, not
  cryptographic keys — recorded as a known property in README.md).
- `PATH`/`PYTHONPATH` are the only ambient environment facts that matter,
  and only because the OS needs them to resolve `python3` / import `axos`;
  AXOS itself never inspects them.
