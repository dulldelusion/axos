# Environment

The exact environment the frozen AXOS release
(`551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`,
migration v12) was built and verified on, and what a machine must
provide to reproduce it. Derived by read-only inspection of the frozen
source; unverifiable items are marked UNKNOWN rather than guessed.
See also `config/RUNTIME_REQUIREMENTS.md` and
`config/requirements.lock`.

## Required

| Item | Requirement | Why |
|---|---|---|
| OS | **Linux, x86_64** | Hard requirement, not merely "tested on". The code reads `/proc/<pid>/stat` and `/proc/stat` for process start-time identity, lists `/proc` for process-group fencing, and uses POSIX-only process control (`start_new_session=True`, `os.killpg(pgid, SIGTERM/SIGKILL)`). There is no Windows/macOS fallback path in source. |
| Python | **3.12.3** | Recorded in `release_manifest.json` (`python_version`); the only tested version. Source uses `X \| None` union syntax (3.10+), but minimum supported version below 3.12.3 is UNKNOWN. |
| SQLite | **3.45.1** via the stdlib `sqlite3` module | Recorded in `release_manifest.json`. `Store` enforces `PRAGMA journal_mode=WAL` (raises `StoreError` if not WAL), plus `synchronous=NORMAL`, `foreign_keys=ON`, `busy_timeout=5000` ms, `page_size=4096` on fresh DBs; readers additionally get `PRAGMA query_only=ON`. |
| Third-party packages | **None** | The complete import closure over `exec/`, `store/`, `audit/`, `tests/` is stdlib-only (argparse, ast, contextlib, dataclasses, datetime, hashlib, inspect, json, math, os, platform, random, re, shutil, signal, sqlite3, subprocess, sys, tempfile, textwrap, threading, time, unittest, uuid, typing) plus intra-package `axos.*` imports. No `pyproject.toml`, no `requirements*.txt`, no lockfile in the tree — nothing to install, no package manager needed. |
| Filesystem | Local POSIX filesystem, writable | WAL requires `-wal`/`-shm` sidecars next to the DB file; behavior on NFS or other network filesystems is UNKNOWN/unverified. The state root, the DB directory, and the artifact staging root must be writable. |
| `/tmp` | Writable | Workers log non-authoritative debug output to `/tmp/axos-<proc_id>.log`; tests use temp databases. |
| Process control | Permission to spawn child processes and signal process groups | `os.killpg` for fencing; container/seccomp constraints are UNKNOWN. |
| Network | None | No `socket`, `urllib`, `http`, `ssl`, or `requests` imports anywhere. No ports, no listeners, no outbound connections. Fully offline-capable. |

## Configuration surface: zero environment variables

AXOS reads **no** environment variables for configuration — verified by
exhaustive grep of the source tree. The only `os.environ` touches are
pass-through copies when spawning child processes (the worker inherits
the parent's environment unchanged), never configuration reads. There
is no config file format, no dotenv loading, no settings module.

Everything is constructor / function-argument based:

- `open_store(path)` / `open_readonly_store(path)` — database location
  (any writable path; `open_store` does not create parent directories).
- `TransitionGate(store, staging_root=...)` — artifact staging root
  (parents auto-created).
- `Supervisor(db_path, actor=...)`, `SchedulerConfig`,
  `WatchdogConfig`, `RecoveryConfig`, `ReconcilerConfig`,
  `ResilienceConfig`, `FinalizationConfig`, `PolicyConfig` — behavior.
- `python -m axos.exec.worker` flags: `--db`, `--worker-id`,
  `--proc-id`, `--job-id`, `--ttl-s`, `--behavior`,
  `--hb-interval-s`, `--no-renew`, `--expect-token`.

Do not invent `AXOS_*` variables: no code reads them. (`AXOS_REPO_ROOT`
and `AXOS_STATE_ROOT` are read by the repository's bootstrap *scripts*,
not by AXOS itself — see `docs/operations/bootstrap.md`.)

## Optional

| Item | Notes |
|---|---|
| `pytest` | Not imported by any source file; the documented test runner is stdlib `python3 -m unittest discover -s tests -v`. The R14 file's own docstring prescribes pytest — install via your OS package manager if needed. Without it, the bootstrap self-test uses functional fallback probes. |
| `sqlite3` CLI | Never invoked by AXOS code; useful for operators inspecting the DB file manually. |

## Check your machine

```bash
python3 --version            # must be 3.12.3
python3 -c "import sqlite3; print(sqlite3.sqlite_version)"  # must be 3.45.1
uname -sm                    # must be Linux x86_64
test -w /tmp && echo /tmp writable
```

The bootstrap stage 1 (`scripts/bootstrap/validate_environment.py`)
performs these checks automatically, plus manifest and import checks.

## Known unknowns

1. Minimum Python version below 3.12.3 (only 3.12.3 tested).
2. Minimum SQLite version below 3.45.1.
3. Behavior on non-local filesystems (NFS) — WAL sidecar requirements.
4. Minimum Linux kernel / glibc versions (never recorded).
5. macOS/BSD portability (expected broken: `/proc` + `killpg`).
6. Whether `open_store` callers guarantee parent-dir existence (pass an
   existing directory).
7. Container/seccomp constraints for process-group signaling.
