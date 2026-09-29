# AXOS Portable Runtime — Runtime Requirements

**Release:** 551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192
(migration v12). Derived 2026-09-28 by read-only inspection of
`~/workspace/axos/` (imports, manifests, scripts, tests, docs). Nothing here
is guessed; unverifiable items are marked UNKNOWN.

## 1. Operating system

- **Linux, x86_64** (release_manifest.json: `system=Linux`,
  `machine=x86_64`, `release=7.0.0-38-generic`).
- **Hard Linux requirement**, not merely "tested on": the code reads
  `/proc/<pid>/stat` and `/proc/stat` for process start-time identity
  (`exec/boot.py`, `exec/supervisor.py`), lists `/proc` for process-group
  fencing, and uses POSIX-only process control — `start_new_session=True`,
  `os.killpg(pgid, SIGTERM/SIGKILL)`, `signal.SIGKILL`. There is no
  Windows/macOS fallback path in source. macOS/BSD portability: UNKNOWN
  (untested; `/proc` absence would break PID-identity and fencing).
- Kernel/glibc minimum versions: UNKNOWN (never recorded).

## 2. Language runtime

- **Python == 3.12.3** (release_manifest.json `python_version`; verified on
  this VM: `python3 --version` → 3.12.3). Source uses `X | None` union
  syntax (3.10+) but the only *tested* version is 3.12.3 — minimum
  supported version UNKNOWN.
- **Package manager: none needed.** Zero third-party packages (see
  `requirements.lock`). A bare CPython 3.12.3 install suffices.
- `python3` must be on PATH: the supervisor spawns workers via
  `sys.executable`, and audit/synthetic helpers spawn `python -c` children.

## 3. Third-party dependencies

**None.** The complete import closure over `exec/`, `store/`, `audit/`,
`tests/` is stdlib-only (argparse, ast, contextlib, dataclasses, datetime,
hashlib, inspect, json, math, os, platform, random, re, shutil, signal,
sqlite3, subprocess, sys, tempfile, textwrap, threading, time, unittest,
uuid, typing) plus intra-package imports. No `pyproject.toml`, no
`requirements*.txt`, no `Pipfile`, no lockfiles exist in the tree.
README.md states: "Standard library only (sqlite3, hashlib, json,
threading) — no frameworks, no external services."

## 4. Database engine

- **SQLite == 3.45.1**, accessed exclusively through the stdlib `sqlite3`
  module (`store/db.py`). No sqlite3 CLI dependency, no ORM, no driver.
- **WAL mode is mandatory**: `Store` executes `PRAGMA journal_mode=WAL` and
  raises `StoreError` if the mode is not WAL. Also enforced:
  `synchronous=NORMAL`, `foreign_keys=ON`, `busy_timeout=5000` (ms),
  `page_size=4096` on fresh DBs; readers additionally get
  `PRAGMA query_only=ON`.
- Filesystem implication: WAL requires a filesystem that supports
  shared-memory/WAL files (`-wal`, `-shm` sidecars next to the DB).
  Behavior on NFS or other network filesystems: UNKNOWN / unverified —
  use a local POSIX filesystem.

## 5. Required commands

| Command | Why |
|---|---|
| `python3` (3.12.3) | interpreter; also re-invoked as `sys.executable` for worker/child processes |

## 6. Optional commands

| Command | Why optional |
|---|---|
| `sqlite3` CLI | Never invoked by AXOS code (stdlib `sqlite3` module only); useful for operators to inspect `store.db` manually |
| `pytest` | Not imported by any source file; documented test runner is stdlib `python3 -m unittest discover -s tests -v`. pytest 9.1.1 appears only in stale `__pycache__` filenames from a historical run |

## 7. Network

**None.** No `socket`, `urllib`, `http`, `ssl`, or `requests` imports
anywhere. No ports, no listeners, no outbound connections. Fully offline
capable.

## 8. Runtime directories & filesystem

- **Database file**: path is an argument to `open_store(path)`; any
  writable location. `open_store` does NOT create parent directories
  (whether callers guarantee them: UNKNOWN — pass an existing directory).
  WAL sidecar files (`<db>-wal`, `<db>-shm`) are created alongside it.
- **Artifact staging root**: constructor argument (`staging_root`);
  the gate creates parents with `os.makedirs(..., exist_ok=True)` and
  writes bytes durably (file fsync + directory fsync).
- **Temp directories**: `tempfile` is used only in `audit/` scripts and
  tests — not in `exec/` or `store/`. No `TMPDIR` dependency at runtime.
- The tree ships two zero-byte placeholder files (`store.db`,
  `store/store.db`); they are not required inputs (UNKNOWN why they exist
  — likely test residue).

## 9. Permissions

- Read access to the AXOS source tree (frozen; must remain unmodified).
- Read/write access to the directory containing the database file
  (DB file + WAL/SHM sidecars are written in place).
- Read/write access to the artifact staging root.
- Permission to spawn child processes and signal process groups
  (`os.killpg`) — i.e. not a seccomp/apparmor profile that blocks
  signaling non-children. Containerization constraints: UNKNOWN.

## 10. Environment variables

**None.** See `ENVIRONMENT_CONTRACT.md`. All configuration is passed as
function/constructor arguments (DB path, staging root, worker argv,
heartbeat intervals).

## 11. UNKNOWN items (could not be verified from source evidence)

1. Minimum Python patch/minor version below 3.12.3 (only 3.12.3 tested).
2. Minimum SQLite version below 3.45.1.
3. Behavior on non-local filesystems (NFS) — WAL sidecar requirements.
4. Minimum Linux kernel / glibc versions.
5. macOS/BSD portability (expected broken: `/proc` + `killpg`).
6. Whether `open_store` callers guarantee parent-dir existence.
7. Purpose of the zero-byte `store.db` / `store/store.db` placeholders.
8. Container/seccomp constraints for process-group signaling.
