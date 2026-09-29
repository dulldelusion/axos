#!/usr/bin/env python3
"""AXOS repository — initialize runtime state.

Creates the repo-local state and config directories (default
<repo>/.axos-state, overridable via AXOS_STATE_ROOT), then initializes
the authoritative SQLite database using AXOS's OWN initialization code
path (never invented logic):

    axos.store.open_store(db_path)   # store/db.py — opens/creates the DB,
                                     # applies verified pragmas (WAL, etc.)
    axos.store.migrate(store)        # store/migrations.py — applies
                                     # versioned migrations atomically
    axos.store.applied_versions(store)  # verifies schema migration version

Writes SAFE DEFAULT governance state:
  * desired_state table empty, desired_state_head at the migration-seeded
    version 0 (empty snapshot) — this is how "STOPPED" is represented in
    AXOS core: there is no global DESIRED_STATE/SUPERVISOR_STOP flag in the
    axos source; the core expresses "not running" as an empty desired-state
    authority with zero registered workers, zero tasks, zero jobs.
  * runtime/governance.json records DESIRED_STATE=STOPPED and workers=0
    as the repo-level governance contract.

A fresh install can NEVER resume an old workload: if the state database
already contains any tasks, jobs, desired-state items, or worker
registrations, initialization REFUSES to proceed (the operator must
explicitly wipe the state database). Idempotent: safe to re-run on an
already initialized, empty database.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

EXPECTED_MIGRATION_VERSION = 12


def repo_root() -> str:
    """Repo root: AXOS_REPO_ROOT (or legacy AXOS_HOME) if set, else computed.

    This file lives at <repo>/scripts/bootstrap/, so the repo root is
    two directories up.
    """
    env = os.environ.get("AXOS_REPO_ROOT") or os.environ.get("AXOS_HOME")
    if env:
        return os.path.abspath(env)
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, os.pardir, os.pardir))


def src_dir() -> str:
    """Directory containing the ``axos`` package (<repo>/src)."""
    return os.path.join(repo_root(), "src")


def axos_dir() -> str:
    """Frozen AXOS source tree (<repo>/src/axos)."""
    return os.path.join(src_dir(), "axos")


def state_root() -> str:
    """Runtime state root. Defaults to <repo>/.axos-state (gitignored)."""
    env = os.environ.get("AXOS_STATE_ROOT")
    if env:
        return os.path.abspath(env)
    return os.path.join(repo_root(), ".axos-state")


def load_dotenv(path: str) -> None:
    """Load runtime/.env (KEY=VALUE lines) into os.environ when present.

    Never overrides variables that are already set. Not a substitute for
    a real secret store — runtime config only.
    """
    if not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                print(f"[initialize_runtime] WARN: runtime/.env line {lineno}: "
                      f"no '=' — skipped")
                continue
            key, value = line.split("=", 1)
            key, value = key.strip(), value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    print(f"[initialize_runtime] loaded runtime/.env")


def die(msg: str) -> int:
    print(f"[initialize_runtime] FAIL: {msg}")
    return 1


def main() -> int:
    root = repo_root()
    src = src_dir()
    if src not in sys.path:
        sys.path.insert(0, src)
    print(f"[initialize_runtime] repo root: {root}")

    asrc = axos_dir()
    if not os.path.isdir(asrc):
        return die(f"frozen axos/ source not found at {asrc} — "
                   "the checkout is incomplete")

    # --- create state/ subdirs and runtime/ under the state root ---
    sroot = state_root()
    state_dir = os.path.join(sroot, "state")
    staging_dir = os.path.join(state_dir, "axos-staging")
    runtime_dir = os.path.join(sroot, "runtime")
    for d in (state_dir, staging_dir, runtime_dir):
        os.makedirs(d, exist_ok=True)
    print("[initialize_runtime] PASS: state/ state/axos-staging/ runtime/ exist")

    # --- load runtime config ---
    load_dotenv(os.path.join(runtime_dir, ".env"))

    db_path = os.path.join(state_dir, "axos.db")

    # --- import AXOS's own initialization code ---
    try:
        from axos.store import open_store, migrate, applied_versions
        from axos.store import TransitionGate  # noqa: F401 (boundary import check)
    except ImportError as exc:
        return die(f"cannot import axos.store from the repo source: {exc}")

    # --- open the store and apply migrations (AXOS's own init path) ---
    try:
        store = open_store(db_path)
    except Exception as exc:
        return die(f"open_store({db_path}) failed: {exc}")
    try:
        try:
            applied_now = migrate(store)
        except Exception as exc:
            return die(f"migrate() failed: {exc}")
        versions = applied_versions(store)
        if not versions or versions != sorted(versions) or max(versions) != EXPECTED_MIGRATION_VERSION:
            return die(f"schema migration versions {versions} — expected 1..{EXPECTED_MIGRATION_VERSION}")
        print(f"[initialize_runtime] PASS: migrations applied {applied_now or '(none needed — idempotent)'}, "
              f"schema versions now {versions}")
    finally:
        store.close()

    # --- governance gate: never adopt an existing workload ---
    ro = None
    try:
        from axos.store import open_readonly_store
        ro = open_readonly_store(db_path)
        counts = {}
        for table in ("tasks", "jobs", "workers", "desired_state"):
            counts[table] = ro.execute(
                f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        head = ro.execute(
            "SELECT version, snapshot_hash FROM desired_state_head WHERE key='head'"
        ).fetchone()
    except Exception as exc:
        return die(f"could not read governance tables: {exc}")
    finally:
        if ro is not None:
            ro.close()

    non_empty = {t: c for t, c in counts.items() if c}
    if non_empty:
        return die(
            "state database already contains workload rows "
            f"{non_empty} — refusing to adopt a foreign workload. "
            "Delete the state database explicitly if a truly fresh install is intended."
        )
    if head is None or int(head[0]) != 0:
        return die(f"desired_state_head is not at the seeded v0 stop state: {head}")
    print("[initialize_runtime] PASS: governance tables empty, "
          f"desired_state_head at seeded v0 (snapshot {head[1][:12]}...)")

    # --- write SAFE DEFAULT governance state ---
    governance = {
        "DESIRED_STATE": "STOPPED",
        "workers": 0,
        "note": ("AXOS core has no global DESIRED_STATE/SUPERVISOR_STOP flag; "
                 "the implementation represents STOPPED as an empty "
                 "desired_state table, desired_state_head at seeded v0, and "
                 "zero registered workers/tasks/jobs. This bootstrap enforces "
                 "that state at init and refuses to adopt any foreign workload."),
        "schema_migration_version": EXPECTED_MIGRATION_VERSION,
        "db": "state/axos.db",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "initialized_by": "bootstrap/initialize_runtime.py",
    }
    gov_path = os.path.join(runtime_dir, "governance.json")
    with open(gov_path, "w", encoding="utf-8") as fh:
        json.dump(governance, fh, indent=2, sort_keys=True)
        fh.write("\n")
    print(f"[initialize_runtime] PASS: wrote runtime/governance.json "
          f"(DESIRED_STATE=STOPPED, workers=0)")

    print("[initialize_runtime] all checks passed — runtime initialized, "
          "governance STOPPED, no workers, no workload adopted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
