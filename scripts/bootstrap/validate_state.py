#!/usr/bin/env python3
"""AXOS repository — state validation.

Validates the initialized repo state without mutating it:
  1. <state-root>/state/axos.db exists and opens (read-only handle)
  2. schema_migrations == 1..12 and max matches the release manifest's
     migration_version
  3. release identity: every file hash recorded in
     src/axos/release_manifest.json identity.file_hashes matches the
     source file on disk
  4. DB integrity: AXOS's own Store.integrity_check()
     (PRAGMA integrity_check + foreign_key_check)
  5. governance flags: desired_state empty, desired_state_head at seeded
     v0, workers/tasks/jobs all empty, <state-root>/runtime/governance.json
     records DESIRED_STATE=STOPPED and workers=0
  6. no worker registrations active (workers table empty)

Exits 0 when everything validates; exits non-zero with a clear message
otherwise.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys

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


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    root = repo_root()
    src = src_dir()
    if src not in sys.path:
        sys.path.insert(0, src)
    sroot = state_root()
    fails: list[str] = []

    def fail(msg: str) -> None:
        print(f"[validate_state] FAIL: {msg}")
        fails.append(msg)

    def ok(msg: str) -> None:
        print(f"[validate_state] PASS: {msg}")

    print(f"[validate_state] repo root: {root}")
    print(f"[validate_state] state root: {sroot}")

    # --- 1. DB file exists and opens ---
    db_path = os.path.join(sroot, "state", "axos.db")
    if not os.path.isfile(db_path):
        fail(f"database missing: {db_path} (run initialize_runtime.py first)")
        print(f"[validate_state] {len(fails)} check(s) FAILED")
        return 1
    ok("state database exists")

    try:
        from axos.store import open_store, open_readonly_store, applied_versions
    except ImportError as exc:
        fail(f"cannot import axos.store from bundled source: {exc}")
        print(f"[validate_state] {len(fails)} check(s) FAILED")
        return 1

    # --- 2. schema/migration version ---
    try:
        probe = open_store(db_path)
        versions = applied_versions(probe)
        probe.close()
    except Exception as exc:
        fail(f"could not read schema_migrations: {exc}")
        versions = []
    if versions and versions == list(range(1, EXPECTED_MIGRATION_VERSION + 1)):
        ok(f"schema migration versions 1..{EXPECTED_MIGRATION_VERSION}")
    elif versions:
        fail(f"unexpected migration versions {versions}")

    # --- 3. release identity vs manifest ---
    manifest_path = os.path.join(axos_dir(), "release_manifest.json")
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        manifest = None
        fail(f"release_manifest.json unreadable: {exc}")
    if manifest is not None:
        ident = manifest.get("identity", {})
        if ident.get("migration_version") == EXPECTED_MIGRATION_VERSION:
            ok("manifest migration_version == 12 matches DB")
        else:
            fail(f"manifest migration_version {ident.get('migration_version')} != 12")
        hashes = ident.get("file_hashes", {})
        asrc = axos_dir()
        mismatched = []
        missing = []
        checked = 0
        for rel, expected in hashes.items():
            full = os.path.join(asrc, rel)
            if not os.path.isfile(full):
                missing.append(rel)
                continue
            if sha256_file(full) != expected:
                mismatched.append(rel)
            checked += 1
        if missing:
            fail(f"{len(missing)} manifest files missing from checkout: {missing[:5]}")
        if mismatched:
            fail(f"{len(mismatched)} manifest file hashes mismatch: {mismatched[:5]}")
        if not missing and not mismatched:
            ok(f"release identity: {checked} manifest file hashes match "
               f"(release {manifest.get('release_id', '')[:12]}...)")

    # --- 4. DB integrity (AXOS's own check) ---
    try:
        st = open_store(db_path)
        ok_integrity, detail = st.integrity_check()
        st.close()
    except Exception as exc:
        ok_integrity, detail = False, str(exc)
    if ok_integrity:
        ok(f"DB integrity_check: {detail}")
    else:
        fail(f"DB integrity_check failed: {detail}")

    # --- 5/6. governance flags: STOPPED, no workers, no workload ---
    try:
        ro = open_readonly_store(db_path)
        counts = {}
        for table in ("tasks", "jobs", "workers", "desired_state"):
            counts[table] = ro.execute(
                f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        head = ro.execute(
            "SELECT version, snapshot_hash FROM desired_state_head WHERE key='head'"
        ).fetchone()
        ro.close()
    except Exception as exc:
        fail(f"could not read governance tables: {exc}")
        counts, head = {}, None

    if counts:
        for table in ("tasks", "jobs", "workers", "desired_state"):
            if counts.get(table) == 0:
                ok(f"{table}: 0 rows")
            else:
                fail(f"{table}: {counts[table]} row(s) present — governance is not STOPPED")
        if counts.get("workers") == 0:
            ok("no worker registrations active")
    if head is not None:
        if int(head[0]) == 0:
            ok(f"desired_state_head at seeded v0 (empty snapshot)")
        else:
            fail(f"desired_state_head at version {head[0]}, expected seeded v0")

    gov_path = os.path.join(sroot, "runtime", "governance.json")
    try:
        with open(gov_path, "r", encoding="utf-8") as fh:
            gov = json.load(fh)
        if gov.get("DESIRED_STATE") == "STOPPED" and gov.get("workers") == 0:
            ok("governance.json: DESIRED_STATE=STOPPED, workers=0")
        else:
            fail(f"governance.json has unexpected governance: {gov}")
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"governance.json unreadable: {exc}")

    if fails:
        print(f"[validate_state] {len(fails)} check(s) FAILED")
        return 1
    print("[validate_state] all checks passed — state is fresh, "
          "schema v12, release identity verified, governance STOPPED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
