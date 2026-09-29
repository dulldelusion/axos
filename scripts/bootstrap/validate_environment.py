#!/usr/bin/env python3
"""AXOS repository — environment validation.

Checks that this machine can host the AXOS runtime before anything is
initialized. Exits 0 on success; exits non-zero with a clear message on
the first failed check.

Checks:
  1. python >= 3.12 (floor from the release manifest's python_version)
  2. release manifest present and readable at src/axos/release_manifest.json
  3. required modules importable (sqlite3, hashlib, json, subprocess,
     threading, tempfile, ...) plus the repo's axos package itself
     (resolved via <repo>/src on sys.path)
  4. sqlite3 runtime version >= the manifest's sqlite_version
  5. filesystem writable: can create state/ and runtime/ dirs under the
     state root (default <repo>/.axos-state) and write a probe file
  6. pytest availability (WARNING only — run_self_test.py falls back to
     functional checks if pytest is absent)
"""
from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys

failures: list[str] = []
warnings: list[str] = []


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


def fail(msg: str) -> None:
    print(f"[validate_environment] FAIL: {msg}")
    failures.append(msg)


def ok(msg: str) -> None:
    print(f"[validate_environment] PASS: {msg}")


def main() -> int:
    root = repo_root()
    print(f"[validate_environment] repo root: {root}")

    # --- 1. python version (>= 3.12 per release manifest floor) ---
    if sys.version_info < (3, 12):
        fail(f"python {sys.version.split()[0]} < 3.12 (AXOS release requires >= 3.12)")
    else:
        ok(f"python {sys.version.split()[0]} >= 3.12")

    # --- 2. release manifest present ---
    manifest_path = os.path.join(axos_dir(), "release_manifest.json")
    manifest = None
    try:
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)
    except OSError as exc:
        fail(f"release_manifest.json unreadable at axos/release_manifest.json: {exc}")
    except json.JSONDecodeError as exc:
        fail(f"release_manifest.json is not valid JSON: {exc}")
    if manifest is not None:
        rid = manifest.get("release_id")
        if not rid:
            fail("release_manifest.json has no release_id")
        else:
            ok(f"release manifest readable (release {rid[:12]}...)")
    ident = (manifest or {}).get("identity", {})

    # --- 3. required modules importable ---
    src = src_dir()
    if src not in sys.path:
        sys.path.insert(0, src)
    required = [
        "sqlite3", "hashlib", "json", "subprocess", "threading",
        "tempfile", "uuid", "datetime", "platform",
    ]
    for mod in required:
        try:
            importlib.import_module(mod)
        except ImportError:
            fail(f"required module not importable: {mod}")
    try:
        import axos.store  # noqa: F401
        import axos.exec  # noqa: F401
        ok("bundled axos.store and axos.exec importable")
    except ImportError as exc:
        fail(f"bundled axos package not importable: {exc}")

    # --- 4. sqlite3 version >= manifest's sqlite_version ---
    expected_sqlite = str(ident.get("sqlite_version", "") or "")
    if expected_sqlite:
        def tup(v: str) -> tuple:
            return tuple(int(p) for p in v.split(".") if p.isdigit())
        if tup(sqlite3.sqlite_version) < tup(expected_sqlite):
            fail(f"sqlite {sqlite3.sqlite_version} < manifest sqlite {expected_sqlite}")
        else:
            ok(f"sqlite {sqlite3.sqlite_version} >= manifest {expected_sqlite}")
    else:
        warnings.append("manifest has no identity.sqlite_version; runtime sqlite is "
                        f"{sqlite3.sqlite_version}")
        ok(f"sqlite available ({sqlite3.sqlite_version})")

    # --- 5. filesystem writable: create state/ and runtime/ under the
    # state root (default <repo>/.axos-state) and write a probe ---
    sroot = state_root()
    for d in ("state", "runtime"):
        path = os.path.join(sroot, d)
        try:
            os.makedirs(path, exist_ok=True)
        except OSError as exc:
            fail(f"cannot create directory {d}/ under bundle root: {exc}")
            continue
        probe = os.path.join(path, ".write_probe")
        try:
            with open(probe, "w", encoding="utf-8") as fh:
                fh.write("probe")
            os.remove(probe)
        except OSError as exc:
            fail(f"directory {d}/ is not writable: {exc}")
            continue
        ok(f"directory {d}/ creatable and writable")

    # --- 6. pytest (warning only; self-test has functional fallbacks) ---
    try:
        import pytest  # noqa: F401
        ok("pytest available (used by run_self_test.py)")
    except ImportError:
        warnings.append("pytest not installed — run_self_test.py will use "
                        "functional fallback checks instead of the pytest suites")

    if warnings:
        for w in warnings:
            print(f"[validate_environment] WARN: {w}")
    if failures:
        print(f"[validate_environment] {len(failures)} check(s) FAILED")
        return 1
    print("[validate_environment] all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
