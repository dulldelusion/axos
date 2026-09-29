#!/usr/bin/env python3
"""Independent stray-process/resource audit for R14.

Checks that no AXOS worker/supervisor/synthetic processes survive outside
a test run. Run AFTER the test suite completes. Excludes this script
itself and the audit's own process tree.

A "stray" is any live process whose cmdline mentions axos worker roles
(supervisor, synthetic worker, _supdrv) that is not part of the current
audit invocation.
"""
import os
import sys

ME = os.getpid()
# Walk up to find our process tree root to exclude (in case run under pytest).
MY_PGID = os.getpgid(ME)

STRAYS = []
for pid in filter(str.isdigit, os.listdir("/proc")):
    ipid = int(pid)
    if ipid == ME:
        continue
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            raw = f.read()
        if not raw:
            continue
        cmd = raw.replace(b"\0", b" ").decode("utf-8", "replace")
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        continue
    # AXOS execution roles: supervisor driver, synthetic worker, worker proc.
    markers = ("_supdrv.py", "axos.exec.supervisor", "exec/synthetic",
               "synthetic.py")
    if any(m in cmd for m in markers):
        # Exclude anything in our own process group (the audit itself).
        try:
            if os.getpgid(ipid) == MY_PGID:
                continue
        except (ProcessLookupError, PermissionError):
            continue
        STRAYS.append((ipid, cmd.strip()[:160]))

if STRAYS:
    print(f"STRAY PROCESSES FOUND: {len(STRAYS)}")
    for pid, cmd in STRAYS:
        print(f"  pid={pid} cmd={cmd}")
    sys.exit(1)
print("STRAY-PROCESS AUDIT: zero stray AXOS processes")
