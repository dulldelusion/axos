# Troubleshooting

Real failure modes, in roughly the order a fresh operator meets them.
Every entry names the observed symptom, the cause, and the resolution.
Nothing here is speculative — each is grounded in the bootstrap scripts,
the frozen source, or recorded release evidence.

## Fresh clone / bootstrap

### `bash scripts/bootstrap/bootstrap.sh` aborts at stage 1 (environment)

Symptom: `[BOOTSTRAP] FAIL: validate_environment.py`, `BOOTSTRAP ABORTED`.

Causes and fixes:

- `python X.Y < 3.12` — the release was built and tested on Python
  3.12.3; the floor is 3.12. Install or select a suitable interpreter
  and re-run.
- `release_manifest.json unreadable` — the checkout is incomplete or
  `AXOS_REPO_ROOT` points at the wrong directory. The manifest must be
  at `<repo>/src/axos/release_manifest.json`.
- `bundled axos package not importable` — same root cause as above;
  `bootstrap.sh` sets `PYTHONPATH=<repo>/src` itself, so this on a
  default invocation means the repo root resolution is wrong.
- `sqlite X < manifest sqlite 3.45.1` — upgrade the Python build (the
  `sqlite3` module version follows the interpreter's SQLite linkage).
- `directory state/ is not writable` — the state root
  (`AXOS_STATE_ROOT`, default `<repo>/.axos-state`) is not writable.
  Fix permissions or point `AXOS_STATE_ROOT` at a writable directory.
- `WARN: pytest not installed` — not fatal; the self-test falls back
  to functional probes (see the pytest note below). Install pytest via
  your OS package manager for the real suites.

### `initialize_runtime.py` refuses: "state database already contains workload rows"

This is the governance gate working as designed — a fresh install can
never silently adopt an old workload. Either you are reconnecting an
existing deployment (in which case run `boot_recover()` and read the
report first — see [verification.md](verification.md)), or you want a
truly fresh install (delete the state database file explicitly, then
re-run).

### `validate_state.py` reports manifest hash mismatches

The source tree on disk differs from the frozen release. Do not edit
the manifest to match. Either restore the tree
(`git status` / `git checkout -- src/axos/`) or treat the checkout as
not-the-release and investigate. Note: `__pycache__` directories are
excluded from the manifest and never cause this; a mismatch names real
source files.

### `validate_state.py` reports a dirty governance state

Non-empty `tasks`/`jobs`/`workers`/`desired_state`, `desired_state_head`
not at v0, or `governance.json` not STOPPED — the state root is not a
fresh install. See the refusal entry above.

## Tests

### The full suite is slow

Expected. The documented command takes ~366 s per pass in release
evidence; tests spawn real processes and send real signals. Do not
parallelize with `pytest -n` — the tree has no such configuration and
concurrent suites contend on timing assertions and process tables.

### Timing-sensitive tests fail on a loaded machine

Real monotonic-timing assertions (e.g. fence→death latency bounds) are
the first thing to wobble under CPU contention. Re-run on an idle
machine before concluding the tree is broken. The evidence baseline
(three consecutive full-green runs) was measured on an idle VM.

### One subtest in `test_boot_r6.py` fails with an import error

Known frozen quirk: the driver for subtest R6-19b hardcodes
`os.path.expanduser("~/workspace")` as a subprocess import path. If the
checkout lives anywhere else, that single subtest's driver cannot
import. Environmental, not a product defect — do not edit the frozen
test.

### `git status` shows `src/axos/release_manifest.json` modified after a test run

Expected. The R14 manifest tests regenerate the manifest on disk with a
fresh `meta.generated_at`. The `release_id` must still be
`551d559c...`; only the unhashed `meta` block changes. Restore it:

```bash
git checkout -- src/axos/release_manifest.json
```

### Self-test check failures with pytest missing

Without pytest, `run_self_test.py` runs functional fallback probes. A
few of those probes are stale relative to the frozen API (changed
`update_job_progress` / `select_rung` signatures, worker-row
preconditions) and fail for that reason alone. Install pytest and
re-run for the real evidence.

## Runtime operation

### `Supervisor.close()` left worker processes running

By design. `close()` stops the sweep and closes store handles but
**deliberately does not terminate worker OS processes** — shutting
down the supervisor is not shutting down the work. Stop tracked workers
first:

```python
sup.stop_worker(worker_id, timeout=5.0)  # SIGTERM, escalate to SIGKILL, reap; idempotent
```

Remaining live workers lose authority at lease expiry (R4 → R1) if
their supervisor is gone.

### A task is stuck in `PAUSED_FOR_HUMAN`

The pause is sticky across restarts by design (I-17). Resuming
requires a recorded human approval — automation must never manufacture
it:

```python
gate.create_approval(None, task_id, reason="...", requested_action="resume", actor="operator")
gate.decide_approval(approval_id, "APPROVED", decided_by="operator", actor="operator")
gate.transition_task(task_id, "EXECUTING", actor="operator", approval_ref=approval_id)
```

### `LeaseError` / claim returns `False`

Normal operation, not a bug. A worker has authority only while it is
the current owner with the current fencing token and an unexpired
lease. On `LeaseError` the worker must stop immediately — only a fresh
claim restores authority. A lost claim race exits the worker; nothing
executes.

### UNCERTAIN jobs after a crash

Resolve via the R5 resolver path (`commit_artifact` from `UNCERTAIN`
by a reclaim-authority actor, with artifact token lineage predating
the reclaim — adopt if staged bytes verify, requeue if not) before
declaring the system healthy. Do not delete the rows.

### Breakers stuck OPEN / repeated DEAD verdicts

Investigate the workload or task, not AXOS. Breakers deny admission
fail-closed by design; repeated `DEAD` verdicts mean the work itself is
failing. Open recovery incidents with terminal policy states are
decided, not broken — leave them.

### `python -m axos.exec.worker` fails with `ModuleNotFoundError: axos`

`PYTHONPATH` must include `<repo>/src`:

```bash
PYTHONPATH=<repo>/src python3 -m axos.exec.worker --db <db> \
  --worker-id w-1 --proc-id p-1 --job-id <job-id> --ttl-s 60 \
  --behavior '{"kind":"success_immediate"}'
```

## Environment mistakes

- **Writing to the DB with raw SQL.** Never. All authoritative writes
  go through `TransitionGate`; the gate serializes via
  `BEGIN IMMEDIATE` and raw concurrent writers do not participate in
  fencing. Use `open_readonly_store()` for inspection.
- **Handing a writable `Store` to a worker or scheduler.** The writable
  capability goes to `TransitionGate` (or transient migration/setup
  code) only; non-gate components get `open_readonly_store()`.
- **`/tmp` not writable.** Worker debug logs go to
  `/tmp/axos-<proc_id>.log`; execution-layer tests need it too.
- **Database on NFS or another network filesystem.** WAL mode requires
  working `-wal`/`-shm` sidecars; behavior on non-local filesystems is
  unverified. Use a local POSIX filesystem.
- **Expecting controllers to daemonize.** `start()` runs a background
  *thread* in the current process, not a daemon. Surviving
  session exit (systemd/nohup, log rotation) is the operator's
  responsibility and is unverified territory.
- **Treating exit codes, heartbeats, or process existence as completion.**
  Exit codes are process evidence only — never job success. Only
  `stage_artifact → begin_commit → verify_artifact → commit_artifact`
  completes a job.
