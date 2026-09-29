# Reproducibility

How to reproduce the frozen AXOS release — its identity, its schema,
its evidence — from this checkout alone. The release is
`551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
(migration v12, policy `r9-policy/v1`).

## 1. Release identity is computed, not built

There is no build step: the tree is pure Python, stdlib-only, run in
place. The release identity is computed as:

```
release_id = sha256(canonical JSON of identity)
```

where `identity` contains:

- `file_hashes`: sorted per-file sha256 of every regular file under
  `src/axos/exec/`, `src/axos/store/`, `src/axos/audit/`,
  `src/axos/tests/` (relative path → sha256), excluding `__pycache__`;
  `release_manifest.json` and `R14-PLAN.md` are excluded. 50 file
  hashes on the frozen tree.
- `migration_version`: 12 (canonical DDL over `MIGRATIONS`).
- `policy_version`: `r9-policy/v1` (canonical recovery-ladder hash).
- Per-suite content ids, and the python / sqlite / OS identity.

The generator is `test_final_hardening_r14.py`
(`TestR14ReleaseManifest` / `write_release_manifest`). Building twice
yields the same `release_id` — it is deterministic by design.

The manifest's `meta` block (`generated_at`, generator, hashing
contract) is **unhashed**: regenerating the manifest updates
`meta.generated_at` but never changes `release_id`.

### Verifying identity without running tests

```bash
python3 scripts/bootstrap/validate_state.py
```

Step 3 of bootstrap ("release identity") recomputes all 50 file hashes
and compares them to the manifest. Any mismatch means the checkout is
not the frozen release.

### The manifest rewrite caveat

Running the R14 manifest tests rewrites
`src/axos/release_manifest.json` on disk with a fresh
`meta.generated_at`. This is expected and does not change the release
identity — but it dirties the working tree. Restore it after test runs:

```bash
git checkout -- src/axos/release_manifest.json
```

## 2. Schema reproducibility

The schema is built by `migrate(store)` through the versioned
`MIGRATIONS` list in `src/axos/store/migrations.py`, v1 (Phase 1A
initial schema) through v12:

```python
from axos.store import open_store, migrate, applied_versions

store = open_store("/tmp/repro.db")
migrate(store)                    # applies v1..v12; re-running is a no-op
assert applied_versions(store) == list(range(1, 13))
```

Properties verified by the test suite: re-running migrations is a
no-op; a failed migration rolls back fully and records nothing
(`MigrationError`); the store clock is DB-backed and monotonic
(`axos_meta.last_commit_ts` floor).

## 3. Reproducing the evidence

| Evidence | Command | Baseline at freeze |
|---|---|---|
| Full suite (R1–R13) | `PYTHONPATH=<repo>/src python3 -m unittest discover -s src/axos/tests` | 735/735 test methods, three consecutive full-green runs, ~366 s per pass |
| R14 internal campaign | `PYTHONPATH=<repo>/src python3 -m pytest src/axos/tests/test_final_hardening_r14.py` | 123/123 ×3; 12/12 fault injections (FI-01..FI-12) ×2, PASS/PASS |
| Adversarial audits | `PYTHONPATH=<repo>/src python3 src/axos/audit/0N_*.py` (01–09) and `stray_process_audit.py` | All green; audit 09: 177/177; 0 stray processes |

Suite discipline: real SQLite/WAL files, real subprocesses, real
SIGTERM/SIGKILL, real monotonic timing — no mocks for the critical
guarantees. `FakeClock` only for deterministic timing assertions;
injected in-transaction failures only to prove atomic rollback.

Practical notes for reproduction:

- Run on an idle Linux x86_64 machine with Python 3.12.3 and SQLite
  3.45.1 (see [environment.md](environment.md)). Timing assertions
  are the first casualty of CPU contention.
- Tests use their own temp databases and assert zero stray AXOS
  processes on teardown; `/tmp` must be writable.
- One frozen quirk: the driver for subtest R6-19b in
  `tests/test_boot_r6.py` hardcodes `os.path.expanduser("~/workspace")`
  as a subprocess import path. On a machine where the checkout lives
  elsewhere, that single subtest's driver cannot resolve imports —
  environmental, not a product defect. Do not edit the frozen test.

## 4. What cannot be reproduced from this checkout

- **R14 external validation (BLOCKED).** The genuine hypervisor power-off
  proof (FI-04 ×2, FI-12 VM-restart variant ×2) was never executed: the
  build VM had no hypervisor tooling, no metadata service, and no
  external observer host. Reproducing it requires a hypervisor with
  hard power-off control and an external observer, following the frozen
  spec in `reports/historical/` (`R14-EXTERNAL-VALIDATION.md`/`.json`).
  No substitute simulation was run and no mocked evidence was created —
  do not manufacture it.
- **Exact wall-clock timings.** Latency bounds (e.g. fence→death
  < heartbeat interval) are asserted as bounds against real monotonic
  clocks; re-running reproduces the *property*, not the exact
  millisecond values.
- **The `meta.generated_at` timestamp.** Unhashed by design; every
  regeneration stamps the current time.
- **A perfectly green full-suite run under heavy system load.**
  `test_R4_14_expiry_to_reclaim_to_r2_fencing`
  (`src/axos/tests/test_expiry_r4.py`) drives a real worker process
  with a 0.1 s heartbeat interval and asserts an exact heartbeat count
  across a 1.0 s window after the fence sweep. Under sustained system
  load the sweep's process termination can be delayed just enough for
  one in-flight heartbeat to land, failing the exact-count assertion
  (observed: 12 vs 11). The test passes reliably in isolation and at
  file level (5/5 and 19/19 in verification runs); the gate's
  stale-token rejection is enforced by design (`gate.ingest_heartbeat`
  raises `LeaseError` on fenced tokens). This is a timing-sensitive
  test, not a product defect. The frozen test is not edited; re-run the
  single test in isolation to confirm.
- **A perfectly green full-suite run under extreme concurrent load.**
  Four timing/concurrency-sensitive tests have been observed to flake
  only when multiple full-suite runs execute concurrently on the same
  machine:
  - `test_R4_14_expiry_to_reclaim_to_r2_fencing`
    (`src/axos/tests/test_expiry_r4.py`): real worker process, 0.1 s
    heartbeat interval, exact-count assertion across a 1.0 s window.
    Observed failure: 12 vs 11 heartbeats.
  - `test_R2_07_owner_divergence_detected`
    (`src/axos/tests/test_fence_enforce.py`): real processes through
    supervisor restart and fencing; timing windows missed under load.
  - `test_R3_11_stale_heartbeat_never_wins_against_reclaim`
    (`src/axos/tests/test_heartbeat_r3.py`): failed in-suite with
    `sqlite3.OperationalError: database is locked` under concurrent
    load.
  - `test_RACE_1_reconcile_racing_head_bump`
    (`src/axos/tests/test_reconciliation_r11.py`): a deliberate race
    test; inherently timing-marginal under CPU contention.

  All four pass reliably in isolation and at file level (5/5, 19/19,
  18/18, 56/56 x3, and isolation re-verified for R3-11 and RACE-1). The gate's stale-token
  rejection is enforced by design (`ingest_heartbeat` raises
  `LeaseError` on fenced tokens). These are timing-sensitive tests,
  not product defects. The frozen tests are not edited; re-run the
  single test in isolation to confirm, or re-run the suite without
  concurrent load.

## 5. Determinism inventory

| Deterministic | Non-deterministic (by design) |
|---|---|
| `release_id` across regenerations | `meta.generated_at` |
| Canonical JSON payloads (`sort_keys=True`, compact separators) | Wall-clock latency measurements |
| Migration application order and resulting schema | Process IDs, temp paths |
| Desired-state snapshot identity (same spec → same head hash) | Worker debug log contents in `/tmp` |
| Checkpoint/finalization canonical ids and manifest hashes | |
| Ledger hash chain over a fixed event sequence | |
