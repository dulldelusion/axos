# Testing

AXOS is tested by a `unittest`-based suite plus standalone adversarial
audit scripts, all shipped frozen under `src/axos/`. The suite standard,
stated in nearly every module docstring, is: **real SQLite/WAL files,
real subprocesses, real SIGTERM/SIGKILL, real monotonic timing — no
mocks for the critical guarantees.** `FakeClock` is used only for
deterministic timing assertions; failures are injected inside real
transactions to prove atomic rollback.

## Layout

```
src/axos/tests/
├── test_store.py              # Phase 1A — authoritative store + gate API (25)
├── test_remediation.py        # Phase 1A F1/F2/F3 remediation regressions (18)
├── test_exec.py               # Phase 1B — execution substrate (36)
├── test_reclaim.py            # R1  lease-reclaim primitive (23)
├── test_fence_enforce.py      # R2  enforced process-group self-fencing (18)
├── test_heartbeat_r3.py       # R3  durable heartbeat/progress (22)
├── test_expiry_r4.py          # R4  lease-expiry observation (19)
├── test_artifacts_r5.py       # R5  verified artifacts & integrity (37)
├── test_boot_r6.py            # R6  boot recovery (27)
├── test_watchdog_r7.py        # R7  watchdog verdicts (34)
├── test_recovery_r8.py        # R8  recovery controller (39)
├── test_recovery_r9.py        # R9  recovery ladder/budgets/escalation (55)
├── test_scheduler_r10.py      # R10 scheduler/admission (47)
├── test_reconciliation_r11.py # R11 desired-state reconciliation (56)
├── test_resilience_r12.py     # R12 circuit breakers (74)
├── test_finalization_r13.py   # R13 finalization/release checkpoint (82)
├── test_final_hardening_r14.py# R14 final hardening & release proof (123)
├── _crasher.py                # crash-injection helper (SIGKILLs itself mid-txn)
├── _supdrv.py                 # supervisor driver helper
└── r5_helpers.py              # r5_complete completion helper

src/axos/audit/
├── 01_write_path.py .. 08_retest.py  # adversarial write-path/transition/lease/
│                                      # ledger/crash/race audits
├── 09_authority_audit.py     # F1 authority-boundary audit (177/177 at freeze)
├── stray_process_audit.py    # independent stray-process/resource audit
├── _killpoints.py, _migkill.py       # kill-point injector helpers (not audits)
```

Total: **735 test methods** (`def test_` count). Test counts in
parentheses above are the frozen release's per-file counts.

## Running the suite

From the repository root (the documented commands):

```bash
# Full suite — the documented command. ~366 s per pass in evidence.
PYTHONPATH=<repo>/src python3 -m unittest discover -s src/axos/tests -v
```

or equivalently:

```bash
cd src/axos
python3 -m unittest discover -s tests -v
```

`PYTHONPATH=<repo>/src` is required so that `from axos.store ...` /
`from axos.exec ...` resolve to the frozen tree. (The test modules
themselves insert path entries for spawned subprocesses.)

```bash
# R14 final-hardening suite. Its own docstring prescribes pytest.
# pytest is NOT a declared dependency (the tree is stdlib-only);
# install it via your OS package manager if it is absent.
PYTHONPATH=<repo>/src python3 -m pytest src/axos/tests/test_final_hardening_r14.py
# ~218 s in evidence.
```

```bash
# Adversarial audits — standalone scripts with top-level code.
# Audit 09 exits non-zero on any failing check: any failure blocks the gate.
PYTHONPATH=<repo>/src python3 src/axos/audit/01_write_path.py
# ... 02_transitions.py ... 08_retest.py
PYTHONPATH=<repo>/src python3 src/axos/audit/09_authority_audit.py
PYTHONPATH=<repo>/src python3 src/axos/audit/stray_process_audit.py
```

The bootstrap self-test (`scripts/bootstrap/run_self_test.py`) runs a
17-check subset of the above automatically; see [bootstrap.md](bootstrap.md).

## What the suite proves (evidence baseline at freeze)

- **R1–R13 COMPLETE:** 735/735 test methods, three consecutive
  full-green runs (~366 s each). R5 37/37 after the v12 fix; R9 55/55
  after a test-expectation fix (a stale hardcoded migration list in the
  test, classified TEST DEFECT — production v12 was correct).
- **R14 internal campaign VERIFIED:** `test_final_hardening_r14.py`
  123/123, three consecutive runs; 12/12 fault injections (FI-01..FI-12)
  each run twice consecutively, PASS/PASS; audits 01–09 plus
  `stray_process_audit` all green; 0 stray processes.
- **R14 external validation BLOCKED:** the genuine hypervisor power-off
  proof (FI-04/FI-12) was never executed — no substitute simulation was
  run and no mocked evidence was created. See [verification.md](verification.md).

## Practical notes

- **Slow and timing-sensitive by design.** Tests spawn real worker and
  supervisor processes, send real signals, and assert on real monotonic
  timing. Do not run the suite on a heavily loaded or throttled machine
  and expect the same margins; timing assertions are the first thing to
  wobble under CPU contention.
- **Real side effects, contained.** Tests use their own temp databases
  and temp staging roots, and assert zero stray AXOS processes on
  teardown. They write worker debug logs to `/tmp/axos-<proc_id>.log`,
  so `/tmp` must be writable when running the execution-layer tests.
- **Do not remove difficult tests.** Timing-sensitive, slow, or
  adversarial tests are the evidence; replacing them with smoke tests
  invalidates the release's verification status.

## Known test-environment quirks (frozen — do not edit the tests)

1. **`tests/test_boot_r6.py` hardcodes a host path.** Line 87 inserts
   `os.path.expanduser("~/workspace")` into `sys.path` for a spawned
   subprocess driver (subtest R6-19b). On a machine where the checkout
   does not live under `~/workspace`, that single subtest's driver
   cannot resolve its imports. This is a pre-existing, frozen
   environment dependency of that one subtest — document the failure as
   environmental, do not edit the frozen test to work around it.
2. **R14 manifest tests rewrite `src/axos/release_manifest.json`.**
   `test_final_hardening_r14.py::write_release_manifest` regenerates the
   manifest on disk with a fresh `meta.generated_at` timestamp. The
   `release_id` is deterministic and must remain
   `551d559c...`; only the unhashed `meta` block changes. After any run
   that executes the R14 manifest tests, restore the file to keep the
   tree clean:
   ```bash
   git checkout -- src/axos/release_manifest.json
   ```
   (`__pycache__` directories created under `src/axos/` by test runs
   are excluded from the release manifest and can simply be deleted.)
3. **Functional fallbacks when pytest is absent.** Without pytest,
   `run_self_test.py` runs direct functional probes instead of the
   real suites. A few of those probes are stale relative to the frozen
   API (e.g. changed `update_job_progress` / `select_rung`
   signatures) and will fail — that is a limitation of the fallback
   path, not of AXOS. Install pytest and re-run for the real evidence.
