# Verification

Verification answers one question: **is this checkout the frozen AXOS
release, and is it behaving as the evidence says?** Every check below
reads durable state and runs real code — a bounded mission never
self-certifies from its own framing.

## 1. Release identity

`src/axos/release_manifest.json` carries the release identity:

- `release_id`: `sha256` of the canonical identity document —
  `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`.
- `identity.file_hashes`: per-file sha256 of every regular file under
  `exec/`, `store/`, `audit/`, `tests/` (relative path → hash),
  excluding `__pycache__`; `release_manifest.json` and `R14-PLAN.md`
  are excluded. 50 file hashes on the frozen tree.
- `identity.migration_version`: 12; `policy_version`: `r9-policy/v1`;
  `python_version`: 3.12.3; `sqlite_version`: 3.45.1; OS identity
  (Linux x86_64).
- `meta`: unhashed block (`generated_at`, generator, hashing contract).
  Regenerating the manifest changes `meta.generated_at` but never the
  `release_id`.

Verify (no test run needed):

```bash
# Stage 3 of bootstrap performs this check (step "release identity").
python3 scripts/bootstrap/validate_state.py
```

Or manually: recompute sha256 of each listed file and compare to
`identity.file_hashes`, then compare
`sha256(canonical JSON of identity)` to `release_id`.

**Gate rule:** if any hash mismatches on a fresh checkout, the checkout
is not the frozen release — stop and investigate. Do not "fix" the
manifest to match modified source.

## 2. Ledger verification

```python
from axos.store import open_store, TransitionGate

gate = TransitionGate(open_store("<state>/axos.db"))
ok, detail = gate.verify_ledger_chain()
assert ok, detail
```

Detects naive tamper, missing/reordered/duplicate events, and payload
mutation. **Known limitation:** the ledger is tamper-*evident*, not
tamper-proof — a writer with raw DB access can rewrite the whole chain.
Protect the state database at the filesystem level. Run this after any
crash before resuming work.

## 3. Database integrity and backup

```python
store = open_store("<state>/axos.db")
ok, detail = store.integrity_check()        # PRAGMA integrity_check + foreign_key_check
store.backup_to("/path/to/axos-backup.db") # consistent backup via the SQLite backup API
```

The backup API is crash-consistent under write load (audit 06, 29/29).
Backup scheduling and rotation are operator responsibilities — there is
no automated backup runner in the tree.

## 4. Authority boundary (F1)

```bash
PYTHONPATH=<repo>/src python3 src/axos/audit/09_authority_audit.py
```

The authority-boundary audit (177/177 PASS at freeze): no `write_txn()`
in `exec/`, no private store connections from `exec/`, no SQL writes in
`exec/`, `worker_reported_ts` never used for authority decisions, every
authoritative mutation through `TransitionGate.write_txn` with fencing
rejections inside the transaction. It exits non-zero on any failing
check — **any failure blocks the gate**.

## 5. Boot and ledger (manual verification snippet)

```python
from axos.exec.supervisor import Supervisor
from axos.exec.boot import boot_recover

sup = Supervisor("<state>/axos.db")
report = boot_recover(sup)                 # idempotent post-restart recovery pass
assert report["phase"] == "READY"          # STARTING -> RECOVERING -> READY | BLOCKED
ok, detail = sup.gate.verify_ledger_chain()
assert ok, detail
```

`READY` means the recovery pass completed — not that every
contradiction is resolved; read `report["fully_recovered"]`,
`report["unresolved"]`, and `report["dispositions"]` (`ADOPT`, `FENCE`,
`ALREADY_DEAD`, `PID_REUSE`, `ORPHAN`, `UNCERTAIN`). No new worker may
start until `phase == "READY"` — the scheduler, finalizer, resilience
controller, and policy controller all consult this gate.

## 6. Health checks

```python
ok, detail = gate.verify_ledger_chain();            assert ok, detail
report = boot_recover(sup);                          assert report["phase"] == "READY"
stuck = gate.jobs_in_states(["UNCERTAIN"])           # expect [] in steady state
open_inc = gate.open_recovery_incidents()            # review, do not auto-close
brk = [b for b in gate.list_breaker_states() if b["state"] == "OPEN"]  # expect [] normally
```

Operational judgment (not gate rules):

- `UNCERTAIN` jobs present → resolve via the R5 resolver path before
  declaring healthy.
- Open recovery incidents with terminal policy states
  (`RECOVERY_COMPLETE`, `BUDGET_EXHAUSTED`, `BLOCKED_*`, `SUPERSEDED`)
  are *decided*, not *broken* — leave them.
- Repeated `DEAD` verdicts or `OPEN` breakers → investigate the
  workload or task; breakers deny admission fail-closed by design.

## 7. Honest status of R1–R14

From actual evidence only (see `reports/historical/` for the preserved
records):

- **R1–R13: COMPLETE.** 735/735 test methods, three consecutive
  full-green runs.
- **R14 internal campaign: VERIFIED.** 123/123 ×3; 12/12 fault
  injections ×2 (FI-01..FI-12); audits 01–09 and the stray-process audit
  green; 0 stray processes. FI-04 (full VM restart) and the FI-12 pause
  variant were executed as **honest equivalents** (real kill -9 of all
  AXOS processes → boot-from-disk recovery; 8/8 jobs COMPLETE) —
  recorded as such in the evidence, not relabeled as exact.
- **R14 external validation: BLOCKED — REAL-VM PROOF INCOMPLETE.**
  Genuine hypervisor/OS hard power-off (page-cache loss, wall-clock
  jumps) was never executed: the build VM had no hypervisor tooling, no
  metadata service, no external observer host. No substitute simulation
  was executed and no mocked evidence was created. The frozen validation
  spec (`R14-EXTERNAL-VALIDATION.md`/`.json` in
  `reports/historical/`) prescribes the procedure, observer contract,
  and acceptance rules for a future run; promotion to R14_COMPLETE
  requires FI-04 ×2 and the FI-12 VM-restart variant ×2 under those
  rules with zero manual healing.

Do not claim the system is proven against hard power loss. Report the
blocked proof as blocked.
