# AXOS Dependency Graph — As-Is

Source: `~/workspace/axos/`, inspected 2026-09-28. Edges extracted by parsing
actual `import`/`from … import` statements with Python's `ast` module over the
real files (no guessing). Relative imports (`from . import X`, `from ..store
import Y`) resolved to their real targets.

## 1. Module → module edges (axos-internal)

Notation: `A → B` means A's source contains an import statement naming B.
Stdlib-only modules omitted as sources of internal edges (they have none).

### store layer

| From | To | Evidence |
|---|---|---|
| `store/__init__.py` | `store/db.py`, `store/migrations.py`, `store/gate.py` | `from .db import …`, `from .migrations import …`, `from .gate import …` |
| `store/gate.py` | `store/db.py` | `from .db import Store, TransitionRejected, LeaseError, PolicyConflict, DesiredStateConflict, BreakerConflict, FinalizationConflict` |
| `store/gate.py` | `store/transitions.py` | `from . import transitions as T` |
| `store/migrations.py` | `store/db.py` | `from .db import Store, MigrationError` |
| `store/db.py` | — | stdlib only (sqlite3, time, contextlib, typing) |
| `store/transitions.py` | — | no imports at all |

### exec layer

| From | To | Evidence |
|---|---|---|
| `exec/boot.py` | `store` | `from ..store import TransitionRejected, LeaseError, StoreError` |
| `exec/supervisor.py` | `store` | `from ..store import open_store, migrate, TransitionGate, TransitionRejected, StoreError` |
| `exec/supervisor.py` | `store/transitions.py` | `from ..store import transitions as T` |
| `exec/supervisor.py` | `exec/boot.py` | `from .boot import boot_recover, proc_start_jiffies` |
| `exec/supervisor.py` | `exec/identity.py` | `from .identity import new_proc_id, WorkerIdentity, ProcessInstance` |
| `exec/worker.py` | `store` | `from axos.store import open_store, TransitionGate, LeaseError, TransitionRejected, StoreError` |
| `exec/worker.py` | `exec/identity.py` | `from axos.exec.identity import LeaseRef, ProcessInstance` |
| `exec/worker.py` | `exec/synthetic.py` | `from axos.exec.synthetic import SyntheticExecutor, BehaviorSpec, FencedError, InterruptedExecution` |
| `exec/synthetic.py` | `store` | `from ..store import TransitionGate, LeaseError` |
| `exec/synthetic.py` | `exec/identity.py` | `from .identity import LeaseRef` |
| `exec/identity.py` | — | stdlib only (uuid, dataclasses) |
| `exec/scheduler.py` | `store` | `from axos.store import open_store, migrate, TransitionGate` |
| `exec/scheduler.py` | `store/db.py` | `from axos.store.db import Store, StoreError, TransitionRejected, LeaseError` |
| `exec/watchdog.py` | `store` | `from ..store import StoreError, TransitionGate, TransitionRejected, open_store, migrate` |
| `exec/watchdog.py` | `exec/boot.py` | `from . import boot as boot_mod` |
| `exec/recovery.py` | `store` | `from ..store import StoreError, TransitionGate, TransitionRejected, LeaseError, open_store, migrate` |
| `exec/recovery.py` | `exec/boot.py` | `from . import boot as boot_mod` |
| `exec/policy.py` | `store` | `from ..store import PolicyConflict, StoreError, TransitionRejected` |
| `exec/policy.py` | `store/transitions.py` | `from ..store import transitions as T` |
| `exec/policy.py` | `exec/boot.py` | `from . import boot as boot_mod` |
| `exec/policy.py` | `exec/recovery.py` | `from .recovery import RecoveryConfig, RecoveryController, RecoveryError, RungContext, RungProvider` |
| `exec/reconciler.py` | `store` | `from axos.store import open_store, migrate, TransitionGate` |
| `exec/reconciler.py` | `store/db.py` | `from axos.store.db import Store, StoreError, TransitionRejected, DesiredStateConflict, StorageEngineError` |
| `exec/reconciler.py` | `store/gate.py` (private helpers) | `from axos.store.gate import _canonical_desired_job_id, _desired_snapshot_hash, _desired_spec_hash, _normalize_desired_spec` |
| `exec/resilience.py` | `store` | `from axos.store import open_store, migrate, TransitionGate, BreakerConflict` |
| `exec/resilience.py` | `store/transitions.py` | `from axos.store import transitions as T` |
| `exec/resilience.py` | `store/db.py` | `from axos.store.db import Store, StoreError, TransitionRejected` |
| `exec/finalizer.py` | `store` | `from axos.store import open_store, migrate, TransitionGate, FinalizationConflict, canonical_release_generation, canonical_finalization_id, build_finalization_manifest, finalization_manifest_hash` |
| `exec/finalizer.py` | `store/db.py` | `from axos.store.db import Store, StoreError, TransitionRejected` |

Notes:
- Two import styles coexist: relative (`from ..store import …` in boot.py,
  supervisor.py, synthetic.py, watchdog.py, recovery.py, policy.py) and
  absolute (`from axos.store import …` in finalizer.py, reconciler.py,
  resilience.py, scheduler.py, worker.py). Both resolve to the same modules.
- The only sanctioned private cross-module import is `exec/reconciler.py →
  store/gate.py` private canonical-identity helpers (used by
  `ensure_job_for_desired_state`; audited).
- OS-process edge (not a Python import): `exec/supervisor.py` spawns
  `python -m axos.exec.worker` as real subprocesses.

### audit/ and tests/ layers

All `audit/*.py` and `tests/*.py` import `axos.store` (gate, open_store,
migrate, error types) and, where needed, `axos.exec.supervisor.Supervisor`,
the controllers (`Watchdog`, `Reconciler`, `Scheduler`,
`ResilienceController`, `RecoveryController`, `PolicyController`,
`Finalizer`), `boot_recover`, `MIGRATIONS`/`applied_versions`, and
`axos.store.transitions`. They are downstream consumers only — no runtime
module imports anything from `audit/` or `tests/`.

`audit/09_authority_audit.py` additionally imports `CANONICAL_LADDER`
(`exec/policy.py`), `_CANONICAL_RUNGS` (`exec/recovery.py`),
`SchedulerConfig` (`exec/scheduler.py`), finalization helpers
(`store/gate.py`), and `FinalizationConfig` (`exec/finalizer.py`).

## 2. Architectural layers (as observed)

```
┌──────────────────────────────────────────────────────────┐
│ Layer 3 — audit / tests  (evidence harnesses; import-only │
│           downstream; nothing in the runtime imports them)│
└─────────────────────────┬────────────────────────────────┘
                          │ imports
┌─────────────────────────▼────────────────────────────────┐
│ Layer 2 — exec  (process lifecycle + control plane)       │
│   exec/supervisor.py  (spawns worker procs; R2 fencing)   │
│   exec/worker.py  (claim → renew → stage/verify → commit) │
│   exec/boot.py  (R6 boot recovery; reconstructs runtime)  │
│   exec/scheduler.py  (R10 admission + atomic claim)       │
│   exec/watchdog.py  (R7 STALLED/DEAD detection only)      │
│   exec/recovery.py  (R8: smallest action, verify, escalate│
│   exec/policy.py  (R9: rung choice, budgets, escalation)  │
│   exec/reconciler.py  (R11: desired-state convergence)    │
│   exec/resilience.py  (R12: circuit breakers)             │
│   exec/finalizer.py  (R13: release finalization)          │
│   exec/identity.py, exec/synthetic.py  (value types,      │
│        deterministic fault-injection executor)            │
└─────────────────────────┬────────────────────────────────┘
                          │ all writes through TransitionGate;
                          │ exec/ never calls write_txn(), never
                          │ opens sqlite3 connections, never
                          │ issues SQL writes (audit 09 enforces)
┌─────────────────────────▼────────────────────────────────┐
│ Layer 1 — store/gate.py  (TransitionGate: the single      │
│           enforcement boundary for all authoritative      │
│           state transitions; sole holder of the writable  │
│           Store; hash-chained ledger)                     │
└─────────────────────────┬────────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────────┐
│ Layer 0 — store/db.py  (SQLite/WAL handle, monotonic     │
│           store clock, atomic write_txn),                 │
│           store/migrations.py  (v1–v12 schema),          │
│           store/transitions.py  (explicit graphs + actor  │
│           sets; import-free)                             │
└──────────────────────────────────────────────────────────┘
```

Direction of allowed dependency: Layer 3 → Layer 2 → Layer 1 → Layer 0.
No edge runs upward (nothing in `store/` imports `exec/`, `audit/`, or
`tests/`; nothing in `exec/` imports `audit/` or `tests/`).

## 3. Revalidation: does any AXOS module import or reference Word Pics material?

**Verdict: AXOS_CORE → WORD_PICS = NONE.**

This was re-done from scratch on the current filesystem (2026-09-28); no
prior audit's conclusion was inherited.

### Methodology

1. Enumerated all 50 Python files under `store/`, `exec/`, `audit/`,
   `tests/` (via `find`, excluding `__pycache__`).
2. Case-insensitive `grep` over all 50 files for each search term, then
   inspected every hit in context to classify it.
3. Additionally grepped for any `import` statement mentioning the terms, and
   searched `.py` files for the compound strings `"word pics"` and
   `"word-pics"` (catching config values, paths, string literals).

### Term-by-term results

| Search term | Hits | Disposition |
|---|---|---|
| `puzzle` | 0 | — |
| `pic1` | 0 | — |
| `pic2` | 0 | — |
| `distractor` | 0 | — |
| `noun` | 0 | — |
| `word pics` / `word-pics` | 0 | — |
| `\bword\b` (whole word) | 0 | — |
| `\bfir\b` (whole word) | 0 | — |
| `\bpics\b` (whole word) | 0 | — |
| `image` | 0 | — |
| `visual` | 0 | — |
| `\blevel\b` / `\blevels\b` | 12 hits | ALL false positives: "application-level", "row-level", "byte-level", "incident-level", "job-level", "filesystem-level", "task-level" — generic engineering phrasing, unrelated to puzzle levels |
| `canonical` | ~25 hits | ALL false positives: "canonical identity" (deterministic desired-job identity), "canonical JSON" (deterministic JSON serialization for hashing), "canonical DDL", "canonical manifest", "canonical 5-rung recovery ladder", `canonical_release_generation`, `canonical_finalization_id` — generic AXOS canonicalization vocabulary |
| `composition` | ~20 hits | ALL false positives: "R1–R6 composition", "R5 composition", "full R1-R8 composition" — refers to composition of AXOS recovery/execution layers (R-numbers), not Word Pics composition |
| import statements mentioning word/pics/puzzle | 0 | No `import` in any AXOS module names such a module |

### Conclusion

No AXOS module imports, names, or string-references any Word Pics concept
(puzzle slots, levels, nouns, pic1/pic2, distractors, images, canonical
content in the Word Pics sense, composition in the Word Pics sense). The
terms that do appear (`canonical`, `composition`, `level`) are pre-existing
generic AXOS vocabulary with unrelated meanings, verified hit-by-hit in
context. The dependency graph is closed: `store/ ← exec/ ← audit/,tests/`,
with no edges to any external workload module.
