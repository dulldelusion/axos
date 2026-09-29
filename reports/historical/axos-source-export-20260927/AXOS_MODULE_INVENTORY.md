# AXOS Module Inventory (as-is, read-only export)

Source: `~/workspace/axos/`, inspected 2026-09-28. Every claim below is grounded
in actual module source (imports extracted by AST from the real files; roles
from module docstrings; interfaces from top-level `def`/`class` statements).
Nothing is invented. Release identity: `release_manifest.json` → release_id
`551d559c…`, migration_version 12, Python 3.12.3, sqlite 3.45.1.

**Key architectural fact (from `store/db.py` module docstring, F1 remediation):**
- `Store` = the writable capability; only `TransitionGate` (plus transient
  migration/setup code) may hold one. `Store.conn` is deliberately READ-ONLY
  (`PRAGMA query_only=ON`). `write_txn()` is the gate-owned transaction
  capability — the sole sanctioned mutation path.
- `ReadOnlyStore` = the interface for every non-gate component; exposes no
  writable connection and no transaction capability; cannot be escalated.
- This boundary is application-level: raw `sqlite3.connect()` on the file is
  outside the boundary by definition (equivalent to filesystem access).

**Notation:** CALLED BY is derived from observed `import` statements elsewhere
in the tree (AST evidence). CALLS INTO is derived from the module's own import
statements.

---

## 1. `store/` — the authoritative state layer

### 1.1 `store/__init__.py`

- **PATH:** `store/__init__.py`
- **ROLE:** Public package surface of the store layer. Re-exports the
  writable/read-only handles, error hierarchy, migration runner, gate, and
  finalization manifest helpers. Its `__all__` is the sanctioned store API.
- **IMPORTS (actual):** `from .db import open_store, open_readonly_store,
  Store, ReadOnlyStore, StoreError, TransitionRejected, LeaseError,
  MigrationError, PolicyConflict, DesiredStateConflict, BreakerConflict,
  FinalizationConflict`; `from .migrations import migrate, applied_versions`;
  `from .gate import TransitionGate, canonical_release_generation,
  canonical_finalization_id, build_finalization_manifest,
  finalization_manifest_hash`.
- **DEPENDENCIES:** `store/db.py`, `store/migrations.py`, `store/gate.py`
  (relative imports within the package).
- **PUBLIC INTERFACES:** re-exports only (no classes/functions defined).
  Note: it does NOT export `StorageEngineError` (which exists in
  `store/db.py` as `sqlite3.Error` alias) nor `storage/transitions` graphs.
- **STATE OWNERSHIP:** none of its own; pure re-export.
- **CALLED BY:** every consumer of the store package — `exec/finalizer.py`,
  `exec/reconciler.py`, `exec/resilience.py`, `exec/scheduler.py`,
  `exec/supervisor.py`, `exec/synthetic.py`, `exec/watchdog.py`,
  `exec/worker.py` (absolute `axos.store` imports), `exec/policy.py`,
  `exec/recovery.py`, `exec/boot.py` (relative `..store` imports), all
  `audit/*.py` and all `tests/*.py` (relative or absolute).
- **CALLS INTO:** `store/db.py`, `store/migrations.py`, `store/gate.py`.

### 1.2 `store/db.py`

- **PATH:** `store/db.py`
- **ROLE:** Authoritative store handle. Connection setup with verified pragmas
  (WAL mode enforced, `synchronous=NORMAL`, `foreign_keys=ON`), the monotonic
  persisted store clock (`axos_meta.last_commit_ts`; every write txn stamps rows
  with a single `now` floored at last_commit+0.001 — worker clocks never
  trusted), atomic write transactions (`BEGIN IMMEDIATE`), consistent backup
  via the SQLite online-backup API, and `integrity_check`.
- **IMPORTS (actual):** `from __future__ import annotations`; `import sqlite3`;
  `import time`; `from contextlib import contextmanager`; `from typing import
  Callable, Iterator, Tuple`. No axos imports (base of the tree).
- **DEPENDENCIES:** stdlib only.
- **PUBLIC INTERFACES:**
  - Exceptions: `StoreError`, `TransitionRejected`, `LeaseError`,
    `MigrationError`, `PolicyConflict`, `DesiredStateConflict`,
    `BreakerConflict`, `FinalizationConflict` (all subclasses of `StoreError`).
  - `StorageEngineError = sqlite3.Error` (module-level alias; untranslated
    storage-engine errors; for fail-closed classification only; not re-exported
    by `store/__init__.py`).
  - `ReadOnlyStore` — methods: `__init__(path)`, `path` (property),
    `execute(sql, params)` (read queries only; mutation raises
    `sqlite3.OperationalError`), `close()`.
  - `Store` — methods: `__init__(path, clock=None)`, `read_only()` →
    `ReadOnlyStore`, `_apply_pragmas()`, `pragma_report()` → dict,
    `current_time()` → float (authoritative, monotonic), `write_txn()` →
    contextmanager yielding `(conn, now)` (GATE-OWNED; raises on nested txn),
    `backup_to(dest_path)`, `integrity_check()` → `(bool, str)`,
    `close()`, `conn` property (read-only connection).
  - `open_store(path, clock=None)` → `Store` (WRITABLE capability — hand only
    to TransitionGate or transient migration/setup code).
  - `open_readonly_store(path)` → `ReadOnlyStore` (for every non-gate
    component).
- **STATE OWNERSHIP:** the SQLite database file itself (connections, pragmas,
  `axos_meta.last_commit_ts`, WAL). Holds no domain semantics.
- **CALLED BY:** `store/__init__.py`; `store/gate.py` (TransitionGate holds
  the Store); `store/migrations.py` (migrate takes a Store); `exec/finalizer.py`,
  `exec/reconciler.py`, `exec/resilience.py`, `exec/scheduler.py` (import
  `Store`, `StoreError`, etc. from `axos.store.db` for thread-local handles);
  `audit/09_authority_audit.py` (imports `TransitionRejected` from
  `axos.store.db`); tests.
- **CALLS INTO:** stdlib `sqlite3`/`time`/`contextlib`/`typing` only.

### 1.3 `store/gate.py`

- **PATH:** `store/gate.py` (largest module in the tree, ~293 KB)
- **ROLE:** The transition-gate API — "the single narrow API through which
  authoritative state transitions occur: read state → validate transition →
  begin transaction → verify preconditions → apply transition → record
  authoritative transaction time → record ledger event → commit." The only
  holder of the writable `Store`. Enforces the explicit transition graphs in
  `store/transitions.py`; rejects invalid transitions transactionally with
  state left unchanged. Owns: tasks/jobs/workers lifecycle, lease triples
  (owner_worker_id, lease_expires_at, fencing_token), heartbeats, progress,
  R5 artifact staging/verification/commit, checkpoints, approvals, incidents,
  recovery attempts/policy, desired-state + reconciliation runs, R12 circuit
  breakers, R13 finalization runs, the hash-chained ledger.
- **IMPORTS (actual):** `from __future__ import annotations`; `import hashlib`;
  `import json`; `import os`; `import sqlite3`; `import uuid`;
  `from .db import Store, TransitionRejected, LeaseError, PolicyConflict,
  DesiredStateConflict, BreakerConflict, FinalizationConflict`;
  `from . import transitions as T`.
- **DEPENDENCIES:** `store/db.py` (Store, error types), `store/transitions.py`
  (graphs + actor sets).
- **PUBLIC INTERFACES:**
  - Exceptions (private-prefixed, internal control flow): `_ArtifactVerifyFailed`,
    `_FencedHeartbeat`, `_VerdictNoOp`, `_AdmissionDenied`, `_EvidenceFailure`.
  - `TransitionGate(store, staging_root=None)` — `staging_root` defaults to a
    sibling `axos-staging/` dir of the DB file. Method surface (all observed
    public methods):
    - Tasks: `create_task`, `transition_task`, `get_task`.
    - Jobs: `create_job`, `transition_job`, `get_job`, `jobs_in_states`,
      `pending_jobs`, `owned_active_jobs`, `sched_claimed_orphans`,
      `refresh_scheduler_claim_beats`, `withdraw_scheduler_claim_beat`,
      `clear_scheduler_claim_beats`, `scheduler_claim_live`.
    - Leases: `claim_job`, `claim_job_bounded`, `claim_job_resilient`,
      `renew_lease`, `release_lease`, `observe_expired_leases`,
      `expired_leases` (legacy), `reclaim_lease`; fencing evidence:
      `fencing_ledger_for_job`; `unreaped_proc_spawns`, `latest_spawn_generation`.
    - Progress: `update_job_progress`, `progress_evidence_for_job`.
    - Workers: `create_worker`, `transition_worker`, `get_worker`,
      `mark_worker_seen`, `ingest_heartbeat`, `heartbeats_for`.
    - R5 artifacts: `stage_artifact`, `get_artifact`, `verify_artifact`,
      `commit_artifact`, `create_artifact`, `transition_artifact`,
      `record_validation`, `quarantine_validator`, `validations_for_validator`,
      `classify_staging_orphans`, `stage_checkpoint`, `verify_checkpoint`,
      `invalidate_checkpoint`, `create_checkpoint`,
      `set_checkpoint_verification`, `latest_known_good`,
      `all_latest_known_good`, `commit_job_result` (REMOVED in R5 — raises).
    - Execution outcomes: `fail_job_execution`, `inspect_uncertain_completion`,
      `_completion_integrity_problems`.
    - Approvals/incidents: `create_approval`, `decide_approval`,
      `create_incident`, `set_incident_outcome`, `set_incident_escalated`.
    - R8 recovery: `record_recovery_attempt`, `recovery_attempts_for`,
      `open_recovery_incidents`, `find_or_create_recovery_incident`,
      `get_recovery_incident`, `create_recovery_attempt`,
      `get_recovery_attempt`, `open_recovery_attempts`,
      `claim_recovery_attempt`, `set_attempt_verify_after`,
      `note_attempt_dispatch`, `set_attempt_budget_context`,
      `transition_recovery_attempt`, `complete_recovery_attempt`,
      `mark_recovery_attempt_uncertain`, `escalated_recovery_incidents`,
      `recovery_incidents_with_open_policy`.
    - R9 policy: `get_recovery_policy`, `ensure_recovery_policy`,
      `cas_update_recovery_policy`, `consume_policy_attempts`.
    - R11 desired state: `set_desired_item`, `retire_desired_item`,
      `get_desired_head`, `get_desired_item`, `list_desired_items`,
      `get_desired_job_map`, `get_desired_work_id_for_job`,
      `ensure_job_for_desired_state`, `begin_reconciliation_run`,
      `checkpoint_reconciliation_run`, `finish_reconciliation_run`,
      `get_reconciliation_run`, `open_reconciliation_runs`.
    - R12 breakers: `get_breaker_state`, `list_breaker_states`,
      `ensure_breaker_state`, `record_breaker_signal`, `transition_breaker`,
      `claim_half_open_probe`, `breaker_allows`.
    - R13 finalization: `begin_finalization_run`, `get_finalization_run`,
      `list_finalization_runs`, `evaluate_finalization`,
      `publish_finalization`.
    - Watchdog verdicts: `latest_watchdog_verdict`, `watchdog_verdicts_for`,
      `record_watchdog_verdict`.
    - Ledger: `append_event`, `verify_ledger_chain`.
  - Module functions: `canonical_release_generation()`,
    `canonical_finalization_id(...)`, `build_finalization_manifest(...)`,
    `finalization_manifest_hash(manifest)`; constants
    `_DESIRED_SPEC_FIELDS`, `_BREAKER_SCOPES` (`GLOBAL/JOB/TASK/DESIRED`),
    `_BREAKER_STATES` (`CLOSED/OPEN/HALF_OPEN`), `_BREAKER_FAILURE_KINDS`,
    `_BREAKER_TRANSITIONS`, `_FINALIZATION_STATES`,
    `_FINALIZATION_BLOCKER_CATEGORIES`, `_RELEASE_CHECKPOINT_TRIGGER`;
    `REQUIRED_VALIDATORS = (("axos-structural", "1"),)` (the R5 structural bar).
- **STATE OWNERSHIP:** every domain table in the store (tasks, jobs, workers,
  artifacts, artifact_stagings, checkpoints, checkpoint_pointers, approvals,
  incidents, recovery_attempts, recovery_policy, heartbeats, validations,
  desired_state*, reconciliation_runs, breaker_state, breaker_signals,
  finalization_runs, scheduler_claim_beats, watchdog_verdicts, ledger), the
  `axos-staging/` directory (staged artifact bytes), and `axos_meta`.
- **CALLED BY:** everything that mutates or reads authoritative state —
  `exec/boot.py` (via supervisor-provided gate), `exec/finalizer.py`,
  `exec/policy.py`, `exec/reconciler.py`, `exec/recovery.py`,
  `exec/resilience.py`, `exec/scheduler.py`, `exec/supervisor.py`,
  `exec/synthetic.py`, `exec/watchdog.py`, `exec/worker.py`, all `audit/*.py`,
  all `tests/*.py`.
- **CALLS INTO:** `store/db.py`, `store/transitions.py`, stdlib only.

### 1.4 `store/migrations.py`

- **PATH:** `store/migrations.py`
- **ROLE:** Deterministic, versioned, atomic DB migrations. Requirements:
  applied in order; re-running `migrate()` on a current DB is a no-op;
  migration state recorded in the DB itself (`schema_migrations`); a failing
  migration leaves the schema untouched (runs inside a transaction).
- **IMPORTS (actual):** `from __future__ import annotations`; `import hashlib`;
  `from .db import Store, MigrationError`. No gate import (migrations run
  before/around the gate).
- **DEPENDENCIES:** `store/db.py`.
- **PUBLIC INTERFACES:** `migrate(store)` (applies pending migrations),
  `applied_versions(store)`; module constants: `V1_SCHEMA` (initial schema),
  `V2_HEARTBEATS` (phase1b heartbeats + commit evidence),
  `V3_PROGRESS_UPDATED_AT`, `V4_ARTIFACT_INTEGRITY`, `V5_WATCHDOG_VERDICTS`,
  `V6_RECOVERY_ATTEMPT_STATE`, `_V9_DESIRED_STATE_DDL` (+ seed head SQL),
  `MIGRATIONS` (12 ordered entries, versions 1–12: phase1a initial schema;
  heartbeats; progress_updated_at; artifact/checkpoint integrity; watchdog
  verdicts; recovery attempt state; recovery policy; scheduler claim beats;
  desired-state reconciliation; circuit breakers; finalization runs;
  per-job artifact staging provenance). Tables owned (from DDL): tasks, jobs,
  workers, artifacts, artifact_stagings, checkpoints, checkpoint_pointers,
  approvals, incidents, recovery_attempts, recovery_policy, heartbeats,
  validations, desired_state, desired_state_head, desired_job_map,
  reconciliation_runs, breaker_state, breaker_signals, finalization_runs,
  scheduler_claim_beats, watchdog_verdicts, ledger, axos_meta,
  schema_migrations.
- **STATE OWNERSHIP:** schema itself — creates/alters tables, records
  `schema_migrations`. Transient writer (holds a `Store` during setup only).
- **CALLED BY:** `store/__init__.py` (re-export); `exec/finalizer.py`,
  `exec/reconciler.py`, `exec/resilience.py`, `exec/scheduler.py`,
  `exec/supervisor.py`, `exec/watchdog.py` (each opens its store and runs
  `migrate` during startup); `audit/06_crash_backup_migration.py`,
  `audit/_migkill.py`; tests.
- **CALLS INTO:** `store/db.py`.

### 1.5 `store/transitions.py`

- **PATH:** `store/transitions.py`
- **ROLE:** Pure data module — the explicit, exact state-transition graphs
  (taken verbatim from the corrected Phase 0 documents). No code, no imports.
  "State is never a free-form string: the gate rejects any transition not
  listed here, transactionally."
- **IMPORTS (actual):** none.
- **DEPENDENCIES:** none.
- **PUBLIC INTERFACES:** `TASK_TRANSITIONS`, `JOB_TRANSITIONS`,
  `WORKER_TRANSITIONS`, `ARTIFACT_TRANSITIONS`, `APPROVAL_TRANSITIONS`,
  `CHECKPOINT_TRANSITIONS` (dicts: state → tuple of legal next states);
  `HUMAN_GATED_FROM = {"PAUSED_FOR_HUMAN"}` (I-17: cannot be exited without a
  recorded human `APPROVED` approval, sticky across restarts);
  `WORK_CREATOR_ACTORS = {"human","scheduler","system","test","reconciler"}`
  (I-16: anything starting `worker:` refused at creation);
  `RECLAIM_ACTORS = {"recovery-controller","reconciler","system","operator","test"}`
  (R1 lease-reclaim authority; supervisor deliberately absent).
- **STATE OWNERSHIP:** none (constants only).
- **CALLED BY:** `store/gate.py` (`from . import transitions as T`);
  `exec/supervisor.py`, `exec/policy.py`, `exec/resilience.py`
  (`from ..store import transitions as T` / `from axos.store import transitions
  as T`); `audit/02_transitions.py` (`from axos.store import transitions as T`);
  `audit/09_authority_audit.py`.
- **CALLS INTO:** nothing.

---

## 2. `exec/` — the execution layer (Phase 1B/1C)

Authority law for this layer (from module docstrings + audit 09):
`exec/` **never** calls `write_txn()`, **never** opens its own `sqlite3`
connections, **never** issues SQL write statements. All writes go through
`TransitionGate` methods. Each controller opens its own `Store` (one per
thread — a `Store` connection is not shared across threads) and builds
thread-local gates via `_gate_for_thread()` / `_store_for_thread()`.

### 2.1 `exec/__init__.py`
- **PATH:** `exec/__init__.py` — 86 bytes; **empty** (no imports, no exports).
- **ROLE:** package marker only.
- **PUBLIC INTERFACES:** none.

### 2.2 `exec/boot.py` (Phase 1C R6)
- **ROLE:** Durable boot recovery & runtime reconstruction. "The runtime
  disappeared — what does durable state say actually happened, and what runtime
  state must be reconstructed?" Reconstructs the authoritative baseline from
  durable evidence ONLY. The pass is idempotent: re-running observes the
  durable results of the previous pass and converges. Composes R1–R5 at boot
  (observation only, never mutation — the R4→R1 mutation path is the only
  mutating composition).
- **IMPORTS (actual):** `from __future__ import annotations`; `import os`;
  `import sqlite3`; `import uuid`; `from dataclasses import dataclass, field`;
  `from ..store import TransitionRejected, LeaseError, StoreError`.
- **DEPENDENCIES:** `store` package (errors only); stdlib. Note: `boot.py`
  takes the `supervisor` as a parameter (duck-typed) rather than importing it —
  `boot_recover(supervisor, actor, boot_id)`.
- **PUBLIC INTERFACES:** `boot_recover(supervisor, actor, boot_id)` → boot
  report dict (also stored on supervisor as `_boot_report`; supervisor sets
  execution-readiness from `report["phase"]`); helpers `proc_start_jiffies`,
  `proc_start_wall`, `proc_state`, `_classify_spawn`; constants `BOOT_PHASES`
  = ("STARTING","RECOVERING","READY","BLOCKED"),
  `DISPOSITIONS` = ("ADOPT","FENCE","ALREADY_DEAD","PID_REUSE","ORPHAN",
  "UNCERTAIN"…) — one disposition per persisted runtime record: ADOPT only
  when process identity matches AND durable job authority is current;
  otherwise FENCE / ALREADY_DEAD / PID_REUSE (never kill) / ORPHAN / UNCERTAIN.
- **STATE OWNERSHIP:** reads workers/jobs/leases/spawn generations and the
  ledger (read-only observation); writes dispositions through the gate only
  (via supervisor-provided gate handles) — e.g. `worker.proc_spawned` /
  `worker.proc_reaped` ledger milestones.
- **CALLED BY:** `exec/supervisor.py` (`from .boot import boot_recover,
  proc_start_jiffies`); `exec/policy.py` (`from . import boot as boot_mod`);
  `exec/recovery.py`; `exec/watchdog.py`; `tests/test_final_hardening_r14.py`
  (imports `boot_recover` from `axos.exec.boot`).
- **CALLS INTO:** `store` (error types); operates on the supervisor object
  passed in.

### 2.3 `exec/supervisor.py` (Phase 1B)
- **ROLE:** Deterministic supervisor. Owns worker PROCESS lifecycle
  (spawn/observe/terminate/reap), never job authority. Spawns real OS
  subprocesses (`python -m axos.exec.worker`), one `proc_id` per spawn; records
  worker identity (`workers` table) and process-instance evidence through the
  `TransitionGate`; enforces R2 fencing (process-group kill) when a lease is
  revoked; reaps; adopts orphans at boot via `boot_recover`.
- **IMPORTS (actual):** `from __future__ import annotations`; `import json/os/
  signal/sqlite3/subprocess/sys/threading/time`; `from dataclasses import
  dataclass, field`; `from ..store import open_store, migrate, TransitionGate,
  TransitionRejected, StoreError`; `from ..store import transitions as T`;
  `from .boot import boot_recover, proc_start_jiffies`;
  `from .identity import new_proc_id, WorkerIdentity, ProcessInstance`.
- **DEPENDENCIES:** `store`, `exec/boot.py`, `exec/identity.py`.
- **PUBLIC INTERFACES:** `Supervisor` class — methods: `__init__`,
  `start_worker`, `stop_worker`, `kill_worker`, `reap`, `observe`,
  `fence_sweep`, `restart_worker`, `reconstruct`, `close`, context-manager
  `__enter__`/`__exit__`; module constants `WORKSPACE_ROOT`,
  `TERMINAL_WORKER_STATES`, `TERMINAL_JOB_STATES`; private `_FenceRefused`
  (raised to tests), `_ProcInfo`, `_AdoptedProc`.
- **STATE OWNERSHIP:** OS processes it spawned (tracked in memory);
  `workers` rows + spawn generations + ledger milestones via the gate.
  Deliberately holds NO lease authority.
- **CALLED BY:** `exec/boot.py` (boot_recover takes the supervisor);
  `tests/_supdrv.py` (subprocess driver); all `tests/*.py` that construct
  `Supervisor(...)` directly.
- **CALLS INTO:** `store` (gate/migrate/open_store), `exec/boot.py`,
  `exec/identity.py`; spawns `axos.exec.worker` as OS subprocesses.

### 2.4 `exec/identity.py` (Phase 1B)
- **ROLE:** Identity model. Five distinct identities, never collapsed:
  worker_id (durable logical worker), proc_id (one OS process instance —
  restart always mints a new one), job_id (unit of work), lease_id =
  (job_id, fencing_token) (one lease epoch), plus fencing token lineage.
- **IMPORTS (actual):** `from __future__ import annotations`; `import uuid`;
  `from dataclasses import dataclass`. Stdlib only.
- **PUBLIC INTERFACES:** dataclasses `WorkerIdentity`, `ProcessInstance`,
  `LeaseRef` (property `lease_id`, `__str__`); function `new_proc_id()`.
- **STATE OWNERSHIP:** none (value types).
- **CALLED BY:** `exec/supervisor.py`, `exec/synthetic.py`, `exec/worker.py`,
  `tests/test_exec.py` (`new_proc_id`, `LeaseRef`).
- **CALLS INTO:** stdlib only.

### 2.5 `exec/worker.py` (Phase 1B)
- **ROLE:** Worker process entrypoint. Run as a real OS subprocess:
  `python -m axos.exec.worker --db PATH --worker-id W --proc-id P --job-id J
  --ttl-s 60 --behavior '{...}' [--hb-interval-s 0.5] [--no-renew]`. The
  worker NEVER manufactures authority. Protocol: (1) `claim_job` via gate —
  claim failure → exit 3; (2) read the authoritative lease triple back from
  the store (received, not invented); (3) CLAIMED→RUNNING; (4) run the
  `SyntheticExecutor`: heartbeat + progress through the gate; a daemon thread
  renews the lease via the sanctioned `gate.renew_lease` path; `LeaseError`
  mid-execution → stop immediately (`FencedError`) — only a fresh claim can
  restore authority; (5) commit via R5 protocol
  `stage_artifact → begin_commit → verify_artifact → commit_artifact` with real
  artifact bytes; on failure `fail_job_execution`. No artifact-free success.
- **IMPORTS (actual):** `from __future__ import annotations`; `import argparse/
  json/os/signal/sys/threading/time`; `from axos.store import open_store,
  TransitionGate, LeaseError, TransitionRejected, StoreError`;
  `from axos.exec.identity import LeaseRef, ProcessInstance`;
  `from axos.exec.synthetic import SyntheticExecutor, BehaviorSpec,
  FencedError, InterruptedExecution`.
- **DEPENDENCIES:** `store`, `exec/identity.py`, `exec/synthetic.py`.
- **PUBLIC INTERFACES:** module functions `main()`, `_parse_args()`,
  `_on_sigterm()`, `_emit()`, `_result_record_bytes()`, `_renew_loop()`,
  `_best_effort_milestone()`; module constants `EXIT_OK=0`,
  `EXIT_COMMITTED_FAILURE=10`, `EXIT_CLAIM_FAILED=3`, `EXIT_FENCED=4`,
  `EXIT_COMMIT_REJECTED=5`, `EXIT_FATAL=6`, `EXIT_SIGTERM=143`,
  `EXIT_STALE_TOKEN=7`. Exit codes are process evidence only — NEVER job
  success. CLI surface: `--db`, `--worker-id`, `--proc-id`, `--job-id`,
  `--ttl-s`, `--behavior`, `--hb-interval-s`, `--no-renew`, `--expect-token`.
  F1 note: constructs its own `TransitionGate`s over `Store`s (one per thread)
  but uses ONLY gate methods — never `write_txn()`, never `store.conn` for
  writes, never its own `sqlite3` connection.
- **STATE OWNERSHIP:** none durable of its own; mutates jobs/leases/artifacts
  exclusively through gate methods.
- **CALLED BY:** the OS (spawned by `Supervisor`); tests via subprocess.
- **CALLS INTO:** `store` (gate/open_store), `exec/identity.py`,
  `exec/synthetic.py`.

### 2.6 `exec/synthetic.py` (Phase 1B)
- **ROLE:** Deterministic synthetic executor. Runs INSIDE the worker process.
  Performs no real work: fault injection for the execution substrate. No LLMs,
  no external APIs, no network, no real side effects, no randomness — every
  behavior is a deterministic function of its spec. Behavior kinds
  (`VALID_KINDS`): success_immediate, delayed, heartbeat_loop, hang, wedged
  (variants), crash, crash_after_stage, sigkill_self, expire_then_commit.
- **IMPORTS (actual):** `from __future__ import annotations`; `import os/signal/
  time`; `from dataclasses import dataclass, field`;
  `from ..store import TransitionGate, LeaseError`; `from .identity import
  LeaseRef`; `import subprocess`; `import sys`.
- **PUBLIC INTERFACES:** exceptions `FencedError`, `InterruptedExecution`;
  `BehaviorSpec` dataclass (`.from_dict()`, `__post_init__` validation);
  `SyntheticExecutor` — `run()` plus private `_do_*` per behavior kind.
- **STATE OWNERSHIP:** heartbeats/progress/artifact staging via the gate only.
- **CALLED BY:** `exec/worker.py`.
- **CALLS INTO:** `store` (TransitionGate, LeaseError), `exec/identity.py`.

### 2.7 `exec/scheduler.py` (Phase 1C R10)
- **ROLE:** Scheduler & admission-control gate. Admits PENDING jobs and
  dispatches worker processes. Deliberately narrow — admission + atomic claim +
  dispatch; owns NO other authority: never creates/deletes jobs (R11's
  domain), never reconciles desired state, never trips breakers, never
  load-sheds. (BLOCKED jobs and task-level PAUSED_FOR_HUMAN never admitted.)
- **IMPORTS (actual):** `from __future__ import annotations`; `import json/
  threading/time/uuid`; `from dataclasses import dataclass`;
  `from axos.store import open_store, migrate, TransitionGate`;
  `from axos.store.db import Store, StoreError, TransitionRejected, LeaseError`.
- **PUBLIC INTERFACES:** `SchedulerConfig` (dataclass, `__post_init__`
  validation); `Scheduler` — `__init__`, `evaluate_once()`, `start()`,
  `_loop()`, `stop()`, `close()`; private evaluators cover breaker admission
  (`_breaker_scopes`, `_breaker_denies`), claimed-orphan detection,
  unreaped spawn keys, open-recovery job ids, paused task ids.
- **STATE OWNERSHIP:** scheduler claim liveness beats
  (`scheduler_claim_beats`) via gate; worker processes it dispatches.
- **CALLED BY:** tests (`test_scheduler_r10.py`,
  `test_final_hardening_r14.py`, `test_finalization_r13.py`,
  `test_reconciliation_r11.py`, `test_resilience_r12.py`).
- **CALLS INTO:** `store`, `store/db.py` (Store type for thread-local handles);
  dispatches to `exec/worker.py` via `exec/supervisor.py` semantics.

### 2.8 `exec/watchdog.py` (Phase 1C R7)
- **ROLE:** Deterministic STALLED/DEAD detection. "Is this execution still
  making authoritative progress, or should it be considered STALLED/DEAD?"
  DETECTS and CLASSIFIES only — never recovers, never reclaims, never fences,
  never schedules. R1 remains the sole lease-reclaim authority; the watchdog
  never calls `reclaim_lease`. Verdicts: `WATCHDOG_VERDICTS` =
  ("HEALTHY","STALLED","DEAD").
- **IMPORTS (actual):** `from __future__ import annotations`; `import math/
  threading/time`; `from dataclasses import dataclass`;
  `from ..store import StoreError, TransitionGate, TransitionRejected,
  open_store, migrate`; `from . import boot as boot_mod`.
- **PUBLIC INTERFACES:** errors `WatchdogError`, `WatchdogNotReady`;
  `WatchdogConfig` (dataclass); `Watchdog` — `__init__`, `evaluate(...)` (per
  job: `_evaluate_job`), `_death_evidence`, `_process_identity_evidence`,
  `_record`, `start()`, `_loop()`, `stop()`, `close()`.
- **STATE OWNERSHIP:** `watchdog_verdicts` rows via
  `gate.record_watchdog_verdict` only; reads heartbeats/progress evidence.
- **CALLED BY:** `exec/recovery.py` consumes its verdicts; tests
  (`test_watchdog_r7.py`, `test_final_hardening_r14.py`).
- **CALLS INTO:** `store`, `exec/boot.py` (module ref).

### 2.9 `exec/recovery.py` (Phase 1C R8)
- **ROLE:** Recovery controller & recovery-contract gate. "A failure was
  detected — what is the smallest authorized recovery action, did it actually
  work, and when do we escalate?" CONSUMES R7 verdicts (never redefines
  health); creates durable recovery incidents/attempts carrying the full
  Recovery Contract fields; dispatches the smallest authorized action ONLY
  through existing authorities (reclaim → R1 gate op; fence → supervisor; restart
  → scheduler-class path); verifies attempts against progress deltas; records
  results; escalates after TWO consecutive non-success attempts (contract
  §E.4 fixed rule — not a configurable budget) to `r9-policy`.
- **IMPORTS (actual):** `from __future__ import annotations`; `import math/
  threading/time/json/uuid`; `from dataclasses import dataclass`;
  `from typing import Protocol`; `from ..store import StoreError,
  TransitionGate, TransitionRejected, LeaseError, open_store, migrate`;
  `from . import boot as boot_mod`.
- **PUBLIC INTERFACES:** errors `RecoveryError`, `RecoveryNotReady`;
  `RecoveryConfig` (dataclass); `RungContext` (dataclass);
  `RungProvider` (Protocol: `select_rung`); `CanonicalRungProvider`;
  `RecoveryController` — `__init__`, `evaluate()`, `dispatch_restart()`,
  `start()`, `_loop()`, `stop()`, `close()`; constants
  `RECOVERY_FAILURE_CLASSES` = ("STALLED","DEAD","LEASE_EXPIRED",
  "STALE_AUTHORITY","UNCERTAIN"), `RECOVERY_ACTIONS` =
  ("reclaim","fence","restart"), `_CANONICAL_RUNGS` (canonical 5-rung ladder
  names verbatim from contract E.1 — R8 records the rung context, does not
  implement ladder policy), `_TWO_ATTEMPT_ESCALATION_RUN = 2`,
  `_ESCALATION_TARGET = "r9-policy"`.
- **STATE OWNERSHIP:** `incidents`, `recovery_attempts` rows via gate only.
- **CALLED BY:** `exec/policy.py` (`from .recovery import RecoveryConfig,
  RecoveryController, RecoveryError, RungContext, RungProvider`);
  tests (`test_recovery_r8.py`, `test_final_hardening_r14.py`).
- **CALLS INTO:** `store`, `exec/boot.py`.

### 2.10 `exec/policy.py` (Phase 1C R9)
- **ROLE:** Recovery ladder, budgets & escalation policy — the policy layer
  above R8's execution/verification substrate. CHOOSES the recovery rung (the
  canonical 5-rung ladder, contract §E.1), BOUNDS recovery (durable per-rung
  and per-incident attempt budgets), DECIDES terminal escalation. Never
  executes a recovery action and never verifies one. Owns human-gated pause
  (`PAUSED_FOR_HUMAN` convergence) and corrupt-policy blocking.
- **IMPORTS (actual):** `from __future__ import annotations`; `import json/
  math/threading`; `from dataclasses import dataclass, field`;
  `from ..store import PolicyConflict, StoreError, TransitionRejected`;
  `from ..store import transitions as T`; `from . import boot as boot_mod`;
  `from .recovery import RecoveryConfig, RecoveryController, RecoveryError,
  RungContext, RungProvider`.
- **PUBLIC INTERFACES:** errors `PolicyError`, `PolicyNotReady`,
  `PolicyCorrupt`; `PolicyConfig` (dataclass); `RungDecision` (dataclass);
  `PolicyRungProvider` (a `RungProvider`: `select_rung`); `PolicyController`
  — `__init__`, `evaluate()`, `start()`, `_loop()`, `stop()`, `close()`,
  plus `_ensure_policy`, `_cas` (compare-and-swap against the durable policy
  row; loser raises `PolicyConflict`), `_consume_created`, `_advance_rung`,
  `_apply_terminal`, `_converge_terminal`, `_block_corrupt`,
  `_pause_task_for_human`, `_maybe_execute`, `_execute_restart`;
  module function `select_rung(...)`; `CANONICAL_LADDER: dict[int, str]`
  (rung index → name); terminal-signal constants `T_RECOVERY_COMPLETE`,
  `T_BUDGET_EXHAUSTED`, `T_R5_TERMINAL`, `T_BLOCKED_CONTRADICTORY`,
  `T_BLOCKED_STATE_UNAVAILABLE`, `T_SUPERSEDED`; `_validate_policy_row`.
- **STATE OWNERSHIP:** `recovery_policy` rows via gate
  (`ensure_recovery_policy` / `cas_update_recovery_policy` /
  `consume_policy_attempts`) only.
- **CALLED BY:** tests (`test_recovery_r9.py`,
  `test_final_hardening_r14.py`).
- **CALLS INTO:** `store`, `exec/boot.py`, `exec/recovery.py`.

### 2.11 `exec/reconciler.py` (Phase 1C R11)
- **ROLE:** Desired-state reconciliation (observe-and-converge). Converges
  actual jobs toward a declared desired set. Deliberately the narrowest
  authority in the system — narrower than the scheduler: never creates jobs
  except through the gate's single idempotent op
  `ensure_job_for_desired_state` (literal actor `"reconciler"`); never deletes
  anything, never moves a job between states directly.
- **IMPORTS (actual):** `from __future__ import annotations`; `import json/
  threading/time`; `from dataclasses import dataclass`;
  `from axos.store import open_store, migrate, TransitionGate`;
  `from axos.store.db import Store, StoreError, TransitionRejected,
  DesiredStateConflict, StorageEngineError`;
  `from axos.store.gate import _canonical_desired_job_id,
  _desired_snapshot_hash, _desired_spec_hash, _normalize_desired_spec`
  (private gate helpers — the one place a private cross-module import is
  sanctioned).
- **PUBLIC INTERFACES:** `ReconcilerConfig` (dataclass);
  `Reconciler` — `__init__`, `reconcile(...)`, `evaluate_once()`, `start()`,
  `_loop()`, `stop()`, `close()`; module functions `canonical_job_id(...)`,
  `diff_item(...)`.
- **STATE OWNERSHIP:** `desired_state*`, `reconciliation_runs`,
  `desired_job_map` via gate only; reads jobs.
- **CALLED BY:** tests (`test_reconciliation_r11.py`,
  `test_finalization_r13.py`, `test_final_hardening_r14.py`,
  `test_resilience_r12.py`).
- **CALLS INTO:** `store`, `store/db.py`, `store/gate.py` (incl. private
  canonical-identity helpers).

### 2.12 `exec/resilience.py` (Phase 1C R12)
- **ROLE:** Circuit-breaker resilience controller — the reader/evaluator half
  of R12's circuit breaker. Owns NO execution authority and never mutates
  jobs, tasks, leases, workers, processes, artifacts, checkpoints, recovery
  budgets, or recovery rungs. Only writes go through the gate's breaker API;
  never touches a DB connection or raw SQL directly.
- **IMPORTS (actual):** `from __future__ import annotations`; `import json/
  math/threading/time`; `from dataclasses import dataclass`;
  `from axos.store import open_store, migrate, TransitionGate,
  BreakerConflict`; `from axos.store import transitions as T`;
  `from axos.store.db import Store, StoreError, TransitionRejected`.
- **PUBLIC INTERFACES:** `ResilienceConfig` (dataclass);
  `ResilienceController` — `__init__`, `resilience_allows(...)` (admission
  query used on the claim path), `evaluate_once()`, `start()`, `_loop()`,
  `stop()`, `close()`; module constants `GLOBAL_SCOPE_TYPE = "GLOBAL"`;
  `_R7_DEAD`, `_R8_ATTEMPT_FAILED`, `_R9_ESCALATED`, `_RECOVERY_PRESSURE`.
  Breaker machine: CLOSED → OPEN → HALF_OPEN → CLOSED|OPEN; scopes
  hierarchical GLOBAL/TASK/DESIRED/JOB; an OPEN row with elapsed cooldown
  still denies until the controller transitions it (fail closed).
- **STATE OWNERSHIP:** `breaker_state`, `breaker_signals` via gate's breaker
  API only.
- **CALLED BY:** tests (`test_resilience_r12.py`,
  `test_final_hardening_r14.py`, `test_finalization_r13.py`).
- **CALLS INTO:** `store`, `store/db.py`.

### 2.13 `exec/finalizer.py` (Phase 1C R13)
- **ROLE:** Release finalization controller — the reader/evaluator half of
  R13's finalization. Owns NO execution authority and never mutates jobs,
  tasks, leases, workers, processes, artifacts, checkpoints, recovery budgets,
  recovery rungs, desired state, or breakers. Only writes go through the
  gate's finalization API (`begin_finalization_run` / `evaluate_finalization` /
  `publish_finalization` on `finalization_runs` rows), plus deterministic
  checkpoint/release-manifest helpers.
- **IMPORTS (actual):** `from __future__ import annotations`; `import math/
  threading/time`; `from dataclasses import dataclass`;
  `from axos.store import open_store, migrate, TransitionGate,
  FinalizationConflict, canonical_release_generation,
  canonical_finalization_id, build_finalization_manifest,
  finalization_manifest_hash`;
  `from axos.store.db import Store, StoreError, TransitionRejected`.
- **PUBLIC INTERFACES:** `FinalizationConfig` (dataclass); `Finalizer` —
  `__init__`, `evaluate_once()`, `start()`, `_loop()`, `stop()`, `close()`.
- **STATE OWNERSHIP:** `finalization_runs` rows via gate finalization API only.
- **CALLED BY:** tests (`test_finalization_r13.py`,
  `test_final_hardening_r14.py`); `audit/09_authority_audit.py` (imports
  `FinalizationConfig`).
- **CALLS INTO:** `store`, `store/db.py`.

---

## 3. `audit/` — authority audit scripts (Phase 1B + R14)

Standalone scripts (each run as `python3 audit/NN_*.py <db>`-style); they are
evidence harnesses, not part of the runtime.

- `audit/01_write_path.py` — `check()`: single-write-path claim — can
  authoritative state be mutated without `TransitionGate`? Uses only the public
  `Store` API. Historical note: pre-F1-remediation evidence (11/12 bypasses
  succeeded); post-fix `Store.conn` is read-only. Imports:
  `axos.store.{open_store, migrate, TransitionGate, TransitionRejected}`.
- `audit/02_transitions.py` — `attack()`, `worker_state()`: attacks the
  transition graph — every illegal transition, terminal-state escape, replay,
  malformed evidence. Imports `axos.store` + `axos.store.transitions as T`.
- `audit/03_leases.py` — `check()`: attacks lease authority — simultaneous
  acquisition, expiry edges, wrong owner/token, stale tokens, release races,
  clock rollback/jump, worker-supplied timestamps, malformed durations.
- `audit/04_i17_i18.py` — `check()`, `paused()`, `attempt()`: attacks I-17
  (human-gated stickiness) and I-18 (recovery evidence: 'success'
  unrepresentable without positive progress evidence; 'process restarted'
  never counts as recovery).
- `audit/05_ledger.py` — `check()`, `canon()`: attacks the ledger — hash
  chaining, ordering, atomicity, tamper/missing/reorder/duplicate/payload
  mutation detection, actor attribution, timestamp semantics.
- `audit/06_crash_backup_migration.py` — `check()`, `kill_at()`, `writer()`,
  `loader()`: crash consistency at kill points (real SIGKILLs), backup under
  write load, interrupted migration. Imports `MIGRATIONS`,
  `applied_versions`.
- `audit/07_race.py` — (no module docstring; race harness). Imports
  `axos.store` only.
- `audit/08_retest.py` — re-test of the 11/12 bypasses after F1 remediation,
  through `Store.conn` (now query_only), `Store.read_only()`,
  `open_readonly_store()`; expects `sqlite3.OperationalError` everywhere.
- `audit/09_authority_audit.py` — the big one (~119 KB): Phase 1B authority
  audit extended for R5. Functions: `check`, `py_files`, `read`, `code_lines`,
  `method_ast`, `txn_span`, `conn_writes`, `tree_py_files`, `r3_writes`,
  `_body_src`, `_write_txn_blocks`, `_calls`. Checks include: A1 `exec/` never
  calls `write_txn()`; A2 `exec/` never opens its own `sqlite3` connections;
  A3 `exec/` contains no SQL write statements; plus R6 boot composition
  authority checks. Imports `TransitionGate`, `Supervisor`, `CANONICAL_LADDER`
  (policy), `_CANONICAL_RUNGS` (recovery), `SchedulerConfig`, finalization
  helpers, `_canonical_desired_job_id`/`_desired_snapshot_hash`/`_canon`,
  `transitions`, `FinalizationConflict`, `canonical_release_generation`,
  `canonical_finalization_id`, `build_finalization_manifest`,
  `finalization_manifest_hash`, `_release_checkpoint_id`,
  `_release_checkpoint_entries`, `FinalizationConfig`.
- `audit/_killpoints.py` — kill-point injector helper (points:
  before_txn | after_begin | after_entity | after_ledger | before_commit |
  after_commit; SIGKILLs itself mid-transaction).
- `audit/_migkill.py` — migration kill helper; imports
  `_split_statements, V1_SCHEMA, _ensure_migrations_table` from
  `axos.store.migrations`.
- `audit/stray_process_audit.py` — independent stray-process/resource audit
  for R14: no AXOS worker/supervisor/synthetic processes may survive outside
  a test run.

State ownership of audit scripts: they create throwaway DBs in temp dirs;
they own no production state.

## 4. `tests/` — test suite + helpers (evidence, not runtime)

19 files per the brief. Helpers (runtime-adjacent):

- `tests/_crasher.py` — `main()`: crash-injection helper; SIGKILLs itself
  mid-transaction. Imports `from axos.store import open_store, migrate`.
- `tests/_supdrv.py` — `_ensure_job()`, `_wait_for()`, `main()`: supervisor
  subprocess driver. Imports `Supervisor` from `axos.exec.supervisor`.
- `tests/r5_helpers.py` — `_job_lock()`, `r5_complete(...)`: shared R5
  completion helper used across suites. Imports `TransitionRejected` from
  `axos.store`.

Test files (each a `unittest` suite against real DB files): `test_store.py`
(25 tests, incl. real SIGKILL crash injection, backup/restore, I-16/17/18);
`test_remediation.py` (18 regression tests for F1/F2/F3);
`test_exec.py` (Phase 1B: fencing, heartbeat, idempotency, crash);
`test_heartbeat_r3.py`; `test_expiry_r4.py`; `test_artifacts_r5.py`
(R5-01…R5-30, incl. `MIGRATIONS` import); `test_boot_r6.py` (R601–R625);
`test_watchdog_r7.py` (R701–R733); `test_recovery_r8.py` (R801–R835 + race/
composition); `test_recovery_r9.py` (R901–R936 + composition; imports
`CANONICAL_LADDER`, `PolicyController`, `RungDecision`, `select_rung`,
terminal-signal constants); `test_scheduler_r10.py` (R1000–R1040 + races);
`test_reconciliation_r11.py` (R1101–R1150 + full-path; imports
`canonical_job_id`, `diff_item`); `test_resilience_r12.py` (R1201–R1266 +
races; imports `ResilienceConfig/Controller`); `test_finalization_r13.py`
(R1301–R1380 + S31; imports `Finalizer`, `canonical_release_generation`,
`canonical_finalization_id`, `build_finalization_manifest`,
`finalization_manifest_hash`); `test_final_hardening_r14.py` (R14-A…E,
FI-01…FI-12, clean-boot; builds `release_manifest.json` via
`build_release_manifest`/`write_release_manifest` defined in the test file
itself); `test_fence_enforce.py` (R201–R214; imports `Supervisor`,
`_FenceRefused`); `test_reclaim.py` (R1/D1 reclaim).

## 5. Root files

- `README.md` — Phase 1A store-layer readme (layout, key laws, run instructions).
- `AUDIT-REPORT.md`, `PHASE1A-REPORT.md`, `PHASE-1C-FINAL-STATUS.md`,
  `R14-EVIDENCE.md`, `R14-PLAN.md`, `R14-CONTRACT-AMENDMENT-PROPOSAL.md`,
  `R14-EXTERNAL-VALIDATION.md`/`.json` — report/evidence docs (not code).
- `release_manifest.json` — release identity: `release_id`
  `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`,
  per-file sha256 of every regular file under `exec/`, `store/`, `audit/`,
  `tests/` (excl. `__pycache__`), `migration_version: 12`, `policy_version:
  "r9-policy/v1"`, canonical DDL hash, canonical recovery-ladder hash,
  platform/python/sqlite identity. Generated 2026-09-23T23:21:36Z by
  `tests/test_final_hardening_r14.py`.
- `store.db` (repo root) and `store/store.db` — 0-byte placeholders.
- `.pytest_cache/` — pytest artifacts from prior runs.
