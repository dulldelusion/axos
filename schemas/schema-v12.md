# AXOS durable schema — v12

> **Derived documentation.** Transcribed from
> `src/axos/store/migrations.py` (`MIGRATIONS`, v1 through v12), release
> `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`.
> The migration module is authoritative; if this document disagrees with
> it, the module wins.

## Migration history

| Version | Name (from `migrations.py`) | Adds |
|---|---|---|
| 1 | phase1a initial schema | core tables (below) |
| 2 | phase1b heartbeats + commit evidence | `heartbeats`; `jobs.commit_outcome`, `jobs.commit_evidence` |
| 3 | phase1c r3 jobs.progress_updated_at | `jobs.progress_updated_at` (informational only) |
| 4 | phase1c r5 artifact/checkpoint integrity | artifact/checkpoint integrity columns; `checkpoint_pointers` |
| 5 | phase1c r7 watchdog verdict records | `watchdog_verdicts` |
| 6 | phase1c r8 recovery attempt state | recovery-attempt state columns; incident signature uniqueness |
| 7 | phase1c r9 recovery policy state | `recovery_policy` |
| 8 | phase1c r10 scheduler claim liveness beats | `scheduler_claim_beats` |
| 9 | phase1c r11 desired-state reconciliation | `desired_state`, `desired_state_head`, `desired_job_map`, `reconciliation_runs` |
| 10 | phase1c r12 circuit-breaker state + failure signals | `breaker_state`, `breaker_signals` |
| 11 | phase1c r13 finalization runs | `finalization_runs` |
| 12 | phase1c r14 per-job artifact staging provenance | `artifact_stagings` |

The `schema_migrations` table (version, name, applied_at) records what is
applied. `migrate()` applies pending versions in order; each version runs
in a single transaction — a failure rolls back fully and the version is
not recorded. Re-running on a current database is a no-op.

## v12 tables

### v1 — core

**tasks** — `task_id` PK; `status`, `objective`, `budgets`, `pause_reason`,
`pause_diagnostic`, `created_at`, `updated_at`.

**jobs** — `job_id` PK; `task_id` → tasks; `stage_id`; `status`; `attempt`
(default 0); `max_attempts` (default 3); `owner_worker_id`;
`fencing_token` (default 0); `lease_acquired_at`, `lease_expires_at`;
`worker_reported_ts`; `progress_done` (default 0), `progress_total`;
`result_artifact_id`; `policy`; `created_at`, `updated_at`. Indexes:
`(task_id, status)`; `lease_expires_at` (partial, non-NULL). Later
migrations add: `commit_outcome`, `commit_evidence` (v2);
`progress_updated_at` (v3); `content_hash` (v4).

**workers** — `worker_id` PK; `status`; `task_id` → tasks;
`capabilities`; `created_at`, `updated_at`, `last_seen_at`.

**approvals** — `approval_id` PK; `task_id` → tasks; `job_id` → jobs
(nullable); `stage_id`; `reason`; `requested_action`; `evidence_refs`;
`risk`; `options`; `deadline`; `checkpoint_id`; `on_timeout`; `status`
(default `'PENDING'`); `decided_by`, `decided_at`, `decision_reason`;
`created_at`, `updated_at`.

**incidents** — `incident_id` PK; `task_id` → tasks; `scope`;
`failure_class`; `signature`; `detection`; `classification`;
`diagnosis`; `actions`; `outcome`; `escalated_to`; `created_at`,
`updated_at`. Index: `(signature)`. v6 adds
`UNIQUE(signature) WHERE scope='recovery'`.

**recovery_attempts** — `attempt_id` PK; `incident_id` → incidents;
`rung`; `action`; `observed_effect`; `progress_delta`;
`output_health_delta`; `decision`; `spend`; `actor`; `recorded_at`.
Later: `job_id`, `fencing_token`, `attempt_number` (v6), `attempt_state`
(v6), `rung_name`, `started_at`, `verify_after`, `controller_id`,
`claimed_at`, `failure_class`, `success_criterion`,
`failure_criterion`, `resulting_state`, `escalation_target`,
`evidence_before`, `evidence_after`, `idempotency_key`,
`budget_context` (v6). Unique index on `(incident_id, attempt_number)`
(v6).

CHECK constraints:
- `decision IN ('retry','escalate','success','quarantine','stop')`
- `attempt_state IN
  ('CREATED','RUNNING','VERIFYING','SUCCEEDED','FAILED','BLOCKED','UNCERTAIN')`
  (v6)
- **I-18 zero-progress-delta rejection** (v1, authoritative):
  `CHECK (NOT (decision = 'success' AND progress_delta <= 0))` —
  a recovery recorded as `'success'` with no positive progress delta is
  contradictory and is rejected by the store itself. The comment in the
  migration names it explicitly: "Recovery is verified by progress, not
  by action."

**checkpoints** — `checkpoint_id` PK; `task_id` → tasks; `stage_id`;
`ledger_tip_seq`; `completed_units`, `remaining_units`;
`state_snapshot_ref`; `artifact_manifest`; `input_version`,
`schema_version`, `methodology_version`; `capability_versions`;
`os_version`; `supersedes` → checkpoints; `verification_status`
(default `'UNVERIFIED'`); `verified_at`, `verify_method`,
`sample_rate`, `sample_seed`, `sample_count`, `sample_passed`;
`manifest_full` (default 0), `ledger_chain_verified` (default 0);
`created_at`, `updated_at`. v4 adds: `trigger`, `canonical_manifest`,
`verification_receipt`.

**artifacts** — `artifact_id` PK; `task_id` → tasks; `kind`; `size`;
`uri`; `status` (default `'STAGING'`); `producer`; `provenance_id`;
`created_at`, `updated_at`. v4 adds: `job_id`, `content_hash`
(backfilled from the legacy `artifact_id`), `attempt`,
`owner_worker_id`, `fencing_token`; index on `(job_id)`.

**validations** — `validation_id` PK; `artifact_id` → artifacts;
`validator_id`; `validator_version`;
`result CHECK(result IN ('PASS','FAIL','INCONCLUSIVE'))`;
`method`; `sampled` (default 0); `sample_desc`; `receipt_ref`;
`quarantined` (default 0); `notes`; `validated_at`. Index on
`(validator_id, validator_version)`. v4 adds `content_hash` (backfilled
from `artifact_id`).

**ledger** — `seq` INTEGER PK AUTOINCREMENT; `event_type`; `payload`;
`actor`; `ts`; `prev_hash`; `hash`. The hash-chained, append-only
event stream. Tamper-evident, not tamper-proof (see
`docs/decisions/architecture-decisions.md`).

### v2 — heartbeats

**heartbeats** — `seq` PK AUTOINCREMENT; `worker_id` → workers;
`proc_id`; `job_id` → jobs (nullable); `fencing_token`; `hb_seq`;
`worker_state`; `current_operation`; `worker_reported_ts`; `ts` (store
time on ingest). Indexes: `(worker_id, proc_id, hb_seq)`,
`(job_id, ts)`. Heartbeats are liveness records only — v3 deliberately
adds no progress columns to them.

### v4 — artifact/checkpoint integrity

**checkpoint_pointers** — `name` PK; `checkpoint_id` → checkpoints;
`task_id`; `updated_at`. The latest-known-good pointer, advanced only by
the verified-checkpoint path.

### v5 — watchdog verdicts

**watchdog_verdicts** — `verdict_id` PK; `job_id` → jobs;
`fencing_token`; `verdict`; `evidence`; `evaluated_at`; `actor`. Index on
`(job_id, fencing_token, evaluated_at)`. Verdicts are per execution
identity (`job_id`, `fencing_token`); rows are written only on verdict
transitions. The watchdog detects and classifies only — never reclaims,
fences, or recovers.

### v7 — R9 recovery policy

**recovery_policy** — one row per recovery incident:
`incident_id` PK → incidents; `policy_version`; `version` (CAS counter,
default 1); `current_rung`, `rung_name`; `incident_budget`;
`per_rung_budgets`; `attempt_count`; `per_rung_attempts`;
`remaining_budget`; `consumed_attempt_number`;
`result_consumed_attempt_number`; `zero_progress_count`;
`last_attempt_id`, `last_attempt_result`; `escalation_state` (default
`'none'`); `escalation_target`; `terminal_state`; `superseded_by`;
`updated_at`. All policy mutations are compare-and-swap UPDATEs on
`version` inside a single `write_txn`.

### v8 — scheduler claim liveness

**scheduler_claim_beats** — one row per scheduler-owned claim:
`worker_id` PK; `scheduler_id`; `last_beat`. Index on
`(scheduler_id)`. Lets a scheduler's orphan-redispatch path distinguish a
live scheduler mid-dispatch from a dead scheduler's true orphan.

### v9 — desired-state reconciliation

**desired_state** — `desired_work_id` PK; `spec` (canonical JSON);
`retired` (tombstone, default 0); `version`; `created_at`, `updated_at`.

**desired_state_head** — `key` PK; `version`; `snapshot_hash`; `updated_at`.
Single CAS row (`key='head'`), seeded at version 0 with the
empty-snapshot hash.

**desired_job_map** — `desired_work_id` PK → desired_state; `job_id`
UNIQUE; `spec_hash`; `desired_version`; `created_at`. Canonical identity:
one deterministic `job_id` per desired-work item.

**reconciliation_runs** — `reconciliation_id` PK; `desired_state_version`;
`snapshot_hash`; `started_at`; `completed_at`; `result`;
`items_examined`, `items_created`; `last_item_id`; `discrepancies`
(canonical JSON); `actor`.

### v10 — circuit breaker

**breaker_state** — `(scope_type, scope_id)` PK; `state`
(`CLOSED`/`OPEN`/`HALF_OPEN`); `version`; `failure_count`;
`window_started_at`; `cooldown_s`; `opened_at`; `cooldown_until`;
`last_failure_id`; `half_open_probe_id`; `half_open_probes_used`;
`half_open_probe_at`; `half_open_probe_baseline`; `updated_at`.

**breaker_signals** — `signal_id` PK (canonical dedupe key);
`scope_type`; `scope_id`; `failure_kind`; `incident_id`; `attempt_id`;
`observed_at`; `actor`. Index on `(scope_type, scope_id, observed_at)`.
`INSERT OR IGNORE` semantics make signal delivery exactly-once.

### v11 — finalization

**finalization_runs** — `finalization_id` PK; `release_generation` UNIQUE;
`desired_state_version`; `state CHECK(state IN
('OPEN','EVALUATING','READY','BLOCKED','FAILED','FINALIZED'))`;
`manifest_hash`; `checkpoint_id`; `version` (CAS); `started_at`,
`completed_at`; `result`; `blockers`; `updated_at`. Index on `(state)`.
`EVALUATING` is an in-transaction intermediate, never persisted.

### v12 — artifact staging provenance

**artifact_stagings** — `(artifact_id, job_id)` PK; `worker_id`;
`fencing_token`; `uri`; `staged_at`. Index on `(job_id)`.
Per-job staging provenance: the authority for "this job staged these
bytes" under content-addressed dedup (artifact_id = sha256(bytes)).
