"""Phase 1A — deterministic, versioned database migrations.

Requirements (from the Phase 1A brief, grounded in Phase 0's durability
demands):
- versioned migrations, applied in order
- repeatable: re-running migrate() on a current database is a no-op
- migration state recorded in the database itself (schema_migrations)
- a failing migration must not silently leave a partially migrated schema:
  each migration runs inside one transaction; on failure it rolls back and
  its version is not recorded
- a fresh database can be created from zero
- an existing database upgrades deterministically

Kept deliberately small: a list of (version, name, sql) tuples, no framework.
"""
from __future__ import annotations

import hashlib

from .db import Store, MigrationError


V1_SCHEMA = """
CREATE TABLE axos_meta(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);

CREATE TABLE tasks(
  task_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  objective TEXT NOT NULL,
  budgets TEXT NOT NULL,
  pause_reason TEXT,
  pause_diagnostic TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE jobs(
  job_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id),
  stage_id TEXT,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  owner_worker_id TEXT,
  fencing_token INTEGER NOT NULL DEFAULT 0,
  lease_acquired_at REAL,
  lease_expires_at REAL,
  worker_reported_ts REAL,
  progress_done REAL NOT NULL DEFAULT 0,
  progress_total REAL,
  result_artifact_id TEXT,
  policy TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE INDEX jobs_task_status ON jobs(task_id, status);
CREATE INDEX jobs_lease_expiry ON jobs(lease_expires_at)
  WHERE lease_expires_at IS NOT NULL;

CREATE TABLE workers(
  worker_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  task_id TEXT REFERENCES tasks(task_id),
  capabilities TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  last_seen_at REAL
);

CREATE TABLE approvals(
  approval_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id),
  job_id TEXT REFERENCES jobs(job_id),
  stage_id TEXT,
  reason TEXT NOT NULL,
  requested_action TEXT NOT NULL,
  evidence_refs TEXT NOT NULL DEFAULT '[]',
  risk TEXT,
  options TEXT NOT NULL DEFAULT '[]',
  deadline REAL,
  checkpoint_id TEXT,
  on_timeout TEXT,
  status TEXT NOT NULL DEFAULT 'PENDING',
  decided_by TEXT,
  decided_at REAL,
  decision_reason TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE incidents(
  incident_id TEXT PRIMARY KEY,
  task_id TEXT REFERENCES tasks(task_id),
  scope TEXT NOT NULL,
  failure_class TEXT NOT NULL,
  signature TEXT NOT NULL,
  detection TEXT,
  classification TEXT,
  diagnosis TEXT,
  actions TEXT NOT NULL DEFAULT '[]',
  outcome TEXT,
  escalated_to TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE INDEX incidents_signature ON incidents(signature);

CREATE TABLE recovery_attempts(
  attempt_id TEXT PRIMARY KEY,
  incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
  rung INTEGER NOT NULL,
  action TEXT NOT NULL,
  observed_effect TEXT NOT NULL,
  progress_delta REAL NOT NULL,
  output_health_delta REAL NOT NULL,
  decision TEXT NOT NULL
    CHECK(decision IN ('retry','escalate','success','quarantine','stop')),
  spend TEXT NOT NULL DEFAULT '{}',
  actor TEXT NOT NULL,
  recorded_at REAL NOT NULL,
  -- I-18 (Phase 0): recovery is verified by progress, not by action.
  -- A recovery recorded as 'success' with no positive progress delta is
  -- contradictory and is rejected by the store itself.
  CHECK (NOT (decision = 'success' AND progress_delta <= 0))
);

CREATE TABLE checkpoints(
  checkpoint_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id),
  stage_id TEXT,
  ledger_tip_seq INTEGER,
  completed_units TEXT NOT NULL DEFAULT '[]',
  remaining_units TEXT NOT NULL DEFAULT '[]',
  state_snapshot_ref TEXT,
  artifact_manifest TEXT NOT NULL DEFAULT '[]',
  input_version TEXT,
  schema_version TEXT,
  methodology_version TEXT,
  capability_versions TEXT NOT NULL DEFAULT '{}',
  os_version TEXT,
  supersedes TEXT REFERENCES checkpoints(checkpoint_id),
  verification_status TEXT NOT NULL DEFAULT 'UNVERIFIED',
  verified_at REAL,
  verify_method TEXT,
  sample_rate REAL,
  sample_seed INTEGER,
  sample_count INTEGER,
  sample_passed INTEGER,
  manifest_full INTEGER NOT NULL DEFAULT 0,
  ledger_chain_verified INTEGER NOT NULL DEFAULT 0,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE artifacts(
  artifact_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES tasks(task_id),
  kind TEXT,
  size INTEGER,
  uri TEXT,
  status TEXT NOT NULL DEFAULT 'STAGING',
  producer TEXT NOT NULL DEFAULT '{}',
  provenance_id TEXT,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);

CREATE TABLE validations(
  validation_id TEXT PRIMARY KEY,
  artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id),
  validator_id TEXT NOT NULL,
  validator_version TEXT NOT NULL,
  result TEXT NOT NULL CHECK(result IN ('PASS','FAIL','INCONCLUSIVE')),
  method TEXT,
  sampled INTEGER NOT NULL DEFAULT 0,
  sample_desc TEXT,
  receipt_ref TEXT,
  quarantined INTEGER NOT NULL DEFAULT 0,
  notes TEXT,
  validated_at REAL NOT NULL
);
CREATE INDEX validations_validator ON validations(validator_id, validator_version);

CREATE TABLE ledger(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  actor TEXT NOT NULL,
  ts REAL NOT NULL,
  prev_hash TEXT NOT NULL,
  hash TEXT NOT NULL
);
"""

# v2 (Phase 1B): durable heartbeat time-series (Phase 0 04-domain-model:
# Heartbeat is a durable record: worker_id, job_id?, ts = store time on
# ingest, worker_state, current_operation, ...). worker_reported_ts is
# informational only. Plus commit evidence columns on jobs so an execution
# result is reconstructible from the job row itself.
V2_HEARTBEATS = """
CREATE TABLE heartbeats(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id TEXT NOT NULL REFERENCES workers(worker_id),
  proc_id TEXT NOT NULL,
  job_id TEXT REFERENCES jobs(job_id),
  fencing_token INTEGER,
  hb_seq INTEGER NOT NULL,
  worker_state TEXT,
  current_operation TEXT,
  worker_reported_ts REAL,
  ts REAL NOT NULL
);
CREATE INDEX heartbeats_worker_proc ON heartbeats(worker_id, proc_id, hb_seq);
CREATE INDEX heartbeats_job_ts ON heartbeats(job_id, ts);
ALTER TABLE jobs ADD COLUMN commit_outcome TEXT;
ALTER TABLE jobs ADD COLUMN commit_evidence TEXT;
"""

# v3 (Phase 1C R3 / contract D9): durable progress-evidence signal.
# jobs.progress_updated_at is stamped with authoritative store time by
# update_job_progress (and by heartbeat ingestion when it carries progress).
# It is informational only — never recovery evidence (D6: the recovery
# controller computes observed progress from durable diffs). Nullable with
# no backfill: NULL honestly means "no informational progress recorded
# since this column existed". Heartbeat rows stay liveness-only (D9): no
# progress columns are added to heartbeats.
V3_PROGRESS_UPDATED_AT = """
ALTER TABLE jobs ADD COLUMN progress_updated_at REAL;
"""

# v4 (Phase 1C R5): verified artifact/checkpoint integrity (I-4 correction).
# - artifacts: job linkage, content hash identity, execution provenance
#   (owner/token/attempt at staging). content_hash backfilled from the
#   legacy artifact_id, which already was the content hash by convention.
# - validations: the exact content hash the validator saw, recorded
#   per receipt (receipts never apply across content hashes).
# - jobs: content_hash referenced by the atomic completion transaction.
# - checkpoints: deterministic identity support (canonical_manifest),
#   durable verification receipts, and the creation trigger.
# - checkpoint_pointers: the latest-known-good pointer, advanced only by
#   the verified-checkpoint path (never by the legacy record API).
V4_ARTIFACT_INTEGRITY = """
ALTER TABLE artifacts ADD COLUMN job_id TEXT;
ALTER TABLE artifacts ADD COLUMN content_hash TEXT;
ALTER TABLE artifacts ADD COLUMN attempt INTEGER;
ALTER TABLE artifacts ADD COLUMN owner_worker_id TEXT;
ALTER TABLE artifacts ADD COLUMN fencing_token INTEGER;
CREATE INDEX artifacts_job ON artifacts(job_id);
UPDATE artifacts SET content_hash = artifact_id WHERE content_hash IS NULL;
ALTER TABLE validations ADD COLUMN content_hash TEXT;
UPDATE validations SET content_hash = artifact_id WHERE content_hash IS NULL;
ALTER TABLE jobs ADD COLUMN content_hash TEXT;
ALTER TABLE checkpoints ADD COLUMN trigger TEXT;
ALTER TABLE checkpoints ADD COLUMN canonical_manifest TEXT;
ALTER TABLE checkpoints ADD COLUMN verification_receipt TEXT;
CREATE TABLE checkpoint_pointers(
  name TEXT PRIMARY KEY,
  checkpoint_id TEXT NOT NULL REFERENCES checkpoints(checkpoint_id),
  task_id TEXT,
  updated_at REAL NOT NULL
);
"""

# v5 (Phase 1C R7): watchdog verdict records. The watchdog DETECTS and
# CLASSIFIES only — it never reclaims, fences, or recovers. Verdicts are
# recorded per execution identity (job_id, fencing_token): once DEAD for an
# identity, that identity stays DEAD; a new fencing token starts a new
# identity. Rows are written only on verdict transitions (idempotent by
# construction); the authoritative mutation stream stays in the ledger
# (watchdog.verdict events).
V5_WATCHDOG_VERDICTS = """
CREATE TABLE watchdog_verdicts(
  verdict_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(job_id),
  fencing_token INTEGER NOT NULL,
  verdict TEXT NOT NULL,
  evidence TEXT NOT NULL,
  evaluated_at REAL NOT NULL,
  actor TEXT NOT NULL
);
CREATE INDEX watchdog_verdicts_job_token
  ON watchdog_verdicts(job_id, fencing_token, evaluated_at);
"""

# v6 (Phase 1C R8): recovery-controller attempt state. The store already
# records terminal recovery evidence (recovery_attempts, Phase 1A); R8 adds
# the durable attempt STATE MACHINE the recovery controller needs:
# CREATED -> RUNNING -> VERIFYING -> SUCCEEDED | FAILED, CREATED -> BLOCKED,
# RUNNING -> UNCERTAIN. Every attempt binds the execution identity
# (job_id, fencing_token) it may act on, carries the full Recovery Contract
# fields (incident, classification, rung, attempt number, budget context,
# success/failure criteria, progress evidence before/after, resulting
# state, escalation target), and is claimed by exactly one controller via
# compare-and-swap. UNIQUE(incident_id, attempt_number) keeps attempt
# numbers monotonic per incident; the I-18 CHECK (success requires
# positive progress delta) is inherited from the base table.
V6_RECOVERY_ATTEMPT_STATE = """
ALTER TABLE recovery_attempts ADD COLUMN job_id TEXT;
ALTER TABLE recovery_attempts ADD COLUMN fencing_token INTEGER;
ALTER TABLE recovery_attempts ADD COLUMN attempt_number INTEGER;
ALTER TABLE recovery_attempts ADD COLUMN attempt_state TEXT NOT NULL
  DEFAULT 'CREATED'
  CHECK(attempt_state IN ('CREATED','RUNNING','VERIFYING','SUCCEEDED',
                          'FAILED','BLOCKED','UNCERTAIN'));
ALTER TABLE recovery_attempts ADD COLUMN rung_name TEXT;
ALTER TABLE recovery_attempts ADD COLUMN started_at REAL;
ALTER TABLE recovery_attempts ADD COLUMN verify_after REAL;
ALTER TABLE recovery_attempts ADD COLUMN controller_id TEXT;
ALTER TABLE recovery_attempts ADD COLUMN claimed_at REAL;
ALTER TABLE recovery_attempts ADD COLUMN failure_class TEXT;
ALTER TABLE recovery_attempts ADD COLUMN success_criterion TEXT;
ALTER TABLE recovery_attempts ADD COLUMN failure_criterion TEXT;
ALTER TABLE recovery_attempts ADD COLUMN resulting_state TEXT;
ALTER TABLE recovery_attempts ADD COLUMN escalation_target TEXT;
ALTER TABLE recovery_attempts ADD COLUMN evidence_before TEXT;
ALTER TABLE recovery_attempts ADD COLUMN evidence_after TEXT;
ALTER TABLE recovery_attempts ADD COLUMN idempotency_key TEXT;
ALTER TABLE recovery_attempts ADD COLUMN budget_context TEXT;
CREATE UNIQUE INDEX recovery_attempts_incident_number
  ON recovery_attempts(incident_id, attempt_number);
-- One incident per (job_id, failure_class): the signature is the stable
-- identity, so concurrent controllers collapse to one row (the loser
-- re-reads the winner). Partial: the pre-R8 incident API keeps its own
-- non-unique signature semantics for other scopes.
CREATE UNIQUE INDEX incidents_recovery_signature
  ON incidents(signature) WHERE scope='recovery';
"""

# v9 DDL (Phase 1C R11): kept as module constants so the migration entry
# stays a plain 3-tuple; the seed INSERT is computed, not a literal.
_V9_DESIRED_STATE_DDL = """
CREATE TABLE desired_state(
  desired_work_id TEXT PRIMARY KEY,
  spec TEXT NOT NULL,
  retired INTEGER NOT NULL DEFAULT 0,
  version INTEGER NOT NULL,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE desired_state_head(
  key TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  snapshot_hash TEXT NOT NULL,
  updated_at REAL NOT NULL
);
CREATE TABLE desired_job_map(
  desired_work_id TEXT PRIMARY KEY
    REFERENCES desired_state(desired_work_id),
  job_id TEXT NOT NULL UNIQUE,
  spec_hash TEXT NOT NULL,
  desired_version INTEGER NOT NULL,
  created_at REAL NOT NULL
);
CREATE TABLE reconciliation_runs(
  reconciliation_id TEXT PRIMARY KEY,
  desired_state_version INTEGER NOT NULL,
  snapshot_hash TEXT NOT NULL,
  started_at REAL NOT NULL,
  completed_at REAL,
  result TEXT NOT NULL,
  items_examined INTEGER NOT NULL DEFAULT 0,
  items_created INTEGER NOT NULL DEFAULT 0,
  last_item_id TEXT,
  discrepancies TEXT NOT NULL DEFAULT '[]',
  actor TEXT NOT NULL
);
"""
_V9_SEED_HEAD_SQL = (
    "INSERT INTO desired_state_head(key, version, snapshot_hash,"
    " updated_at) VALUES('head', 0, '"
    + hashlib.sha256(b"[]").hexdigest()
    + "', 0);"
)


# (version, name, sql). Versions strictly increasing; each applied atomically.
MIGRATIONS = [
    (1, "phase1a initial schema", V1_SCHEMA),
    (2, "phase1b heartbeats + commit evidence", V2_HEARTBEATS),
    (3, "phase1c r3 jobs.progress_updated_at", V3_PROGRESS_UPDATED_AT),
    (4, "phase1c r5 artifact/checkpoint integrity", V4_ARTIFACT_INTEGRITY),
    (5, "phase1c r7 watchdog verdict records", V5_WATCHDOG_VERDICTS),
    (6, "phase1c r8 recovery attempt state", V6_RECOVERY_ATTEMPT_STATE),
    (
        7,
        "phase1c r9 recovery policy state",
        # One row per recovery incident: the durable home of R9's ladder
        # position, budgets, and escalation state. All R9 policy mutations
        # are compare-and-swap UPDATEs on `version` inside a single
        # write_txn; budget consumption is additionally guarded by
        # remaining_budget bounds in the same UPDATE, so two concurrent
        # controllers produce exactly one authoritative consumption.
        # No time budget: the contract (§E.2) defines only per-rung and
        # per-incident attempt budgets, so none is invented here.
        """
        CREATE TABLE recovery_policy(
          incident_id TEXT PRIMARY KEY REFERENCES incidents(incident_id),
          policy_version TEXT NOT NULL,
          version INTEGER NOT NULL DEFAULT 1,
          current_rung INTEGER NOT NULL,
          rung_name TEXT NOT NULL,
          incident_budget INTEGER NOT NULL,
          per_rung_budgets TEXT NOT NULL DEFAULT '{}',
          attempt_count INTEGER NOT NULL DEFAULT 0,
          per_rung_attempts TEXT NOT NULL DEFAULT '{}',
          remaining_budget INTEGER NOT NULL,
          consumed_attempt_number INTEGER NOT NULL DEFAULT 0,
          result_consumed_attempt_number INTEGER NOT NULL DEFAULT 0,
          zero_progress_count INTEGER NOT NULL DEFAULT 0,
          last_attempt_id TEXT,
          last_attempt_result TEXT,
          escalation_state TEXT NOT NULL DEFAULT 'none',
          escalation_target TEXT,
          terminal_state TEXT,
          superseded_by TEXT,
          updated_at REAL NOT NULL
        );
        """,
    ),
    (
        8,
        "phase1c r10 scheduler claim liveness beats",
        # One row per scheduler-owned claim worker_id: the durable
        # liveness signal that lets a scheduler's orphan-redispatch path
        # distinguish "the owning scheduler is alive and mid-dispatch"
        # from "the owning scheduler died and its claim is a true orphan".
        # The beat is planted atomically inside claim_job_bounded's
        # write_txn (claim ⟹ beat, no window), refreshed by the owning
        # scheduler on every evaluate_once, withdrawn when dispatch
        # fails, and cleared on graceful close. A beat older than the
        # scheduler's stale_after_s means the owner is dead: the orphan
        # becomes re-dispatchable. Crash between claim and dispatch is
        # therefore self-healing with a bounded delay, while a live
        # scheduler's fresh claim is never stolen (R10-04: exactly one
        # dispatch under racing schedulers).
        """
        CREATE TABLE scheduler_claim_beats(
          worker_id TEXT PRIMARY KEY,
          scheduler_id TEXT NOT NULL,
          last_beat REAL NOT NULL
        );
        CREATE INDEX scheduler_claim_beats_sid
          ON scheduler_claim_beats(scheduler_id);
        """,
    ),
    (
        9,
        "phase1c r11 desired-state reconciliation",
        # One row per declared desired-work item. spec is canonical JSON
        # (task_id + optional stage_id/max_attempts/policy, normalized by
        # the gate so the hash is stable). retired=1 is a tombstone: the
        # reconciler records OBSOLETE for it but never deletes the row,
        # never deletes the job, and never transitions the job — PENDING
        # has no legal cancel edge in JOB_TRANSITIONS, and the reconciler
        # owns no transition authority at all.
        #
        # desired_state_head is the single CAS row (key='head'): `version`
        # starts at 0 and every set/retire recomputes `snapshot_hash`
        # (sha256 of the canonical JSON of ALL items, retired included)
        # and bumps version by exactly +1 inside one write_txn. BEGIN
        # IMMEDIATE serializes writers; the UPDATE's WHERE clause pins the
        # expected version, so a lost race raises PolicyConflict and the
        # loser re-reads the authoritative head instead of forking the
        # desired state. The reconciler pins the version it read and
        # refuses to create anything on a stale pin (CONFLICT, fail
        # closed).
        #
        # desired_job_map is the canonical identity: one deterministic
        # job_id per desired_work_id, bound to the spec_hash it was
        # created from. A job row without its map row, a map row without
        # its job row, or a spec_hash drift under an existing mapping is
        # contradictory actual state (DesiredStateConflict): the gate
        # never adopts a foreign job, never deletes, never recreates —
        # creation is idempotent only when identity AND spec both match.
        #
        # reconciliation_runs is the durable record of each converge pass:
        # RUNNING -> CONVERGED | CHANGED | BLOCKED | CONFLICT | FAILED,
        # with a per-batch cursor (last_item_id, items_examined,
        # items_created) so a crashed pass is observable and resumable by
        # inspection. discrepancies is canonical JSON (a list).
        #
        # No semicolons inside string literals anywhere below (the
        # _split_statements constraint).
        #
        # The head row is seeded at version 0 with the empty-snapshot
        # hash; it is concatenated (not part of the DDL literal) because
        # the hash is computed, not a string literal.
        _V9_DESIRED_STATE_DDL + _V9_SEED_HEAD_SQL,
    ),
    (
        10,
        "phase1c r12 circuit-breaker state + failure signals",
        # Admission control in front of claims (R12): one breaker row per
        # scope (GLOBAL / TASK / DESIRED / JOB), with the canonical
        # CLOSED -> OPEN -> HALF_OPEN -> CLOSED|OPEN state machine. The
        # gate owns every mutation; writers serialize on BEGIN IMMEDIATE
        # and every state change is a version-pinned CAS UPDATE, so two
        # racing controllers produce exactly one winner and the loser
        # re-reads (BreakerConflict). breaker_signals is the append-only,
        # exactly-once failure journal feeding the per-scope windowed
        # failure_count: the (failure_kind, incident_id, attempt_id) tuple
        # is the canonical dedupe key (INSERT OR IGNORE), so a retried
        # signal delivery can never double-count a failure.
        #
        # No semicolons inside string literals anywhere below (the
        # _split_statements constraint). Inline -- comments are mid-line,
        # so the splitter is unaffected.
        """
        CREATE TABLE breaker_state(
          scope_type TEXT NOT NULL,       -- 'GLOBAL' | 'JOB' | 'TASK' | 'DESIRED'
          scope_id TEXT NOT NULL,         -- 'global' | job_id | task_id | desired_work_id
          state TEXT NOT NULL DEFAULT 'CLOSED',
          version INTEGER NOT NULL DEFAULT 0,
          failure_count INTEGER NOT NULL DEFAULT 0,
          window_started_at REAL,         -- NULL until first counted failure in window
          cooldown_s REAL NOT NULL DEFAULT 60.0,
          opened_at REAL,
          cooldown_until REAL,
          last_failure_id TEXT,
          half_open_probe_id TEXT,        -- current probe allocation, NULL when none
          half_open_probes_used INTEGER NOT NULL DEFAULT 0,
          half_open_probe_at REAL,        -- allocation time (authoritative)
          half_open_probe_baseline TEXT,  -- canonical JSON {progress_done, progress_total, fencing_token, status}
          updated_at REAL NOT NULL,
          PRIMARY KEY (scope_type, scope_id)
        );
        CREATE TABLE breaker_signals(
          signal_id TEXT PRIMARY KEY,     -- canonical dedupe key
          scope_type TEXT NOT NULL,
          scope_id TEXT NOT NULL,
          failure_kind TEXT NOT NULL,     -- 'R7_DEAD' | 'R8_ATTEMPT_FAILED' | 'R9_ESCALATED' | 'RECOVERY_PRESSURE'
          incident_id TEXT,
          attempt_id TEXT,
          observed_at REAL NOT NULL,
          actor TEXT NOT NULL
        );
        CREATE INDEX breaker_signals_scope ON breaker_signals(scope_type, scope_id, observed_at);
        """,
    ),
    (
        11,
        "phase1c r13 finalization runs",
        # One row per release generation: the durable home of R13's
        # finalization state machine OPEN -> EVALUATING -> READY | BLOCKED
        # | FAILED, with FINALIZED terminal. release_generation is the
        # canonical identity ("ds-v" + desired_state_version, UNIQUE); the
        # finalization_id is the deterministic "fin-" + sha256
        # domain-tagged identity (PRIMARY KEY, same canonical pattern as
        # R11's desired-job identities). Every state change is a
        # version-pinned CAS UPDATE inside one write_txn with a ledger
        # event; the loser gets FinalizationConflict and re-reads.
        # manifest_hash commits to the canonical release manifest (no
        # wall-clock, no pids, no uuids inside the hashed content —
        # timestamps live on this row, outside the hash); checkpoint_id
        # names the R5 release checkpoint (staged + verified exclusively
        # through stage_checkpoint/verify_checkpoint — no second
        # checkpoint authority); blockers is canonical JSON. EVALUATING is
        # an in-transaction intermediate and is never persisted.
        #
        # No semicolons inside string literals anywhere below (the
        # _split_statements constraint). Inline -- comments are mid-line,
        # so the splitter is unaffected.
        """
        CREATE TABLE finalization_runs(
          finalization_id TEXT PRIMARY KEY,
          release_generation TEXT NOT NULL UNIQUE,
          desired_state_version INTEGER NOT NULL,
          state TEXT NOT NULL
            CHECK(state IN ('OPEN','EVALUATING','READY','BLOCKED','FAILED',
                            'FINALIZED')),
          manifest_hash TEXT,
          checkpoint_id TEXT,
          version INTEGER NOT NULL DEFAULT 1,
          started_at REAL,
          completed_at REAL,
          result TEXT,
          blockers TEXT NOT NULL DEFAULT '[]',
          updated_at REAL NOT NULL
        );
        CREATE INDEX finalization_runs_state ON finalization_runs(state);
        """,
    ),
    (
        12,
        "phase1c r14 per-job artifact staging provenance",
        # R14-125 finding: content-addressed artifacts dedup at the byte
        # level (artifact_id = sha256(bytes), INSERT ... ON CONFLICT DO
        # NOTHING), so when two jobs produce identical bytes the second
        # job's staging is a dedup hit and the single artifact row keeps
        # the FIRST stager's job_id. The R5 linkage rule (begin_commit /
        # commit_artifact require art.job_id == job_id) then made the
        # second job's legitimate output uncompletable — a liveness defect
        # in the completion contract, contradicting the dedup design.
        #
        # artifact_stagings records per-job staging provenance: one row
        # per (artifact_id, job_id) that staged the bytes under a live
        # lease. The row's own job_id remains first-writer metadata; the
        # staging record is the authority for "this job staged these
        # bytes". Rows predate v12 lack staging records; for those the
        # legacy art.job_id == job_id check still applies.
        #
        # No semicolons inside string literals (the _split_statements
        # constraint).
        """
        CREATE TABLE artifact_stagings(
          artifact_id TEXT NOT NULL,
          job_id TEXT NOT NULL,
          worker_id TEXT NOT NULL,
          fencing_token INTEGER NOT NULL,
          uri TEXT NOT NULL,
          staged_at REAL NOT NULL,
          PRIMARY KEY(artifact_id, job_id)
        );
        CREATE INDEX artifact_stagings_job ON artifact_stagings(job_id);
        """,
    ),
]


def _ensure_migrations_table(store: Store) -> None:
    # Privileged setup path: migrations hold the writable Store transiently,
    # so they use its private writable connection. (store.conn is read-only.)
    store._conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations("
        "version INTEGER PRIMARY KEY, "
        "name TEXT NOT NULL, "
        "applied_at REAL NOT NULL)"
    )
    store._conn.commit()


def applied_versions(store: Store) -> list[int]:
    _ensure_migrations_table(store)
    rows = store.conn.execute(
        "SELECT version FROM schema_migrations ORDER BY version"
    ).fetchall()
    return [r[0] for r in rows]


def _split_statements(sql: str) -> list[str]:
    # Our migrations are plain DDL/DML with no semicolons inside string
    # literals or triggers, so a simple split is exact. Each migration is
    # reviewed with this constraint in mind; a future migration needing
    # triggers must extend this splitter rather than silently mis-splitting.
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p and not p.startswith("--")]


def migrate(store: Store, migrations: list | None = None) -> list[int]:
    """Apply pending migrations in order. Returns the versions applied now.

    Each migration runs inside a single transaction: if its SQL fails, the
    transaction rolls back, the version is not recorded, and MigrationError
    is raised. The database is left exactly as it was before the attempt.
    """
    migrations = MIGRATIONS if migrations is None else migrations
    _ensure_migrations_table(store)
    done = set(applied_versions(store))
    applied_now: list[int] = []
    for version, name, sql in sorted(migrations, key=lambda m: m[0]):
        if version in done:
            continue
        conn = store._conn  # privileged: migrations run DDL, see above
        if conn.in_transaction:
            raise MigrationError("cannot migrate inside an open transaction")
        conn.execute("BEGIN IMMEDIATE")
        try:
            # NOTE: executescript() cannot be used here — it implicitly
            # commits any open transaction, which would break atomicity.
            for stmt in _split_statements(sql):
                conn.execute(stmt)
            conn.execute(
                "INSERT INTO schema_migrations(version, name, applied_at) "
                "VALUES(?, ?, ?)",
                (version, name, store.current_time()),
            )
            conn.execute("COMMIT")
        except Exception as exc:
            conn.execute("ROLLBACK")
            raise MigrationError(
                f"migration v{version} ({name}) failed and was rolled back: {exc}"
            ) from exc
        applied_now.append(version)
        done.add(version)
    return applied_now
