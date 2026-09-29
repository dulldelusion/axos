# 33 — Glossary

- **Actual state:** what the store says exists right now (workers, jobs,
  leases, artifacts, breakers).
- **Artifact:** a content-addressed output unit (`artifact_id` = content
  hash), with lifecycle STAGING → VALIDATED → RELEASED → QUARANTINED.
- **Authorization envelope:** the per-task declaration of allowed
  capabilities, domains, write scopes, external-action policy, and budgets.
- **Blast radius:** the set of components/jobs affected by an action or
  failure, monitored after every recovery action.
- **Bootstrap monitor:** the minimal external process that watches and
  restarts control-plane services; supervised by the init system.
- **Capability:** a versioned, typed unit of what can be done (e.g.
  `web-discovery`), composed into workers; never a fixed agent.
- **Checkpoint:** a verified recovery point (manifest + versions + ledger
  tip), not a timestamp.
- **Circuit breaker:** a per-scope state machine (CLOSED/OPEN/HALF_OPEN)
  that stops retries against repeatedly failing dependencies.
- **Composite verdict:** a worker's overall health = worst of the three
  axes, with defined overrides.
- **Continuation gate:** the guardrail-engine check "is it safe to continue?"
  evaluated before consequential transitions.
- **Desired state:** what should exist, declared from task spec + graph +
  policy.
- **Execution health:** is the work advancing? (progress vs expectations.)
- **Fencing token:** monotonic per-job ownership counter; only the holder of
  the current token can renew the lease or commit output.
- **Fenced commit:** the atomic transaction (artifact + provenance + job
  COMPLETE) conditional on the current fencing token.
- **Idempotency key:** deterministic job identity = hash(task, stage, unit);
  prevents duplicate materialization.
- **Integrity gate:** the finalization checklist; PASS → FINALIZED.
- **Latest-known-good:** the newest VERIFIED checkpoint; the only valid
  recovery target.
- **Lease:** the (owner, expiry, fencing token) triple on a job row; the
  ownership primitive.
- **Ledger:** the append-only, hash-chained event log; the authority on
  history.
- **Methodology:** versioned record of what "good" means for a task
  (success criteria, evidence, quality bar, progress expectations).
- **Orphan artifact:** bytes in staging (or rows) with no committed owner;
  classified by the artifact reconciler, never silently deleted.
- **Process health:** is the worker alive and communicating?
- **Provenance:** the hash-chained record of an artifact's lineage
  (inputs, transform, worker, validations, decisions, versions).
- **Reconciler:** the stateless, idempotent loop repairing desired−actual
  differences.
- **Recovery controller:** the owner of DETECT→CLASSIFY→DIAGNOSE→RECOVER→
  VERIFY→RESUME/ESCALATE and the escalation ladder.
- **Safe stop:** CHECKPOINT → PRESERVE → DIAGNOSE → REPLAN-or-ESCALATE; a
  first-class outcome.
- **Stage:** a named phase of a task's execution graph.
- **Stall:** heartbeats alive but no progress past `max_silence`; distinct
  from crash.
- **Transition gate:** the single validated store-access API; rejects
  illegal transitions and out-of-envelope actions.
- **UNCERTAIN:** the job state for "output may exist, completion unknown";
  resolved by inspection/validation, never blind retry.
- **UNKNOWN:** the health state for "insufficient evidence"; reported as
  unknown, never coerced.
- **Viability evaluation:** the task-level watchdog asking whether the
  objective is still achievable.
- **Work/output health:** is what the worker produces valid?
- **Worker:** a disposable execution instance composed from capabilities;
  holds no authority.
