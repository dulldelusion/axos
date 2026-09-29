# 28 — Failure-Injection Test Suite

Each test specifies: initial state, injected failure, expected detection,
expected classification, expected recovery, expected persisted state,
expected final state, and invariants that must hold. These are design
acceptance criteria: the architecture is production-ready only when every
test is answerable and the answers are implemented and verified.

---

**TEST-001 — Kill worker during execution.**
Initial: job RUNNING under worker W, lease valid, heartbeats flowing.
Inject: SIGKILL W mid-execution, no output staged.
Detection: L1 — heartbeat age crosses dead threshold; lease expires.
Classification: WORKER_FAILURE.
Recovery: lease reclaimed (fencing token bumped), job → PENDING, fresh
worker claims, attempt incremented.
Persisted: incident, `worker_dead`, `job_lease_reclaimed`, new attempt
record.
Final: job COMPLETE under new worker.
Invariants: I-3 (only current token commits), I-4 (no completion without
committed artifact), I-6 (attempt counted against budget).

**TEST-002 — Kill worker after output creation but before acknowledgement.**
Initial: job RUNNING; W staged valid artifact bytes but crashed before the
fenced commit.
Inject: SIGKILL after staging, before commit.
Detection: L1 dead-worker verdict; job has dead owner → UNCERTAIN.
Classification: JOB_FAILURE (uncertain completion).
Recovery: UNCERTAIN reconciliation — staged artifact found, hash valid,
validators pass, token lineage clean → commit → COMPLETE. No re-execution.
Persisted: `job_uncertain_opened/resolved`, commit under original attempt.
Final: job COMPLETE with the staged artifact; no duplicate work.
Invariants: I-8 (uncertainty reconciled, not blindly retried), I-4.

**TEST-003 — Worker sends heartbeat but makes no progress.**
Initial: job RUNNING; W heartbeats every interval, `last_progress_ts` frozen.
Inject: worker wedged on a blocking call (simulated).
Detection: L1 execution-health axis — `now − last_progress_ts >
max_silence` → STALLED verdict while process HEALTHY.
Classification: WORKER_FAILURE (stall), not a crash.
Recovery: rung 3 — terminate instance, reclaim lease, fresh worker re-claims;
if the replacement also stalls on the same job → poison suspected →
quarantine job.
Persisted: STALLED verdict, incident, replacement records.
Final: job COMPLETE (or QUARANTINED if poison).
Invariants: I-15 (stall reported as STALLED, never "healthy"), I-6.

**TEST-004 — Worker loses network connectivity.**
Initial: job RUNNING; W needs an external API mid-job.
Inject: drop egress for W only.
Detection: execution STALLED (no progress) + sibling workers healthy.
Classification: EXTERNAL_SERVICE_FAILURE (worker-scoped) — *not*
WORKER_FAILURE, because the worker is fine.
Recovery: no worker restart (wouldn't help); job backoff-requeued; if
siblings also fail → service breaker opens (rung: breaker, not worker).
Persisted: incident with scope evidence (one worker vs many).
Final: job COMPLETE after connectivity returns; no worker churn.
Invariants: classification discipline (`14`); I-6 (no pointless restarts).

**TEST-005 — Worker repeatedly fails.**
Initial: healthy task; one worker fails every job it touches.
Inject: worker with corrupted capability config.
Detection: work-health axis — output pass rate collapses for W only.
Classification: WORKER_FAILURE with `fast_but_wrong` flag.
Recovery: quarantine W (not restart — restarts don't fix config), reassign
its jobs; incident triggers capability-version review.
Persisted: quarantine record, job reassignments, decision journal entry.
Final: jobs COMPLETE under healthy workers; W RETIRED.
Invariants: I-9 not triggered spuriously; composite-verdict override (`10`).

**TEST-006 — Lease expires while worker is still alive.**
Initial: W RUNNING but partitioned from the store (can't renew); lease lapses.
Inject: partition W's store path only.
Detection: L2 — `lease_expires_at < store now`; fencing token bumped on
reclaim; job re-claimed by W2.
Classification: JOB_FAILURE (stale lease), worker W SUSPECT.
Recovery: W2 executes; if W's partition heals mid-work, W's commit is
rejected (stale token) → W self-fences back to IDLE. W2's commit wins.
Persisted: token bump history, rejected-commit record (evidence, not error).
Final: exactly one committed artifact; no overwrite race.
Invariants: I-3 (fencing decides the winner — this is the test that proves
it).

**TEST-007 — Duplicate worker attempts to claim the same job.**
Initial: job PENDING; two workers race to claim.
Inject: simultaneous claim transactions.
Detection: none needed — prevention, not detection.
Classification: n/a (correctness property).
Recovery: n/a.
Persisted: exactly one `job_claimed` event; the loser's conditional UPDATE
affects 0 rows.
Final: one owner, one fencing token.
Invariants: I-3; duplicate-claim impossible by construction.

**TEST-008 — Supervisor crashes.**
Initial: task EXECUTING; kill all control-plane service processes at once.
Inject: SIGKILL the control plane (workers keep running).
Detection: L5 — peer liveness records go stale; bootstrap monitor fires.
Classification: SYSTEM_FAILURE.
Recovery: bootstrap monitor restarts services with backoff; each boots
stateless, runs reconciler once, resumes. Workers with valid leases never
stopped.
Persisted: `service_crashed_detected`, restart records, reconciliation
summary.
Final: task EXECUTING, no jobs lost, no duplicate claims.
Invariants: I-1 (no truth lived in the dead processes), I-13.

**TEST-009 — Recovery controller crashes mid-recovery.**
Initial: incident open; controller executing rung 3 (worker restart).
Inject: kill controller mid-action.
Detection: L5; incident remains open with partial action records.
Classification: SYSTEM_FAILURE.
Recovery: restarted controller reads incident + budgets from the store,
sees rung 3 was *issued* but not *verified* → re-verifies (did the
replacement appear?) before acting — never blindly re-issues.
Persisted: budgets unchanged (no double-spend); verification-first resume.
Final: recovery completes or escalates correctly.
Invariants: I-6 (budgets in store, not memory), verify-before-act.

**TEST-010 — Scheduler crashes mid-fan-out.**
Initial: FAN_OUT expanding 1,000 jobs; scheduler dies after 400.
Inject: kill scheduler.
Detection: L5; expansion cursor in store shows 400/1000.
Classification: SYSTEM_FAILURE.
Recovery: restarted scheduler resumes expansion; inserts are idempotent on
`idempotency_key` → jobs 1–400 linked, 401–1000 created. No duplicates.
Persisted: single `fanout_expanded` completion; 1000 job rows, not 1400.
Final: all jobs materialized exactly once.
Invariants: idempotency keys (`07`), I-2.

**TEST-011 — Persistent storage temporarily unavailable.**
Initial: task EXECUTING; store unreachable for 90s.
Inject: block store I/O.
Detection: L5/L6 — write failures; health monitors → UNKNOWN (not DEAD).
Classification: SYSTEM_FAILURE (storage).
Recovery: workers pause claims/commits (can't — store is the commit path);
heartbeats buffer locally (bounded) and flush on recovery; control plane
waits (no destructive action while blind). On recovery: flush, reconcile,
resume. If outage exceeds threshold → safe stop with checkpoint attempt on
return.
Persisted: gap in heartbeat series marked as outage, not as worker deaths.
Final: task resumes; no worker was falsely declared DEAD for the outage.
Invariants: I-15 (UNKNOWN, not DEAD); no action while blind.

**TEST-012 — VM restarted during active execution.**
Initial: task EXECUTING, 50 jobs in flight.
Inject: hard reboot.
Detection: boot script (L6).
Classification: SYSTEM_FAILURE (infrastructure).
Recovery: the 15-step boot lifecycle (`11.4`): workers marked dead,
leases reclaimed, UNCERTAIN reconciliation, checkpoint verify, diff repair,
reprovision, resume.
Persisted: `vm_boot_recovery_started/complete`, all transitions ledgered.
Final: task EXECUTING; completed work preserved via checkpoints +
UNCERTAIN commits; in-flight work re-executed at most once effectively.
Invariants: I-1, I-7 (only verified checkpoints trusted), I-4.

**TEST-013 — VM abruptly terminated (no reboot).**
Initial: as TEST-012, but the VM never comes back.
Inject: terminate instance; restore from backup on a new VM.
Detection: operator/platform initiates restore (out of scope for
self-detection — declared).
Classification: SYSTEM_FAILURE (infrastructure replacement).
Recovery: provision new VM from image + latest verified store backup;
run boot recovery; verify ledger chain from backup.
Persisted: backup manifest; recovery report notes the backup's timestamp —
work after the backup is reconstructed from artifacts where possible.
Final: task resumes from latest verified checkpoint; post-backup completed
units recovered via artifact reconciliation where evidence exists,
otherwise re-executed.
Invariants: I-7; honest reporting of the recovery point (never claim
continuity that didn't happen).

**TEST-014 — Latest checkpoint is corrupted.**
Initial: checkpoints 1, 2 VERIFIED; checkpoint 3 written but torn.
Inject: corrupt checkpoint 3's manifest.
Detection: checkpoint verification fails (hash mismatch) → marked CORRUPT.
Classification: INTEGRITY_FAILURE.
Recovery: fall back to checkpoint 2 (latest VERIFIED); replay ledger after
its tip; UNCERTAIN-reconcile the gap.
Persisted: `checkpoint_corrupt` incident; checkpoint 3 retained as evidence.
Final: task resumes from checkpoint 2 + replay; no reliance on corrupt data.
Invariants: I-7 (unverified ≠ recovery point).

**TEST-015 — Output artifact exists but state says it doesn't.**
Initial: staged valid artifact for job J (from a crashed attempt); job
PENDING.
Inject: (the crash); then run artifact reconciliation.
Detection: orphan scan finds staged bytes with valid hash, no artifact row.
Classification: JOB_FAILURE (uncertain completion) → resolved by adoption.
Recovery: validate → adopt → commit → job COMPLETE without re-execution.
Persisted: adoption record with provenance to the original attempt.
Final: job COMPLETE; work not duplicated.
Invariants: I-8, I-4.

**TEST-016 — State says artifact exists but artifact is missing.**
Initial: job COMPLETE, artifact row present; bytes deleted from storage.
Inject: delete the bytes.
Detection: integrity check / gate finds hash target unreachable.
Classification: INTEGRITY_FAILURE.
Recovery: completion distrusted → job → UNCERTAIN → re-execute (bytes are
gone; nothing to adopt); incident raised; storage layer investigated.
Persisted: incident; job state regression is ledgered (forward transition,
not history rewrite).
Final: job re-COMPLETE with fresh bytes; old artifact row marked
`bytes_missing`, retained for forensics.
Invariants: state never trusted over reality (`24.2` mirror case).

**TEST-017 — External API becomes unavailable.**
Initial: 20 workers calling api.vendor.com; API goes down.
Inject: blackhole the API.
Detection: L2/L3 — correlated failures across workers; breaker threshold
crossed.
Classification: EXTERNAL_SERVICE_FAILURE.
Recovery: service breaker OPENS; affected jobs → BLOCKED (not FAILED);
checkpoint taken; no retry storm. Half-open probes after cooldown; on
success, drain BLOCKED queue.
Persisted: breaker transitions, BLOCKED (not FAILED) job states.
Final: jobs COMPLETE after API returns; attempt budgets unspent by the
outage.
Invariants: breakers suppress retries (`15.4`); correlated failures
escalate once, not 20 times (`10.4`).

**TEST-018 — External API changes schema.**
Initial: extraction jobs parsing api.vendor.com responses; vendor ships v2.
Inject: change response schema mid-task.
Detection: validators reject outputs (schema mismatch) across many jobs;
viability evaluation flags source-layer change.
Classification: DATA_FAILURE (schema change), not CONTENT_FAILURE.
Recovery: quarantine affected units; planner/methodology engine assesses:
adapter possible? → replan with new parser version (journaled). Not
possible? → PAUSED_FOR_HUMAN with the schema diff as evidence.
Persisted: quarantine reasons, replan decision, new capability version pin.
Final: task resumes on new schema or waits for human — never keeps
"retrying" the old parser.
Invariants: `14` rule 4 (never blindly retry methodology/data failures).

**TEST-019 — Validation failure rate suddenly spikes.**
Initial: steady 2% validation failures; spike to 60%.
Inject: poison a batch of inputs (or break a validator version).
Detection: stage work-health axis; spike alert.
Classification: ambiguous initially → diagnosis: if failures cluster by
input batch → DATA_FAILURE (poison); if by validator version → 
VALIDATION_FAILURE; if by worker → WORKER_FAILURE.
Recovery: pause affected stage (breaker), quarantine the batch, diagnose
before any mass retry. Fix the cause (purge batch / roll back validator),
then resume.
Persisted: spike incident with cluster analysis; quarantine records.
Final: stage resumes; poisoned units stay quarantined with reasons.
Invariants: I-9 (stop before burning budget); classification discipline.

**TEST-020 — Recovery loop occurs.**
Initial: job fails → requeued → fails identically, 4 cycles.
Inject: poisoned input that always fails the same validator.
Detection: loop detector — same job, same error class, N cycles in window.
Classification: recovery_loop (escalation trigger); underlying: DATA_FAILURE.
Recovery: recovery-loop breaker opens for that signature; job →
QUARANTINED (poison_suspected); incident escalated; no 5th retry.
Persisted: loop incident, quarantine, journal entry ("why we stopped
retrying").
Final: task continues with remaining work; quarantined job awaits human or
replan.
Invariants: I-6 (bounded recovery); I-9.

**TEST-021 — Resource limit reached.**
Initial: task near storage quota; a stage fans out aggressively.
Inject: quota hit mid-execution.
Detection: resource governor — hard limit.
Classification: RESOURCE_FAILURE.
Recovery: THROTTLED → PAUSED for the task; in-flight jobs drain; no new
claims; checkpoint; alert. Resume only after capacity (retention cleanup or
human quota raise).
Persisted: pressure transitions, pause record.
Final: task resumes when capacity returns; nothing deleted to make room.
Invariants: `22.3` (never solve exhaustion by deleting evidence).

**TEST-022 — Execution graph becomes invalid.**
Initial: task EXECUTING; a CONDITIONAL references a stage removed by an
operator edit (simulated corruption of graph doc).
Inject: corrupt the active graph version.
Detection: graph validation on load / transition gate rejects transitions
referencing unknown nodes.
Classification: INTEGRITY_FAILURE.
Recovery: safe stop; fall back to last valid graph version; jobs already
materialized keep their ids; replan to a corrected version (journaled) or
human.
Persisted: safe-stop record, version fallback.
Final: task resumes on corrected graph; no job silently redefined.
Invariants: graph immutability (`08`); I-9.

**TEST-023 — Methodology becomes invalid.**
Initial: research task; methodology assumes source X is authoritative;
source X is shown to be fabricated (evidence contradicts).
Inject: viability evaluation receives contradicting evidence.
Detection: methodology invalidation signal → METHODOLOGY_FAILURE.
Classification: METHODOLOGY_FAILURE (never retried).
Recovery: pause affected stages; methodology engine proposes alternative
(journaled); replan with new methodology version; already-validated work
under the old version is re-examined only where the invalidation touches
it (provenance-scoped revalidation, not blanket redo).
Persisted: invalidation decision, replan record, revalidation scope.
Final: task continues under corrected methodology or PAUSED_FOR_HUMAN.
Invariants: `14` rule 4; versioned methodology (`16`).

**TEST-024 — Task must safely pause.**
Initial: task EXECUTING; human issues pause (or continuation gate fires).
Inject: pause command.
Detection: command / gate trigger.
Classification: n/a (deliberate).
Recovery (the pause itself): stop scheduling; workers drain at lease
boundaries (finish atomic unit, commit, release); verified checkpoint;
state preserved; auto-recovery stood down for the pause scope.
Persisted: `task_paused` with reason; checkpoint id; drain confirmations.
Final: task PAUSED_FOR_HUMAN (or PAUSED); zero active leases; resume is a
single explicit action that re-runs reconciliation.
Invariants: I-9; "a stop must hold" — nothing restarts behind the pauser.

**TEST-025 — Task resumes after manual intervention.**
Initial: task PAUSED_FOR_HUMAN with diagnostic package; human resolves
(e.g. approves replan, raises budget).
Inject: human resume command with decision.
Detection: command authorized.
Classification: n/a.
Recovery: decision journaled; reconciler runs full diff (leases all
expired by now → reclaim; workers reprovisioned); health re-baselined
from fresh evidence; execution resumes. Pre-pause "healthy" verdicts are
not inherited.
Persisted: resume decision, re-baselining record.
Final: task EXECUTING; progress continues from verified checkpoint.
Invariants: I-15 (fresh evidence); I-7.

**TEST-026 — OS upgraded while a task exists.**
Initial: task EXECUTING under OS v1.4.
Inject: run upgrade protocol to v1.5.
Detection: upgrade coordinator (deliberate).
Classification: n/a.
Recovery: quiesce → checkpoint → backup → migrate → verify (canary task)
→ resume (`27.3`).
Persisted: upgrade ledger events; task manifest records os_version change
boundary (units before/after).
Final: task EXECUTING under v1.5; provenance shows the version boundary.
Invariants: `27.3` compatibility contract; canary before resume.

**TEST-027 — Multiple tasks run simultaneously and one fails.**
Initial: tasks A and B EXECUTING, sharing the VM.
Inject: task A's external dependency dies → A's task breaker opens.
Detection: A's viability evaluation.
Classification: EXTERNAL_SERVICE_FAILURE scoped to A.
Recovery: A pauses safely (checkpoint, preserve). B is unaffected:
separate workers, budgets, breakers; scheduler fairness rebalances idle
capacity to B.
Persisted: A's pause + B's continuity both ledgered.
Final: A resumes when its dependency returns; B never noticed.
Invariants: task isolation (`23`); failure domains don't leak.

**TEST-028 — A task attempts to access another task's state.**
Initial: tasks A and B EXECUTING; a compromised/misconfigured worker of A
requests B's job rows.
Inject: cross-task read attempt via the store API.
Detection: transition gate — task_id mismatch.
Classification: SECURITY_FAILURE.
Recovery: request denied; worker quarantined immediately; incident with
full context; A's task continues with a replacement worker; human notified.
No auto-resumption of the offending worker, ever.
Persisted: `guardrail_denial`, quarantine, incident.
Final: B untouched; A continues minus one worker.
Invariants: constitution item 6 (`26`); I-14.
