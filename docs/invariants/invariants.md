# AXOS Invariant Registry

Invariants are statements that must hold unconditionally across the
system. They are enforced by the authoritative store layer
(`src/axos/store/`) through the `TransitionGate` and, where a second
backstop is warranted, by database `CHECK` constraints. AI reasoning,
worker reports, cached summaries, and operator expectations are never
authoritative — the durable store is.

Conventions for this registry:

- **ID**: the invariant's canonical identifier as used in code and tests.
- **Statement**: the enforced proposition, grounded in implementation and
  test wording (the portable-bundle recovery export preserves several
  statements verbatim).
- **Implementation**: file and mechanism that enforces it.
- **Tests**: exact test file and test names that prove it.
- **Audit evidence**: which audit or report re-verified it independently.
- **Current status**: one of `ENFORCED_IN_CODE + TESTED`, `TESTED`,
  `AUDITED` — never marked proven without evidence.

## Registry table

| ID | Statement (short) | Status |
|---|---|---|
| [I-3](#i-3--heartbeats-are-liveness-evidence-never-authority) | Heartbeats are liveness evidence only; they never renew leases, grant authority, or count as progress. | ENFORCED_IN_CODE + TESTED |
| [I-4](#i-4--no-artifact-free-path-to-complete) | A job reaches `COMPLETE` only through the artifact-backed commit protocol. | ENFORCED_IN_CODE + TESTED, AUDITED |
| [I-16](#i-16--workers-never-create-work) | Workers cannot create tasks or jobs; only scheduler-class actors can. | ENFORCED_IN_CODE + TESTED, AUDITED |
| [I-17](#i-17--human-gated-pause-is-sticky) | `PAUSED_FOR_HUMAN` exits only via a recorded human `APPROVED` approval — across restarts, unconditionally. | ENFORCED_IN_CODE + TESTED, AUDITED |
| [I-18](#i-18--recovery-success-requires-positive-authoritative-progress) | A recovery attempt recorded as `success` must carry a positive authoritative progress delta; heartbeats never count. | ENFORCED_IN_CODE + TESTED, AUDITED (structural; see gap) |

No other invariant IDs appear in the test suite (a full scan for
`I-[0-9]+` across `src/axos/tests/` yields exactly these five, plus
`FI-*` fault-injection scenario tags and `R*` release milestones, which
are test labels, not invariants).

---

## I-3 — heartbeats are liveness evidence, never authority

**Statement.** A worker heartbeat is a time-series evidence record, not an
authority token. A heartbeat never extends a lease, never renews authority,
never reclaims a lease, never bumps the fencing token, and is never
evidence of job success. The heartbeat row schema carries only liveness
fields plus the fencing/identity fields the fencing model needs.

**Implementation.** `src/axos/store/gate.py`, `ingest_heartbeat()`:
`ts` is stamped with store time on ingest (the worker-supplied
`worker_reported_ts` is stored as informational metadata only);
`hb_seq` must be strictly increasing per `(worker_id, proc_id)`;
duplicates and out-of-order rows are rejected without mutation; a
heartbeat naming a job requires the current owner and matching fencing
token; stale/fenced heartbeats journal `worker.heartbeat_fenced` and raise
`LeaseError`. No ledger event is written per accepted heartbeat
(summaries and milestones only).

**Tests.**

- `src/axos/tests/test_heartbeat_r3.py::test_R3_S1_heartbeat_rows_carry_no_progress_columns`
  — the `heartbeats` table must contain no `progress_done`,
  `progress_total`, `last_progress_ts`, `progress_rate`, `resources`,
  `checkpoint_ref`, or `task_id` columns (liveness-only), while keeping
  the fencing/identity fields the I-3 model needs (`worker_id`,
  `proc_id`, `job_id`, `fencing_token`, `hb_seq`, `worker_state`,
  `current_operation`, `worker_reported_ts`, `ts`).

**Audit evidence.** R14-B fault-injection campaign, `FI-10` (fencing
race): stale workers' process groups are terminated and fencing is
checked before state — see
`src/axos/tests/test_final_hardening_r14.py` §FI-10. Preserved in
`reports/historical/R14-EVIDENCE.md`.

**Current status.** `ENFORCED_IN_CODE + TESTED`.

---

## I-4 — no artifact-free path to COMPLETE

**Statement.** A job reaches `COMPLETE` only through the artifact-backed
commit protocol:

    stage_artifact() -> begin_commit() -> verify_artifact() -> commit_artifact()

The old direct-completion path (`CLAIMED`/`RUNNING` -> `COMPLETE` on a
result string with no artifact, no content hash, no validation) violated
this invariant and no longer exists; `commit_job_result()` was removed in
Phase 1C R5. Scheduling may dispatch work, but only the worker's R5
contract can complete it — the `job.committed` ledger actor is always
`worker:*`, never `scheduler:*`.

**Implementation.** `src/axos/store/gate.py`: `commit_artifact()` is the
single write that flips a job to `COMPLETE`, gated on a `VERIFIED`
artifact with byte-level re-verification inside the commit transaction
(`store/gate.py:1574` area; fencing is checked before state — a stale
token is rejected even against an already-`COMPLETE` job).

**Tests.**

- `src/axos/tests/test_artifacts_r5.py::test_R5_32_explicit_i4_regression`
  — explicit I-4 regression: no artifact-free path to `COMPLETE`.
- `src/axos/tests/test_store.py::test_05_valid_transitions` — reaches
  `COMPLETE` only via stage → begin_commit → verify → commit.
- `src/axos/tests/test_scheduler_r10.py::test_R10_24_completion_only_via_worker_r5_contract`
  — every `job.committed` ledger actor starts with `worker:`, never
  `scheduler:`.
- `src/axos/tests/test_remediation.py::test_f3_progress_after_complete_rejected_state_identical`
  — completes through the artifact protocol before asserting the F3
  terminal guard.

**Audit evidence.** The Phase 1C gate treated I-4 as the completion
correction: `reports/historical/PHASE-1C-FINAL-STATUS.md`. Historical
`COMPLETE` rows written by the pre-R5 path are preserved untouched by
migration v4 (`test_R5_33_v4_migration_preserves_historical_complete`) and
are marked historical, not adopted as precedent.

**Current status.** `ENFORCED_IN_CODE + TESTED`, `AUDITED`.

---

## I-16 — workers never create work

**Statement.** Workers cannot create tasks or jobs. The actor check on the
creation paths rejects any `worker:*` actor (and other non-scheduler
actor classes); only scheduler-class actors may create work.

**Implementation.** `src/axos/store/gate.py`, `_check_work_creator()`
(invoked by `create_task()`/`create_job()`): actors matching the worker
pattern are rejected with `TransitionRejected` before any mutation.

**Tests.**

- `src/axos/tests/test_store.py::test_24_i16_workers_cannot_create_work` —
  `worker:w-9` is rejected on both `create_task` and `create_job`;
  `scheduler` succeeds.
- `src/axos/tests/test_scheduler_r10.py::test_R10_35_no_job_creation` —
  the scheduler exposes no creation API, its source contains no
  `INSERT INTO JOBS`, and its own actor is refused by the I-16
  work-creator check.

**Audit evidence.** `reports/historical/AUDIT-REPORT.md`: I-16 VERIFIED
(through gate) — the audit's write-path attacks (audit 01, 02) confirmed
`worker:*`/`loader`-style actors are rejected on creation paths. Note the
audit's qualification: this held "through the gate"; the F1 remediation
(the authority boundary, `Store.read_only()`/`ReadOnlyStore`) was built
specifically to make the gate the mechanism, not just the convention —
regression-covered in `src/axos/tests/test_remediation.py`.

**Current status.** `ENFORCED_IN_CODE + TESTED`, `AUDITED`.

---

## I-17 — human-gated pause is sticky

**Statement.** `PAUSED_FOR_HUMAN` cannot be exited without a recorded
`APPROVED` approval — across restarts, unconditionally. A restart never
moves the task forward; a `DENIED` approval does not release the gate; a
stale, foreign, wrong-task, or duplicated approval does not release it.
While a task is paused, the scheduler admits nothing under it and the
reconciler creates nothing under it.

**Implementation.** `src/axos/store/gate.py`, `transition_task()`: the
`PAUSED_FOR_HUMAN` exit clause requires an `approval_ref` naming an
`APPROVED` approval for that task; the F1 remediation made the
`approvals` table gate-protected so a forged `APPROVED` row cannot be
written through any public application API (`store/gate.py:413–423`
comments record the requirement explicitly).

**Tests.**

- `src/axos/tests/test_store.py::test_14_human_gated_state_sticky_across_restart`
  — pause survives close/reopen; resume without a decision is rejected;
  `DENIED` is rejected; a genuine `APPROVED` releases the gate.
- `src/axos/tests/test_remediation.py::test_f1_i17_forged_approval_impossible_via_public_api`
  — the audit's critical forged-approval attack is re-tested against
  every publicly reachable API (read-only `Store.conn`, read-only
  handles, gate APIs).
- `src/axos/tests/test_final_hardening_r14.py::TestR14BFI12` (FI-12) —
  operator pause mid-flight: in-flight work drains, no new claims are
  admitted, the pause survives a supervisor restart, and resume requires
  an explicit `APPROVED` approval.
- `src/axos/tests/test_scheduler_r10.py::test_R10_23_blocked_and_paused_never_scheduled`
  — the real scheduler's admission pass skips paused tasks.

**Audit evidence.** `reports/historical/AUDIT-REPORT.md`: I-17 VERIFIED
(through gate), 12/12 attacks — restart, reopen, worker loss, lease
expiry, recovery attempts, wrong/stale/foreign/duplicate approvals all
handled; the audit also exposed the pre-F1 weakness (forged approvals via
direct writes), which F1 remediation closed and `test_remediation.py`
re-tests.

**Current status.** `ENFORCED_IN_CODE + TESTED`, `AUDITED`.

---

## I-18 — recovery success requires positive authoritative progress

**Statement.** A recovery attempt recorded as `success` must carry a
positive authoritative progress delta. Zero progress can never be recorded
as success; neither can an empty `observed_effect`. Honest non-success
decisions (`retry`, `escalate`, `BLOCKED`) with zero progress remain
representable. Heartbeats never count as progress.

**Implementation.** `src/axos/store/gate.py`,
`record_recovery_attempt()` (`store/gate.py:2468–2497`) and
`complete_recovery_attempt()` (`store/gate.py:2823–2865`): the gate
rejects `decision="success"` with `progress_delta <= 0`, and rejects
missing `observed_effect`. A database `CHECK` constraint on
`recovery_attempts` rejects the same contradiction even on direct
inserts — the rule holds below the application layer.

**Tests.**

- `src/axos/tests/test_store.py::test_16_recovery_attempt_requires_progress_evidence`
  — gate rejects zero-progress success; the raw-SQL insert is rejected
  by the `CHECK` (`sqlite3.IntegrityError`); empty `observed_effect`
  is rejected.
- `src/axos/tests/test_recovery_r8.py::test_R8_08_i18_success_requires_positive_delta`
  — the R8 contract test: the gate refuses `decision=success` with
  `progress_delta <= 0`.
- `src/axos/tests/test_recovery_r8.py::test_R8_M1_v5_to_v6_migration` —
  the I-18 `CHECK` still rejects success-without-progress after the v6
  migration; legacy rows survive byte-identical.
- `src/axos/tests/test_final_hardening_r14.py` — R14-B's universal
  recovery contract: every incident's attempts carry a measured progress
  delta and an I-18-legal decision; R14-61 asserts a successful attempt
  has `progress_delta > 0`.
- `src/axos/tests/test_resilience_r12.py::test_R12_19_probe_success_via_progress_delta_closes`
  — a half-open breaker closes only on strictly positive
  `progress_done` delta over the probe baseline (I-18 at the resilience
  layer).

**Audit evidence.** `reports/historical/AUDIT-REPORT.md`: I-18 verified as
**structural** — the gate and the DB `CHECK` reject fabricated success
shape (11/11 attacks), but the audit recorded the known gap: the store
enforces the *shape* of evidence (present, positive delta), not its
*truth*; a plausible-but-fabricated `observed_effect` with a positive
delta is storable. Semantic verification of evidence is the
recovery-controller's responsibility (R8's `_progress_delta` and
`_verify` in `src/axos/exec/recovery.py` compare authoritative
snapshots before/after the action), not the store's. This gap is
documented, not closed.

**Current status.** `ENFORCED_IN_CODE + TESTED`, `AUDITED` (structural
enforcement; semantic-truth gap recorded above).

---

## Invariant interactions

- I-18 is the hinge of the recovery contract: every recovery attempt row
  must satisfy it ([recovery model](../recovery/recovery-model.md)),
  and R9's fixed escalation rule (two consecutive zero-progress attempts
  → escalate) exists precisely because I-18 prevents zero-progress
  success.
- I-17 is the terminal rung of the recovery ladder: rung 5
  ("Replan / pause") enters `PAUSED_FOR_HUMAN`, which exits only by I-17's
  rule ([escalation](escalation.md)).
- I-4 is the completion contract: no recovery path, no scheduler path,
  and no boot path may move a job to `COMPLETE` except through the
  artifact commit protocol ([contracts](../contracts/README.md)).
- I-16 keeps the authority graph clean: recovery and scheduling never
  grant workers the power to create work, so a wedged or hostile worker
  cannot expand its blast radius.

## R14 honesty note

The internal R14 fault-injection campaign is complete (12/12 fault
injections, each executed twice; R14 suite 123/123; full repository
735/735 + 10 subtests; authority audits 01–09 + A20 green). The external
real-VM lifecycle proof — genuine hypervisor-level power-cycle for FI-04
and the FI-12 VM-restart variant — remains **BLOCKED**: the environment
cannot provide a genuine hard power-off, and the gap was not closed by
substituting simulated evidence. See
`reports/historical/R14-EXTERNAL-VALIDATION.md` (specification) and
`reports/historical/R14-EXTERNAL-VALIDATION.json` (machine-readable
status). Historical evidence is preserved as it stands; the blocker is a
property of the environment, not a defect in the invariants above.
