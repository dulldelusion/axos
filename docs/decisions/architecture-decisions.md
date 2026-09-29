# Architecture decisions

A curated, ADR-style summary of the key architectural decisions behind
AXOS. Each decision is grounded in the phase-0 design documents
(`docs/decisions/phase-0/`, preserved as **design history**) or in the
frozen implementation (`src/axos/`), which is authoritative where the
two differ.

## Provenance

- Design history: `docs/decisions/phase-0/` (33 Phase 0 documents,
  written 2026-09-23; see `32-phase-0-summary.md` for the original
  context).
- Implementation authority: frozen release
  `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
  (see `src/axos/release_manifest.json`).
- Where phase-0 text was later **corrected**, the corrected form is the
  one the implementation follows — e.g. the module docstring of
  `src/axos/store/transitions.py` states the transition graphs are
  "taken verbatim from the corrected Phase 0 documents" (05, 06, 07, 04,
  13). Such corrections are marked below.

---

## ADR-1 — Durable state is the authority (reconciliation over assumptions)

**Decision.** The SQLite store is the single authoritative record of
task, job, worker, artifact, checkpoint, and recovery state. No
in-memory model, no AI reasoning, and no operator belief outranks it.
The reconciler converges *actual* state toward *declared desired*
state by reading the store, never by assuming it.

**Grounded in.** `docs/decisions/phase-0/02-architecture.md`,
`12-reconciliation.md`; implemented as the `Store`/`TransitionGate`
pair in `src/axos/store/`.

---

## ADR-2 — The gate is the single mutation API (authority boundary / F1)

**Decision.** All durable-state mutation goes through the
`TransitionGate`. The gate alone owns write transactions (`write_txn`);
`Store.conn` — the connection handed to every non-gate component — is
read-only (a separate read connection; the writable `_conn` is private
to the store and used only by migrations). Components that need to
change state must call a gate operation; there is no back door.

**Grounded in.** The remediation labeled F1: `Store.conn` read-only for
non-gate components, gate-owned `write_txn()`; see `src/axos/store/db.py`
(`_open_read_connection`, the split between `_conn` and `_ro_conn`) and
`src/axos/store/gate.py`. Phase 0 origin: `02-architecture.md`,
`26-security-and-permissions.md`.

---

## ADR-3 — Lease inputs are validated before any mutation (TTL validation / F2)

**Decision.** `claim_job` and `renew_lease` reject non-positive or
malformed TTLs (`ttl_s must be a positive number of seconds`) *before*
any state change. Phase 0 defines a TTL as a duration added to the
acquisition time; a zero or negative TTL is meaningless and is refused
up front rather than producing a corrupt lease.

**Grounded in.** The remediation labeled F2; implemented in
`src/axos/store/gate.py::_check_ttl`. Phase 0 origin: the lease model
in `04-domain-model.md`.

---

## ADR-4 — Progress updates are rejected unless the job is live (terminal progress guard / F3)

**Decision.** Progress reports are refused for jobs that are not
`CLAIMED` or `RUNNING`. A terminal job (e.g. `COMPLETE`) never accepts
new progress, closing a class of late-write corruption where stale
worker output could rewrite finished state.

**Grounded in.** The remediation labeled F3; implemented in
`src/axos/store/gate.py` (the `update_job_progress` guard). Phase 0
origin: `10-health-model.md`.

---

## ADR-5 — Explicit ownership and fencing

**Decision.** Every claim mints a monotonically increasing fencing
token bound to the job (`jobs.fencing_token`). A worker acts only under
its live token; any action carrying a stale token is rejected. Lease
expiry plus fencing is what makes workers *replaceable*: a dead worker's
lease expires, its token is superseded, and the replacement's authority
is unambiguous. Only `recovery-controller`, `reconciler`, `system`,
`operator`, and `test` actors may revoke a lease (`RECLAIM_ACTORS`;
the supervisor is deliberately excluded — it owns process
termination, never lease authority).

**Grounded in.** `docs/decisions/phase-0/04-domain-model.md`,
`06-worker-lifecycle.md`, `09-heartbeat-and-watchdog.md`; implemented
in `src/axos/store/transitions.py` (`RECLAIM_ACTORS`) and
`src/axos/store/gate.py`.

---

## ADR-6 — Recovery success is determined by verified progress (I-18)

**Decision.** Performing a recovery action is not success. The store
itself rejects a recovery attempt recorded as `'success'` with a
non-positive progress delta:
`CHECK (NOT (decision = 'success' AND progress_delta <= 0))` on
`recovery_attempts` (migration v1). The contract-normative escalation
rule is fixed: zero authoritative progress across two consecutive
recovery attempts is failed recovery — escalate to `r9-policy`.

**Grounded in.** Phase 0 invariant I-18 (`03-core-invariants.md`);
implemented in `src/axos/store/migrations.py` (v1 DDL) and
`src/axos/exec/recovery.py` (`_TWO_ATTEMPT_ESCALATION_RUN = 2`).

---

## ADR-7 — Hash-chained ledger; tamper-evident, not tamper-proof

**Decision.** Every durable mutation appends an event to the ledger
(`seq`, `event_type`, `payload`, `actor`, `ts`, `prev_hash`, `hash` =
H(prev_hash || canonical(payload))). The chain makes edits, deletions,
or reorderings detectable by walking it; checkpoints bind to it via
`ledger_tip_seq`. This is explicitly tamper-**evident**, not
tamper-**proof**: a holder of the database file with sufficient
privilege can rewrite history, but cannot do so without breaking the
chain. The implementation makes no stronger claim.

**Grounded in.** `docs/decisions/phase-0/20-execution-ledger.md` ("The
hash chain makes the ledger tamper-evident"); `src/axos/store/` ledger
write path.

---

## ADR-8 — Idempotency and bounded recovery

**Decision.** Operations are idempotent where repetition is possible
(content-addressed artifacts dedup at the byte level,
`INSERT OR IGNORE` for exactly-once breaker signals, idempotent
`ensure_job_for_desired_state`). Recovery is bounded: per-rung and
per-incident attempt budgets (R9 policy rows, compare-and-swap on
`version`); rung 4 has budget 0 (no stage-widening executor exists) and
rung 5 has budget 0 (terminal). There is no infinite retry anywhere.

**Grounded in.** `docs/decisions/phase-0/11-recovery-engine.md`,
`14-failure-taxonomy.md`; implemented in `src/axos/exec/policy.py`
(`PolicyConfig`) and `src/axos/exec/recovery.py`.

---

## ADR-9 — Safe stopping is a first-class outcome

**Decision.** The ladder's rung 5 is "Replan / pause", and the human
gate (`PAUSED_FOR_HUMAN`) is sticky across restarts: the gate refuses
to leave a human-gated state without an explicit, recorded human
decision (`HUMAN_GATED_FROM`). Terminal states (including
`RECOVERY_COMPLETE`, `BUDGET_EXHAUSTED`, `SUPERSEDED`) are sticky by
construction — no further rung selection, budget consumption, or
execution happens for the incident.

**Grounded in.** `docs/decisions/phase-0/05-task-lifecycle.md`
(corrected), `03-core-invariants.md` (I-17); implemented in
`src/axos/store/transitions.py` and `src/axos/exec/policy.py`.

---

## ADR-10 — Observable recovery

**Decision.** Recovery is not a silent loop: every attempt is a durable
row (`recovery_attempts`) carrying incident identity, classification,
rung, attempt number, budget context, success/failure criteria, progress
evidence before/after, resulting state, and escalation target; R9 policy
state is a durable row per incident (`recovery_policy`). A crashed
controller's state is fully reconstructible from the store.

**Grounded in.** `docs/decisions/phase-0/25-observability.md`,
`11-recovery-engine.md`; implemented in migrations v6/v7
(`src/axos/store/migrations.py`).

---

## ADR-11 — Failure is tested, not asserted

**Decision.** Correctness claims rest on adversarial evidence:
kill-point fault injection (`audit/_killpoints.py`), crash/restart
tests, race tests, watchdog and recovery suites, and the R-series
verification evidence preserved in `reports/`. See
`docs/reproducibility/` for how to reproduce the evidence.

**Grounded in.** `docs/decisions/phase-0/28-failure-injection-tests.md`;
preserved evidence in `reports/audits/` and `reports/verification/`.

---

## ADR-12 — AI reasoning is not authoritative state

**Decision.** Model output — including any analysis, diagnosis, or plan
produced during operation — never constitutes authoritative state.
Authority flows only from durable records written through the gate.
This is why MUSE_BOOTSTRAP instructs a fresh operator to trust the
store and the evidence, not the narrative.

**Grounded in.** Standing operating principle; enforced structurally by
ADR-2 (the gate is the only writer).

---

## ADR-13 — stdlib-only implementation; zero-env-var core configuration

**Decision.** The implementation uses only the Python standard library
plus `sqlite3` (verified: the entire `src/axos/` tree imports nothing
outside stdlib except a local test helper). All core configuration is
constructor-injected (`RecoveryConfig`, `PolicyConfig`, `Clock`); the
store, gate, and migrations read no environment variables. (`os.environ`
appears only where subprocesses are spawned — the supervisor passes the
ambient environment to child worker processes — never as core
configuration.)

**Grounded in.** Source inspection of `src/axos/` (frozen release
`551d559c…`).

---

## ADR-14 — Workload logic stays out of the core

**Decision.** AXOS is a reusable execution substrate. Application
business logic lives in workload adapters that use the public
`TransitionGate` API; the core never imports, embeds, or depends on
workload-specific logic. The only in-repo adapter is the toy example in
`examples/minimal-workload-adapter/`.

**Grounded in.** `docs/decisions/phase-0/02-architecture.md`
(workload/core separation); the example at
`examples/minimal-workload-adapter/`.

---

## Superseded or partially-superseded design history

The phase-0 documents are a design snapshot from 2026-09-23, not a
description of the final implementation. Known divergences (documented
in the code, not hidden):

- The lifecycle graphs in `05-task-lifecycle.md`, `06-worker-lifecycle.md`,
  `07-job-lifecycle.md`, and `13-checkpoint-model.md` were
  **corrected** after Phase 0; the corrected graphs are the ones in
  `src/axos/store/transitions.py` (per that module's docstring) and in
  `contracts/transitions.json`.
- Invariant I-4 (artifact/checkpoint integrity) received an explicit
  **correction** during Phase 1C R5 (migration v4: content hashes,
  provenance columns, checkpoint pointers).
- The R14 proposal to amend the contract for a real-VM lifecycle proof
  is preserved as a **proposal**, not as enacted change
  (`reports/historical/R14-CONTRACT-AMENDMENT-PROPOSAL.md`); the R14
  external real-VM proof remains **blocked** (`reports/historical/R14-EXTERNAL-VALIDATION.md`).

Where this ADR summary and a phase-0 document disagree, this summary
and the frozen implementation are authoritative.
