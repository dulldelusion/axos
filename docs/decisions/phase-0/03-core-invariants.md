# 03 — Core Invariants

Non-negotiable properties. If an implementation choice violates one of these,
the choice is wrong — not the invariant. Each invariant names its enforcement
mechanism, because an invariant without enforcement is a wish.

## I-1. No ephemeral component is the sole source of truth.

Agents, workers, processes, supervisors, and VMs are disposable. Authoritative
state lives only in the durable store. A claim made by any ephemeral
component (e.g. "7,382 records are complete") is an *input*, never a fact,
until it is validated and committed.
**Enforced by:** transition gate (all mutations validated), ledger append on
every consequential mutation, worker claims never written as state.

## I-2. Every consequential state transition is validated and journaled.

A transition is consequential if losing it, duplicating it, or misordering it
could corrupt work. All such transitions pass the transition gate
(legal-transition check + guardrail authorization) and append a ledger event.
**Enforced by:** single store-access API; no direct table writes from any
component.

## I-3. Ownership is fenced.

A claim is a lease: `(owner, lease_expiry, fencing_token)`. The fencing token
is monotonic per job. Every heartbeat renewal and every output commit must
present the current token; a stale token is rejected and the worker must stop.
This makes "worker A alive but partitioned while worker B reclaims the job"
safe: at most one token can commit.
**Enforced by:** lease manager; commit transactions check the token.

## I-4. Completion requires committed, validated, content-addressed output.

A job is COMPLETE only when: (a) its artifact bytes are staged and hashed,
(b) validators pass, (c) a single transaction commits the artifact record and
the job state, referencing the content hash. Anything less is not completion.
**Enforced by:** artifact manager + transition gate; `COMPLETE` unreachable
otherwise (see `07-job-lifecycle.md`).

## I-5. Desired state and actual state are continuously reconciled.

The machine declares desired state (from task, graph, policy) and repairs the
measured difference with actual state. The reconciler is stateless and
idempotent; it may be killed at any point without harm.
**Enforced by:** reconciler loop; repair actions are themselves validated
transitions.

## I-6. Recovery is bounded and escalating.

Every automatic recovery action has a budget (attempts, backoff, cooldown).
Exhaustion escalates exactly one level (see `11-recovery-engine.md`).
Unbounded retry is architecturally impossible: budgets live in the store, not
in worker memory.
**Enforced by:** recovery controller; recovery-loop detector; circuit
breakers.

## I-7. Only verified checkpoints are recovery points.

A checkpoint becomes a recovery point only after verification (manifest
hashes recomputed, ledger tip consistent, sample outputs revalidated).
Unverified state is never a restore target. On corruption, fall back to the
latest verified checkpoint.
**Enforced by:** checkpoint manager; `13-checkpoint-model.md`.

## I-8. Uncertainty is explicit and has a procedure.

`UNKNOWN` (health) and `UNCERTAIN` (job completion) are first-class states.
Each has a defined reconciliation procedure (inspect, validate, verify
ownership, commit-or-retry). The system never "retries blindly" out of an
uncertain state.
**Enforced by:** job lifecycle (`07`), health model (`10`), artifact
reconciliation (`24`).

## I-9. The system stops safely when continuation risks correctness.

Stopping — CHECKPOINT → PRESERVE → DIAGNOSE → REPLAN-or-ESCALATE — is a
first-class outcome, not a failure of autonomy. Triggers include validation
spikes, corruption, recovery loops, budget exhaustion, and security
violations.
**Enforced by:** continuation gate in the guardrail engine; `PAUSED_FOR_HUMAN`
with diagnostic package.

## I-10. Resources are governed, never assumed.

Worker count, concurrency, API rates, storage, and cost are explicit budgets.
Pressure moves the system NORMAL → DEGRADED → THROTTLED → PAUSED, never off a
cliff.
**Enforced by:** resource governor; scheduler consults it before placement.

## I-11. Tasks are isolated; reuse is explicit.

State, workspaces, ledgers, and artifacts are namespaced per task. Cross-task
reuse happens only through explicit, version-pinned imports with provenance
links. No silent reuse of old artifacts as authoritative inputs.
**Enforced by:** namespacing in store and filesystem; import protocol
(`23-task-isolation.md`).

## I-12. Every output is reproducible and explainable.

For any artifact: inputs (hashes), transforms (capability + version), worker,
methodology version, validations, and decisions are recorded. A completed
task ships a version manifest sufficient to reproduce it.
**Enforced by:** provenance store (`19`), version manifest (`27`).

## I-13. No component supervises only itself.

Every layer has an independent watcher (see `09-heartbeat-and-watchdog.md`).
The regress terminates at the VM's init system and the hosting platform, with
boot-time recovery reconstructing everything from the store.

## I-14. The machine never exceeds its authority.

Every worker action and state transition is evaluated against the task's
authorization envelope and the system constitution. Irreversible or
external-side-effect actions require explicit approval nodes or policy.
**Enforced by:** guardrail engine; task authorizer; `26-security-and-permissions.md`.

## I-15. Health is reported from verified signals, never from stale ones.

"Healthy" requires fresh evidence. Absence of evidence is `UNKNOWN`, reported
as unknown — never as healthy, never as dead without the defined criteria.
**Enforced by:** health model transition rules (`10-health-model.md`).

## I-16. Workers cannot create work.

Only the scheduler materializes jobs; only the lease manager grants leases.
Workers may request decomposition through the scheduler; they may never
spawn sub-work, sub-workers, or sub-leases. (ADR-009.)

## I-17. Human-gated states are sticky.

A task that is PAUSED_FOR_HUMAN or waiting on a durable approval stays that
way across VM restarts, supervisor restarts, and recovery cycles.
Auto-recovery is stood down for the paused scope until an explicit human
resume. A stop must hold. (ADR-015.)

## I-18. Recovery is verified by progress, not by action.

A recovery action succeeds only when observed *effect* shows progress
(progress delta over a defined window, output-health delta) — "worker
restarted and heartbeats" is not success. (ADR-016.)

---

### Invariant interaction notes

- I-3 + I-4 together give **exactly-once effect** for job outputs despite
  at-least-once execution: duplicate executions produce content-addressed
  artifacts; only the holder of the current fencing token can commit; the
  commit is idempotent on content hash.
- I-6 + I-9 together prevent both infinite loops *and* premature surrender:
  bounded retries escalate; escalation eventually reaches a safe stop rather
  than silent spinning.
- I-1 + I-13 together answer "who watches the watcher": watchers are
  replaceable because what they watch (state) and what they know (nothing
  private) both live in the store.
