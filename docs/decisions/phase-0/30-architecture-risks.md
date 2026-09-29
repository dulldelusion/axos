# 30 — Architecture Risks

Brutally honest. For each: the failure, why the naive reading of this
architecture fails, alternatives, the recommendation, and the tradeoff being
accepted.

---

## R-1. The durable store is the single point of failure.

**Failure:** store corruption, disk loss, or operator error destroys the one
authoritative state. Every recovery path in this architecture starts at the
store — if it's gone, the machine is amnesiac.
**Why naive fails:** "the store is durable" is asserted, not engineered.
SQLite on a single disk is one `rm` away from catastrophe; backups "we'll
set up later" don't exist.
**Alternatives:** (a) replicated store (Postgres + standby), (b) continuous
WAL shipping to cold storage, (c) multi-region from day one.
**Recommendation:** v1: SQLite + hourly verified backups to separate
storage + pre-upgrade snapshots + ledger chain verification on boot. v2:
Postgres with streaming replication when multi-node is needed.
**Tradeoff:** v1 accepts a recovery-point objective of up to one hour and a
manual restore step. Documented, not hidden.

## R-2. Fencing tokens don't help if the store is partitioned from everyone.

**Failure:** network partition isolates workers *and* control plane from the
store. Nobody can claim, renew, or commit. The system is frozen, not broken
— but every layer's detector is blind.
**Why naive fails:** assuming "the store is always reachable" turns a
partition into a cascade of false DEAD verdicts and mass lease reclamation
when connectivity flaps.
**Alternatives:** (a) local write-ahead buffers with merge on reconnect
(complex, risks divergence), (b) freeze-and-wait (simple, availability hit).
**Recommendation:** (b) freeze-and-wait with UNKNOWN health, bounded local
heartbeat buffering, and reconciliation on reconnect (`TEST-011`). The
store is a CP system by choice: correctness over availability during
partitions.
**Tradeoff:** partitions halt progress. Accepted: a frozen correct system
beats a "progressing" divergent one.

## R-3. The A/B lease race can livelock.

**Failure:** worker A is slow but alive; its lease expires; B reclaims and
starts; A's partition heals and A keeps working (its commits rejected, but
it burns compute); A's operator sees "working" while B owns the job.
Repeated across many jobs = churn without progress.
**Why naive fails:** fencing guarantees *safety* (one committer), not
*liveness*. The architecture proves no duplicate commits but doesn't prove
the system converges quickly.
**Alternatives:** (a) worker self-fencing on any failed renewal (already in
design — A must stop when renewal fails), (b) aggressive kill of fenced-out
workers, (c) longer leases (fewer false reclamations, slower real recovery).
**Recommendation:** (a) strictly enforced + lease TTLs tuned per stage from
methodology expectations. Monitor "fenced-out worker-seconds" as a metric;
alert if it grows.
**Tradeoff:** self-fencing relies on the worker honoring renewal failure —
a truly wedged worker can't. The DEAD threshold bounds the damage.

## R-4. Stall detection depends on methodology honesty.

**Failure:** `max_silence` / `min_progress_rate` come from the methodology.
A bad methodology (too-generous timeouts) makes stalls invisible; a nervous
one murders healthy slow workers.
**Why naive fails:** treating these as "config" rather than as load-bearing
correctness parameters. They are as important as the validators.
**Alternatives:** (a) global defaults (wrong for heterogeneous work),
(b) adaptive baselines from observed history (complex, gameable),
(c) methodology-declared + integrity-gate-audited (chosen).
**Recommendation:** (c): the methodology must declare expectations, the
planner sanity-bounds them, and the integrity gate reports stall-detection
performance (false-stall rate) per task so bad methodologies are visible.
**Tradeoff:** first-of-kind tasks have weak expectations. Accept higher
UNKNOWN rates early; tighten as evidence accumulates.

## R-5. Recovery can cause the failure it treats.

**Failure:** restart storm after a bad deploy; mass requeue after a
transient blip; "healing" that amplifies. Self-healing systems have a
privileged position to do damage quickly.
**Why naive fails:** "recover" actions that don't measure their own blast
radius. Most watchdog designs only watch the workers, never the recovery
actions.
**Alternatives:** (a) no automatic recovery beyond retry (gives up autonomy),
(b) recovery with blast-radius monitoring + rollback (chosen),
(c) human approval for all recovery (gives up autonomy differently).
**Recommendation:** (b): the recovery-loop breaker (`15`), blast-radius
watch after every action (`11.2`), and budgets that survive restarts.
**Tradeoff:** bounded autonomy means some recoverable situations escalate
to humans unnecessarily. That's the correct side to err on.

## R-6. Checkpoint verification cost vs safety.

**Failure:** verifying every checkpoint fully (re-hash all artifacts,
revalidate large samples) is expensive at scale; sampling risks missing
corruption; skipping verification makes checkpoints decorative.
**Why naive fails:** "we checkpoint every 15 minutes" without saying what
verification ran is how TEST-014 happens in production.
**Alternatives:** (a) verify everything always (cost-prohibitive at 5k+
artifacts), (b) hash-manifest always + sampled revalidation (chosen),
(c) trust-but-verify lazily (corruption discovered at the worst time).
**Recommendation:** (b) with the sample rate as explicit policy, plus full
verification at release checkpoints. The residual risk (corruption in the
unsampled set) is bounded by hash-manifest integrity + the integrity gate.
**Tradeoff:** a corrupted-but-unverified checkpoint can become the recovery
target between verifications. Mitigated by keeping K verified checkpoints,
not just the latest.

## R-7. The watchdog regress terminates at the platform — which we don't control.

**Failure:** VM host dies and doesn't come back; init system itself is
broken; the bootstrap monitor has a bug and restart-loops the control plane.
**Why naive fails:** "supervisors all the way down" is comforting and false.
Something is always unwatched.
**Recommendation:** terminate the regress explicitly: bootstrap monitor
watched by init, init by the platform, platform covered by backups +
documented manual restore (`TEST-013`). The bootstrap monitor is kept
minimal (<200 lines, no task logic) so its bug surface is tiny, and it has
its own backoff + alerting so a restart loop pages a human instead of
spinning forever.
**Tradeoff:** the top of the stack is operational, not architectural. That's
honest — architecture can't fix the host.

## R-8. State/artifact divergence during partial failures.

**Failure:** commit transaction succeeds in the store but artifact bytes
never landed (crash between); or bytes landed but commit rolled back.
Windows of divergence are inherent in any split store/bytes design.
**Why naive fails:** assuming the transaction covers the bytes. It can't —
bytes aren't in the database.
**Alternatives:** (a) store bytes in the DB (kills performance, bloats
backups), (b) write-ahead staging + reconciliation (chosen),
(c) two-phase commit with a coordinator (coordinator becomes the SPOF).
**Recommendation:** (b): staging is the "prepared" state; the fenced commit
adopts staged bytes; the orphan reconciler (`24.2`) continuously closes the
divergence windows. TEST-015/016 prove the paths.
**Tradeoff:** brief divergence windows exist by design; they're detected
and reconciled, not prevented. The integrity gate is the final backstop.

## R-9. LLM workers are nondeterministic; idempotency is structural, not assumed.

**Failure:** re-executing a "deterministic" research or generation job
produces different output; naive dedup by "same job = same output" breaks.
**Why naive fails:** designing the commit path as if workers were pure
functions.
**Recommendation:** idempotency is on *keys and effects*, not on bytes:
same `idempotency_key` → same job identity; content-addressed artifacts mean
two different outputs are two different artifacts, and the fenced commit
picks exactly one. Validators (not determinism) decide acceptability.
**Tradeoff:** duplicate executions cost compute even when safely resolved.
Bounded by attempt budgets.

## R-10. Task isolation vs shared-resource exhaustion.

**Failure:** task A legitimately consumes 95% of disk; task B's checkpoints
start failing. Per-task quotas help, but system-level pressure is shared.
**Why naive fails:** per-task budgets that sum to more than the system has.
**Recommendation:** two-level governance: per-task quotas *plus*
system-level pressure modes (`22`) that throttle everyone by policy, with
the pressure state explicit and alerted. Quota sums are validated against
system capacity at admission.
**Tradeoff:** a large legitimate task can still squeeze small ones into
THROTTLED. Fairness policy (priority bands + queue-age) mitigates; perfect
isolation on shared hardware is impossible.

## R-11. The human bottleneck: PAUSED_FOR_HUMAN without a human.

**Failure:** the machine correctly pauses for human judgment at 03:00; no
human is watching; work sits for 8 hours. "Safe" but not "autonomous".
**Why naive fails:** treating escalation as resolution. It's not — it's a
deferral.
**Recommendation:** pauses carry SLAs by severity; notification is
multi-channel; the diagnostic package includes a recommended default action
the human can approve in one tap; and *some* pauses (resource pressure,
breaker cooldowns) auto-resume on condition-clear without human action —
only judgment pauses wait. Distinguish "waiting on condition" from "waiting
on judgment" explicitly in the state model.
**Tradeoff:** one-tap defaults risk rubber-stamping. Mitigated by making
defaults conservative and journaled.

## R-12. Observability derived from the store can lie if the store lags.

**Failure:** dashboards show "healthy" from materialized state that is
minutes behind a fast-moving incident.
**Why naive fails:** confusing "the store is the authority" with "the store
is real-time". Authority ≠ freshness.
**Recommendation:** every materialized view carries an `as_of` timestamp;
health verdicts require fresh evidence (I-15); alerts fire on
*observation* latency too (UNKNOWN persisting = observability degraded).
**Tradeoff:** operators must read `as_of`. Better than the alternative
(dual sources of truth that disagree).

## R-13. Upgrade/migration risk on the store itself.

**Failure:** a schema migration corrupts or mis-migrates live task state;
the canary passes but a rare state shape breaks.
**Why naive fails:** "migrations are tested" without a tested *rollback*
and without per-task version boundaries in provenance.
**Recommendation:** forward-only migrations, each transactional;
pre-migration backup with *tested restore*; canary task exercising the
full lifecycle; per-task `os_version` boundaries in manifests (`27`);
rollback = restore from backup (documented as the procedure, not
downgrade-migrations, which are a second source of bugs).
**Tradeoff:** rollback loses post-backup work (reconstructed via
artifacts/ledger replay where possible). Accepted and documented.

## R-14. "Self-healing" as a marketing claim.

**Failure:** the organization believes the architecture doc and stops
watching; the machine encounters a novel failure outside the taxonomy and
spins within its budgets doing nothing useful.
**Why naive fails:** architecture is not operation. No design survives
first contact with production without an operator who understands it.
**Recommendation:** the 28 failure-injection tests (`28`) must be *run*,
not just written; chaos drills on a schedule; every real incident feeds
back into the taxonomy and the tests. The final report's readiness decision
is REVIEW, not READY — review by a human who will operate it.
**Tradeoff:** this costs ongoing operational effort. There is no
architecture that removes it.
