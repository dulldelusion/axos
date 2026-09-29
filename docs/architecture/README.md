# AXOS Architecture Documentation

This directory documents the AXOS (Autonomous Execution OS) architecture
**as implemented** in the frozen source under `src/axos/` (release
`551d559c…`, migration v12). Every document is grounded in the actual
source files: `store/{db,gate,transitions,migrations}.py` and
`exec/{supervisor,worker,scheduler,watchdog,boot,recovery,policy,resilience,reconciler,finalizer,identity,synthetic}.py`.

- [overview.md](overview.md) — What AXOS is: the execution model, the
  core control loop, key platform facts, and an honest status summary
  (implemented / tested / audited / verified / blocked / historical).
- [control-plane.md](control-plane.md) — The control-plane components
  (Supervisor, Scheduler, Worker, Watchdog, and the recovery/policy/
  resilience controllers), how they interact, heartbeats, and the
  authority boundary that separates them from the store.
- [state-machine.md](state-machine.md) — The explicit state-transition
  graphs for tasks, jobs, workers, artifacts, approvals, checkpoints,
  recovery attempts, breakers, and policy rows, as defined in
  `store/transitions.py`, with guards and terminal states.
- [ownership-model.md](ownership-model.md) — Explicit ownership:
  worker → attempt → claim → fencing → artifact → verification →
  authoritative completion. The lease triple, `fencing_token` semantics,
  and fencing enforcement.
- [data-flow.md](data-flow.md) — The write path through `TransitionGate`
  (the single narrow API), the hash-chained ledger, checkpoints and
  artifacts, and how reconciliation and reads work.

Conventions used throughout:

- **Terminology matches the implementation.** Identifiers such as
  `TransitionGate`, `ReadOnlyStore`, `fencing_token`, `claim_job`,
  `reclaim_lease`, `PAUSED_FOR_HUMAN`, and `UNCERTAIN` are the literal
  names in the source.
- **Claims name their source.** Anything not verifiable from the frozen
  snapshot is marked "not verified in this snapshot" rather than
  invented.
- **Historical evidence is preserved as historical.** Reports under
  `reports/historical/` describe the evidence that existed at export
  time; they are not re-proven here.

Related documentation:

- `docs/contracts/` — the authoritative contracts (recovery, checkpoint,
  artifact, lease, fencing, reconciliation, finalization).
- `docs/invariants/` — the invariant registry (I-4, I-16, I-17, I-18,
  and others).
- `docs/recovery/` — the recovery model, escalation, and failure
  handling.
- `docs/operations/` — bootstrap, testing, verification,
  troubleshooting.
- `docs/reproducibility/MUSE_BOOTSTRAP.md` — the cross-session bootstrap
  procedure.
