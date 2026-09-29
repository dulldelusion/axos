# 32 — Phase 0 Summary

> **Review completed 2026-09-23.** The architecture review package lives at
> `~/workspace/your_files/axos-phase-0-review/` (7 documents: risk briefing,
> 14 question decisions, 18 ADRs, recovery kernel spec, 12-test injection
> plan, Phase 1 plan, final summary). 14 documents in this package were
> corrected in place per the review (see the ADR's correction list); 3 new
> invariants added (I-16 no nested work, I-17 pause stickiness, I-18
> progress-anchored recovery). Final status:
> **ARCHITECTURE_READY_FOR_IMPLEMENTATION** — ready to be built and then
> proven; the self-healing claim is earned only when the 12 injection tests
> pass on a real VM (Phase 1, Milestone 4).

## What this package is

The complete architecture for AXOS (Autonomous Execution OS), a
general-purpose machine designed around one property: **work must survive
failure**. 32 documents + glossary. No implementation has been started, per
the Phase 0 constraint.

## Document index

| # | Document | Covers |
|---|---|---|
| 01 | system-vision.md | Problem, what the machine is/isn't, central invariant |
| 02 | architecture.md | Layered view, control plane (challenged), store decision, topology |
| 03 | core-invariants.md | 15 non-negotiable invariants + enforcement |
| 04 | domain-model.md | Entities, fields, relationships |
| 05 | task-lifecycle.md | Task states, viability evaluation, finalization flow |
| 06 | worker-lifecycle.md | Worker states, self-fencing, retirement |
| 07 | job-lifecycle.md | Job states, fenced commit, UNCERTAIN reconciliation, idempotency |
| 08 | execution-graph.md | Node/edge types, dynamic expansion, graph validation, versioning |
| 09 | heartbeat-and-watchdog.md | DETECT→…→ESCALATE pipeline, 6 layers, regress termination |
| 10 | health-model.md | 3 axes × 5 states, transitions, composite verdicts |
| 11 | recovery-engine.md | 11-rung ladder, budgets, loop detection, supervisor/VM recovery |
| 12 | reconciliation.md | Desired-vs-actual loop, repair semantics, anti-oscillation |
| 13 | checkpoint-model.md | Verified recovery points, corruption fallback, rollback |
| 14 | failure-taxonomy.md | 12 classes with detection + policy |
| 15 | circuit-breakers.md | 4 scopes, state machine, interaction with recovery |
| 16 | methodology-engine.md | What "good" means, versioned, invalidation |
| 17 | capability-registry.md | 6 families, interface, composition, sandboxing |
| 18 | memory-model.md | 7 layers, separation rules |
| 19 | provenance-model.md | Hash-chained lineage, query API |
| 20 | execution-ledger.md | Event catalog, hash chain, snapshots, replay |
| 21 | decision-journal.md | Consequential decisions with evidence |
| 22 | resource-governance.md | Budgets, pressure levels, scheduler feedback |
| 23 | task-isolation.md | Namespaces, import protocol, multi-task fairness |
| 24 | output-management.md | Artifact lifecycle, orphans, packaging, integrity gate |
| 25 | observability.md | 4 levels, analytics, alerting |
| 26 | security-and-permissions.md | Constitution, envelopes, sandboxing, approvals |
| 27 | versioning-and-reproducibility.md | Manifests, upgrade protocol, compatibility |
| 28 | failure-injection-tests.md | 28 tests with expected behavior + invariants |
| 29 | task-demonstrations.md | Same OS, 3 workloads (research, curation, generation) |
| 30 | architecture-risks.md | 14 risks, 5-part analysis each |
| 31 | open-questions.md | 14 decisions needed before implementation |
| 32 | phase-0-summary.md | This file |

## How to review

1. Read 01, 03 (invariants), 02 (architecture) — the 20-minute version.
2. Read 07 (fenced commit), 09–11 (watchdog → recovery) — the correctness
   core. If the fencing or the escalation ladder is wrong, say so here.
3. Read 30 (risks) — check that the tradeoffs are acceptable and none are
   hidden.
4. Read 31 (open questions) — decisions needed from you before anything is
   built.
5. Skim the rest by interest; 28 is the test plan the implementation will
   be held to.

## What's decided vs open

- **Decided:** invariants, lifecycles, lease/fencing/commit semantics,
  watchdog layers, escalation ladder, checkpoint verification, taxonomy,
  breaker design, isolation, integrity gate, versioning protocol.
- **Open:** store implementation, deployment target, worker technology,
  cost metering, human interaction surface, retention horizons, and the
  other items in 31. None of the open items invalidate the decided core —
  they parameterize it.

## Status

Per the Phase 0 exit criteria (`01-system-vision.md`): every failure mode
has a recovery path, every injection test is answerable, generality is
demonstrated, risks are stated with tradeoffs. The package is ready for
review. Nothing is implemented; nothing should be implemented until review
completes and the open questions are decided.
