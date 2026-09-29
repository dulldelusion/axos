# 01 — System Vision

**AXOS: Autonomous Execution OS — Phase 0 Architecture**

## The problem

Long autonomous work dies in boring ways. Not from hard problems, but from a
worker that crashed at 02:13, a lease nobody reclaimed, a checkpoint that was
never verified, a supervisor that was the only thing that knew what "done"
meant. Every long-running agent system eventually faces the same truth:

> The work outlives the workers. If the system's memory of the work lives
> inside the workers, the work dies with them.

AXOS is designed around a single primary property:

> **Work must survive failure.**

Workers crash. Agents hang. Processes disappear. The supervisor crashes. The VM
restarts. Networks partition. APIs fail. Schemas change. Outputs corrupt.
Checkpoints corrupt. Recovery itself fails. The machine is designed so that,
wherever safe, it reconstructs its state and continues execution **without
human intervention** — and where continuation would risk correctness, it stops
safely, preserves everything, and asks for help with a useful diagnostic
package.

## What the machine is

AXOS is a general-purpose execution substrate, not a task-specific pipeline.
The human provides a high-level objective — research a market, curate a
dataset, generate image assets. The machine:

1. Interprets the task and establishes success criteria.
2. Selects a methodology (what counts as evidence, quality, completeness).
3. Derives an execution graph (stages, jobs, gates, branches).
4. Composes workers from reusable capabilities (never a fixed agent list).
5. Executes under leases, heartbeats, and validation.
6. Continuously reconciles *desired state* against *actual state*.
7. Detects, classifies, diagnoses, recovers, and verifies — or escalates.
8. Checkpoints only verified recovery points.
9. Runs a final integrity gate before declaring anything finished.

The same OS runs research, content curation, and image generation. Only the
task configuration, methodology, capabilities, and execution graph change.

## What the machine is not

- Not a prompt that tells an agent to "be careful". Care is architected:
  leases, fencing, validation gates, verified checkpoints, bounded recovery.
- Not a cron that restarts things. Restarting is the *last* resort in a
  detect → classify → diagnose → recover → verify → resume/escalate pipeline.
- Not optimistic about its own health. It never reports healthy from stale
  signals. Unknown is a first-class state, reported as unknown.
- Not always continuing. Safe stopping is a core capability. A machine that
  cannot stop itself is not autonomous; it is merely unsupervised.

## The central invariant

> **No ephemeral component may be the sole source of truth for task state.**

Agents, workers, processes, supervisors, and VMs are disposable and
replaceable. Authoritative state — task, stage, job, worker, lease, progress,
checkpoints, artifacts, validation, failures, recovery, decisions,
methodology, versions, provenance — lives in durable persistent storage,
mutated only through validated transitions, and every consequential mutation
is appended to an execution ledger. If an agent claims "7,382 records are
complete," that claim is an *input* to be verified against durable state, not
a fact.

## Design philosophy

1. **State over process.** Processes are cheap and disposable; verified state
   is precious. Every recovery path starts from the store, never from a
   process's memory.
2. **Skepticism over trust.** Workers are untrusted with respect to state.
   Heartbeats are signals, not facts. Outputs are validated before they count.
3. **Bounded autonomy.** Every automatic action has a budget, a backoff, and
   an escalation. Unbounded retry is forbidden by architecture, not by policy
   document.
4. **Explicit uncertainty.** `UNKNOWN` and `UNCERTAIN` are real states with
   defined reconciliation procedures — not gaps papered over with retries.
5. **Reconciliation over orchestration.** The machine does not "drive" work
   like a script; it declares desired state and continuously repairs the
   difference with actual state. This is what makes it survive the death of
   any orchestrator.
6. **Boring correctness over cleverness.** Atomic claims, fencing tokens,
   content-addressed artifacts, hash-chained ledgers. Distributed-systems
   fundamentals, applied without shortcuts.

## Success criteria for the architecture (Phase 0)

- Every failure mode in `14-failure-taxonomy.md` has a defined
  detect → classify → diagnose → recover → verify → escalate path.
- Every one of the 28 failure-injection tests in
  `28-failure-injection-tests.md` is answerable from the design.
- The same OS demonstrably constructs valid plans for three unrelated
  workloads (`29-task-demonstrations.md`) with no architecture changes.
- The risks in `30-architecture-risks.md` are stated honestly, with
  tradeoffs — not hidden.
- A reviewer can read `32-phase-0-summary.md` and know exactly what is
  decided, what is open (`31-open-questions.md`), and what the safe build
  order is — before a single worker is spawned.
