# 31 — Open Questions

Decisions needed before implementation. Each names the options, the lean,
and what breaks if it's decided wrong.

1. **Store implementation for v1.** Lean: SQLite/WAL. Decides backup tooling
   and the transaction API everything is built on. Wrong choice = painful
   migration later, but the architecture isolates it behind the store-access
   layer, so it's the cheapest big decision to revisit.

2. **Single-VM vs multi-node target for v1.** Lean: single VM; design
   multi-node-ready (leases already assume a shared store). If multi-node is
   actually needed in v1, choose Postgres now — migrating the store under
   live tasks is the riskiest operation in `27`.

3. **Worker implementation technology.** Processes? Containers? In-process
   threads? This decides sandboxing strength, startup cost, and how
   self-fencing is enforced. Lean: OS processes with capability-scoped
   sandboxing; containers if untrusted code execution is in scope.

4. **Who pays for external actions — budget model.** Cost caps need a real
   metering source per capability. Without it, `max_cost` is decorative.

5. **Human interaction surface.** Where do PAUSED_FOR_HUMAN packages go,
   and how does approval return? The architecture assumes a channel exists;
   it must be built (or designated) before any task with approval nodes runs.

6. **Heartbeat/ledger write amplification budget.** At 5,000 workers ×
   heartbeats, the store's write load needs measuring. Sampling policy for
   `job_heartbeat_summary` events needs real numbers, not defaults.

7. **Retention horizons.** Ledger events, heartbeats, evidence, quarantined
   bytes — each needs a horizon and a cold-storage story, or storage grows
   unboundedly (violating the "no unbounded anything" rule, `02.6`).

8. **Clock strategy.** Store-time is authoritative, but wall-clock still
   matters for SLAs and humans. Decide: monotonic vs wall for lease math
   (lean: store transaction time), and NTP requirements for the VM.

9. **Nested work: can workers spawn sub-work?** Lean: no in v1 — all
   parallelism is expressed in the graph. Sub-worker spawning entangles
   leases and budgets; revisit only with a nested-lease design.

10. **Data privacy and PII.** Evidence memory and artifacts may contain
    personal data. Decide the data-handling policy (per-task envelope field
    exists; the default policy doesn't) before any task touches real user
    data.

11. **Multi-tenancy.** Is one AXOS instance per human, or shared? Affects
    the authorization model, quota design, and whether task isolation needs
    stronger (cryptographic) boundaries.

12. **What "verified" means for checkpoint sampling.** The 5%/min-3 default
    is a guess. It should be set from a risk analysis of artifact value vs
    verification cost per task type — a wrong default either wastes compute
    or misses corruption.

13. **Canary task design for upgrades.** What the synthetic upgrade-canary
    exercises (claim → work → commit → checkpoint → gate) needs to be a
    real, maintained test workload, or `27.3` step 5 is theater.

14. **Incident SLA and on-call.** `R-11`: pauses need humans. Decide
    severity tiers, notification channels, and who is paged before the first
    production task runs — not after the first 03:00 pause.
