# 16 — Methodology Engine

The planner decides *how the work is organized*. The methodology engine
decides *what the work means*: what question is actually being answered, what
counts as success, what evidence is required, what "complete" and "good"
mean. This separation is load-bearing — it is what lets the same OS run
research, content curation, and image generation without task-specific code
in the machine.

## 16.1 Methodology record

```
methodology_id, version, task_type,
question            — what is actually being answered
success_criteria[]  — observable, checkable conditions
evidence_required[] — what must exist for a claim to count
scope_rules         — in/out of scope, with rationale hooks
source_layers[]     — which input layers matter, in priority order
validation_standard — which validators, thresholds, sampling
completeness_def    — what "all of it" means (enumeration rule)
quality_standard    — acceptance bar, with examples
integrity_checks[]  — cross-checks beyond per-unit validation
progress_expectations {heartbeat_interval, max_silence, min_rate}
failure_policies    — per failure class: retry/quarantine/escalate defaults
```

The methodology is **versioned and immutable**. A task records which
methodology version governed which units (provenance). Changing "what good
means" mid-task without versioning would silently invalidate prior QA.

## 16.2 Sources of methodology

1. **Reusable methodology** from the registry (e.g. `web-research/v3`,
   `visual-asset-qa/v2`) — preferred; these carry validated defaults.
2. **Task-type defaults** composed from capability families (`17`).
3. **Previously validated methodology** — a prior task's methodology that
   passed its integrity gate can be proposed for a similar task.
4. **Newly constructed** — the engine drafts one from the task model; this
   path requires stronger validation (see 16.4) because untested methodology
   is a leading cause of METHODOLOGY_FAILURE.

The engine selects the most specific applicable methodology and records
*why* (decision journal): alternatives considered, confidence, what would
invalidate the choice.

## 16.3 Methodology vs plan (the boundary)

| Methodology engine | Planner |
|---|---|
| What counts as evidence | What order to collect it in |
| What "complete" means | How to parallelize collection |
| Quality bar and validators | Which capabilities to compose |
| Progress expectations (for stall detection) | Job sizing and fan-out bounds |
| When the approach is invalid | What to do when it is (recovery branches) |

The planner may not weaken the methodology's standards to make scheduling
easier, and the methodology may not dictate orchestration. They meet at the
execution graph: stages reference methodology sections (validation standard,
checkpoint policy derived from integrity checks).

## 16.4 Methodology validation and invalidation

- **At plan time:** the methodology is checked for internal consistency
  (success criteria measurable? completeness rule enumerable? validators
  exist in the registry?). A methodology that can't be executed fails the
  task back to AUTHORIZED.
- **During execution:** the viability evaluation watches for invalidation
  signals — evidence contradicting the question framing, success criteria
  proven unachievable, source layers disappearing. Invalidation is a
  METHODOLOGY_FAILURE (`14`): never retried, routed to replan or human.
- **After execution:** the integrity gate checks the methodology version
  record and whether the success criteria were actually evaluated (not just
  asserted).

## 16.5 Why progress expectations live here

Stall detection (`09`, `10`) needs to know the difference between "legitimate
3-hour render" and "hung API call". Only the methodology knows the shape of
the work. A generic timeout would murder the render or miss the hang. This
is a concrete case of the general principle: **domain knowledge enters the
machine through the methodology, not through hardcoded thresholds.**
