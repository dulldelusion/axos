# 17 — Capability Registry

There is no fixed list of task-specific agents. There is a registry of
**capabilities** — typed, versioned, composable units of *what can be done*.
Workers are disposable instances composed from capabilities. Capabilities are
reusable system knowledge.

## 17.1 Capability interface

```
capability_id, version,
inputs[]  {name, schema, required},
outputs[] {name, schema},
side_effects  — NONE | READ_EXTERNAL | WRITE_EXTERNAL | IRREVERSIBLE
idempotent     — bool (safe to re-execute with same inputs?)
deterministic  — bool (same inputs → same outputs?)
resource_profile {cpu, mem, network, typical_duration},
validators[]   — which validators apply to its outputs,
sandbox_class  — derived from side_effects
```

The `side_effects` and `idempotent` declarations are the most important
fields in this architecture: they determine sandboxing, retry safety, and
whether the capability may run before a HUMAN_APPROVAL node. A capability
that lies about its side effects is a SECURITY_FAILURE.

## 17.2 Capability families

Six families (an improvement on the illustrative list: organized by *effect
on the world* rather than by domain, so the same families serve research,
content, and image work):

| Family | Capabilities | Typical side effects |
|---|---|---|
| ACQUIRE | web-discovery, structured-fetch, enumeration, extraction, listening/polling | READ_EXTERNAL |
| TRANSFORM | parsing, normalization, deduplication, analysis, entity-resolution | NONE |
| REASON | research-synthesis, classification, quality-evaluation, source-reconciliation, planning-assist | NONE |
| CREATE | text-generation, image-generation, code-generation, rewriting | NONE (bytes) — but outputs need validation |
| VERIFY | schema-validation, visual-qa, consistency-check, evidence-validation, provenance-check | NONE |
| OPERATE | packaging, artifact-management, filesystem, reporting, notification | WRITE_EXTERNAL (scoped) |

Domain flavor comes from *configuration and methodology*, not from separate
agent types: `web-discovery` configured for "HubSpot partner directories"
with the research methodology is a different worker than the same capability
configured for "image reference gathering" — but it is the same capability,
with the same interface and sandboxing.

## 17.3 Worker composition

The planner maps graph stages to required capabilities → the scheduler
requests workers with capability bundles → provisioning verifies versions
against the registry and applies the sandbox class:

- **NONE side effects:** standard sandbox; freely retryable; outputs still
  validated (capability correctness ≠ output correctness).
- **READ_EXTERNAL:** network egress restricted to the task's allowed domains
  (`26`); rate-limited by the resource governor.
- **WRITE_EXTERNAL / IRREVERSIBLE:** only under an authorization envelope
  that permits it, and IRREVERSIBLE only downstream of HUMAN_APPROVAL or
  explicit policy. Non-idempotent capabilities get exactly-once *effect*
  via the fenced commit (`07`) — the commit, not the capability, carries
  the guarantee.

## 17.4 Versioning and deprecation

Capabilities are versioned independently of the OS. A task pins the versions
it used (provenance, `19`). A new capability version does not affect running
tasks. Deprecated versions are marked; the planner prefers current versions
for new tasks but will never silently upgrade a running task's workers —
that would invalidate the methodology's validation assumptions.

## 17.5 Why this instead of fixed agents

Fixed agents entangle *what* (capability), *how* (prompt/config), and
*orchestration* (when to run) into one unversioned blob that can't be
reasoned about, fenced, or recovered cleanly. The registry separates them:
capabilities are versioned interfaces, workers are configured instances,
orchestration is the graph. Each layer can fail and be replaced without
taking the others down.
