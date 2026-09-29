# 26 — Security and Permissions

## 26.1 System constitution (non-overridable)

1. Workers are untrusted with respect to state; their outputs are validated
   before they count.
2. No component may exceed the task's authorization envelope.
3. Secrets never enter the store, ledger, logs, heartbeats, or provenance
   records. They are resolved by reference at worker start via the secrets
   broker and held in worker memory only.
4. Irreversible external actions require explicit human approval or an
   explicit policy exception, journaled.
5. Security failures never auto-recover beyond safe-stop; resumption
   requires human authorization.
6. Cross-task access is forbidden and treated as an attack, not a bug.

Task envelopes and policies may be *stricter* than the constitution, never
looser.

## 26.2 Task authorization envelope

Declared at submission, validated by the task authorizer before PLANNED:

```
allowed_capabilities[] (+ versions),
allowed_domains[] (egress),
write_scopes[] (what may be written, where),
external_action_policy (never | ask | auto_under_budget),
budgets {workers, cost, wallclock, ...},
data_handling (retention, PII rules),
human_approval_points[] (graph nodes requiring approval)
```

The guardrail engine evaluates every state transition and every worker
action request against the envelope. Denials are ledger events
(`guardrail_denial`) with the rule cited — denials are observable, so a
misconfigured envelope is debuggable rather than mysterious.

**Default data-handling policy (ADR-010):** every task declares
`data_handling` at authorization; the default `standard` means: minimize PII
at acquisition (fetch only what the methodology's evidence standard
requires); PII in the ledger and memory layers by reference only, never by
value; secrets never in durable state (constitutional); evidence artifacts
carrying PII are tagged and retention-bounded even in Tier 0 — purge by
policy is the single Tier 0 exception (hash + purge record retained, backups
inherit the purge obligation).

## 26.3 Worker sandboxing

Derived from capability side-effect class (`17`): filesystem jailed to the
task workspace, egress limited to allowed domains, no access to the store
except through the transition-gate API (claims, heartbeats, fenced commits),
no access to other tasks' namespaces, no secret materialization beyond
resolved references. A worker attempting an out-of-envelope action is
quarantined and the attempt is a SECURITY_FAILURE incident.

## 26.4 Human approval policy

HUMAN_APPROVAL nodes in the graph carry: what is being asked, the evidence,
the proposed action's blast radius, and a recommended default. Approvals are
explicit (grant/deny with reason), ledger-appended, and expire — a granted
approval covers the stated action, not a blank check for similar actions
later. Denial routes to the graph's recovery branch.

## 26.5 Supply-chain honesty

Capabilities, validators, and methodologies are versioned components from
the registry. The OS records which versions ran (`27`). A compromised or
buggy component version is contained by: version pinning (running tasks
unaffected by registry changes), quarantine on anomalous output rates, and
the ability to roll the registry entry back with a journaled decision.
