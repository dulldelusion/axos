# 14 — Failure Taxonomy

Twelve classes. The class determines the recovery policy — which is why
misclassification is itself a failure mode. The cardinal rule:

> Do not classify a bad research result as a crashed worker. Do not classify
> an external API outage as a content failure. Do not blindly retry a
> methodology failure.

| Class | Definition | Examples | Detected by | Default policy | Retryable? |
|---|---|---|---|---|---|
| SYSTEM_FAILURE | The machine's own machinery broke | Scheduler wedged, ledger write stall, store unreachable | L5 watchers, bootstrap monitor | Restart service, replay from store; escalate if persistent | Yes (with backoff) |
| WORKER_FAILURE | An execution instance died or wedged | Process crash, OOM, hang, heartbeat loss | L1 health monitor | Reclaim lease, replace worker (rung 3–4) | Yes |
| JOB_FAILURE | A unit of work failed for job-local reasons | Timeout, exception, missing output, attempt exhausted | Lease manager, validators | Backoff, requeue, reassign; quarantine on budget exhaustion | Yes, bounded |
| EXTERNAL_SERVICE_FAILURE | A dependency outside our control failed | API outage, DNS failure, rate limiting, network partition | L2/L3 (correlated stalls), breaker probes | Open circuit breaker, pause affected work, probe for recovery | Only via breaker |
| DATA_FAILURE | Inputs are wrong or changed | Schema change, source disappeared, corrupt input, unexpected encoding | Validators, planner checks, viability eval | Quarantine affected units; replan if systemic; human if ambiguous | No — diagnose first |
| CONTENT_FAILURE | Output is wrong relative to the standard | Bad research result, off-spec image, incoherent text | Validators, QA gates, work-health axis | Quarantine output; do NOT restart the worker (it did its job, badly) | No — fix the prompt/methodology |
| VALIDATION_FAILURE | The check itself failed or is misconfigured | Validator crash, contradictory validators, threshold mis-set | Validation gate, meta-monitoring | Quarantine the validator's verdicts, not the work; fix validator | No — fix the check |
| INTEGRITY_FAILURE | State and reality disagree | Checkpoint corrupt, artifact missing for COMPLETE job, ledger gap | Checkpoint verification, artifact reconciler, integrity gate | Fall back to verified state; incident; human if unresolvable | No |
| METHODOLOGY_FAILURE | The plan for "what good means" is wrong | Success criteria unachievable, evidence standard contradicted by reality | Viability evaluation, decision review | Replan (new methodology version); human if objective itself is suspect | No — replan |
| RESOURCE_FAILURE | A budget or capacity limit was hit | Disk full, API quota exhausted, cost cap reached, too many workers | Resource governor | Throttle/degrade/pause; never "try harder" | Only after capacity returns |
| SECURITY_FAILURE | A trust boundary was crossed or threatened | Worker exceeded envelope, secret leaked toward logs, suspicious output pattern | Guardrail engine | Immediate safe stop of affected scope; preserve evidence; human | Never automatic |
| UNKNOWN_FAILURE | None of the above with confidence | Novel correlated failure, contradictory evidence | Loop detector, viability eval | Treat as INTEGRITY-adjacent: preserve, bound retries tightly, escalate fast | Minimal, then escalate |

## Classification discipline

1. **Classify from evidence, not from the loudest signal.** A worker that
   crashed while an API was down is EXTERNAL_SERVICE_FAILURE wearing a
   WORKER_FAILURE costume. The diagnosis step (`09`) checks siblings:
   one dead worker = worker; all dead = service.
2. **Content vs worker:** if outputs are invalid but the worker executed
   correctly (valid process, real progress), restarting the worker changes
   nothing. Route to CONTENT_FAILURE: quarantine output, fix
   prompt/methodology/capability version.
3. **Validation vs content:** if the validator itself is broken, failing the
   work punishes the innocent. Validators are versioned components with
   their own health; contradictory validator results are VALIDATION_FAILURE.
4. **Methodology failures must never be retried.** Retrying a task whose
   success criteria are unachievable burns budget and produces nothing.
   The only correct moves are replan or human.
5. **Security failures never auto-recover.** Any recovery action beyond
   safe-stop requires explicit human authorization. This is a hard rule in
   the guardrail engine, not a policy default.
6. **UNKNOWN is a class, not an admission of defeat.** It has the tightest
   retry budget and the fastest escalation path, because unknown failures
   are where automated recovery does the most damage.
