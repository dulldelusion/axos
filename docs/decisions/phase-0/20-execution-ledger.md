# 20 — Execution Ledger

The append-only execution ledger is the machine's memory of *what happened*,
in order. It is the reconstruction source of last resort and the audit trail
of first resort.

## 20.1 Record format

```
seq (monotonic, assigned by the store),
ts (store time),
task_id?, type, actor (component or worker_id),
payload (JSON: the facts of the event),
prev_hash, hash (= H(prev_hash || canonical(payload)))
```

The hash chain makes the ledger tamper-evident: any edit, deletion, or
reordering breaks the chain at the point of interference. Verification walks
the chain; checkpoints bind to it via `ledger_tip_seq` (`13`).

## 20.2 Event catalog

Task: `task_proposed, task_authorized, task_rejected, task_planned,
task_replanned, task_paused, task_resumed, task_cancelled, task_finalized,
task_failed`

Graph: `graph_created, graph_versioned, stage_started, stage_completed,
fanout_expanded`

Workers: `worker_provisioned, worker_retired, worker_suspected,
worker_replaced, worker_quarantined`

Jobs: `job_materialized, job_claimed, job_started, job_heartbeat_summary
(sampled — not every heartbeat), job_progress_milestone, job_requeued,
job_failed, job_quarantined, job_committed, job_lease_expired,
job_lease_reclaimed, job_uncertain_opened, job_uncertain_resolved`

Health/recovery: `health_verdict, incident_opened, incident_updated,
incident_closed, recovery_action, recovery_verified, recovery_escalated,
recovery_loop_detected, breaker_opened, breaker_half_open,
breaker_closed, safe_stop, checkpoint_created, checkpoint_verified,
checkpoint_corrupt`

Decisions/governance: `decision_recorded, guardrail_denial,
approval_requested, approval_granted, approval_denied, budget_exceeded`

Finalization: `integrity_gate_started, integrity_gate_passed,
integrity_gate_failed, outputs_packaged, report_produced`

System: `service_started, service_stopped, service_crashed_detected,
vm_boot_recovery_started, vm_boot_recovery_complete, os_upgrade_started,
os_upgrade_complete`

Note: raw heartbeats are **not** ledger events (write amplification). The
ledger records heartbeat *summaries* and milestones; full heartbeats live in
their retention-bounded table. The ledger must stay replayable.

## 20.3 Snapshots and replay

- Every M events (default 10,000) or N minutes, the store records a
  `snapshot_ref`: a materialized dump of derived state + the `seq` it covers.
- Recovery replays from the latest snapshot + subsequent events, not from
  genesis. Snapshots are themselves hash-verified.
- **The ledger is the authority on history; the store tables are the
  authority on current state.** They must agree; the checkpoint verification
  (`13`) and the integrity gate (`24`) check this agreement. Disagreement is
  an INTEGRITY_FAILURE.

## 20.4 Retention

Ledger events are never deleted by normal operation. A retention job may
*archive* events older than the policy horizon to cold storage (content-
addressed, still verifiable), and the archival itself is a ledger event.
Checkpoints, released artifacts, and decisions are never archived out of
reach — they are the permanent record.

Retention tiers (ADR-007): **Tier 0** (authoritative — task records,
verified checkpoints, released artifacts + provenance, decision journal,
ledger) is permanent-hot, archived-never-deleted. **Tier 1** (telemetry —
raw heartbeats 7 days, worker logs 30 days/100MB, metric rollups 13
months) is bounded. **Tier 2** (staging orphans, temp files) is ephemeral.
All cleanup happens only through ledgered retention jobs; Tier 0 has one
declared exception — purge of PII-tagged bytes by policy (hash + purge
record retained).
