# 25 — Observability

Four levels. Every metric is derived from **durable state**, never from logs
or filesystem walks — logs are for forensics, the store is for truth.

## 25.1 System level

CPU, memory, storage, network, process health (control-plane services alive?),
store health (write latency, WAL size, backup age), bootstrap monitor status.
Answers: "is the machine itself OK?"

## 25.2 Execution level

- Active workers by state/health; queue depth by stage; jobs/sec;
  retries/min; failures by class (`14`); stale jobs (non-terminal, no
  progress > threshold); lease expiries/min; **lease_churn_rate (reclaims
  per job per hour, alerted)**; **fenced_out_worker_seconds (alerted)**;
  checkpoint age per task; recovery events/min; breaker states;
  reconciliation diff size and convergence trend.
- Answers: "is work flowing, and is the machinery coping?"

## 25.3 Work level (task-specific, from methodology)

- Research: discovered → enumerated → resolved → validated → reconciled
  (counts + rates between stages reveal where work stalls).
- Content: analyzed → accepted → redesigned → rejected → manual-review.
- Image generation: generated → QA passed → QA failed → regenerated →
  blocked.
- Answers: "is *this task* actually progressing through its methodology?"

The work-level counters come from the methodology's stage definitions —
the OS doesn't hardcode them; it renders whatever the methodology declares.

## 25.4 Output level

Released artifacts by kind; validation pass rates; integrity gate results;
provenance completeness (% of released artifacts with verified chains);
quarantine counts with reasons; packaging status.
Answers: "can the human rely on what's been produced?"

## 25.5 Analytics (persistent operational record)

Derived and stored (not just graphed): tasks completed/failed, durations,
jobs completed, retries, recovery events + success rate, worker utilization,
worker failure rate, queue age percentiles, throughput, checkpoint
frequency, validation failures, blocked work, manual interventions,
resource usage, cost per task. This is the dataset for improving the
machine: which methodologies have the best gate-pass rates, which
capabilities produce the most quarantines, where recovery succeeds vs
escalates. Analytics never feed back into authoritative status — they inform
humans and future planning priors.

## 25.6 Alerting rules (examples)

- Any SECURITY_FAILURE → immediate alert.
- Breaker opened (service scope) → alert with incident link.
- PAUSED_FOR_HUMAN → alert with diagnostic package.
- Reconciliation not converging → alert.
- Checkpoint age > 2× policy → warning.
- UNKNOWN health persisting > threshold → warning (observability itself is
  degraded).
- Cost > 80% of cap → warning; at cap → pause + alert.

Alerts carry links to the incident, the ledger range, and the relevant
state — never just "something is wrong".
