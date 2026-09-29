# 23 — Task Isolation

Every task is a sealed failure domain: its own namespace in the store, its
own workspace on disk, its own ledger stream, its own workers, its own
budgets. Tasks must not silently reuse old artifacts as authoritative inputs
— stale data wearing a fresh task's clothes is a correctness bug.

## 23.1 Isolation mechanisms

- **Store namespace:** every row carries `task_id`; all queries are
  task-scoped. There is no cross-task query path in the normal API.
- **Filesystem:** `workspaces/{task_id}/` — jobs, staging, checkpoints,
  reports. Workers are jailed to their task's workspace.
- **Workers:** one task per worker (v1). A worker cannot hold leases for two
  tasks, which keeps budgets, fencing, and provenance clean.
- **Secrets:** resolved per task via the secrets broker; a task's secrets
  are never visible to another task's workers.
- **Resource budgets:** per-task quotas prevent a runaway task from
  starving others (noisy-neighbor protection, `22`).

A task attempting to access another task's state is a SECURITY_FAILURE:
the transition gate rejects it, an incident is raised, and the offending
worker is quarantined. (Test TEST-028 covers this.)

## 23.2 Explicit cross-task reuse (the import protocol)

Reuse is allowed when *intentionally requested*, through imports:

1. The planner declares an import: `{from_task, artifact_id or query,
   pinned_version}`.
2. The task authorizer checks the source task's sharing policy (tasks
   default to private; sharing is opt-in per artifact set).
3. The import is materialized as a **new artifact in the importing task**
   with provenance linking to the source (`source_task_id`,
   `source_artifact_id`, pinned version). The bytes are copied or
   hard-linked, never referenced live — the source task may be archived or
   its artifacts revalidated later; the importer's record must not move
   under it.
4. The import is a ledger event in *both* tasks' streams.

What this prevents: "the new research task silently picked up last month's
partner list as current truth." What it enables: deliberate, auditable
reuse with full lineage.

## 23.3 Multi-task operation

The control plane serves many tasks concurrently. Scheduling fairness: the
scheduler allocates worker capacity across tasks by priority and by
queue-age (longest-waiting runnable job first, within priority bands) —
never strict FIFO across tasks, which starves small tasks behind large
ones. Per-task breakers and budgets mean one task's failure (even a task-
level breaker) does not pause others; only system-level pressure (`22`)
affects everyone, and then by explicit degraded modes.
