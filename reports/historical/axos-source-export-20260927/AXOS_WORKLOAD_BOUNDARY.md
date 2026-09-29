# AXOS ↔ Workload Boundary

Read-only source export, 2026-09-28. Grounded in `~/workspace/axos/` at
release `551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
(migration v12). AXOS is the **general-purpose execution correctness
kernel**; Word Pics Modern is a **workload deployed on it**. Nothing in
this document designs or implements anything new.

## 1. The actual boundary

```
AXOS CORE (store/, exec/)
    ↓  TransitionGate — the single narrow API for all authoritative mutations
    ↓  generic execution protocol (claim → heartbeat/progress → commit/fail)
EXTERNAL WORKLOAD ADAPTER (not in the AXOS tree)
    ↓  workload-specific semantics (objective interpretation, real work)
ACTUAL WORKLOAD (e.g. Word Pics Modern harness — outside AXOS)
```

AXOS does not know what a workload *does*. It knows only: create a task
with an objective, create jobs under it, claim them, renew leases,
record heartbeats and progress, stage/verify/commit artifacts, checkpoint,
recover, fence stale owners, and finalize a release. Every authoritative
mutation goes through one object:

- `store/gate.py` — `class TransitionGate`, docstring verbatim:
  *"Enforcement boundary for all authoritative state transitions."*
  Public surface (all public `def`s, no `def _`): `create_task`,
  `transition_task`, `get_task`, `create_job`, `transition_job`,
  `get_job`, `fencing_ledger_for_job`, `unreaped_proc_spawns`,
  `latest_spawn_generation`, `update_job_progress`, `claim_job`,
  `claim_job_bounded`, `renew_lease`, `release_lease`,
  `observe_expired_leases`, `expired_leases`, `reclaim_lease`,
  `stage_artifact`, `get_artifact`, `begin_commit`, `verify_artifact`,
  `commit_artifact`, `fail_job_execution`, `inspect_uncertain_completion`,
  `classify_staging_orphans`, `stage_checkpoint`, `verify_checkpoint`,
  `invalidate_checkpoint`, `latest_known_good`, `commit_job_result`
  (refused shim), worker ops (`create_worker`, `transition_worker`,
  `get_worker`, `mark_worker_seen`, `ingest_heartbeat`,
  `heartbeats_for`), approvals (`create_approval`, `decide_approval`),
  incidents (`create_incident`, `set_incident_outcome`), recovery
  (`record_recovery_attempt`, `recovery_attempts_for`,
  `open_recovery_incidents`, `progress_evidence_for_job`,
  `find_or_create_recovery_incident`, `get_recovery_incident`,
  `create_recovery_attempt`, `get_recovery_attempt`,
  `open_recovery_attempts`, `claim_recovery_attempt`,
  `set_attempt_verify_after`, `note_attempt_dispatch`,
  `set_attempt_budget_context`, `transition_recovery_attempt`,
  `complete_recovery_attempt`, `mark_recovery_attempt_uncertain`,
  `set_incident_escalated`, `get_recovery_policy`,
  `ensure_recovery_policy`, `cas_update_recovery_policy`), plus breaker
  and finalization ops.
- `store/db.py` — `Store` handle and `ReadOnlyStore`; the gate holds the
  only writable `Store` (`Store.conn` is read-only, `PRAGMA
  query_only=ON`; `Store.write_txn()` is gate-owned). This is the F1
  authority boundary — application-level, not filesystem-level.
- `exec/supervisor.py` — spawns real OS worker processes:
  `python -m axos.exec.worker --db PATH --worker-id W --proc-id P
  --job-id J --ttl-s 60 --behavior '{...}' [--hb-interval-s 0.5]
  [--no-renew]` (supervisor.py:329–345). It owns process lifecycle
  (spawn/observe/terminate/reap) and nothing else; its in-memory `_procs`
  holds process handles only, never job authority.

## 2. Workload payloads are opaque — the evidence

AXOS stores workload intent and results as canonicalized JSON it never
interprets:

- **Task objective.** `tasks.objective TEXT NOT NULL`
  (`store/migrations.py:32`). `TransitionGate.create_task()` stores
  `_canon(objective)` where `_canon` is verbatim
  `json.dumps(obj, sort_keys=True, separators=(",", ":"))`
  (`store/gate.py:37–39`). No code anywhere in AXOS parses, validates,
  or branches on the contents of an objective.
- **Job rows.** `jobs` (`store/migrations.py:40–54`) carry `stage_id`
  (an opaque label), `status`, `attempt`, lease triple
  (`owner_worker_id`, `lease_expires_at`, `fencing_token`),
  `progress_done`/`progress_total` (numbers), and `policy`
  (canonicalized JSON, `_canon(policy or {})`). No column encodes what
  the job *computes*.
- **Artifact bytes.** `artifacts` (`store/migrations.py:158`) store
  `kind` (an opaque label), `size`, `uri`, and the gate computes
  `content_hash = sha256(bytes)` itself (`stage_artifact`,
  `store/gate.py:1137–1226`). The gate verifies byte identity and
  validator receipts — it never inspects byte *meaning*.
- **Ledger events.** `ledger` (`store/migrations.py:~183`) stores
  `event_type` + canonicalized JSON `payload`; the chain is
  hash-linked for tamper-evidence (README: tamper-evident, not
  tamper-proof).

No branch anywhere in `store/` or `exec/` conditions on workload
semantics. The word `kind` in `gate.py` refers only to artifact labels
and breaker failure-kinds (verified by grep at `gate.py:1138, 1197,
4065–4144`); `BehaviorSpec` appears only in `exec/synthetic.py`.

## 3. Zero Word Pics coupling — verified by direct inspection

Own grep (2026-09-28), case-insensitive, over `store/ exec/ audit/`
`*.py`:

- `word.?pics` → **zero hits**.
- `puzzle` → **zero hits**.
- `level` → hits only in incidental senses: *"artifact storage level"*
  comment (`store/migrations.py:590`), "job-level"/"stage-level"/
  "task-level"/"filesystem-level" phrases, "level (artifact_id…)"
  comment. No game-level concept exists in the code.
- Imports across all 55 `.py` files in `store/ exec/ tests/ audit/` are
  **stdlib only** (`sqlite3`, `hashlib`, `json`, `threading`, `argparse`,
  `ast`, `datetime`, `math`, `os`, `platform`, `random`, `re`, `shutil`,
  `signal`, `subprocess`, `sys`, `tempfile`, `textwrap`, `time`,
  `unittest`, `uuid`, `dataclasses`, `contextlib`, `typing`, `inspect`)
  plus intra-package `axos.*` imports. No workload SDK, no HTTP client,
  no image library, no LLM client.
- The only workload-shaped code in the tree is
  `exec/synthetic.py` — a **test double**: its docstring states verbatim
  *"It performs no real work: its purpose is fault injection for the
  execution substrate. It never uses LLMs, external APIs, network
  services, or real production side effects."* `BehaviorSpec.kind` has
  12 deterministic fault-injection behaviors
  (`success_immediate`, `crash`, `sigkill_self`, …); a real workload's
  behavior JSON would be supplied by an external adapter.

Conclusion: the AXOS tree contains no Word Pics runtime coupling — no
imports, no schemas, no branches, no objective-shape assumptions. Any
Word Pics harness lives outside `~/workspace/axos/` (this worker did
not inspect its location; it is out of scope for this tree).

## 4. The worker protocol (the de facto execution contract)

`exec/worker.py` docstring defines the protocol every workload must
follow — authority is *received*, never manufactured:

1. `claim_job` through the gate (lost race → exit 3, nothing executes).
2. Read the authoritative lease triple `(job_id, owner, fencing_token)`
   back from the store.
3. `CLAIMED → RUNNING` (informational only).
4. Heartbeat + progress through the gate; lease renewal via the
   sanctioned `renew_lease` path; `LeaseError` mid-execution →
   `FencedError`, stop immediately.
5. Commit via the R5 artifact protocol
   `stage_artifact → begin_commit → verify_artifact → commit_artifact`
   with real bytes; the gate computes the hash and atomically verifies
   bytes + content hash + validator evidence in the transaction that
   moves the job to `COMPLETE`. `transition_job` *refuses* a direct
   `→ COMPLETE` (I-4; `gate.py:467–486`). On failure,
   `fail_job_execution` records `FAILED`. There is no artifact-free
   success path.

Worker exit codes are *process evidence only — NEVER job success*
(`worker.py` docstring).

## 5. Honest limitation: the contract is implicit, not formal

AXOS has **no formal workload plugin/adapter interface**. Verified:

- `grep -riE "adapter|plugin"` over `store/ exec/` returns nothing
  workload-related (only unrelated "interface" prose in
  `store/db.py:126,312` and `exec/recovery.py:46,180`).
- There is no `WorkloadAdapter` ABC, no `Protocol`, no entry-point
  registry, no callback schema. The "adapter" for any real workload is
  whatever external process implements the §4 worker protocol against
  the gate — i.e. an out-of-tree subprocess like `axos.exec.worker`,
  driven by its own harness.

What exists instead is: (a) the **opaque-payload schema** (§2 —
canonical JSON objective/policy, lease triple, artifact bytes +
gate-computed hash), (b) the **TransitionGate method surface** (§1 —
the only mutation API), and (c) the **worker protocol convention**
(§4 — claim/heartbeat/commit/fence discipline, documented in
docstrings and enforced by the gate's rejection paths, not by a type
signature). This is a real, enforced contract — the gate rejects
violations transactionally — but it is implicit rather than a declared
plugin interface. Per task scope, no interface is designed or proposed
here.
