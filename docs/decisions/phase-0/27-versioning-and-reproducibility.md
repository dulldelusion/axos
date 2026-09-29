# 27 — Versioning and Reproducibility

A completed task must be explainable *and* reproducible: given the version
manifest, the inputs, and the methodology, the same outputs should be
derivable — or any divergence should be explainable from recorded decisions.

## 27.1 Task version manifest

Produced at FINALIZED, stored as a RELEASED artifact:

```
task_id, objective_hash,
input_versions {each input: hash + source},
os_version, min_os_version,
methodology_id + version,
capability_versions {capability: version},
worker_config_hashes,
schema_versions {input, output, graph},
graph_version,
checkpoint_ids[],
ledger_tip_seq, ledger_chain_head_hash,
output_artifact_ids[],
decision_ids[],
completed_at
```

This manifest plus the (retained) inputs is sufficient to re-derive or audit
every output. Provenance chains (`19`) provide the per-artifact detail; the
manifest provides the task-level summary.

## 27.2 Component versioning rules

- Methodologies, capabilities, validators, graph: immutable versions; new
  version = new record, never an edit.
- A running task pins its versions at plan time. Registry updates never
  affect running tasks.
- Replanning may adopt new versions, but the adoption is a journaled
  decision and the old versions remain in provenance for already-produced
  artifacts. A task's outputs may legitimately span two methodology
  versions — the manifest records which units used which.

## 27.3 OS upgrade while tasks are active

The upgrade coordinator runs the protocol:

1. **Quiesce:** stop scheduling new claims; let in-flight jobs reach
   terminal states or safe lease boundaries; block new task admission.
2. **Checkpoint:** verified checkpoint for every active task.
3. **Backup:** full store backup + artifact storage snapshot; verify backup
   restorability (a backup never tested is not a backup).
4. **Migrate:** apply versioned store migrations in order; each migration
   is itself transactional and ledger-appended.
5. **Verify:** schema checks, ledger chain verification, then the
   **maintained upgrade canary** (ADR-013): a versioned synthetic task
   exercising claim → work → commit → checkpoint → gate, with scripted
   failure injection. **Scheduling does not resume until the canary passes
   on the migrated store.** Canary failure → restore from the pre-upgrade
   backup (the documented rollback). The canary also runs nightly against
   production binaries as a standing health proof.
6. **Resume:** re-enable scheduling; tasks continue from verified
   checkpoints under the new OS version, which is recorded in their
   manifests.

Compatibility contract: the OS supports tasks with `min_os_version <=
current_version`. A task requiring a *newer* OS than installed is rejected
at authorization with a clear message. Downgrades are not supported —
forward-only, with backups as the escape hatch.

## 27.4 Reproducibility limits (honest)

- Nondeterministic capabilities (LLM generation, network fetches) cannot
  bit-reproduce; the guarantee is *process* reproducibility: same inputs +
  same versions + same methodology ⇒ outputs that pass the same validators,
  with divergences explainable via attempt records and decision journal.
- External sources may change; inputs are hashed at acquisition time, so
  "the source changed since" is detectable rather than silent.
