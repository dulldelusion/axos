# Finalization Contract

## Purpose

Finalization releases a generation — declares the release's durable
evidence complete and publishes it — only when durable evidence proves
it is safe to do so. The finalizer is the **reader/evaluator half** of
finalization: it owns no execution authority and never mutates jobs,
tasks, leases, workers, processes, artifacts, checkpoints, recovery
budgets, recovery rungs, desired state, or breakers.

## Authoritative state

- **`finalization_runs`** — one row per release generation: `state`,
  `version`, `manifest_hash`, `checkpoint_id`, evaluation blockers.
- The canonical release-generation identity and the release-manifest
  hash, re-exported from the gate so the finalizer defines nothing of
  its own: `canonical_release_generation`, `canonical_finalization_id`,
  `build_finalization_manifest`, `finalization_manifest_hash`.
- The R5 release checkpoint, staged through the gate's checkpoint API.

## Allowed transitions

One `evaluate_once()` pass does four things, in order:

1. **Boot gate**: no finalization before boot recovery reaches `READY`
   (R6) — finalizing from pre-boot evidence could fork authority.
2. **Begin**: the current desired-state head's generation gets its
   finalization run via `begin_finalization_run()` (idempotent —
   re-begins converge).
3. **Evaluate**: every non-finalized run is re-evaluated from durable
   evidence via `evaluate_finalization()` — compare-and-swap on `version`;
   the loser re-reads, never overwrites. A run evaluates to `READY` or to
   a blocked state with named blockers.
4. **Publish**: a run that evaluates `READY` is published via
   `publish_finalization()` — the gate re-validates everything
   authoritatively inside **one** `write_txn` and flips the run to
   `FINALIZED` atomically, or refuses.

**No-op discipline**: a pass over an already-FINALIZED generation performs
zero writes (verifying a finalized record does not mutate it). A lost CAS
is reported, not retried — the next pass re-reads.

## Safety invariant

- The finalizer never clears a human gate, never completes a job, never
  moves a breaker, and never creates/claims/transitions jobs or
  fences/reclaims workers. Its only writes go through the gate's
  finalization API (begin/evaluate/publish on `finalization_runs` rows,
  plus the deterministic finalization task row and the R5 release
  checkpoint those ops stage).
- The `FinalizationConfig.actor` must never start with `worker:` —
  worker actors cannot own controller authority; misconfiguration fails
  the pass loudly, not silently.
- Publication is atomic-or-refuse: the run is either `FINALIZED` with
  the gate re-validating everything in the same transaction, or nothing
  changed.

## Failure behavior

- Per-run failures (evaluate/publish) are collected into the pass's
  `errors` list and never abort the pass.
- A publish that fails gate re-validation leaves the run
  non-FINALIZED; the next pass re-evaluates from current evidence.
- Unreadable evidence for a run fails that run closed, not the pass.

## Verification mechanism

- `evaluate_finalization()` re-derives the run's state from durable
  evidence on every pass (desired satisfaction, job states, checkpoint
  state) and records named blockers; a run is `READY` only when every
  gate holds.
- The canonical release-manifest hashing (`finalization_manifest_hash`)
  gives each generation a deterministic identity the gate recomputes —
  the finalizer cannot invent one.
- The R5 release checkpoint is staged and verified through the
  checkpoint contract before publication, so the release's evidence
  bundle is independently re-validated.

## Relevant implementation

- `src/axos/exec/finalizer.py` — `Finalizer` (`evaluate_once()`, the
  testable core; `start()`/`stop()` background loop),
  `FinalizationConfig` (validated, frozen; worker actors forbidden),
  re-exported canonical identities and manifest hashing.
- `src/axos/store/gate.py` — `begin_finalization_run`,
  `evaluate_finalization`, `publish_finalization` (atomic-or-refuse),
  `list_finalization_runs`, `canonical_release_generation`,
  `canonical_finalization_id`, `build_finalization_manifest`,
  `finalization_manifest_hash`, `FinalizationConflict`.

## Relevant tests

- `src/axos/tests/test_finalization_r13.py` — R13 release finalization:
  fresh generation begin -> OPEN with idempotent re-begin; durable run
  identity (canonical finalization id stable across restarts);
  deterministic snapshot; desired-satisfaction gate blocks on missing,
  PENDING, CLAIMED jobs; blocked-state blockers; publish only when
  READY; finalized generations are zero-write passes.
