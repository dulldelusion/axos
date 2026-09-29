# Checkpoint Contract

## Purpose

A checkpoint is a verifiable resume point: a durable, self-describing
snapshot of the exact state a task's progress rests on. The checkpoint
contract defines what it takes for a staged snapshot to become *trusted*
— and, symmetrically, how a checkpoint that was trusted loses that trust
when the underlying bytes are corrupted afterwards.

Creation without verification is never a checkpoint and is never trusted
for resume.

## Authoritative state

- **`checkpoints`** — one row per checkpoint candidate. The checkpoint
  identity is deterministic: `checkpoint_id = sha256(canonical
  manifest)`. There are no random IDs; equivalent logical manifests
  serialize identically (entries sorted by artifact_id, canonical JSON),
  so equivalent manifests produce the same checkpoint ID. Each manifest
  entry names an `artifact_id` and its `content_hash`; optional fields
  (`size`, `kind`, `job_id`) ride along.
- **`checkpoint_pointers`** — the latest-known-good pointer
  (`lkg:<task_id>`), naming one checkpoint per task. It advances only
  through `verify_checkpoint()` success.

`verification_status` is `UNVERIFIED | VERIFYING | VERIFIED | CORRUPT`
(see `CHECKPOINT_TRANSITIONS` in `src/axos/store/transitions.py`).

## Allowed transitions

- `UNVERIFIED -> VERIFYING` (verification begins; recorded in ledger events).
- `VERIFYING -> VERIFIED` (the full predicate held).
- `VERIFYING -> CORRUPT` (any check failed; terminal, retained for forensics).
- `VERIFIED -> CORRUPT` only via `invalidate_checkpoint()` when the
  manifest/artifact bytes were corrupted *after* successful verification.
- `CORRUPT` is terminal otherwise. No other edges exist; the gate refuses
  any transition not in the graph.

A checkpoint is staged by `stage_checkpoint()` with a trigger from
`CHECKPOINT_TRIGGERS`: `policy` (task/stage policy), `pre_risky_operation`
(before replan / breaker / safe stop / approval), or `operator_request`.
A bare timer without verification capacity is never a trigger. Staging is
idempotent: re-staging the same canonical manifest returns the existing
row, never a duplicate.

Worker actors may not stage or verify checkpoints — stale workers cannot
advance checkpoint state.

## Safety invariant

- The latest-known-good pointer advances **only** through
  `verify_checkpoint()` success: on success it moves to the VERIFIED
  checkpoint with the greatest `created_at` for the task. A failed
  verification can never move it.
- A post-verification corruption (via `invalidate_checkpoint()`) never
  moves the pointer; combined with the `latest_known_good()` hardening,
  an invalidated checkpoint is never returned as trusted again —
  recovery must fall back to the previous intact checkpoint and re-derive
  coverage from the ledger.
- `latest_known_good()` is defined only over VERIFIED checkpoints: a
  pointer row that no longer resolves to a VERIFIED checkpoint returns
  `None` instead of the corrupted checkpoint.

## Failure behavior

- Verification failure marks the candidate `CORRUPT` (terminal, retained
  for forensics) and the pointer never moves. The failure path of
  `verify_checkpoint()` performs the transition atomically with a
  corruption receipt.
- `invalidate_checkpoint()` is restricted to actors `system`,
  `operator`, and `test` — workers and the lease-scoped recovery
  controller can never invalidate a VERIFIED checkpoint. It requires a
  non-empty corruption evidence dict and applies only to a VERIFIED
  checkpoint.
- A failed verification can never replace the latest-known-good pointer,
  and a pointer that points at a non-VERIFIED checkpoint is treated as
  absent.

## Verification mechanism

`VERIFIED` requires a durable verification receipt recorded with full
manifest **and** ledger-chain verification. `verify_checkpoint()`
establishes, atomically in one transaction:

1. The manifest is canonical and `checkpoint_id == sha256(canonical_manifest)`.
2. Every referenced artifact row exists with the expected content hash.
3. Every referenced artifact's bytes are re-read from disk and re-hash to
   the expected content hash (independently confirmed, never trusted).
4. Required validator PASS receipts exist for each exact content hash
   (`REQUIRED_VALIDATORS = (("axos-structural", "1"),)`; a receipt for
   another hash never validates).
5. A durable verification receipt records the exact checkpoint identity
   (stored on the checkpoint row as canonical JSON), including the
   manifest, per-artifact results, and ledger-tip binding: the current
   ledger tip is recorded and chain contiguity is checked (no sequence
   gap at the verification tip).

Release checkpoints (`release=True`) always revalidate 100% of the
manifest — no sampling, no trust in earlier partial verification — and
record the release attestation in the receipt.

## Relevant implementation

- `src/axos/store/gate.py` — `stage_checkpoint`, `verify_checkpoint`
  (full revalidation, receipt, pointer advance), `invalidate_checkpoint`
  (post-verification corruption, VERIFIED -> CORRUPT), `latest_known_good`
  (read-only, VERIFIED-only), `CHECKPOINT_TRIGGERS` (contract D8),
  `REQUIRED_VALIDATORS` (contract D5).
- `src/axos/store/transitions.py` — `CHECKPOINT_TRANSITIONS`.

## Relevant tests

- `src/axos/tests/test_artifacts_r5.py` — checkpoint staging and
  verification alongside the artifact integrity tests.
- `src/axos/tests/test_finalization_r13.py` — R13 stages its release
  checkpoint through the gate's checkpoint API.
- `src/axos/tests/test_final_hardening_r14.py` — checkpoint integrity
  including post-verification invalidation behavior.
- `src/axos/tests/test_boot_r6.py` — checkpoint pointers used during boot
  recovery.
- `src/axos/tests/test_recovery_r8.py` — latest-known-good evidence in
  recovery verification snapshots.
