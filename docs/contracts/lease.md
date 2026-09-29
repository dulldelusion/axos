# Lease Contract

## Purpose

The lease is the exclusive execution authority for a job. Whoever holds
the live lease may execute; everyone else is excluded — by store facts,
not by cooperation. The lease contract defines the lease triple, how it
is granted, renewed, and revoked, and the single primitive that revokes
it.

## Authoritative state

The lease is the `(owner_worker_id, lease_expires_at, fencing_token)`
triple on the **`jobs`** row, mutated **only** through atomic conditional
updates. There is no second lease implementation: `claim_job`,
`claim_job_bounded`, and `claim_job_resilient` all run the same shared
claim core inside their own transaction, so the triple, the fencing-token
bump, and the `job.claimed` ledger event are identical on every claim
path.

All lease timestamps are authoritative store time, stamped inside the
write transaction. A worker-supplied timestamp, if present, is stored in
`worker_reported_ts` as informational metadata and is **never** used for
lease, expiry, ordering, or fencing semantics.

## Allowed transitions

- `claim_job()`: `PENDING -> CLAIMED`. One atomic conditional UPDATE:
  exactly one claimant wins under concurrency; `False` means "the job was
  not claimable — do not execute". The TTL is validated as a positive
  number *before* the transaction opens (`_check_ttl`: bool, NaN, zero,
  and negative are rejected with no mutation, because they would mint
  stillborn or poisoned leases).
- `claim_job_bounded()`: `PENDING -> CLAIMED` with admission control —
  at most `max_concurrent_jobs` jobs in the active execution window
  (`CLAIMED`, `RUNNING`, `COMMITTING`), evaluated *inside the same
  transaction* as the claim so two racing schedulers see the same
  committed count. When `scheduler_id` is given, the scheduler's
  claim-liveness beat is planted atomically with the claim (claim ⟹
  beat), so a live scheduler's fresh claim is never stolen mid-dispatch.
- `renew_lease()`: extends the lease for the *current* owner with the
  *current* fencing token while the job is `CLAIMED` or `RUNNING` and the
  lease is still live. Returns `False` instead of raising on
  conflict/staleness. Renew on a daemon thread well inside the TTL; on
  `False`/`LeaseError`, stop executing immediately — only a fresh claim
  restores authority.
- `release_lease()`: voluntary release by the owner (valid token); clears
  the triple.
- `reclaim_lease()` (R1, the revocation primitive): `CLAIMED ->
  PENDING` (requeue), `RUNNING | COMMITTING -> UNCERTAIN` (needs
  resolution, not blind requeue). Routine reclaim requires the lease to
  be actually expired per authoritative store time. **Forced** reclaim of
  a live lease requires a `DEAD`/`STALLED` watchdog verdict plus an
  `incident_id`. Any other state (terminal, `PENDING`, `UNCERTAIN`) and
  any ownerless job is rejected with zero mutation: reclaim revokes a
  lease, it never invents one. The caller must name the exact lease being
  revoked (`expected_owner`, `expected_token` — a compare-and-swap on the
  triple): a second reclaim holding the old token is rejected; a reclaim
  after a successful one is rejected as ownerless.

**Reclaim actors** (`RECLAIM_ACTORS` in `src/axos/store/transitions.py`):
`recovery-controller`, `reconciler`, `system`, `operator`, `test`. The
supervisor is deliberately excluded — it owns process termination, never
lease authority.

## Safety invariant

- Fencing tokens are monotonic: in the reclaim transaction the token goes
  N -> N+1 atomically with owner clearing, the state transition, and the
  ledger events — so the old owner cannot commit, renew, heartbeat, or
  update progress with the old token afterwards. An old token can never
  become valid again.
- A heartbeat never renews a lease: `ingest_heartbeat()` cannot touch
  lease fields, so heartbeat activity — whatever worker timestamps it
  carries — cannot make an expired lease unexpired. Only `renew_lease`
  moves `lease_expires_at`, and only while the lease is still live.
- `claim_job_resilient()` evaluates breaker scopes in the fixed order
  `GLOBAL -> TASK -> DESIRED -> JOB` inside the same transaction as the
  capacity check and the claim; a corrupt scope can never mean "allow".

## Failure behavior

Every rejection rolls the transaction back: no partial state, no
half-reclaimed lease. Specifically:

- Non-expired lease, wrong owner, stale token, ownerless job, or
  non-reclaim-authority actor -> rejected with zero mutation.
- A malformed TTL (non-positive, NaN, bool) is rejected *before* the
  transaction opens, so invalid input can never mutate state.
- Lost claim race -> `False` (not an error; the loser must not execute).
- `renew_lease` on a conflict -> `False`; the worker must stop executing
  immediately.

## Verification mechanism

**R4 — authoritative lease-expiry detection.** `observe_expired_leases()`
is a pure read answering "has the durable lease actually expired?",
evaluated against the current durable row with the authoritative store
clock:

```
owner_worker_id IS NOT NULL
AND lease_expires_at IS NOT NULL
AND lease_expires_at <= store_now   (boundary inclusive: == is expired)
AND status IN ('CLAIMED','RUNNING','COMMITTING')
```

`PENDING`, terminal, `BLOCKED`, `QUARANTINED`, and `UNCERTAIN` jobs are
never reported, nor are ownerless jobs or jobs without a lease — an old
timestamp alone is not an expired worker lease. Each evidence record
carries `job_id`, `owner_worker_id`, `fencing_token`, `status`,
`lease_acquired_at`, `lease_expires_at`, `observed_at`, the predicate
string, and a human-readable reason — everything needed to reconstruct
the observation and hand it to R1.

R4 is **advisory evidence, not authorization to mutate**: it is SELECT-only
(it runs on the read-only connection, which cannot write) and produces
no verdicts, no recovery policy, no process control. The only mutation
path for an expired lease is `reclaim_lease()`, which revalidates owner,
token, *and* expiry atomically inside its own transaction — never cache
owner/token across a mutation boundary; re-observe, or let
`reclaim_lease` reject stale evidence with `LeaseError` and zero
mutation. Detection can only under-report relative to R1, never
over-report: R4 never flags a lease that R1 would consider live.

## Relevant implementation

- `src/axos/store/gate.py` — `claim_job`, `claim_job_bounded`,
  `claim_job_resilient` (shared `_claim_job_txn` core),
  `renew_lease`, `release_lease`, `reclaim_lease` (R1), `observe_expired_leases`
  (R4), `expired_leases` (legacy projection), `_check_ttl`,
  `_EXPIRY_PREDICATE`.
- `src/axos/store/transitions.py` — `RECLAIM_ACTORS`,
  `JOB_TRANSITIONS` (reclaim outcomes stay inside the graph).
- `src/axos/exec/recovery.py` — consumes R4 evidence and dispatches
  reclaim only through R1 (`_dispatch_reclaim`).

## Relevant tests

- `src/axos/tests/test_reclaim.py` — R1 lease reclaim primitive:
  successful expired reclaim from CLAIMED/RUNNING/COMMITTING;
  non-expired lease rejected with zero mutation; stale token and wrong
  owner rejected; concurrent reclaim with exactly one winner and one
  token increment; old owner cannot commit or renew after reclaim.
- `src/axos/tests/test_expiry_r4.py` — R4 lease-expiry detection:
  unexpired lease observes empty; exact expiry boundary (`==` is
  expired); future worker timestamps cannot prevent expiry; heartbeats
  without renewal leave the lease expired.
