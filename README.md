# AXOS — Autonomous Execution OS

AXOS is a durable, SQLite-backed execution kernel: a **recovery
kernel** for long-running autonomous work. It models work as Tasks and
Jobs, executes them through leased worker claims with fencing tokens,
and recovers from crashes through a verified, budgeted recovery
ladder. The durable store is authoritative; the AI reasoning that
drives workers is not.

This repository is the canonical public home of the AXOS
implementation — frozen at release
`551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
(migration v12). It contains the complete source, the full test suite,
the contracts, the invariant registry, the audit evidence, and the
tooling to reproduce and verify the release byte-for-byte. It contains
no workload data, no prompts, no production assets, and no
application business logic.

## Status

| Aspect | State |
|---|---|
| Implementation | **Complete** — `src/axos/` (`exec/`, `store/`, `tests/`, `audit/`) |
| Tests | **Passing** — full suite green on the frozen release |
| Audits | **Complete** — R12/R13 verified, R14 internal campaign complete |
| Release identity | **Verified** — `tools/verify_release.py` recomputes `551d559c…` |
| External power-cycle proof (R14 FI-04/FI-12) | **Blocked** — `R14_BLOCKED — REAL-VM PROOF INCOMPLETE` (see below) |

**Honest status.** The R14 internal campaign is complete, but the
external hypervisor-level power-cycle proof for failure injection
(FI-04/FI-12) was never obtainable in the build environment — no
hypervisor tools, no socket, no metadata service. No substitute
simulation was run and no evidence was manufactured. The correct
status is `R14_BLOCKED — REAL-VM PROOF INCOMPLETE`; see
`reports/historical/R14-EXTERNAL-VALIDATION.md`. The execution ledger
is tamper-evident, not tamper-proof.

## Quickstart

Prerequisites: Linux x86_64, Python 3.12+, SQLite 3.45+.

```bash
git clone https://github.com/dulldelusion/axos.git
cd axos

# 1. Confirm the release identity (recomputes it from src/axos/)
python3 tools/verify_release.py

# 2. Bootstrap: environment -> init -> validate -> self-test
bash scripts/bootstrap/bootstrap.sh
```

The bootstrap ends with an `AXOS READINESS REPORT` and
`STATUS: READY`. It **never starts workers or production** — governance
boots `STOPPED` with zero workers and no adopted workload.

Optional: the full test suite (~minutes):

```bash
PYTHONPATH="$PWD/src" python3 -m pytest src/axos/tests/ -q -p no:cacheprovider
```

A fresh agent starting from only this repository should follow
[`docs/reproducibility/MUSE_BOOTSTRAP.md`](docs/reproducibility/MUSE_BOOTSTRAP.md).

## Repository layout

```
src/axos/                 frozen AXOS implementation (DO NOT MODIFY)
  exec/                   supervisor, workers, scheduler, recovery,
                          reconciler, watchdog, finalizer, policy
  store/                  SQLite store, TransitionGate, migrations
  tests/                  full test suite
  audit/                  audit scripts (transition attacks, crash, …)
  release_manifest.json   frozen release identity (551d559c…)
  R14-PLAN.md             R14 campaign plan (historical)
docs/                     architecture, contracts, invariants, recovery,
                          operations, reproducibility, decisions
contracts/                machine-readable contracts (transitions.json,
                          recovery-ladder.json, recovery-rungs.json)
schemas/                  schema v12 documentation
migrations/               migration documentation
scripts/
  bootstrap/              four-stage bootstrap (env -> init -> validate
                          -> self-test); never starts workers
  development/ testing/ verification/ operations/
config/                   environment contract, runtime requirements
tools/                    verify_release.py, extract_contracts.py
examples/                 minimal-workload-adapter (toy; NOT production)
reports/
  audits/                 audit reports
  verification/           portability / verification reports
  historical/             R-series and Phase 1 evidence (unchanged)
```

Runtime state created by the bootstrap lives in `.axos-state/`
(gitignored) or `$AXOS_STATE_ROOT` — never in the source tree.

## Core truths

- **Durable state is authoritative.** `TransitionGate` is the narrow
  authoritative mutation API; the gate owns the writable `Store`,
  everything else reads through `ReadOnlyStore`.
- **Leases are the job-row triple** `(owner_worker_id,
  lease_expires_at, fencing_token)`; timestamps are store time.
- **Recovery success requires verified positive progress** (I-18);
  zero-progress "success" is rejected in code and by DB constraint.
- **`PAUSED_FOR_HUMAN` exits only via durable `APPROVED`** — across
  restarts.
- **Workers cannot create tasks or jobs.** Uncertain completion is
  reconciled, never assumed.

Start reading at [`docs/README.md`](docs/README.md), then
[`docs/architecture/overview.md`](docs/architecture/overview.md).

## Contributing / security / license

- [CONTRIBUTING.md](CONTRIBUTING.md) — note: this release is frozen;
  the file governs tooling/docs contributions.
- [SECURITY.md](SECURITY.md) — how to report vulnerabilities.
- [License: MIT](LICENSE)
