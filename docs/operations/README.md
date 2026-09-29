# AXOS Operations

Operator documentation for running, testing, and verifying AXOS
(Autonomous Execution OS), release
`551d559cf3489148fe8966b56376dfab6c5b909278685bd623eed0e7cef4c192`
(migration v12, policy `r9-policy/v1`).

These documents describe **operation of the frozen release**, not its
design. Design, contracts, invariants, and recovery theory live under
`docs/architecture/`, `docs/contracts/`, `docs/invariants/`, and
`docs/recovery/`.

| Document | Covers |
|---|---|
| [bootstrap.md](bootstrap.md) | One-shot installer: the 4 bootstrap stages, environment variables, governance defaults, what bootstrap never does |
| [testing.md](testing.md) | Test suite organization, how to run the full suite, R14, the adversarial audits, known test-environment quirks |
| [verification.md](verification.md) | Proving a checkout is the frozen release: release identity, ledger verification, DB integrity, the authority audit, health checks, and the honest status of R1–R14 |
| [troubleshooting.md](troubleshooting.md) | Real failure modes and their resolutions, including fresh-clone issues |

Conventions used throughout:

- `<repo>` is the repository root (the directory containing `src/`,
  `scripts/`, `docs/`, ...).
- Commands are run from `<repo>` unless stated otherwise.
- AXOS reads **zero** environment variables for its own configuration
  (see `config/ENVIRONMENT_CONTRACT.md`). The `AXOS_REPO_ROOT` and
  `AXOS_STATE_ROOT` variables below belong to the bootstrap *scripts*,
  not to AXOS itself; all AXOS runtime configuration is passed as
  function/constructor arguments.
- There is **no operator CLI** in AXOS. The only shipped CLI entrypoint
  is the worker subprocess (`python -m axos.exec.worker`). Everything
  else is driven through the Python API with `<repo>/src` on
  `PYTHONPATH`.

Safety rules that bind every operator (from the frozen release's
operator handoff):

1. No automatic worker start — workers spawn only through
   `Supervisor.start_worker()` as a consequence of an explicit operator
   action.
2. No automatic production resume — after reconnecting an existing DB,
   run `boot_recover()` and read the report first.
3. No clearing human gates — `PAUSED_FOR_HUMAN` exits only via a
   recorded human `APPROVED` approval. Automation must never
   manufacture it.
4. No gate bypasses — all authoritative writes go through
   `TransitionGate`. Never write to the DB with raw SQL from outside
   the gate.
5. No inventing authority — worker timestamps, heartbeats, exit codes,
   and process existence are evidence, never authority.
