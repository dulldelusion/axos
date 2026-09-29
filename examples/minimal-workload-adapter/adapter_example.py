#!/usr/bin/env python3
"""OPTIONAL workload adapter example — "uppercase-jobs".

Pedagogical demo of the AXOS adapter pattern: a toy workload that consumes
AXOS core **only** through the public TransitionGate API. It claims a job,
runs a pure string transform (the workload's own logic, outside the gate),
stages the result as a content-addressed artifact, and commits it.

Nothing here touches AXOS internals. If the public import path is not
available, this fails loudly instead of faking the API.
"""

from __future__ import annotations

import sys
import tempfile

# --- Public AXOS surface only ---------------------------------------------
# TransitionGate is exported from axos/store/__init__.py (__all__) and
# implemented in axos/store/gate.py. Everything below uses only these names.
try:
    from axos.store import TransitionGate, open_store, migrate, TransitionRejected
except ImportError as exc:  # pragma: no cover - fails loudly, never fakes
    raise SystemExit(
        "OPTIONAL workload example cannot run: the public AXOS import path "
        f"'from axos.store import TransitionGate, open_store, migrate, "
        f"TransitionRejected' failed ({exc}). AXOS core itself is unaffected; "
        "this example is fully skippable."
    ) from exc


# --- Workload logic: the adapter's OWN side of the fence ------------------
# This is the only "domain" code. The gate never sees it.
def transform(payload: str) -> str:
    """Toy execution function: uppercase the payload."""
    return payload.upper()


# --- Adapter: the claim -> commit protocol via TransitionGate -------------
ACTOR = "scheduler"        # scheduler-class actor: may create/transition work
WORKER = "w-example"       # the worker identity for this run


def main() -> None:
    payload = "hello from the optional workload adapter"

    with tempfile.TemporaryDirectory(prefix="axos-adapter-example-") as tmp:
        # Ephemeral throwaway store: this example never touches production.
        store = open_store(f"{tmp}/example.db")
        migrate(store)
        gate = TransitionGate(store, staging_root=f"{tmp}/staging")

        # 1. DEFINE WORK — task + job (payload lives in the task objective).
        task = gate.create_task(
            "task-example-1",
            {"objective": "uppercase payload", "payload": payload},
            {"budget_units": 1},
            ACTOR,
        )
        job = gate.create_job("job-example-1", task["task_id"], "stage-1",
                              ACTOR)
        for state in ("AUTHORIZED", "PLANNED", "EXECUTING"):
            gate.transition_task(task["task_id"], state, ACTOR)

        # 2. CLAIM — PENDING -> CLAIMED, atomically; mints a fencing token.
        if not gate.claim_job(job["job_id"], WORKER, ttl_s=60.0, actor=ACTOR):
            raise SystemExit("claim lost the race (should not happen here)")

        # 3. START — CLAIMED -> RUNNING.
        gate.transition_job(job["job_id"], "RUNNING", ACTOR)

        # Fencing token proving this worker holds the live lease.
        token = gate.get_job(job["job_id"])["fencing_token"]
        wactor = f"worker:{WORKER}"  # worker-class actor for fenced calls

        # 4. EXECUTE — pure workload function, outside the gate.
        result = transform(payload)

        # 5. STAGE — gate writes bytes, computes sha256 itself, registers the
        #    content-addressed artifact as STAGING. Staging is not completion.
        artifact = gate.stage_artifact(
            job_id=job["job_id"], worker_id=WORKER, fencing_token=token,
            task_id=task["task_id"], kind="uppercase-result",
            data=result.encode("utf-8"), actor=wactor,
        )

        # 6. COMMIT PHASE — RUNNING -> COMMITTING, still under the lease.
        gate.begin_commit(job["job_id"], WORKER, token,
                          artifact_id=artifact["artifact_id"], actor=wactor)

        # 7. VERIFY — the GATE re-reads the bytes, recomputes the hash, runs
        #    the structural checks itself. STAGING -> VALIDATED.
        gate.verify_artifact(artifact["artifact_id"], actor=wactor,
                             worker_id=WORKER, fencing_token=token)

        # 8. COMPLETE — the single atomic completion transaction. This is the
        #    ONLY route to COMPLETE; the generic transition API refuses it.
        final = gate.commit_artifact(job["job_id"], WORKER, token,
                                     artifact_id=artifact["artifact_id"],
                                     actor=wactor, evidence={})

        # Sanity: the generic transition API must refuse COMPLETE (I-4).
        try:
            gate.transition_job(job["job_id"], "COMPLETE", ACTOR)
            raise AssertionError("transition_job allowed COMPLETE — wrong")
        except TransitionRejected:
            pass  # expected

        print(f"job {final['job_id']}: {final['status']}")
        print(f"payload : {payload!r}")
        print(f"result  : {result!r}")
        print(f"artifact: {artifact['artifact_id']} "
              f"sha256={artifact['content_hash'][:16]}…")
        print("OPTIONAL example complete — AXOS core ran without this file.")


if __name__ == "__main__":
    main()
