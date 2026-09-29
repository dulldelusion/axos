"""Phase 1B — identity model.

Five distinct identities; never collapsed into one:

- worker_id: durable logical worker identity (the workers table row).
- proc_id: one OS process instance. A restart ALWAYS mints a new proc_id,
  so a resurrected process is distinguishable from its previous instance.
- job_id: the unit of work (store truth).
- lease_id: one lease epoch = (job_id, fencing_token). Each successful claim
  or reclaim mints a new fencing token, i.e. a new lease identity.
- fencing_token: the monotonic authority counter on the job row. Higher
  token == newer authority. An old token can never become valid again.

A worker with the same worker_id but a new proc_id inherits NOTHING: it
must claim a fresh lease (new fencing token) to gain authority.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass


def new_proc_id() -> str:
    """Mint a fresh process-instance identity."""
    return "proc-" + uuid.uuid4().hex[:16]


@dataclass(frozen=True)
class WorkerIdentity:
    """Durable logical worker identity (workers.worker_id)."""
    worker_id: str


@dataclass(frozen=True)
class ProcessInstance:
    """One OS process instance of a worker. Never reused."""
    proc_id: str
    worker_id: str
    pid: int | None = None


@dataclass(frozen=True)
class LeaseRef:
    """One lease epoch: the (job, fencing_token) pair that constitutes a
    single grant of authority. The store row is authoritative; this is the
    handle workers carry and present at the authority boundary."""
    job_id: str
    worker_id: str
    fencing_token: int

    @property
    def lease_id(self) -> str:
        return f"{self.job_id}#{self.fencing_token}"

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.lease_id
