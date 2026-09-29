"""Phase 1A — explicit state-transition graphs.

These graphs are taken verbatim from the corrected Phase 0 documents; the
gate implements exactly these and nothing else.

- task:    05-task-lifecycle.md  (corrected)
- job:     07-job-lifecycle.md   (corrected)
- worker:  06-worker-lifecycle.md (corrected)
- artifact: 04-domain-model.md, Artifact.status
- approval: 04-domain-model.md, Approval.status (ADR-005)
- checkpoint verification: 13-checkpoint-model.md (corrected)

State is never a free-form string: the gate rejects any transition not
listed here, transactionally, leaving authoritative state unchanged.
"""

TASK_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "PROPOSED": ("AUTHORIZED", "REJECTED"),
    "AUTHORIZED": ("PLANNED", "PAUSED_FOR_HUMAN"),
    "PLANNED": ("EXECUTING",),
    "EXECUTING": ("VERIFYING", "PAUSED_FOR_HUMAN", "FAILED", "CANCELLED"),
    "VERIFYING": ("FINALIZED", "PAUSED_FOR_HUMAN"),
    "BLOCKED": ("EXECUTING",),
    "PAUSED_FOR_HUMAN": ("EXECUTING", "CANCELLED"),
    "REJECTED": (),
    "FAILED": (),
    "CANCELLED": (),
    "FINALIZED": (),
}

JOB_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "PENDING": ("CLAIMED",),
    "CLAIMED": ("RUNNING", "PENDING", "FAILED"),
    "RUNNING": ("COMMITTING", "PENDING", "UNCERTAIN", "FAILED"),
    "COMMITTING": ("COMPLETE", "UNCERTAIN", "FAILED"),
    "UNCERTAIN": ("COMPLETE", "PENDING", "QUARANTINED"),
    "FAILED": ("PENDING", "QUARANTINED", "BLOCKED"),
    "QUARANTINED": ("PENDING",),
    "BLOCKED": ("PENDING",),
    "COMPLETE": (),
}

WORKER_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "PROVISIONING": ("IDLE", "DEAD"),
    "IDLE": ("ASSIGNED", "DRAINING"),
    "ASSIGNED": ("RUNNING", "SUSPECT"),
    "RUNNING": ("IDLE", "DRAINING", "SUSPECT"),
    "SUSPECT": ("RUNNING", "DEAD"),
    "DRAINING": ("RETIRED",),
    "DEAD": (),
    "RETIRED": (),
}

ARTIFACT_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "STAGING": ("VALIDATED", "QUARANTINED"),
    "VALIDATED": ("RELEASED", "QUARANTINED"),
    "RELEASED": ("QUARANTINED",),
    "QUARANTINED": (),
}

APPROVAL_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "PENDING": ("APPROVED", "DENIED", "EXPIRED"),
    "APPROVED": (),
    "DENIED": (),
    "EXPIRED": (),
}

CHECKPOINT_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "UNVERIFIED": ("VERIFYING",),
    "VERIFYING": ("VERIFIED", "CORRUPT"),
    "VERIFIED": (),
    "CORRUPT": (),
}

# Human-gated states (I-17): sticky across restarts. The gate refuses to
# leave them without an explicit, recorded human decision.
HUMAN_GATED_FROM = {"PAUSED_FOR_HUMAN"}

# Actors allowed to create work (I-16: workers cannot create work).
# Anything beginning with "worker:" is refused at creation time.
# "reconciler" (Phase 1C R11) may materialize jobs for declared
# desired-state items only, through the gate's idempotent
# ensure_job_for_desired_state op.
WORK_CREATOR_ACTORS = {"human", "scheduler", "system", "test", "reconciler"}

# Actors allowed to invoke lease reclaim (Phase 1C D1 / R1). The supervisor
# is deliberately absent: it owns process termination, never lease
# authority. Only the recovery controller, the reconciler, boot/system
# recovery, an operator-manual action, and tests may revoke a lease.
RECLAIM_ACTORS = {"recovery-controller", "reconciler", "system",
                  "operator", "test"}
