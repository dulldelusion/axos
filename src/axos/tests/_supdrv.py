"""Phase 1B test helper: runs a Supervisor in a subprocess so tests can
SIGKILL the supervisor itself (real supervisor-crash fault injection).

NOT a test file (underscore prefix). Scenarios:
- sup_crash:      start a heartbeat_loop worker, signal readiness, sleep.
- kill_midway:    start a hang worker, signal readiness, kill_worker, sleep.
- restart_midway: start a hang worker, signal readiness, restart_worker,
                  signal readiness2, sleep.

The test SIGKILLs this process at the chosen point, then builds a fresh
Supervisor on the same db file and verifies reconstruction from the store.
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from axos.exec.supervisor import Supervisor


def _ensure_job(gate, job_id="jdrv"):
    try:
        gate.create_task("t", {"objective": "driver"}, {"usd": 1}, "scheduler")
    except Exception:
        pass
    try:
        gate.create_job(job_id, "t", "s", "scheduler")
    except Exception:
        pass


def _wait_for(fn, timeout=20.0):
    import time as _t
    deadline = _t.time() + timeout
    while _t.time() < deadline:
        if fn():
            return True
        _t.sleep(0.1)
    raise RuntimeError("driver wait timed out")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--scenario", required=True,
                    choices=["sup_crash", "kill_midway", "restart_midway",
                             "boot_crash", "fi03"])
    ap.add_argument("--ready", required=True)
    ap.add_argument("--ready2", default=None)
    ap.add_argument("--job-ids", default=None,
                    help="comma-separated job ids for the fi03 scenario")
    ap.add_argument("--worker-ids", default=None,
                    help="comma-separated worker ids for the fi03 scenario")
    args = ap.parse_args()

    sup = Supervisor(args.db, actor="driver-supervisor")
    _ensure_job(sup.gate)

    if args.scenario == "sup_crash":
        sup.start_worker("wdrv", "jdrv",
                         {"kind": "heartbeat_loop", "duration_s": 120.0},
                         ttl_s=120.0, hb_interval_s=0.3)
        _wait_for(lambda: len(sup.gate.heartbeats_for("wdrv")) >= 2)
        open(args.ready, "w").write("ready\n")
        time.sleep(120)
    elif args.scenario == "fi03":
        # R14-B FI-03: a supervisor running two long real workers. The
        # test SIGKILLs THIS driver process mid-flight; the worker
        # processes (separate session leaders) survive, keep renewing
        # their leases, and complete on their own. The task and jobs
        # already exist (created by the test, e.g. from desired state);
        # their ids arrive via --job-ids (comma separated).
        job_ids = (args.job_ids.split(",") if args.job_ids else ["jfi03a",
                                                                 "jfi03b"])
        wids = (args.worker_ids.split(",") if args.worker_ids
                else [f"wdrv-{i}" for i in range(len(job_ids))])
        workers = []
        for wid, jid in zip(wids, job_ids):
            sup.start_worker(wid, jid,
                             {"kind": "success_delayed", "duration_s": 30.0,
                              "steps": 6},
                             ttl_s=120.0, hb_interval_s=0.3, renew=True)
            workers.append(wid)
        _wait_for(lambda: all(
            len(sup.gate.heartbeats_for(w)) >= 2 for w in workers))
        open(args.ready, "w").write("ready\n")
        time.sleep(300)
    elif args.scenario == "boot_crash":
        # R6-19: a live, renewing worker under a supervisor that is about
        # to be SIGKILLed. The worker process (a session leader) survives
        # the supervisor's death; the test SIGKILLs THIS driver process,
        # then rebuilds supervisors on the same db and proves boot
        # recovery converges deterministically.
        sup.start_worker("wdrv", "jdrv",
                         {"kind": "heartbeat_loop", "duration_s": 300.0},
                         ttl_s=120.0, hb_interval_s=0.3, renew=True)
        _wait_for(lambda: len(sup.gate.heartbeats_for("wdrv")) >= 2
                  and sup.gate.get_job("jdrv")["owner_worker_id"] == "wdrv")
        open(args.ready, "w").write("ready\n")
        time.sleep(300)
    elif args.scenario == "kill_midway":
        sup.start_worker("wdrv", "jdrv",
                         {"kind": "hang"}, ttl_s=120.0, hb_interval_s=0.3)
        _wait_for(lambda: sup.gate.get_job("jdrv")["owner_worker_id"]
                  == "wdrv")
        open(args.ready, "w").write("ready\n")
        sup.kill_worker("wdrv")
        time.sleep(120)
    elif args.scenario == "restart_midway":
        sup.start_worker("wdrv", "jdrv",
                         {"kind": "hang"}, ttl_s=120.0, hb_interval_s=0.3)
        _wait_for(lambda: sup.gate.get_job("jdrv")["owner_worker_id"]
                  == "wdrv")
        open(args.ready, "w").write("ready\n")
        sup.restart_worker("wdrv", "jdrv",
                           {"kind": "hang"}, ttl_s=120.0, hb_interval_s=0.3)
        if args.ready2:
            open(args.ready2, "w").write("ready2\n")
        time.sleep(120)
    return 0


if __name__ == "__main__":
    sys.exit(main())
