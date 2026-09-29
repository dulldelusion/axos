import os, sys, tempfile, shutil, threading
_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(_HERE)))  # ~/workspace
from axos.store import open_store, migrate, TransitionGate
tmp = tempfile.mkdtemp(); db = os.path.join(tmp, 'a.db')
st = open_store(db); migrate(st); g = TransitionGate(st)
g.create_task('t', {}, {}, 'scheduler'); g.create_job('j', 't', 's', 'scheduler')
bad = 0
for r in range(20):
    # NOTE(F1): fresh job per round instead of the old direct-UPDATE reset.
    jid = f"j7-{r}"
    g.create_job(jid, "t", "s", "scheduler")
    g.claim_job(jid, "w1", 600.0, "scheduler")
    tok = g.get_job(jid)["fencing_token"]
    res = {}
    def renew(j=jid):
        s = open_store(db); res['renew'] = TransitionGate(s).renew_lease(j, "w1", tok, 600.0, "s"); s.close()
    def release(j=jid):
        s = open_store(db); res['release'] = TransitionGate(s).release_lease(j, "w1", tok, "s"); s.close()
    a, b = threading.Thread(target=renew), threading.Thread(target=release)
    a.start(); b.start(); a.join(); b.join()
    # Both-True is a VALID sequential history (renew won, then release won):
    # what must hold is end-state consistency, not mutual exclusion.
    fin = g.get_job(jid)
    consistent = (
        (res['renew'] and res['release'] and fin['owner_worker_id'] is None) or
        (res['renew'] and not res['release'] and fin['owner_worker_id'] == 'w1'
         and fin['lease_expires_at'] > 0) or
        (not res['renew'] and res['release'] and fin['owner_worker_id'] is None)
    )
    if not consistent:
        bad += 1
        print("round", r, res, fin['owner_worker_id'])
    ok, detail = g.verify_ledger_chain()
    assert ok, detail
print("renew-vs-release race: inconsistent end states =", bad, "/ 20")
shutil.rmtree(tmp, ignore_errors=True)
