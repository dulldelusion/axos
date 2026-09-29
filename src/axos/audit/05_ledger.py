"""Audit 05 — attack the ledger.

Hash chaining, ordering, atomicity, tamper/missing/reorder/duplicate/payload
mutation detection, actor attribution, timestamp semantics. Plus the honest
question: what can an attacker with ordinary DB write access do?
"""
import hashlib
import json
import os
import shutil
import sys
import tempfile

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(_HERE)))

from axos.store import open_store, migrate, TransitionGate

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond), detail))
    print(("ok    " if cond else "DEFECT") + f" {name}" + (f" — {detail}" if detail else ""))


def canon(o):
    return json.dumps(o, sort_keys=True, separators=(",", ":"))


tmp = tempfile.mkdtemp(prefix="audit5-")
db = os.path.join(tmp, "a.db")
st = open_store(db)
migrate(st)
g = TransitionGate(st)
g.create_task("t", {}, {}, "scheduler")
for i in range(5):
    g.append_event("note", {"i": i}, f"actor-{i}")

ok, detail = g.verify_ledger_chain()
check("chain verifies on honest history", ok, detail)

# payload mutation of a middle event
with st.write_txn() as (conn, now):
    conn.execute("UPDATE ledger SET payload=? WHERE seq=3", (canon({"i": 999}),))
ok, detail = g.verify_ledger_chain()
check("payload mutation detected", not ok, detail)

# restore honest state for next attacks: rebuild db fresh
st.close()
shutil.rmtree(tmp, ignore_errors=True)
tmp = tempfile.mkdtemp(prefix="audit5-")
db = os.path.join(tmp, "a.db")
st = open_store(db)
migrate(st)
g = TransitionGate(st)
g.create_task("t", {}, {}, "scheduler")
for i in range(5):
    g.append_event("note", {"i": i}, f"actor-{i}")

# missing middle event
with st.write_txn() as (conn, now):
    conn.execute("DELETE FROM ledger WHERE seq=3")
ok, detail = g.verify_ledger_chain()
check("missing middle event detected", not ok, detail)

st.close()
shutil.rmtree(tmp, ignore_errors=True)
tmp = tempfile.mkdtemp(prefix="audit5-")
db = os.path.join(tmp, "a.db")
st = open_store(db)
migrate(st)
g = TransitionGate(st)
g.create_task("t", {}, {}, "scheduler")
for i in range(5):
    g.append_event("note", {"i": i}, f"actor-{i}")

# tail truncation: delete the last two events — chain still verifies?
with st.write_txn() as (conn, now):
    conn.execute("DELETE FROM ledger WHERE seq >= 5")
ok, detail = g.verify_ledger_chain()
check("tail truncation is UNDETECTABLE by chain verify", ok,
      f"verify says: {detail} — no external anchor exists")

# reordered events: swap payloads of seq 2 and 4
with st.write_txn() as (conn, now):
    conn.execute("UPDATE ledger SET payload=? WHERE seq=2", (canon({"i": 3}),))
    conn.execute("UPDATE ledger SET payload=? WHERE seq=4", (canon({"i": 1}),))
ok, detail = g.verify_ledger_chain()
check("reordered payloads detected", not ok, detail)

st.close()
shutil.rmtree(tmp, ignore_errors=True)
tmp = tempfile.mkdtemp(prefix="audit5-")
db = os.path.join(tmp, "a.db")
st = open_store(db)
migrate(st)
g = TransitionGate(st)
g.create_task("t", {}, {}, "scheduler")
for i in range(5):
    g.append_event("note", {"i": i}, f"actor-{i}")

# FULL REWRITE: attacker recomputes the entire chain with a fabricated event
rows = st.conn.execute("SELECT seq, event_type, payload, actor, ts FROM ledger ORDER BY seq").fetchall()
with st.write_txn() as (conn, now):
    conn.execute("DELETE FROM ledger")
    prev = "0" * 64
    seq = 0
    for r in rows:
        seq += 1
        payload = {"i": "FABRICATED", "was": json.loads(r["payload"])} if seq == 3 else json.loads(r["payload"])
        body = canon({"seq": seq, "type": r["event_type"], "payload": payload,
                      "actor": "attacker", "ts": r["ts"]})
        h = hashlib.sha256(f"{prev}:{body}".encode()).hexdigest()
        conn.execute("INSERT INTO ledger(seq,event_type,payload,actor,ts,prev_hash,hash)"
                     " VALUES(?,?,?,?,?,?,?)",
                     (seq, r["event_type"], canon(payload), "attacker", r["ts"], prev, h))
        prev = h
ok, detail = g.verify_ledger_chain()
check("full chain rewrite is UNDETECTABLE (no secret in hash)", ok,
      f"verify says: {detail} — tamper-EVIDENT, not tamper-PROOF")

# actor attribution is caller-supplied
g.append_event("note", {"x": 1}, "totally-legit-supervisor")
row = st.conn.execute("SELECT actor FROM ledger ORDER BY seq DESC LIMIT 1").fetchone()
check("actor field is self-attested", row[0] == "totally-legit-supervisor",
      "no authentication of actor identity")

# timestamps are store-stamped even for direct appends
with st.write_txn() as (conn, now):
    ts_row = conn.execute("SELECT ts FROM ledger ORDER BY seq DESC LIMIT 1").fetchone()
check("ledger ts comes from store clock", ts_row[0] > 0)

# atomicity: transition + ledger event commit together — kill between them is
# impossible inside one txn; verify a failed transition appends nothing
n0 = st.conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
try:
    g.transition_task("t", "FINALIZED", "scheduler")  # illegal from PROPOSED
except Exception:
    pass
n1 = st.conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
check("rejected transition appends no ledger event", n0 == n1)
check("rejected transition leaves task unchanged",
      g.get_task("t")["status"] == "PROPOSED")

st.close()
shutil.rmtree(tmp, ignore_errors=True)
bad = [r for r in results if not r[1]]
print(f"\n{len(results)-len(bad)}/{len(results)} ledger attacks behaved as expected; {len(bad)} DEFECTS")
for b in bad:
    print("  DEFECT:", b[0], "-", b[2])
