"""Phase 1B authority audit (brief section 13), extended for Phase 1C R5.

Searches the entire new execution layer for authority-boundary violations.
Every check must PASS; any failure blocks the gate.

Checks:
  A1  exec/ never calls write_txn() (the gate-owned transaction capability)
  A2  exec/ never opens its own sqlite3 connections
  A3  exec/ contains no SQL write statements (INSERT/UPDATE/DELETE/DDL)
  A4  every store.conn touch in exec/ is read-only reconstruct() SELECTs
  A5  worker_reported_ts is never used for an authority decision in exec/
  A6  the gate's write-path inventory: every authoritative mutation goes
      through TransitionGate.write_txn; for the fenced write methods
      (stage_artifact, begin_commit, verify_artifact, commit_artifact,
      fail_job_execution, ingest_heartbeat, reclaim_lease) every fencing
      rejection (LeaseError, and state validation) happens inside the
      authoritative transaction, and no write statement precedes the final
      validation (verified structurally with the ast module, not regex)
  A7  the supervisor caches no lease/token/job authority in memory
      (_procs holds process handles + execution evidence only: known_token
      / spawn_store_ts document the observed epoch, never decide it);
      R2 additions: the supervisor never calls reclaim_lease (termination
      only), and known_token is assigned only None or the durable job
      row's fencing_token, always compared against a freshly-read row
  A8  worker.py performs no store access except through gate methods
  A11 (R5) the verified-artifact completion contract: the old
  A20 (R14) no private connection-capability access in exec/:
      Store._conn/_ro_conn are unreachable outside the store (A1-A3
      block write_txn/sqlite3.connect/SQL writes, but the raw writable
      Store._conn is a separate capability from the read-only
      Store.conn that A4 permits for reconstruct() SELECTs).
  A11 (R5) the verified-artifact completion contract: the old
      commit_job_result path is dead (I-4 removal proof); fencing checks
      run inside the authoritative transaction at every protocol step;
      only verify_artifact can mark an artifact VERIFIED and its worker
      path is fenced; the corrupt-verdict quarantine is the only write
      before verify_artifact's final raise; commit_artifact re-verifies
      the full predicate inside the atomic transaction; validation
      receipts bind the exact content hash; latest-known-good advances
      only through verify_checkpoint; COMPLETE is written only by
      commit_artifact and transition_job refuses COMPLETE.

  A15  ... (see section header in-file for the R9 policy checks)

  A16 (R10, extended R12) the scheduler is admission + atomic claim +
      dispatch only:
      claims solely via gate.claim_job_bounded/claim_job_resilient/
      claim_job (R12's claim path adds breaker admission; the scheduler
      additionally consults the read-only breaker_allows preflight and
      resolves the DESIRED scope via get_desired_work_id_for_job, but
      never moves a breaker); no write_txn,
      no sqlite3, no SQL writes, no process signaling/killing, no
      reclaim/release, no COMPLETE path, no job creation/deletion, no
      recovery/policy imports, no reconciliation/circuit-breaker/
      load-shedding; boot-ready gates the admission path; capacity
      predicate shares the claim write_txn; dispatch passes the re-read
      durable token; worker --expect-token mismatch exits side-effect-
      free; capacity validated >= 1.

  A17 (R11) the reconciler is observe-and-converge only: desired-state
      reads go through gate read-only methods, and it never touches a
      store connection or transaction; reconcile() pins a version and
      set/retire CAS-bump the head by exactly +1 inside one write_txn;
      snapshot/job identity is deterministic sha256 over canonical JSON
      with no uuid/random in the identity path; jobs are created only
      through TransitionGate.ensure_job_for_desired_state (canonical
      identity, PRIMARY KEY race -> IntegrityError -> re-read the
      winner, DesiredStateConflict on collision); no claim, dispatch,
      process, lease, fence, artifact, or recovery authority; no
      COMPLETE path (COMPLETE only an observed diff status);
      PAUSED_FOR_HUMAN scopes never get jobs; stale desired pins fail
      closed; the module contains zero SQL strings; "reconciler" is a
      registered work-creator while worker:-prefixed actors stay barred
      (I-16); retired items are tombstones, never deleted.

  A18 (R12) the circuit-breaker authority envelope: the controller is
      read/evaluate only and the scheduler is admit-only. Single store
      path (one Store per controller, all breaker writes through the
      gate's breaker API); CAS on every transition_breaker
      (expected_version in the WHERE clause, loser re-reads); signal
      dedupe via INSERT OR IGNORE on the scope-qualified signal_id;
      breaker timestamps come from write_txn's store-clock `now`,
      never wall time; the controller cannot claim/dispatch/signal-
      kill/complete/reclaim and never writes PAUSED_FOR_HUMAN or
      recovery rungs; the scheduler never moves a breaker (read-only
      preflight + resilient claim only); corrupt breaker rows fail
      closed (deny admission, inert to the controller); OPEN never
      transitions directly to CLOSED; no SQL/sqlite3 in
      exec/resilience.py; no duplicate breaker-state writers outside
      the gate; R10/R11 authority preserved.

  A19 (R13) the release-finalization authority envelope: the finalizer
      is read/evaluate only — zero SQL in exec/finalizer.py (AST), the F1
      boundary intact, and none of the R1–R12 job/lease/fence/dispatch/
      desired-state/policy/breaker/artifact/human-gate authorities are
      callable from it; only the gate's R13 API writes
      finalization_runs (CAS on expected_version, FinalizationConflict
      on the loser); the release generation is bound to the
      desired-state version ("ds-v" + version, no global finalized
      boolean, contradictory re-begins fail closed, worker actors
      barred); evaluate pins the desired head inside the authoritative
      txn and publish re-reads it (STALE_GENERATION on drift); publish
      demands a VERIFIED R5 release checkpoint with a release
      attestation receipt; the manifest is deterministic sha256 over
      canonical JSON (no uuid/random/wall-clock in the manifest
      builders, time.time() in finalizer.py only as _loop telemetry);
      FINALIZED is written by exactly one UPDATE and the idempotent
      re-publish verifies the record with zero writes; no second module
      holds finalization_runs DML.

Run: python3 audit/09_authority_audit.py  (from ~/workspace/axos)
"""
import ast
import os
import re
import sys
import textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
AXOS = os.path.dirname(HERE)
sys.path.insert(0, os.path.dirname(AXOS))

EXEC = os.path.join(AXOS, "exec")
FAILURES = []


def check(name, cond, detail=""):
    print(f"[{'PASS' if cond else 'FAIL'}] {name}"
          + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def py_files():
    out = []
    for root, _, files in os.walk(EXEC):
        for f in files:
            if f.endswith(".py"):
                out.append(os.path.join(root, f))
    return sorted(out)


def read(p):
    with open(p) as fh:
        return fh.read()


def code_lines(p):
    """Yield (lineno, line) excluding comments and docstring/string bodies.

    A small state machine: triple-quoted strings are skipped wholesale so
    that prose like 'never calls store.write_txn()' is not mistaken for
    a call.
    """
    src = read(p)
    out = []
    in_str = None
    for i, line in enumerate(src.splitlines(), 1):
        s = line
        if in_str:
            end = s.find(in_str)
            if end == -1:
                continue
            s = s[end + 3:]
            in_str = None
        while True:
            m = re.search(r'("""|\'\'\')', s)
            if not m:
                break
            q = m.group(1)
            rest = s[m.end():]
            end = rest.find(q)
            if end == -1:
                s = s[:m.start()]
                in_str = q
                break
            s = s[:m.start()] + rest[end + 3:]
        stripped = s.strip()
        if stripped.startswith("#"):
            continue
        out.append((i, s))
    return out


# ------------------------------------------------------------------ A1 / A2
code_hits = [f"{os.path.basename(p)}:{i}"
             for p in py_files()
             for i, line in code_lines(p)
             if "write_txn(" in line]
check("A1 no write_txn() calls in exec/",
      not code_hits, f"hits: {code_hits}")

conn_hits = [f"{os.path.basename(p)}:{i}"
             for p in py_files()
             for i, line in code_lines(p)
             if "sqlite3.connect" in line]
check("A2 no direct sqlite3 connections in exec/",
      not conn_hits, f"hits: {conn_hits}")

# ---------------------------------------------------------------------- A3
write_kw = re.compile(
    r'"[^"]*\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|ALTER\s+TABLE|'
    r"CREATE\s+TABLE|DROP\s+TABLE)\b", re.IGNORECASE)
kw_hits = []
for p in py_files():
    for i, line in code_lines(p):
        if write_kw.search(line):
            kw_hits.append(f"{os.path.basename(p)}:{i}:{line.strip()[:80]}")
check("A3 no SQL write statements in exec/",
      not kw_hits, f"hits: {kw_hits}")

# ---------------------------------------------------------------------- A4
conn_uses = []
for p in py_files():
    src = read(p).splitlines()
    for i, line in code_lines(p):
        if ".conn.execute(" in line or ".conn =" in line:
            window = "\n".join(src[i - 1:i + 2])
            conn_uses.append((os.path.basename(p), i, window))
ok_a4 = True
for base, i, window in conn_uses:
    is_select_only = ("SELECT" in window.upper()
                      and not re.search(r"\b(INSERT|UPDATE|DELETE|ALTER|CREATE|"
                                        r"DROP)\b", window, re.IGNORECASE))
    in_reconstruct = base == "supervisor.py"
    if not (is_select_only and in_reconstruct):
        ok_a4 = False
        print(f"      suspicious conn use: {base}:{i}")
check("A4 store.conn touches are SELECT-only inside reconstruct()",
      ok_a4, f"{len(conn_uses)} touches")

# ---------------------------------------------------------------------- A5
uses = [(os.path.basename(p), i, line.strip())
        for p in py_files()
        for i, line in code_lines(p)
        if "worker_reported_ts" in line]
decision_use = [u for u in uses
                if re.search(r"(<=|>=|==|!=|<|>)", u[2])
                or re.search(r"\b(expir|fencing|owner)\w*\b.*\bts\b", u[2])]
check("A5 worker_reported_ts never drives authority in exec/",
      not decision_use, f"hits: {decision_use}")
print(f"      informational pass-through sites: {len(uses)}")

# ---------------------------------------------------------------------- A6
from axos.store.gate import TransitionGate  # noqa: E402
from axos.exec.supervisor import Supervisor  # noqa: E402
import inspect  # noqa: E402

WRITE_SQL = ("INSERT", "UPDATE", "DELETE")
AUTH_KW = re.compile(r"\b(row|job|owner|fencing|lease|expir|status)\w*\b",
                     re.IGNORECASE)


def method_ast(fn):
    return ast.parse(textwrap.dedent(inspect.getsource(fn)))


def txn_span(tree):
    """(start, end) linenos of the `with self.store.write_txn()` body.

    In source order (earliest block first) when a method holds more than
    one: ast.walk is breadth-first, which would otherwise return a later
    block for methods whose first transaction sits inside a try/.
    """
    spans = []
    for node in ast.walk(tree):
        if isinstance(node, ast.With):
            for item in node.items:
                if "write_txn" in ast.dump(item.context_expr):
                    spans.append((node.lineno, node.end_lineno))
    if not spans:
        raise AssertionError("no write_txn with-block found")
    return min(spans)


def conn_writes(tree):
    """Linenos of conn.execute(INSERT|UPDATE|DELETE...) and _append_event."""
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Attribute) and f.attr == "_append_event":
            out.append(node.lineno)
        elif (isinstance(f, ast.Attribute) and f.attr == "execute"
              and isinstance(f.value, ast.Name)
              and f.value.id == "conn"
              and node.args
              and isinstance(node.args[0], ast.Constant)
              and isinstance(node.args[0].value, str)
              and node.args[0].value.strip().upper().startswith(WRITE_SQL)):
            out.append(node.lineno)
    return sorted(out)


write_methods = []
for name, fn in inspect.getmembers(TransitionGate, inspect.isfunction):
    if "write_txn()" in inspect.getsource(fn):
        write_methods.append(name)
print(f"      gate write-path inventory ({len(write_methods)}): "
      f"{', '.join(sorted(write_methods))}")
check("A6a commit_artifact is a gate write path",
      "commit_artifact" in write_methods)
check("A6b fail_job_execution is a gate write path",
      "fail_job_execution" in write_methods)
check("A6b2 ingest_heartbeat is a gate write path",
      "ingest_heartbeat" in write_methods)

# R5 fenced write methods: the full artifact completion protocol plus the
# pre-existing fenced paths.
_FENCED_METHODS = ("stage_artifact", "begin_commit", "verify_artifact",
                   "commit_artifact", "fail_job_execution",
                   "ingest_heartbeat", "reclaim_lease")
for meth in _FENCED_METHODS:
    tree = method_ast(getattr(TransitionGate, meth))
    t0, t1 = txn_span(tree)
    raises = [n for n in ast.walk(tree) if isinstance(n, ast.Raise)]
    # A rejection outside the transaction is acceptable only when it is pure
    # input validation: it must not reference the fetched authoritative row.
    # Anything that consults row state (owner/token/expiry/status) must be
    # inside the txn, where it serializes with the mutation.
    ok_c = True
    for r in raises:
        inside = t0 <= r.lineno <= t1
        if not inside:
            touches_row = any(
                isinstance(n, ast.Name) and n.id == "row"
                for n in ast.walk(r))
            if touches_row:
                ok_c = False
                print(f"      {meth}: line {r.lineno} rejects on row state"
                      f" outside the txn")
    check(f"A6c {meth}: fencing rejections inside the txn", ok_c)
    writes = conn_writes(tree)
    # Only rejections inside the transaction gate the mutation. A
    # post-transaction re-raise (e.g. surfacing a fenced LeaseError after
    # the rollback) cannot gate a write and is excluded.
    last_raise = max((r.lineno for r in raises if t0 <= r.lineno <= t1),
                     default=0)
    check(f"A6d {meth}: no write before final validation",
          all(w > last_raise for w in writes),
          f"writes {writes}, last raise {last_raise}")

# ------------------------------------------------------------------ A7 / A8
sup_src = read(os.path.join(EXEC, "supervisor.py"))
auth_cache = re.findall(r"self\._(lease\w*|token\w*|fencing\w*|authority\w*)\b",
                        sup_src)
check("A7a supervisor caches no lease/token authority in memory",
      not auth_cache, f"hits: {auth_cache}")
procinfo_body = re.search(r"class _ProcInfo:(.*?)(?=\nclass |\Z)", sup_src,
                          re.DOTALL).group(1)
check("A7b _ProcInfo holds process handles + execution evidence only "
      "(known_token/spawn_store_ts permitted; no fencing_token/lease state)",
      "fencing_token" not in procinfo_body and "lease" not in procinfo_body
      and "known_token" in procinfo_body,
      "must document the observed authority epoch without mirroring the "
      "job row's authority field")

# A7c (R2, refined R6): the supervisor's fencing/enforcement paths own
# process termination ONLY — they must never call reclaim_lease. Lease
# revocation stays a recovery-controller / reconciler / system / operator
# primitive. Phase 1C R6 adds exactly one exception: the boot RECOVERY
# pass (exec/boot.py — not the sweep, not enforcement) invokes the R1
# primitive with the system actor, the actor R1's contract allows for
# boot. Those calls are pinned to actor="system" below.
reclaim_calls = []
for p in py_files():
    for lineno, line in code_lines(p):
        if re.search(r"\breclaim_lease\s*\(", line):
            reclaim_calls.append(f"{os.path.basename(p)}:{lineno}")
non_boot = [h for h in reclaim_calls if not h.startswith("boot.py:")
            and not h.startswith("recovery.py:")]
check("A7c supervisor fencing/enforcement never calls reclaim_lease "
      "(termination only)", not non_boot, f"hits: {non_boot}")
_boot_src_a7c = read(os.path.join(EXEC, "boot.py"))
_boot_tree_a7c = ast.parse(_boot_src_a7c)
_boot_reclaims = [
    n for n in ast.walk(_boot_tree_a7c)
    if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    and n.func.attr == "reclaim_lease"]
_boot_actors = [
    next((kw.value.value for kw in n.keywords if kw.arg == "actor"), None)
    for n in _boot_reclaims]
check("A7c the R6 boot pass reclaims only as the system actor",
      len(_boot_reclaims) == len(_boot_actors)
      and all(a == "system" for a in _boot_actors),
      f"actors={_boot_actors}")

# Phase 1C R8: the recovery controller is the other R1-authorized
# reclaim caller. Its reclaim calls are pinned to the
# "recovery-controller" actor, the actor R1's contract allows.
_rc_src_a7c = read(os.path.join(EXEC, "recovery.py"))
_rc_tree_a7c = ast.parse(_rc_src_a7c)
_rc_reclaims = [
    n for n in ast.walk(_rc_tree_a7c)
    if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
    and n.func.attr == "reclaim_lease"]
_rc_actors = [
    next((kw.value.value for kw in n.keywords if kw.arg == "actor"), None)
    for n in _rc_reclaims]
check("A7c the R8 controller reclaims only as recovery-controller",
      len(_rc_reclaims) == len(_rc_actors)
      and all(a == "recovery-controller" for a in _rc_actors),
      f"actors={_rc_actors}")

# A7d (R2): known_token is execution evidence, never authority. It may
# only ever be assigned None (at spawn) or the durable job row's
# fencing_token; every fencing judgment must compare it against a
# freshly-read durable row (job["fencing_token"]).
tree = ast.parse(sup_src)
bad_assign = []
for node in ast.walk(tree):
    if isinstance(node, ast.Assign):
        for t in node.targets:
            if (isinstance(t, ast.Attribute)
                    and t.attr == "known_token"):
                rhs = ast.dump(node.value)
                ok_rhs = (isinstance(node.value, ast.Constant)
                          and node.value.value is None)
                # `info.known_token = token` where token is the durable
                # row's fencing_token (verified by the name check below).
                ok_rhs = ok_rhs or (isinstance(node.value, ast.Name)
                                    and node.value.id == "token")
                if not ok_rhs:
                    bad_assign.append(
                        f"line {node.lineno}: rhs={rhs[:80]}")
    if isinstance(node, ast.AnnAssign):
        t = node.target
        if isinstance(t, ast.Attribute) and t.attr == "known_token":
            if node.value is not None:
                bad_assign.append(f"line {node.lineno}: annotated assign "
                                  f"with value")
check("A7d known_token assigned only None or the durable job-row token",
      not bad_assign, f"hits: {bad_assign}")
durable_reads = (
    'job["fencing_token"]' in sup_src or "job['fencing_token']" in sup_src)
fence_uses_durable = ("def _is_fenced_tracked" in sup_src
                      and durable_reads)
check("A7d fencing judgment compares against the durable job row",
      fence_uses_durable)

store_accesses = set()
for p in py_files():
    if os.path.basename(p) != "worker.py":
        continue
    for _, line in code_lines(p):
        for m in re.finditer(r"\b(store|gate)\.(\w+)", line):
            store_accesses.add((m.group(1), m.group(2)))
non_gate = {(o, m) for o, m in store_accesses
            if o == "store" and m not in ("close",)}
check("A8 worker.py touches Store only via gate methods (+close)",
      not non_gate, f"hits: {non_gate}")

# ------------------------------------------------------------------ A9 (R3)
# R3: durable heartbeat ingestion + informational progress.
# A9a: heartbeat rows are written only by the gate. The worker/exec layer
# must never emit SQL for heartbeats. (code_lines skips string literals by
# design, so this check parses string constants via AST instead; test and
# audit files are excluded — they must use gate methods too, but the
# invariant under audit is about the shipped source.)
PKG = AXOS  # the axos/ package root


def tree_py_files():
    out = []
    for root, dirs, files in os.walk(PKG):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for f in files:
            if f.endswith(".py"):
                out.append(os.path.join(root, f))
    return sorted(out)


hb_insert_files = set()
for p in tree_py_files():
    rel = os.path.relpath(p, PKG)
    if rel.startswith("tests") or rel.startswith("audit"):
        continue
    tree = ast.parse(read(p))
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and re.search(r"INSERT\s+INTO\s+heartbeats",
                             node.value, re.IGNORECASE)):
            hb_insert_files.add(rel)
check("A9a heartbeat rows are written only by the gate "
      "(INSERT INTO heartbeats appears in store/gate.py and nowhere else)",
      hb_insert_files == {os.path.join("store", "gate.py")},
      f"hits: {sorted(hb_insert_files)}")

# A9b: the ingestion transaction's durable writes are liveness evidence
# (+ informational progress) only. The full R3 write set is the
# ingest_heartbeat txn body plus the two helpers it calls inside the txn
# (_touch_worker, _apply_progress): no write there may touch ownership or
# authority columns (owner, fencing token, lease, status, attempt) and
# nothing may DELETE.
OWNERSHIP_COLS = re.compile(
    r"\b(owner_worker_id|fencing_token|lease_acquired_at|lease_expires_at|"
    r"lease_reclaimed|uncertain_opened|attempt|status)\b", re.IGNORECASE)

ingest_src = inspect.getsource(TransitionGate.ingest_heartbeat)
progress_src = inspect.getsource(TransitionGate._apply_progress)
r3_write_src = ingest_src + progress_src


def r3_writes():
    out = []
    for tree in (method_ast(TransitionGate.ingest_heartbeat),
                 method_ast(TransitionGate._apply_progress)):
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "execute"
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "conn"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[0].value, str)):
                out.append(node.args[0].value.strip())
    return out


r3_sqls = r3_writes()
# The ban targets MUTATION of authoritative rows: no UPDATE may touch
# ownership/authority columns and nothing may DELETE. The INSERT into
# heartbeats legitimately carries fencing_token as evidence (D9 requires
# persisting it) — an evidence row is not an authority mutation.
bad_writes = []
for s in r3_sqls:
    u = s.strip().upper()
    if u.startswith("DELETE"):
        bad_writes.append(s[:70])
    elif u.startswith("UPDATE") and OWNERSHIP_COLS.search(s):
        bad_writes.append(s[:70])
    elif u.startswith("INSERT") and "HEARTBEATS" not in u:
        bad_writes.append(s[:70])
check("A9b ingestion writes are liveness/progress-only "
      "(no ownership, token, lease, status, attempt mutation; no DELETE)",
      not bad_writes and len(r3_sqls) > 0, f"hits: {bad_writes}")

# A9c: the heartbeat path can never reclaim — it must not reference
# reclaim_lease at all (heartbeats are liveness evidence, never ownership
# authority).
check("A9c ingest_heartbeat/_apply_progress never reference reclaim_lease",
      "reclaim_lease" not in r3_write_src)

# A9d: fencing rejection journaling must not swallow the authority error —
# the _FencedHeartbeat handler journals, then raises the LeaseError.
handler_tail = ingest_src.split("except _FencedHeartbeat", 1)[1]
check("A9d fenced-heartbeat journaling re-raises the LeaseError",
      "append_event" in handler_tail and "raise LeaseError" in handler_tail)

# A9e: progress goes through the shared gate path and stamps store time —
# the informational UPDATE must set progress_updated_at alongside the
# progress columns.
prog_update = [s for s in r3_sqls
               if s.upper().startswith("UPDATE JOBS")]
check("A9e _apply_progress stamps jobs.progress_updated_at with the "
      "progress write",
      len(prog_update) == 1
      and "progress_done" in prog_update[0]
      and "progress_total" in prog_update[0]
      and "progress_updated_at" in prog_update[0],
      f"hits: {[s[:60] for s in prog_update]}")

# A9f: store-time authority for the heartbeat row — ts is stamped from the
# transaction's store clock (now); worker_reported_ts is carried through
# as informational payload, never as the row's time. The INSERT columns
# end with (worker_reported_ts, ts) and the bound params end with
# (worker_reported_ts, now).
check("A9f heartbeat ts comes from store time (now), worker timestamp is "
      "informational only",
      "worker_reported_ts, now" in ingest_src
      and "worker_reported_ts,ts)" in ingest_src.replace(" ", ""))

# A9g: validation precedes mutation in the shared progress path — reuse the
# A6d discipline on _apply_progress: every write must come after the last
# fencing/validation rejection.
tree_p = method_ast(TransitionGate._apply_progress)
writes_p = conn_writes(tree_p)
raises_p = [n.lineno for n in ast.walk(tree_p)
            if isinstance(n, ast.Raise)]
check("A9g _apply_progress: no write before final validation",
      all(w > max(raises_p, default=0) for w in writes_p),
      f"writes {writes_p}, raises {raises_p}")

# ------------------------------------------------------------------ R4 A10
# The R4 docstrings legitimately NAME reclaim_lease and the verdict labels
# to document the authority boundary, so the checks below analyze the
# method BODIES (docstring stripped via AST), not raw source text.
def _body_src(fn):
    tree = method_ast(fn)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if (node.body and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                node.body = node.body[1:]
            return ast.unparse(node)
    raise AssertionError(f"no function body for {fn}")


observe_body = _body_src(TransitionGate.observe_expired_leases)
legacy_body = _body_src(TransitionGate.expired_leases)
r4_body = observe_body + legacy_body

# A10a: the R4 expiry detector is SELECT-only. It must contain no
# INSERT/UPDATE/DELETE and must never open a write transaction — repeated
# observation is idempotent by construction.
check("A10a observe_expired_leases/expired_leases are SELECT-only "
      "(no INSERT/UPDATE/DELETE, no write_txn)",
      not re.search(r"\b(INSERT|UPDATE|DELETE)\b", r4_body, re.IGNORECASE)
      and "write_txn" not in r4_body)

# A10b: the R4 path can never reclaim and can never touch processes — it
# must not reference reclaim_lease or any process-termination primitive.
check("A10b R4 detector never references reclaim_lease or process "
      "termination (killpg/SIGTERM/SIGKILL/terminate/supervisor)",
      "reclaim_lease" not in r4_body
      and not re.search(r"killpg|SIGTERM|SIGKILL|\bterminate\b|"
                        r"Popen|supervisor|os\.kill", r4_body))

# A10c: R1 remains the sole reclaim authority. Gate-wide structural scan:
# the only methods whose SQL clears owner_worker_id are reclaim_lease
# (the reclaim: clears owner AND bumps the fencing token AND transitions
# to PENDING/UNCERTAIN) and release_lease (the pre-existing voluntary,
# owner-initiated release: no token bump, no reclaim transition).
# The token-bump pattern (new_token = int(cur_tok) + 1) appears only in
# reclaim_lease, and only reclaim_lease appends job.lease_reclaimed.
clearers, bumpers, reclaim_appenders = set(), set(), set()
for _name in dir(TransitionGate):
    _fn = getattr(TransitionGate, _name, None)
    if not callable(_fn):
        continue
    try:
        _body = _body_src(_fn)
    except (OSError, TypeError, AssertionError, SyntaxError):
        continue
    if "owner_worker_id=NULL" in _body:
        clearers.add(_name)
    if re.search(r"new_token\s*=\s*int\(cur_tok\)\s*\+\s*1", _body):
        bumpers.add(_name)
    if re.search(r"_append_event\([^)]*job\.lease_reclaimed", _body):
        reclaim_appenders.add(_name)
check("A10c only reclaim_lease revokes ownership with a token bump "
      "(owner-clearing methods are exactly reclaim_lease + release_lease)",
      clearers == {"reclaim_lease", "release_lease"}
      and bumpers == {"reclaim_lease"}
      and reclaim_appenders == {"reclaim_lease"},
      f"clearers={sorted(clearers)} bumpers={sorted(bumpers)} "
      f"reclaim_appenders={sorted(reclaim_appenders)}")

# A10d: R4 produces no recovery verdicts — no STALLED/DEAD labels anywhere
# in the detection path. Those belong to the later watchdog layer.
check("A10d R4 detector produces no STALLED/DEAD verdicts",
      "STALLED" not in r4_body and "DEAD" not in r4_body)

# A10e: authoritative store time decides expiry. The detector reads
# store.current_time(); no worker-supplied timestamp may appear in it.
check("A10e R4 expiry uses the authoritative store clock "
      "(current_time(); no worker_reported_ts)",
      "current_time()" in observe_body
      and "worker_reported_ts" not in observe_body)

# A10f: the contract's expiry predicate is present verbatim — owner
# required, lease required, boundary-inclusive comparison, and only the
# reclaimable execution states qualify. (The predicate string lives on
# the class as _EXPIRY_PREDICATE; the method must use it.)
_pred = TransitionGate._EXPIRY_PREDICATE
check("A10f R4 predicate requires owner + lease + store-time expiry "
      "and filters to CLAIMED/RUNNING/COMMITTING",
      "owner_worker_id IS NOT NULL" in _pred
      and "lease_expires_at IS NOT NULL" in _pred
      and "lease_expires_at <= :now" in _pred
      and all(s in _pred for s in ("'CLAIMED'", "'RUNNING'", "'COMMITTING'"))
      and "_EXPIRY_PREDICATE" in observe_body)

# A10g: R1 revalidates expiry atomically inside its own transaction —
# the routine path rejects when the durable lease is not expired, so
# stale R4 evidence can never become a false reclaim.
reclaim_src = inspect.getsource(TransitionGate.reclaim_lease)
check("A10g reclaim_lease revalidates lease expiry in-transaction "
      "(routine path rejects a live lease)",
      "exp_at > now" in reclaim_src
      and "not expired" in reclaim_src
      and "raise LeaseError" in reclaim_src)

# ------------------------------------------------------------------ R5 A11
# The R5 verified-artifact completion contract. The old artifact-free
# commit_job_result path is gone; every completion decision is made by
# commit_artifact inside one write transaction, and every protocol step
# re-checks fencing inside its own transaction.

# A11a: the old completion path is dead — I-4 removal proof. The stub
# raises unconditionally and contains no write capability.
cjr_body = _body_src(TransitionGate.commit_job_result)
check("A11a commit_job_result is dead (no write_txn, always raises)",
      "write_txn" not in cjr_body
      and "raise TransitionRejected" in cjr_body)

# A11b: the fencing check at every protocol step runs inside the
# authoritative transaction — the rejection and the mutation serialize.
fence_ok, fence_callers = True, []
for _m in _FENCED_METHODS:
    _tree = method_ast(getattr(TransitionGate, _m))
    try:
        _t0, _t1 = txn_span(_tree)
    except AssertionError:
        continue  # read-only observers have no txn to be inside
    for _node in ast.walk(_tree):
        if (isinstance(_node, ast.Call)
                and isinstance(_node.func, ast.Attribute)
                and _node.func.attr == "_check_fenced_execution"):
            fence_callers.append(_m)
            if not (_t0 <= _node.lineno <= _t1):
                fence_ok = False
check("A11b fencing checks run inside the authoritative txn at every "
      "protocol step",
      fence_ok and sorted(set(fence_callers)) == sorted(
          ["stage_artifact", "begin_commit", "verify_artifact",
           "commit_artifact", "fail_job_execution"]),
      f"callers={sorted(set(fence_callers))}")

# A11c: only verify_artifact can mark an artifact VERIFIED (no worker can
# set it directly), and its worker path is fenced.
validated_writers = set()
for _name in dir(TransitionGate):
    _fn = getattr(TransitionGate, _name, None)
    if not callable(_fn):
        continue
    try:
        _body = _body_src(_fn)
    except (OSError, TypeError, AssertionError, SyntaxError):
        continue
    if "SET status='VALIDATED'" in _body:
        validated_writers.add(_name)
va_body = _body_src(TransitionGate.verify_artifact)
check("A11c only verify_artifact marks artifacts VERIFIED",
      validated_writers == {"verify_artifact"},
      f"writers={sorted(validated_writers)}")
check("A11c verify_artifact's worker path is fenced",
      "_is_worker_actor" in va_body
      and "_check_fenced_execution" in va_body)

# A11d: verify_artifact's two-transaction forensic discipline. The
# evaluation transaction performs ZERO writes on the failure path (the
# _ArtifactVerifyFailed raise precedes every write in block 1, so its
# rollback is clean and the old "quarantine then raise rolls back" bug
# cannot recur); the quarantine is recorded by a second transaction that
# re-runs _structural_checks on the CURRENT bytes before writing anything
# — a quarantine is never written on stale evidence.
va_tree = method_ast(TransitionGate.verify_artifact)
_txn_blocks = sorted(
    (n.lineno, n.end_lineno) for n in ast.walk(va_tree)
    if isinstance(n, ast.With)
    and any("write_txn" in ast.dump(i.context_expr)
            for i in n.items))
check("A11d verify_artifact uses exactly two write transactions "
      "(evaluation + forensic)", len(_txn_blocks) == 2,
      f"blocks={_txn_blocks}")
if len(_txn_blocks) == 2:
    (_b1s, _b1e), (_b2s, _b2e) = _txn_blocks
    _fail_raise = [n.lineno for n in ast.walk(va_tree)
                   if isinstance(n, ast.Raise) and _b1s <= n.lineno <= _b1e
                   and "ArtifactVerifyFailed" in ast.dump(n)]
    _b1_writes = [w for w in conn_writes(va_tree) if _b1s <= w <= _b1e]
    check("A11d evaluation txn is write-free on the failure path",
          len(_fail_raise) == 1
          and all(w > _fail_raise[0] for w in _b1_writes),
          f"fail-raise={_fail_raise} writes={_b1_writes}")
    _b2_calls = [n for n in ast.walk(va_tree)
                 if isinstance(n, ast.Call) and _b2s <= n.lineno <= _b2e
                 and isinstance(n.func, ast.Attribute)]
    _recheck = [n.lineno for n in _b2_calls
                if n.func.attr == "_structural_checks"]
    _upsert = [n.lineno for n in _b2_calls
               if n.func.attr == "_upsert_validation"]
    _b2_writes = [w for w in conn_writes(va_tree) if _b2s <= w <= _b2e]
    check("A11d forensic txn re-checks bytes before quarantining",
          len(_recheck) == 1 and len(_upsert) == 1 and _b2_writes
          and all(r < min(_b2_writes) for r in _recheck),
          f"recheck={_recheck} upsert={_upsert} writes={_b2_writes}")

# A11e: the atomic commit re-verifies the FULL completion predicate
# inside the transaction — re-reading bytes, the hash, the receipt set,
# and the fencing triple — so nothing observed earlier can be trusted
# across the commit boundary.
ca_tree = method_ast(TransitionGate.commit_artifact)
_ct0, _ct1 = txn_span(ca_tree)
_verify_calls = [n for n in ast.walk(ca_tree)
                 if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute)
                 and n.func.attr == "_assert_artifact_verified"]
av_body = _body_src(TransitionGate._assert_artifact_verified)
check("A11e commit_artifact re-verifies inside the atomic txn",
      len(_verify_calls) >= 1
      and all(_ct0 <= n.lineno <= _ct1 for n in _verify_calls))
check("A11e the predicate re-reads bytes, the hash, and validator "
      "receipts (no trust across the commit boundary)",
      "_read_staged_bytes" in av_body
      and "hashlib.sha256" in av_body
      and "REQUIRED_VALIDATORS" in av_body)

# A11f: validation receipts bind the exact content hash — receipts can
# never be reused across different bytes.
uv_body = _body_src(TransitionGate._upsert_validation)
check("A11f validation receipts bind the exact content hash",
      "content_hash" in uv_body
      and "content_hash=excluded.content_hash" in uv_body)

# A11g: latest-known-good advances only through verify_checkpoint —
# the pointer table has exactly one writer.
ptr_writers = set()
for _name in dir(TransitionGate):
    _fn = getattr(TransitionGate, _name, None)
    if not callable(_fn):
        continue
    try:
        _body = _body_src(_fn)
    except (OSError, TypeError, AssertionError, SyntaxError):
        continue
    if "INSERT INTO checkpoint_pointers" in _body:
        ptr_writers.add(_name)
check("A11g latest-known-good advances only via verify_checkpoint",
      ptr_writers == {"verify_checkpoint"},
      f"writers={sorted(ptr_writers)}")

# A11h: COMPLETE is written only by commit_artifact; the generic
# transition API refuses COMPLETE. There is exactly one authoritative
# completion contract.
completers = set()
for _name in dir(TransitionGate):
    _fn = getattr(TransitionGate, _name, None)
    if not callable(_fn):
        continue
    try:
        _body = _body_src(_fn)
    except (OSError, TypeError, AssertionError, SyntaxError):
        continue
    if "SET status='COMPLETE'" in _body:
        completers.add(_name)
tj_src = inspect.getsource(TransitionGate.transition_job)
check("A11h COMPLETE is written only by commit_artifact",
      completers == {"commit_artifact"},
      f"writers={sorted(completers)}")
check("A11h transition_job refuses COMPLETE (single completion path)",
      "COMPLETE" in tj_src and "commit_artifact" in tj_src)

# ------------------------------------------------------------------ A12
# R6 authority composition: the boot pass DETECTS, R4 evaluates expiry,
# R1 performs reclaim, R2 performs physical fencing. exec/boot.py may not
# perform any mutation of its own — every durable write goes through
# TransitionGate journal/reclaim primitives, and no process is ever
# signalled from the boot module.
_boot_src = read(os.path.join(EXEC, "boot.py"))
_boot_tree = ast.parse(_boot_src)
_boot_gate_calls: set[str] = set()
for _n in ast.walk(_boot_tree):
    if (isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute)
            and isinstance(_n.func.value, ast.Name)
            and _n.func.value.id == "gate"):
        _boot_gate_calls.add(_n.func.attr)
_allowed_gate_calls = {
    "append_event", "verify_ledger_chain", "unreaped_proc_spawns",
    "observe_expired_leases", "reclaim_lease", "create_incident",
    "owned_active_jobs", "inspect_uncertain_completion", "jobs_in_states",
    "all_latest_known_good", "get_worker", "get_job",
}
check("A12a boot.py touches the gate only through journal/reclaim/read "
      "primitives",
      _boot_gate_calls <= _allowed_gate_calls,
      f"gate calls={sorted(_boot_gate_calls)}")
_boot_forbidden = []
for _n in ast.walk(_boot_tree):
    if isinstance(_n, ast.Call):
        _f = _n.func
        if isinstance(_f, ast.Attribute) and isinstance(
                _f.value, ast.Name):
            if _f.value.id == "os" and _f.attr == "killpg":
                _boot_forbidden.append(("os.killpg", _n.lineno))
            # os.kill is allowed ONLY as the signal-0 existence probe
            # (sends no signal; the same primitive the R2 sweep uses to
            # check liveness). Any real signal from the boot module would
            # be an unauthorized kill path.
            if _f.value.id == "os" and _f.attr == "kill":
                _sig = _n.args[1] if len(_n.args) >= 2 else None
                if not (isinstance(_sig, ast.Constant) and _sig.value == 0):
                    _boot_forbidden.append(("os.kill(sig!=0)", _n.lineno))
            if _f.value.id == "subprocess" and _f.attr in (
                    "Popen", "run", "call"):
                _boot_forbidden.append(("subprocess." + _f.attr, _n.lineno))
check("A12b boot.py never signals a process or spawns one",
      not _boot_forbidden, f"hits={_boot_forbidden}")
check("A12c boot.py writes no SQL of its own",
      not any(m in _boot_src for m in
              ("INSERT INTO", "UPDATE jobs", "UPDATE workers", "DELETE FROM")),
      "found raw mutation SQL in exec/boot.py")

_sup_init_src = inspect.getsource(Supervisor.__init__)
check("A12d Supervisor.__init__ runs boot_recover before the sweep "
      "thread starts",
      "boot_recover(self)" in _sup_init_src
      and _sup_init_src.index("boot_recover(self)")
      < _sup_init_src.index("_sweep_thread.start()"))
for _fn_name in ("start_worker", "restart_worker"):
    _fn_src = inspect.getsource(getattr(Supervisor, _fn_name))
    check(f"A12e {_fn_name} refuses new execution before boot is READY",
          "_require_boot_ready()" in _fn_src)
check("A12f blocked boot never emits boot.completed (recovery_blocked "
      "instead)",
      "boot.recovery_blocked" in _boot_src
      and '"boot.completed"' not in
      inspect.getsource(__import__("axos.exec.boot",
                                   fromlist=["_blocked"])._blocked))

# ------------------------------------------------------------------ A13
# R7 watchdog authority: the watchdog DETECTS and CLASSIFIES only. It must
# not reclaim, fence, kill, schedule, retry, reconcile, finalize, or write
# artifact/checkpoint state. Verdict records go through TransitionGate's
# atomic compare-and-swap only; thresholds come from WatchdogConfig, never
# from literals in the evaluation logic.
_wd_src = read(os.path.join(EXEC, "watchdog.py"))
_wd_tree = ast.parse(_wd_src)

_wd_calls: set[str] = set()   # every x.y(...) attribute call, by attr name
_wd_defs: set[str] = set()
for _n in ast.walk(_wd_tree):
    if isinstance(_n, (ast.FunctionDef, ast.AsyncFunctionDef)):
        _wd_defs.add(_n.name)
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute):
        _wd_calls.add(_n.func.attr)

check("A13a watchdog never calls reclaim_lease",
      "reclaim_lease" not in _wd_calls,
      f"calls={sorted(_wd_calls)}")
check("A13b watchdog never bumps the fencing token or clears ownership",
      not any(m in _wd_src for m in
              ("UPDATE jobs", "UPDATE workers", "INSERT INTO",
               "DELETE FROM", "fencing_token =", "fencing_token=",
               "owner_worker_id =", "owner_worker_id=")),
      "found mutation-shaped code in exec/watchdog.py")

_wd_kill = [c for c in _wd_calls
            if c in ("killpg", "Popen", "run", "call", "kill", "terminate",
                     "kill_worker", "restart_worker")]
check("A13c watchdog never signals or spawns a process",
      not _wd_kill, f"hits={_wd_kill}")
check("A13d watchdog imports no process-signalling capability",
      "import signal" not in _wd_src and "from signal" not in _wd_src
      and "import subprocess" not in _wd_src
      and "from subprocess" not in _wd_src,
      "signal/subprocess import in exec/watchdog.py")

_recovery_names = {"schedule_pending_jobs", "retry_job", "recover_incident",
                   "recovery_rung", "recovery_budget", "reconcile_desired_state",
                   "trip_circuit_breaker", "load_shed", "finalize_execution",
                   "restart_worker", "schedule", "reconcile", "finalize"}
_wd_recovery = (_wd_calls | _wd_defs) & _recovery_names
check("A13e watchdog implements no scheduler/recovery/reconciler/"
      "finalization behavior",
      not _wd_recovery, f"hits={sorted(_wd_recovery)}")
check("A13f watchdog never touches artifact/checkpoint authority",
      "artifact" not in _wd_src and "checkpoint" not in _wd_src,
      "artifact/checkpoint reference in exec/watchdog.py")

_wd_gate_calls: set[str] = set()
for _n in ast.walk(_wd_tree):
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute):
        _recv = _n.func.value
        # self.gate.foo(...)  or  gate.foo(...) (thread-bound local)
        if (isinstance(_recv, ast.Attribute) and _recv.attr == "gate") \
                or (isinstance(_recv, ast.Name) and _recv.id == "gate"):
            _wd_gate_calls.add(_n.func.attr)
_allowed_wd_gate = {"owned_active_jobs", "observe_expired_leases",
                    "unreaped_proc_spawns", "get_worker", "heartbeats_for",
                    "latest_watchdog_verdict", "record_watchdog_verdict",
                    "latest_spawn_generation"}
check("A13g watchdog touches the gate only through read primitives + the"
      " atomic verdict CAS",
      _wd_gate_calls <= _allowed_wd_gate,
      f"gate calls={sorted(_wd_gate_calls)}")
check("A13h watchdog's only durable write is record_watchdog_verdict",
      "append_event" not in _wd_calls and "write_txn" not in _wd_calls,
      f"calls={sorted(_wd_calls)}")

# Thresholds must come from the injected config, never from literals in
# the evaluation logic. Scan _evaluate_job for numeric constants that are
# not mere sequence subscripts (heartbeats[0]).
_eval_fn = next(n for n in ast.walk(_wd_tree)
                if isinstance(n, ast.FunctionDef)
                and n.name == "_evaluate_job")
_parents: dict[int, ast.AST] = {}
for _p in ast.walk(_eval_fn):
    for _c in ast.iter_child_nodes(_p):
        _parents[id(_c)] = _p
_wd_literals = []
for _n in ast.walk(_eval_fn):
    if isinstance(_n, ast.Constant) and isinstance(_n.value, (int, float)) \
            and not isinstance(_n.value, bool):
        _par = _parents.get(id(_n))
        if isinstance(_par, ast.Subscript) and _par.slice is _n:
            continue  # sequence index, not a threshold
        _wd_literals.append((_n.value, _n.lineno))
check("A13i no hardcoded timing thresholds in the evaluation logic",
      not _wd_literals, f"literals={_wd_literals}")
check("A13j watchdog never consults worker-reported timestamps",
      "worker_reported_ts" not in _wd_src,
      "worker_reported_ts in exec/watchdog.py")
check("A13k watchdog does not redefine the R4 expiry predicate",
      "observe_expired_leases" in _wd_src
      and "lease_expires_at <=" not in _wd_src
      and "lease_expires_at<" not in _wd_src,
      "expiry predicate duplicated in exec/watchdog.py")

# ------------------------------------------------------------------ A14
# R8 recovery-controller authority: the controller ACTS on failures, but
# only through existing authorities. It must not UPDATE job ownership,
# bump fencing tokens, clear leases, signal processes, bypass R1/R2/R5,
# redefine R7 verdicts, invent progress evidence, classify zero progress
# as success, loop retries unboundedly, or embed R9 ladder/budget policy.
_rc_src = read(os.path.join(EXEC, "recovery.py"))
_rc_tree = ast.parse(_rc_src)

_rc_calls: set[str] = set()   # every x.y(...) attribute call, by attr name
_rc_defs: set[str] = set()
for _n in ast.walk(_rc_tree):
    if isinstance(_n, (ast.FunctionDef, ast.AsyncFunctionDef)):
        _rc_defs.add(_n.name)
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute):
        _rc_calls.add(_n.func.attr)

check("A14a controller never UPDATEs job ownership directly",
      "UPDATE jobs" not in _rc_src and "UPDATE workers" not in _rc_src
      and "INSERT INTO jobs" not in _rc_src
      and "DELETE FROM jobs" not in _rc_src,
      "job-table mutation SQL in exec/recovery.py")
# No ownership/lease writes: forbid actual assignment to ownership
# fields (attribute assignment, subscript assignment), not mere keyword
# arguments in gate calls (e.g. expected_token=...) or comparisons.
_bad_assign = re.compile(
    r"\.(fencing_token|owner_worker_id|lease_expires_at)\s*=(?!=)"
    r"|\[(['\"])(fencing_token|owner_worker_id|lease_expires_at)\2\]"
    r"\s*=(?!=)")
_bad_hits = [f"{i}:{l.strip()[:70]}"
             for i, l in enumerate(_rc_src.splitlines(), 1)
             if _bad_assign.search(l)]
check("A14b controller never assigns fencing tokens or clears leases",
      not _bad_hits, f"hits={_bad_hits}")

_rc_kill = [c for c in _rc_calls
            if c in ("killpg", "Popen", "run", "call", "kill", "terminate",
                     "kill_worker", "restart_worker")]
check("A14c controller never signals or spawns a process directly",
      not _rc_kill, f"hits={_rc_kill}")
check("A14d controller imports no process-signalling capability",
      "import signal" not in _rc_src and "from signal" not in _rc_src
      and "import subprocess" not in _rc_src
      and "from subprocess" not in _rc_src
      and "os.kill" not in _rc_src,
      "signal/subprocess/os.kill import in exec/recovery.py")
check("A14e controller reclaims only through R1",
      "reclaim_lease" in _rc_calls,
      "no reclaim_lease call in exec/recovery.py")
check("A14f controller fences only through the R2 mechanism",
      "fence_sweep" in _rc_calls,
      "no fence_sweep call in exec/recovery.py")
# R5 authority: the controller may READ artifact/checkpoint progress
# evidence (D6 progress bundle), but must never exercise completion
# authority — no stage/verify/commit, no checkpoint creation, no
# pointer advancement, no COMPLETE write.
_r5_verbs = {"stage_artifact", "begin_commit", "verify_artifact",
             "commit_artifact", "create_checkpoint",
             "set_checkpoint_verification", "advance_latest_known_good",
             "transition_job"}
_r5_hits = [c for c in _rc_calls if c in _r5_verbs]
# COMPLETE may be named only for read-only terminal-state filtering
# (_TERMINAL_JOB_STATES membership tests); never written.
_r5_complete_lines = [l.strip() for l in _rc_src.splitlines()
                      if re.search(r"['\"]COMPLETE['\"]", l)]
_r5_complete_bad = [l for l in _r5_complete_lines
                    if "_TERMINAL_JOB_STATES" not in l]
check("A14g controller never exercises artifact/checkpoint/completion"
      " authority",
      not _r5_hits and not _r5_complete_bad,
      f"hits={_r5_hits} complete={_r5_complete_bad}")
check("A14h controller consumes R7 verdicts rather than redefining them",
      "latest_watchdog_verdict" in _rc_src
      and "_is_stale" not in _rc_src
      and "heartbeat_stale" not in _rc_src
      and "progress_stale" not in _rc_src,
      "verdict semantics redefined in exec/recovery.py")
check("A14i controller consumes the R4 expiry observation",
      "observe_expired_leases" in _rc_src
      and "lease_expires_at <=" not in _rc_src,
      "expiry predicate duplicated in exec/recovery.py")
check("A14j controller never consults worker-reported timestamps",
      "worker_reported_ts" not in _rc_src,
      "worker_reported_ts in exec/recovery.py")

# The controller's durable writes go through the gate's R8 helpers only.
_rc_gate_calls: set[str] = set()
for _n in ast.walk(_rc_tree):
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute):
        _recv = _n.func.value
        if (isinstance(_recv, ast.Attribute) and _recv.attr == "gate") \
                or (isinstance(_recv, ast.Name) and _recv.id == "gate"):
            _rc_gate_calls.add(_n.func.attr)
_allowed_rc_gate = {
    "store", "get_job", "latest_watchdog_verdict", "observe_expired_leases",
    "owned_active_jobs", "unreaped_proc_spawns", "latest_known_good",
    "find_or_create_recovery_incident", "get_recovery_incident",
    "create_recovery_attempt", "get_recovery_attempt",
    "open_recovery_attempts", "recovery_attempts_for",
    "open_recovery_incidents", "progress_evidence_for_job",
    "claim_recovery_attempt", "set_attempt_verify_after",
    "note_attempt_dispatch", "set_attempt_budget_context",
    "transition_recovery_attempt", "complete_recovery_attempt",
    "mark_recovery_attempt_uncertain", "set_incident_outcome",
    "set_incident_escalated", "reclaim_lease", "current_time",
}
check("A14k controller touches the gate only through read primitives +"
      " the R8 attempt CAS + R1.reclaim_lease",
      _rc_gate_calls <= _allowed_rc_gate,
      f"gate calls={sorted(_rc_gate_calls)}")

# No unbounded retry loop: no `while` in the control logic may create a
# recovery attempt; the only bound is the contract's two-attempt rule.
_rc_while_creates = []
for _n in ast.walk(_rc_tree):
    if isinstance(_n, ast.While):
        for _c in ast.walk(_n):
            if isinstance(_c, ast.Call) \
                    and isinstance(_c.func, ast.Attribute) \
                    and _c.func.attr == "create_recovery_attempt":
                _rc_while_creates.append(_n.lineno)
check("A14l controller has no retry loop creating attempts",
      not _rc_while_creates, f"while-loops={_rc_while_creates}")
check("A14m two-attempt escalation bound is the contract rule",
      "_TWO_ATTEMPT_ESCALATION_RUN = 2" in _rc_src
      and "escalation_target" in _rc_src,
      "two-attempt escalation rule missing in exec/recovery.py")

# R9 policy must not be embedded: no retry budgets, no ladder traversal,
# no loop breaker, no scheduler/reconciler/finalization.
_r9_names = {"retry_budget", "max_retries", "budget_remaining",
             "ladder", "traverse", "loop_breaker", "circuit_breaker",
             "schedule_pending", "reconcile", "finalize", "load_shed"}
_rc_r9 = set()
for _n in ast.walk(_rc_tree):
    if isinstance(_n, ast.Name) and _n.id in _r9_names:
        _rc_r9.add(_n.id)
    if isinstance(_n, (ast.FunctionDef, ast.AsyncFunctionDef)) \
            and _n.name in _r9_names:
        _rc_r9.add(_n.name)
check("A14n no R9 ladder/budget policy embedded in the controller",
      not _rc_r9, f"hits={sorted(_rc_r9)}")
# budget_context exists only as the deferred-to-R9 interface marker.
check("A14o budget_context is only the deferred-to-R9 marker",
      _rc_src.count("budget_context") >= 1
      and "deferred_to" in _rc_src
      and "r9-policy" in _rc_src,
      "budget interface marker missing in exec/recovery.py")

# Schema proof: migrate a scratch DB through the real migration path and
# inspect the resulting recovery_attempts table.
import sqlite3 as _sqlite3
import tempfile as _tempfile
from axos.store import open_store as _open_store, migrate as _migrate
_tmpdb = _tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
_schema_store = _open_store(_tmpdb)
_migrate(_schema_store)
_schema_ver = _schema_store.conn.execute(
    "SELECT MAX(version) FROM schema_migrations").fetchone()[0]
_schema_cols = {r[1] for r in _schema_store.conn.execute(
    "PRAGMA table_info(recovery_attempts)")}
_schema_idx = [r[0] for r in _schema_store.conn.execute(
    "SELECT name FROM sqlite_master WHERE type='index'")]
_schema_sql = _schema_store.conn.execute(
    "SELECT sql FROM sqlite_master WHERE type='table'"
    " AND name='recovery_attempts'").fetchone()[0]
_schema_store.close()
os.unlink(_tmpdb)
check("A14p v6 schema applied and carries the R8 attempt state columns",
      _schema_ver >= 6
      and {"job_id", "fencing_token", "attempt_number", "attempt_state",
           "rung_name", "idempotency_key", "budget_context", "claimed_at",
           "evidence_before", "evidence_after",
           "escalation_target"} <= _schema_cols,
      f"version={_schema_ver} cols={sorted(_schema_cols)}")
check("A14q UNIQUE(incident_id, attempt_number) exists",
      "recovery_attempts_incident_number" in _schema_idx,
      f"indexes={_schema_idx}")
check("A14r I-18 CHECK (success requires positive progress delta)"
      " survives v6",
      "progress_delta <= 0" in _schema_sql, "CHECK missing")

# The new R8 gate helpers must not mutate job ownership either.
_gate_src = read(os.path.join(AXOS, "store", "gate.py"))
_r8_section = _gate_src.split(
    "# --------------------------------------------- R8 recovery-controller"
    )[1].split("# ------------------------------------------------------------"
               " checkpoints")[0]
check("A14s R8 gate helpers never UPDATE job ownership",
      "UPDATE jobs" not in _r8_section
      and "UPDATE workers" not in _r8_section,
      "job-table mutation in the R8 gate section")
check("A14t R8 gate claim is a conditional CAS UPDATE",
      "AND attempt_state='CREATED'" in _r8_section,
      "claim CAS guard missing in the R8 gate section")

# ------------------------------------------------------------------ A15
# R9 recovery-policy authority: the policy layer CHOOSES rungs, BOUNDS
# budgets, and DECIDES escalation. It must never execute a recovery
# action itself (no R1 reclaim_lease / R2 fence_sweep / start_worker),
# never UPDATE job ownership or assign fencing tokens/leases, never
# signal or spawn processes, never exercise R5 artifact/checkpoint/
# completion authority, never record or redefine R7 verdicts, never
# implement R10+ scheduling/reconciliation/finalization, never rewind
# the ladder, never clear a terminal state, and never retry-loop
# attempts. It touches R8 only through RecoveryController.evaluate()
# and RecoveryController.dispatch_restart(), and mutates its durable
# policy state only through compare-and-swap.
_pol_src = read(os.path.join(EXEC, "policy.py"))
_pol_tree = ast.parse(_pol_src)

_pol_calls: set[str] = set()   # every x.y(...) attribute call, by attr name
_pol_defs: set[str] = set()
for _n in ast.walk(_pol_tree):
    if isinstance(_n, (ast.FunctionDef, ast.AsyncFunctionDef)):
        _pol_defs.add(_n.name)
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute):
        _pol_calls.add(_n.func.attr)

check("A15a policy never UPDATEs job ownership directly",
      "UPDATE jobs" not in _pol_src and "UPDATE workers" not in _pol_src
      and "INSERT INTO jobs" not in _pol_src
      and "DELETE FROM jobs" not in _pol_src,
      "job-table mutation SQL in exec/policy.py")
_pol_bad_hits = [f"{i}:{l.strip()[:70]}"
                 for i, l in enumerate(_pol_src.splitlines(), 1)
                 if _bad_assign.search(l)]
check("A15b policy never assigns fencing tokens or clears leases",
      not _pol_bad_hits, f"hits={_pol_bad_hits}")
_pol_kill = [c for c in _pol_calls
             if c in ("killpg", "Popen", "run", "call", "kill", "terminate",
                      "kill_worker", "restart_worker")]
check("A15c policy never signals or spawns a process directly",
      not _pol_kill and "import signal" not in _pol_src
      and "from signal" not in _pol_src
      and "import subprocess" not in _pol_src
      and "from subprocess" not in _pol_src
      and "os.kill" not in _pol_src,
      f"hits={_pol_kill}")
_r1r2_verbs = {"reclaim_lease", "fence_sweep", "start_worker"}
_r1r2_hits = [c for c in _pol_calls if c in _r1r2_verbs]
check("A15d policy never exercises R1/R2 action authority directly",
      not _r1r2_hits, f"hits={_r1r2_hits}")
# R5 authority: transition_task is allowed ONLY for the rung-5
# PAUSED_FOR_HUMAN pause (checked separately); every other completion
# verb is forbidden.
_r5_hits = [c for c in _pol_calls if c in _r5_verbs]
_pol_complete_lines = [l.strip() for l in _pol_src.splitlines()
                       if re.search(r"['\"]COMPLETE['\"]", l)]
_pol_complete_bad = [l for l in _pol_complete_lines
                     if "in (" not in l
                     and "_TERMINAL_JOB_STATES" not in l]
check("A15e policy never exercises artifact/checkpoint/completion"
      " authority",
      not _r5_hits and not _r5_complete_bad,
      f"hits={_r5_hits} complete={_pol_complete_bad}")
check("A15f policy consumes R7 verdicts, never records or redefines them",
      "record_watchdog_verdict" not in _pol_calls
      and "latest_watchdog_verdict" not in _pol_calls,
      "verdict authority in exec/policy.py")
# R10+ must not be implemented: no scheduler/reconciler/finalization
# identifiers (the policy's own _reconcile_* methods reconcile policy
# state from durable evidence — they are not the R10 reconciler).
_r10_names = {"Scheduler", "Reconciler", "schedule_pending", "finalize",
              "circuit_breaker", "load_shed", "fan_out", "dispatch_due",
              "reconcile_pending", "stage_widen"}
_r10_hits: set[str] = set()
for _n in ast.walk(_pol_tree):
    if isinstance(_n, ast.Name) and _n.id in _r10_names:
        _r10_hits.add(_n.id)
    if isinstance(_n, (ast.FunctionDef, ast.AsyncFunctionDef)) \
            and _n.name in _r10_names:
        _r10_hits.add(_n.name)
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute) \
            and _n.func.attr in _r10_names:
        _r10_hits.add(_n.func.attr)
check("A15g policy implements no R10+ scheduler/reconciler/finalization",
      not _r10_hits, f"hits={sorted(_r10_hits)}")
# The only R8 seams the policy may call.
_allowed_rc_calls = {"_gate_for_thread", "evaluate", "close",
                     "_consecutive_failures", "dispatch_restart"}
_rc_method_calls: set[str] = set()
for _n in ast.walk(_pol_tree):
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute) \
            and isinstance(_n.func.value, ast.Attribute) \
            and _n.func.value.attr == "rc":
        _rc_method_calls.add(_n.func.attr)
check("A15h policy touches R8 only through evaluate/dispatch_restart"
      " (+ owned read helpers)",
      _rc_method_calls <= _allowed_rc_calls
      and "dispatch_restart" in _rc_method_calls
      and "evaluate" in _rc_method_calls,
      f"rc calls={sorted(_rc_method_calls)}")
# The ladder never rewinds: no subtraction on any rung-valued name.
_rung_sub = []
for _n in ast.walk(_pol_tree):
    if isinstance(_n, ast.BinOp) and isinstance(_n.op, ast.Sub):
        for _side in (_n.left, _n.right):
            if isinstance(_side, ast.Name) and "rung" in _side.id:
                _rung_sub.append(_n.lineno)
check("A15i ladder never rewinds: no rung decrement, monotonic advance",
      not _rung_sub and "_advance" in _pol_defs,
      f"rung-sub lines={_rung_sub}")
_r9_section = _gate_src.split(
    "# --------------------------------------------- R9 recovery-policy state"
    )[1].split("# ------------------------------------------------------------"
               " checkpoints")[0]
check("A15j policy mutations are compare-and-swap on version",
      "version=version+1" in _r9_section
      and "AND version=?" in _r9_section
      and "PolicyConflict" in _r9_section,
      "CAS discipline missing in the R9 gate section")
check("A15k budget consumption guarded atomically in the UPDATE",
      "remaining_budget>=?" in _r9_section,
      "remaining_budget bound missing from the consume UPDATE")
_pol_while_creates = []
for _n in ast.walk(_pol_tree):
    if isinstance(_n, ast.While):
        for _c in ast.walk(_n):
            if isinstance(_c, ast.Call) \
                    and isinstance(_c.func, ast.Attribute) \
                    and _c.func.attr in ("create_recovery_attempt",
                                         "dispatch_restart"):
                _pol_while_creates.append(_n.lineno)
check("A15l policy has no retry loop creating attempts",
      not _pol_while_creates, f"while-loops={_pol_while_creates}")
check("A15m terminal state is sticky: never cleared back to None",
      '"terminal_state": None' not in _pol_src
      and "'terminal_state': None" not in _pol_src,
      "terminal_state cleared in exec/policy.py")
_pol_tmpdb = _tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
_pol_schema_store = _open_store(_pol_tmpdb)
_migrate(_pol_schema_store)
_pol_cols = {r[1] for r in _pol_schema_store.conn.execute(
    "PRAGMA table_info(recovery_policy)")}
_pol_schema_store.close()
os.unlink(_pol_tmpdb)
check("A15n v7 recovery_policy table carries the R9 policy columns",
      _schema_ver >= 7
      and {"incident_id", "policy_version", "version", "current_rung",
           "rung_name", "incident_budget", "per_rung_budgets",
           "attempt_count", "per_rung_attempts", "remaining_budget",
           "consumed_attempt_number", "result_consumed_attempt_number",
           "zero_progress_count", "last_attempt_id", "last_attempt_result",
           "escalation_state", "escalation_target", "terminal_state",
           "superseded_by", "updated_at"} <= _pol_cols,
      f"version={_schema_ver} cols={sorted(_pol_cols)}")
check("A15o R9 gate helpers never UPDATE job ownership",
      "UPDATE jobs" not in _r9_section
      and "UPDATE workers" not in _r9_section,
      "job-table mutation in the R9 gate section")
from axos.exec.policy import CANONICAL_LADDER as _r9_ladder
from axos.exec.recovery import _CANONICAL_RUNGS as _r8_rungs
check("A15p one canonical ladder: R9 names §E.1 verbatim, R8 mapping"
      " untouched",
      _r9_ladder == {1: "Retry with backoff", 2: "Restart worker",
                     3: "Replace / reassign", 4: "Widen scope",
                     5: "Replan / pause"}
      and _r8_rungs == {"reclaim": (2, "re-claim/requeue"),
                      "fence": (3, "replace/reassign"),
                      "restart": (3, "replace/reassign")},
      f"r9={_r9_ladder} r8={_r8_rungs}")

# ------------------------------------------------------------------ A16
# R10 scheduler authority: admission + atomic claim + dispatch ONLY. The
# scheduler must claim solely through the gate's bounded/atomic claim
# paths, hold no write/SQL/process capability, never touch lease
# reclaim/release, completion, or job creation, import no recovery/policy
# machinery, implement no reconciliation/circuit-breaker/load-shedding
# behavior, consult boot-ready on the admission path, keep the capacity
# predicate inside the claim transaction, and dispatch only with the
# re-read durable token.
_sch_src = read(os.path.join(EXEC, "scheduler.py"))
_sch_tree = ast.parse(_sch_src)
_sch_code = [line for _, line in code_lines(os.path.join(EXEC,
                                                         "scheduler.py"))]

_sch_calls: set[str] = set()   # every x.y(...) attribute call, by attr name
_sch_defs: set[str] = set()
for _n in ast.walk(_sch_tree):
    if isinstance(_n, (ast.FunctionDef, ast.AsyncFunctionDef)):
        _sch_defs.add(_n.name)
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute):
        _sch_calls.add(_n.func.attr)

# gate.foo(...) attribute calls (thread-bound local named `gate`)
_sch_gate_calls: set[str] = set()
for _n in ast.walk(_sch_tree):
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute):
        _recv = _n.func.value
        if (isinstance(_recv, ast.Attribute) and _recv.attr == "gate") \
                or (isinstance(_recv, ast.Name) and _recv.id == "gate"):
            _sch_gate_calls.add(_n.func.attr)
_allowed_sch_gate = {"claim_job_bounded", "claim_job", "get_job",
                     "get_task",
                     "open_recovery_incidents", "unreaped_proc_spawns",
                     "pending_jobs", "sched_claimed_orphans",
                     # R12 breaker-admission preflight (read-only): the
                     # admission path resolves the claim's scopes and
                     # consults breaker_allows before claiming. The
                     # scheduler never moves a breaker — no
                     # transition/ensure/record/probe-allocation call may
                     # appear here (asserted structurally in A18f/A18k).
                     "claim_job_resilient", "breaker_allows",
                     "get_desired_work_id_for_job",
                     # R10 claim-liveness beats: planted atomically with
                     # the claim, refreshed per pass, withdrawn on
                     # dispatch failure, cleared on close; the orphan
                     # path reads liveness only. No lease/token/job
                     # mutation authority beyond the beat rows.
                     "refresh_scheduler_claim_beats",
                     "withdraw_scheduler_claim_beat",
                     "clear_scheduler_claim_beats",
                     "scheduler_claim_live"}
check("A16a scheduler claims only via gate.claim_job_bounded/"
      "claim_job_resilient/claim_job",
      _sch_gate_calls <= _allowed_sch_gate
      and bool({"claim_job_bounded", "claim_job_resilient",
                "claim_job"} & _sch_gate_calls),
      f"gate calls={sorted(_sch_gate_calls)}")
check("A16b scheduler.py never calls write_txn()",
      not any("write_txn(" in line for line in _sch_code),
      "write_txn call in exec/scheduler.py")
check("A16c scheduler.py never imports sqlite3",
      not any(re.search(r"^\s*(import|from)\s+sqlite3\b", line)
              for line in _sch_code),
      "sqlite3 import in exec/scheduler.py")
_sch_kw_hits = [f"scheduler.py:{i}:{line.strip()[:80]}"
                for i, line in enumerate(_sch_code, 1)
                if write_kw.search('"' + line.strip() + '"')]
check("A16d scheduler.py contains no SQL write statements",
      not _sch_kw_hits, f"hits={_sch_kw_hits}")
_sch_kill = [c for c in _sch_calls
             if c in ("killpg", "Popen", "run", "call", "kill", "terminate",
                      "kill_worker", "restart_worker", "send_signal",
                      "os.kill")]
check("A16e scheduler never signals, kills, or spawns a process",
      not _sch_kill
      and "import signal" not in _sch_src
      and "from signal" not in _sch_src
      and "import subprocess" not in _sch_src
      and "from subprocess" not in _sch_src
      and "os.kill" not in _sch_src,
      f"hits={_sch_kill}")
check("A16f scheduler never calls reclaim_lease/release_lease",
      "reclaim_lease" not in _sch_calls
      and "release_lease" not in _sch_calls,
      f"calls={sorted(_sch_calls)}")
_sch_r5_verbs = {"stage_artifact", "begin_commit", "verify_artifact",
                 "commit_artifact"}
check("A16g scheduler has no COMPLETE path: no artifact/commit authority",
      not (_sch_r5_verbs & _sch_calls)
      and not any("COMPLETE" in line for line in _sch_code),
      f"hits={sorted(_sch_r5_verbs & _sch_calls)}")
check("A16h scheduler never creates jobs or deletes job rows",
      "create_job" not in _sch_calls
      and not any("DELETE FROM jobs" in line.upper()
                  for line in _sch_code)
      and not any("INSERT INTO jobs" in line.upper()
                  for line in _sch_code),
      f"calls={sorted(_sch_calls)}")
_sch_imports: set[str] = set()
for _n in ast.walk(_sch_tree):
    if isinstance(_n, ast.Import):
        _sch_imports.update(a.name for a in _n.names)
    elif isinstance(_n, ast.ImportFrom) and _n.module:
        _sch_imports.add(_n.module)
        _sch_imports.update(f"{_n.module}.{a.name}" for a in _n.names)
check("A16i scheduler imports no exec.recovery/exec.policy machinery",
      not any("exec.recovery" in m or "exec.policy" in m
              for m in _sch_imports),
      f"imports={sorted(_sch_imports)}")
_sch_recon_names = {"reconcile", "reconcile_desired_state",
                    "trip_circuit_breaker", "circuit_breaker", "load_shed",
                    "load_shedding", "finalize", "finalize_execution",
                    "schedule_pending_jobs", "retry_job", "recover_incident",
                    "recovery_rung", "recovery_budget"}
check("A16j scheduler implements no reconciliation/circuit-breaker/"
      "load-shedding behavior",
      not ((_sch_calls | _sch_defs) & _sch_recon_names)
      and not any(k in line for line in _sch_code
                  for k in ("circuit_breaker", "load_shed", "reconcile")),
      f"hits={sorted((_sch_calls | _sch_defs) & _sch_recon_names)}")
_eval_once = next(n for n in ast.walk(_sch_tree)
                 if isinstance(n, ast.FunctionDef)
                 and n.name == "evaluate_once")
_found_boot_gate = False
for _n in ast.walk(_eval_once):
    if isinstance(_n, ast.If) and "_boot_ready" in ast.dump(_n.test):
        _body = "".join(ast.dump(s) for s in _n.body)
        _found_boot_gate = ("boot-not-ready" in _body
                            and "claim_job" not in _body
                            and "start_worker" not in _body)
check("A16k boot-ready gate consulted on the admission path",
      _found_boot_gate,
      "evaluate_once has no claim-free boot-not-ready branch")
_bounded_tree = method_ast(TransitionGate.claim_job_bounded)
_bounded_txns = [n for n in ast.walk(_bounded_tree)
                 if isinstance(n, ast.With)
                 and "write_txn" in ast.dump(n)]
_bounded_body = ast.dump(_bounded_txns[0]) if _bounded_txns else ""
check("A16l capacity predicate + claim UPDATE share one gate write_txn",
      len(_bounded_txns) == 1
      and "COUNT(*)" in _bounded_body
      and "_claim_job_txn" in _bounded_body
      and "claim_job_bounded" in ast.dump(_bounded_tree),
      f"write_txn blocks={len(_bounded_txns)}")
_tok_assign = [l for l in _sch_code
               if re.search(r"\.fencing_token\s*=(?!=)"
                            r"|\[.fencing_token.\]\s*=(?!=)", l)]
check("A16m dispatch passes the re-read durable token; never manufactured",
      _sch_src.count("expect_token=") == 2
      and "get_job(" in _sch_src
      and not _tok_assign,
      f"expect_token sites={_sch_src.count('expect_token=')}"
      f" assigns={_tok_assign}")
# Worker side: the --expect-token mismatch path must exit before any
# authority-bearing call.
_w_src = read(os.path.join(EXEC, "worker.py"))
_w_tree = ast.parse(_w_src)
_w_main = next(n for n in ast.walk(_w_tree)
               if isinstance(n, ast.FunctionDef) and n.name == "main")
_expect_branch = None
for _n in ast.walk(_w_main):
    if isinstance(_n, ast.If) \
            and "expect_token" in ast.dump(_n.test) \
            and isinstance(_n.test, ast.Compare):
        _expect_branch = _n
        break
_branch_body = ("".join(ast.dump(s) for s in _expect_branch.body)
                if _expect_branch is not None else "")
check("A16n worker --expect-token mismatch exits without side effects",
      "EXIT_STALE_TOKEN = 7" in _w_src
      and _expect_branch is not None
      and "claim_job(" not in _branch_body
      and "transition_job" not in _branch_body
      and "EXIT_STALE_TOKEN" in _branch_body
      and re.search(r'add_argument\("--expect-token",\s*type=int,'
                    r'\s*default=None', _w_src, re.DOTALL) is not None,
      "expect-token branch malformed in exec/worker.py")
# Behavioral: capacity config and gate bound reject < 1.
from axos.exec.scheduler import SchedulerConfig as _SchCfg  # noqa: E402
from axos.store.db import TransitionRejected as _TR  # noqa: E402
_o_cfg_ok = False
try:
    _SchCfg(poll_interval_s=1.0, lease_ttl_s=60.0, max_concurrent_jobs=0,
            batch_size=1)
except ValueError:
    _o_cfg_ok = True
_o_gate_ok = False
_o_tmpdb = _tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
try:
    _o_store = _open_store(_o_tmpdb)
    _migrate(_o_store)
    _o_gate = TransitionGate(_o_store)
    _o_gate.create_task("t-a16o", {"objective": "x"}, {"usd": 1},
                        "scheduler")
    _o_gate.create_job("j-a16o", "t-a16o", "s", "scheduler")
    try:
        _o_gate.claim_job_bounded("j-a16o", "w", 60.0, "scheduler:x", 0)
    except _TR:
        _o_gate_ok = True
    _o_store.close()
finally:
    os.unlink(_o_tmpdb)
check("A16o capacity validated >= 1 (config and gate bound)",
      _o_cfg_ok and _o_gate_ok,
      f"config_rejects_0={_o_cfg_ok} gate_rejects_0={_o_gate_ok}")
# A16p (R10): the scheduler never admits work under a PAUSED_FOR_HUMAN
# task (I-17). The admission path consults the task's durable status via
# the read-only gate.get_task and skips paused scopes.
_eval_src = ast.dump(_eval_once)
check("A16p scheduler skips PENDING jobs under PAUSED_FOR_HUMAN tasks",
      "PAUSED_FOR_HUMAN" in _sch_src
      and "get_task" in _sch_gate_calls
      and "paused_task" in _eval_src,
      f"gate calls={sorted(_sch_gate_calls)}")
# A16q (R10): claim-liveness beats. The orphan-redispatch path must skip
# orphans whose owning scheduler is still live (fresh beat) and
# re-dispatch only absent/stale ones; the beat must be planted
# atomically with the claim (inside claim_job_bounded's write_txn, so
# claim ⟹ beat with no window), refreshed every pass, withdrawn when
# dispatch fails, and cleared on close. Beats confer no lease, token,
# or execution authority.
_bounded_src = ast.dump(_bounded_tree)
check("A16q scheduler claim-liveness beats gate orphan redispatch",
      "scheduler_claim_live" in _sch_gate_calls
      and "refresh_scheduler_claim_beats" in _sch_gate_calls
      and "withdraw_scheduler_claim_beat" in _sch_gate_calls
      and "clear_scheduler_claim_beats" in _sch_gate_calls
      and "scheduler_claim_beats" in _bounded_src
      and "INSERT OR REPLACE INTO scheduler_claim_beats" in _bounded_src,
      f"gate calls={sorted(_sch_gate_calls)}")

# ------------------------------------------------------------------ A17
# R11 reconciler authority: observe-and-converge ONLY. Desired-state reads
# go through the gate's read-only methods; the reconciler never touches a
# store connection or transaction; reconcile() pins a version and
# set/retire CAS-bump the head by exactly +1 inside one write_txn;
# snapshot/job identity is deterministic sha256 over canonical JSON;
# jobs are created only through
# TransitionGate.ensure_job_for_desired_state (canonical identity,
# IntegrityError->re-read, DesiredStateConflict on collision); the
# reconciler holds no claim, dispatch, process, lease, fence, artifact,
# or recovery authority; COMPLETE is only an observed diff status;
# human-gated scopes never get jobs; stale pins fail closed; the module
# contains zero SQL strings; "reconciler" is a registered work-creator
# while worker:-prefixed actors stay barred (I-16); retired items are
# tombstones, never deleted.
_rec_src = read(os.path.join(EXEC, "reconciler.py"))
_rec_tree = ast.parse(_rec_src)
_rec_code = [line for _, line in code_lines(os.path.join(EXEC,
                                                         "reconciler.py"))]
_rec_calls: set[str] = set()
_rec_gate_calls: set[str] = set()
for _n in ast.walk(_rec_tree):
    if isinstance(_n, ast.Call) and isinstance(_n.func, ast.Attribute):
        _rec_calls.add(_n.func.attr)
        _rv = _n.func.value
        if (isinstance(_rv, ast.Name) and _rv.id == "gate") \
                or (isinstance(_rv, ast.Attribute) and _rv.attr == "gate"):
            _rec_gate_calls.add(_n.func.attr)
_rec_imports: set[str] = set()
for _n in ast.walk(_rec_tree):
    if isinstance(_n, ast.Import):
        _rec_imports.update(a.name for a in _n.names)
    elif isinstance(_n, ast.ImportFrom) and _n.module:
        _rec_imports.add(_n.module)
        _rec_imports.update(f"{_n.module}.{a.name}" for a in _n.names)
_rec_conn_attrs = [n for n in ast.walk(_rec_tree)
                   if isinstance(n, ast.Attribute) and n.attr == "conn"]

# A17a: desired-state reads in the reconciler go through the gate's
# read-only methods; no direct store/conn access for desired state.
_allowed_rec_gate = {
    "get_desired_head", "get_desired_item", "list_desired_items",
    "get_desired_job_map", "get_job", "get_task",
    "ensure_job_for_desired_state",
    "begin_reconciliation_run", "checkpoint_reconciliation_run",
    "finish_reconciliation_run",
}
check("A17a desired-state reads go only through gate read-only methods; "
      "reconciler never touches a store connection",
      _rec_gate_calls <= _allowed_rec_gate
      and {"get_desired_head", "list_desired_items", "get_desired_job_map",
           "get_job"}.issubset(_rec_gate_calls)
      and not _rec_conn_attrs
      and not any("write_txn(" in line for line in _rec_code),
      f"gate calls={sorted(_rec_gate_calls)}"
      f" conn-attrs={len(_rec_conn_attrs)}")

# A17b: desired-state versions are explicit — reconcile() pins a version;
# set/retire bump the head by exactly +1 in one txn.
_bump_src = inspect.getsource(TransitionGate._bump_desired_head)
_rec_reconcile = next(n for n in ast.walk(_rec_tree)
                      if isinstance(n, ast.FunctionDef)
                      and n.name == "reconcile")
_rec_reconcile_src = "\n".join(
    _rec_src.splitlines()[_rec_reconcile.lineno - 1:_rec_reconcile.end_lineno])


def _write_txn_blocks(fn):
    _t = ast.parse(textwrap.dedent(inspect.getsource(fn)))
    return [n for n in ast.walk(_t) if isinstance(n, ast.With)
            and any("write_txn" in ast.dump(item.context_expr)
                    for item in n.items)]


check("A17b version pinned in reconcile(); set/retire bump head by exactly "
      "+1 inside one write_txn",
      "version=version+1" in _bump_src
      and len(_write_txn_blocks(TransitionGate.set_desired_item)) == 1
      and len(_write_txn_blocks(TransitionGate.retire_desired_item)) == 1
      and "_bump_desired_head" in inspect.getsource(
          TransitionGate.set_desired_item)
      and "_bump_desired_head" in inspect.getsource(
          TransitionGate.retire_desired_item)
      and 'pinned = head["version"]' in _rec_reconcile_src,
      "bump arithmetic or single-txn structure broken")

# A17c: snapshot identity is deterministic — sha256 over canonical JSON;
# canonical_job_id is a pure sha256 function of desired_work_id.
from axos.store.gate import (_canonical_desired_job_id,  # noqa: E402
                             _desired_snapshot_hash, _canon)
_canon_fn_src = inspect.getsource(_canonical_desired_job_id)
_snap_fn_src = inspect.getsource(_desired_snapshot_hash)
_canon_fn_tree = ast.parse(textwrap.dedent(_canon_fn_src))
_canon_fn_names = {n.attr if isinstance(n, ast.Attribute) else n.id
                   for n in ast.walk(_canon_fn_tree)
                   if isinstance(n, (ast.Name, ast.Attribute))}
check("A17c deterministic identity: sha256 over canonical JSON; no uuid/"
      "random in the identity path",
      "sha256" in _canon_fn_src and "sha256" in _snap_fn_src
      and "_canon" in _snap_fn_src
      and "sort_keys=True" in inspect.getsource(_canon)
      and "uuid" not in _canon_fn_names and "random" not in _canon_fn_names
      and "return _canonical_desired_job_id(desired_work_id)" in _rec_src
      and not any(re.search(r"\b(uuid|random)\b", line)
                  for line in _rec_code),
      "identity construction not deterministic")

# A17d: job creation occurs ONLY through TransitionGate — no SQL, no
# sqlite3, no write_txn in reconciler.py.
_rec_kw_hits = [f"reconciler.py:{i}:{line.strip()[:80]}"
                for i, line in enumerate(_rec_code, 1)
                if write_kw.search('"' + line.strip() + '"')]
check("A17d job creation only through TransitionGate: no SQL, no sqlite3, "
      "no write_txn in reconciler.py",
      not _rec_kw_hits
      and not any(re.search(r"^\s*(import|from)\s+sqlite3\b", line)
                  for line in _rec_code)
      and not any("write_txn(" in line for line in _rec_code),
      f"hits={_rec_kw_hits}")

# A17e: canonical identity enforced — ensure_job_for_desired_state derives
# the job_id deterministically; collisions raise DesiredStateConflict.
_ensure_src = inspect.getsource(
    TransitionGate.ensure_job_for_desired_state)
_ensure_tree = ast.parse(textwrap.dedent(_ensure_src))
_ensure_conflict_raises = [n for n in ast.walk(_ensure_tree)
                           if isinstance(n, ast.Raise)
                           and "DesiredStateConflict" in ast.dump(n)]
check("A17e canonical identity enforced: deterministic job_id; collision "
      "raises DesiredStateConflict",
      "_canonical_desired_job_id(desired_work_id)" in _ensure_src
      and len(_ensure_conflict_raises) >= 1,
      f"DesiredStateConflict raises={len(_ensure_conflict_raises)}")

# A17f: duplicate creation impossible under concurrency — the PRIMARY KEY
# race surfaces as IntegrityError and the loser re-reads the winner
# (job row + map row + spec_hash all validated before idempotent return).
_ie_handlers = [n for n in ast.walk(_ensure_tree)
                if isinstance(n, ast.ExceptHandler)
                and "IntegrityError" in ast.dump(n.type)]
_ie_body = ast.dump(_ie_handlers[0]) if _ie_handlers else ""
check("A17f duplicate creation impossible: PK race -> IntegrityError -> "
      "re-read the winner",
      len(_ie_handlers) == 1
      and "SELECT * FROM jobs" in _ie_body
      and "SELECT * FROM desired_job_map" in _ie_body
      and "spec_hash" in _ie_body
      and len(_ensure_conflict_raises) >= 1,
      "IntegrityError re-read pattern missing in ensure_job_for_desired_state")

# A17g-k: the reconciler owns no claim, dispatch, process, lease, or
# fencing authority.
_claim_verbs = {"claim_job", "claim_job_bounded", "claim_recovery_attempt"}
check("A17g reconciler cannot claim jobs",
      not (_claim_verbs & _rec_calls),
      f"hits={sorted(_claim_verbs & _rec_calls)}")
check("A17h reconciler cannot dispatch workers",
      not any("start_worker" in line for line in _rec_code),
      "start_worker in exec/reconciler.py")
check("A17i reconciler cannot kill/terminate processes",
      not any(re.search(r"\bsignal\b|\bSIGTERM\b|\bSIGKILL\b|os\.kill",
                       line) for line in _rec_code)
      and "import signal" not in _rec_src
      and "from signal" not in _rec_src
      and "import subprocess" not in _rec_src
      and "from subprocess" not in _rec_src,
      "signal/kill token in exec/reconciler.py")
check("A17j reconciler cannot fence: no fence-sweep, no fencing authority",
      not any("fence" in line.lower() for line in _rec_code)
      and not any("fence" in c for c in _rec_calls),
      "fence token in exec/reconciler.py")
check("A17k reconciler cannot reclaim leases",
      "reclaim_lease" not in _rec_calls
      and "release_lease" not in _rec_calls,
      f"calls={sorted(_rec_calls)}")

# A17l: the reconciler cannot complete jobs — no commit_artifact, no
# transition_job, and COMPLETE never appears as a transition target
# (only as an observed diff status).
_rec_complete_lines = [line.strip() for line in _rec_code
                       if "COMPLETE" in line]
check("A17l reconciler cannot complete jobs: no commit/transition calls; "
      "COMPLETE only an observed status",
      "commit_artifact" not in _rec_calls
      and "transition_job" not in _rec_calls
      and all(re.search(r'==\s*"COMPLETE"|"DESIRED_COMPLETE"', line)
              for line in _rec_complete_lines),
      f"hits={_rec_complete_lines}")

# A17m/n: the reconciler imports only the store/transition surface plus
# stdlib — no R7/R8/R9 (recovery/policy/scheduler) or watchdog machinery.
check("A17m reconciler imports only store/transition modules + stdlib",
      all(m.split(".")[0] in ("__future__", "json", "threading", "time",
                              "dataclasses")
          or m.startswith("axos.store") for m in _rec_imports),
      f"imports={sorted(_rec_imports)}")
check("A17n reconciler invokes no R7/R8/R9 machinery",
      not any(m.startswith("axos.exec") for m in _rec_imports),
      f"imports={sorted(_rec_imports)}")

# A17o: human-gated states protected — the creation path consults the
# task's durable status via gate.get_task and skips (blocked, recorded,
# never created) when it is PAUSED_FOR_HUMAN.
_create_fn = next(n for n in ast.walk(_rec_tree)
                  if isinstance(n, ast.FunctionDef)
                  and n.name == "_create_for_missing")
_ph_skip = False
for _n in ast.walk(_create_fn):
    if isinstance(_n, ast.If) and "PAUSED_FOR_HUMAN" in ast.dump(_n.test):
        _ph_body = "".join(ast.dump(s) for s in _n.body)
        _ph_skip = ("ensure_job_for_desired_state" not in _ph_body
                    and "blocked" in _ph_body)
        break
check("A17o human-gated states protected: PAUSED_FOR_HUMAN task skips "
      "creation (blocked, recorded, never created)",
      _ph_skip,
      "PAUSED_FOR_HUMAN skip missing on the creation path")

# A17p: stale desired versions cannot overwrite newer state — the version
# is pinned at pass start (a stale pin refuses before any mutation) and
# the head is re-read before every batch; a mismatch stops creating and
# closes the run as CONFLICT.
_batch_for = None
for _n in ast.walk(_rec_reconcile):
    if isinstance(_n, ast.For) and "range" in ast.dump(_n.iter):
        _batch_for = _n
        break
_found_repin = False
for _n in ast.walk(_batch_for) if _batch_for is not None else ():
    if isinstance(_n, ast.If) and "cur_version" in ast.dump(_n.test) \
            and "pinned" in ast.dump(_n.test):
        _found_repin = ("conflict" in ast.dump(_n)
                        and any(isinstance(s, ast.Break)
                                for s in ast.walk(_n)))
        break
check("A17p stale desired versions cannot overwrite newer state: pin + "
      "per-batch re-pin, mismatch aborts mutation",
      _batch_for is not None
      and "get_desired_head" in ast.dump(_batch_for)
      and _found_repin
      and 'if head["version"] != pinned:' in _rec_reconcile_src
      and "CONFLICT" in _rec_reconcile_src,
      "pin/re-pin/abort structure broken in reconcile()")

# A17q: no direct SQL authority bypass — zero SQL strings anywhere in the
# module (SELECT included). String constants are scanned via AST so that
# docstring prose never counts.
_doc_ids = set()
for _n in ast.walk(_rec_tree):
    if isinstance(_n, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                       ast.ClassDef)) and _n.body:
        _first = _n.body[0]
        if (isinstance(_first, ast.Expr)
                and isinstance(_first.value, ast.Constant)
                and isinstance(_first.value.value, str)):
            _doc_ids.add(id(_first.value))
_sql_consts = [n.value for n in ast.walk(_rec_tree)
               if isinstance(n, ast.Constant)
               and isinstance(n.value, str)
               and id(n) not in _doc_ids
               and re.search(r"\b(SELECT|INSERT|UPDATE|DELETE)\b", n.value,
                             re.IGNORECASE)]
check("A17q reconciler.py contains zero SQL strings (SELECT included)",
      not _sql_consts, f"hits={_sql_consts}")

# A17r: R10 remains the only admission/dispatch path — the reconciler
# never transitions PENDING->CLAIMED (and transition_job is absent
# entirely; CLAIMED occurs only as an observed diff status).
_rec_claimed_lines = [line.strip() for line in _rec_code
                      if "CLAIMED" in line]
check("A17r R10 remains the only admission/dispatch path: reconciler never "
      "transitions PENDING->CLAIMED",
      "transition_job" not in _rec_calls
      and all("status in (" in line for line in _rec_claimed_lines),
      f"CLAIMED lines={_rec_claimed_lines}")

# A17s: "reconciler" is registered in WORK_CREATOR_ACTORS and no
# worker:-prefixed actor can create (I-16 intact). Structural plus
# behavioral: a worker:-prefixed actor is rejected by the gate op while
# the "reconciler" actor materializes a PENDING job.
from axos.store import transitions as _T  # noqa: E402
_worker_bar_src = inspect.getsource(TransitionGate._check_work_creator)
_s_tmpdb = _tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
_s_worker_rejected = False
_s_reconciler_created = False
try:
    _s_store = _open_store(_s_tmpdb)
    _migrate(_s_store)
    _s_gate = TransitionGate(_s_store)
    _s_gate.create_task("t-a17s", {"objective": "x"}, {"usd": 1}, "human")
    _s_gate.set_desired_item("dw-a17s", {"task_id": "t-a17s"}, "human")
    try:
        _s_gate.ensure_job_for_desired_state(
            desired_work_id="dw-a17s", task_id="t-a17s", stage_id=None,
            max_attempts=1, policy={}, desired_version=1,
            actor="worker:x")
    except _TR:
        _s_worker_rejected = True
    try:
        _job, _created = _s_gate.ensure_job_for_desired_state(
            desired_work_id="dw-a17s", task_id="t-a17s", stage_id=None,
            max_attempts=1, policy={}, desired_version=1,
            actor="reconciler")
        _s_reconciler_created = _created and _job["status"] == "PENDING"
    except _TR:
        _s_reconciler_created = False
    _s_store.close()
finally:
    os.unlink(_s_tmpdb)
check("A17s 'reconciler' is a registered work-creator; worker:-prefixed "
      "actors cannot create (I-16 intact)",
      "reconciler" in _T.WORK_CREATOR_ACTORS
      and 'startswith("worker:")' in _worker_bar_src
      and _s_worker_rejected and _s_reconciler_created,
      f"actors={sorted(_T.WORK_CREATOR_ACTORS)}"
      f" worker_rejected={_s_worker_rejected}"
      f" reconciler_created={_s_reconciler_created}")

# A17t: obsolete handling never deletes — no DELETE FROM jobs in the
# gate's R11 section or in reconciler.py.
_gate_src = read(os.path.join(AXOS, "store", "gate.py"))
_gate_r11_src = _gate_src.split(
    "# ------------------------------------- R11 desired-state reconciliation",
    1)[1]
check("A17t obsolete handling never deletes: no DELETE FROM jobs in gate "
      "R11 section or reconciler.py",
      not re.search(r"DELETE\s+FROM\s+jobs", _gate_r11_src, re.IGNORECASE)
      and not re.search(r"DELETE\s+FROM\s+jobs", _rec_src, re.IGNORECASE),
      "DELETE FROM jobs found")

# ------------------------------------------------------------------ A18
# R12 circuit-breaker authority envelope: the controller reads and
# evaluates; the scheduler admits; only the gate moves breakers.

_res_src = read(os.path.join(AXOS, "exec", "resilience.py"))
_sched_src = read(os.path.join(AXOS, "exec", "scheduler.py"))
_res_tree = ast.parse(_res_src)
_sched_tree = ast.parse(_sched_src)


def _calls(tree):
    return {n.func.attr for n in ast.walk(tree)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)}


_res_calls = _calls(_res_tree)
_sched_calls = _calls(_sched_tree)

# A18a: single store path — the controller opens exactly one Store via
# open_store and never touches sqlite3 or raw connections directly.
check("A18a single store path: resilience.py opens one Store via "
      "open_store; no sqlite3/connect/cursor",
      _res_src.count("open_store(") >= 1
      and "sqlite3.connect" not in _res_src
      and ".cursor()" not in _res_src,
      "store path violated")

# A18b: CAS discipline — transition_breaker requires expected_version
# (keyword-only) and the UPDATE is version-guarded.
_gate_breaker = _gate_src.split(
    "# ------------------------------------------ R12 circuit breaker",
    1)[1] if "# ------------------------------------------ R12 circuit breaker" in _gate_src else _gate_src
check("A18b CAS on breaker transitions: expected_version keyword-only "
      "and version-guarded UPDATE",
      "expected_version: int" in _gate_src
      and "AND version=?" in _gate_src,
      "CAS missing")

# A18c: signal dedupe — INSERT OR IGNORE on the scope-qualified
# signal_id makes retried deliveries a no-op.
check("A18c signal dedupe: INSERT OR IGNORE INTO breaker_signals with "
      "scope-qualified signal_id",
      "INSERT OR IGNORE INTO breaker_signals" in _gate_src
      and 'f"{scope_type}:{scope_id}:{failure_kind}:' in _gate_src,
      "dedupe missing")

# A18d: authoritative time — the gate's breaker writes use write_txn's
# `now`; resilience.py calls time.time() only in the background loop's
# informational error telemetry.
_parent = {}
for _node in ast.walk(_res_tree):
    for _child in ast.iter_child_nodes(_node):
        _parent[_child] = _node
_wall_time_bad = []
for _node in ast.walk(_res_tree):
    if (isinstance(_node, ast.Call)
            and isinstance(_node.func, ast.Attribute)
            and _node.func.attr == "time"
            and isinstance(_node.func.value, ast.Name)
            and _node.func.value.id == "time"):
        _fn, _fname = _node, "<module>"
        while _fn in _parent:
            _fn = _parent[_fn]
            if isinstance(_fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                _fname = _fn.name
                break
        if _fname != "_loop":
            _wall_time_bad.append((_fname, _node.lineno))
check("A18d authoritative time: no time.time() in resilience.py "
      "authority paths (only _loop telemetry)",
      not _wall_time_bad,
      f"wall time in authority path: {_wall_time_bad}")

# A18e: the controller cannot claim, dispatch, signal/kill, complete,
# reclaim, or touch recovery.
check("A18e controller cannot claim/dispatch/kill/complete/reclaim",
      not ({"claim_job", "claim_job_bounded", "claim_job_resilient",
            "dispatch", "terminate", "kill", "signal",
            "commit_artifact", "reclaim_lease", "release_lease",
            "run_recovery", "create_recovery_attempt",
            "complete_recovery_attempt", "claim_recovery_attempt",
            "find_or_create_recovery_incident",
            "transition_job", "update_job_progress",
            "stage_artifact", "begin_commit", "verify_artifact"}
           & _res_calls),
      f"controller has execution authority: {_res_calls}")

# A18f: the scheduler never moves a breaker — no transition_breaker,
# no record_breaker_signal, no ensure_breaker_state.
check("A18f scheduler never moves a breaker: no transition/record/"
      "ensure calls",
      not ({"transition_breaker", "record_breaker_signal",
            "ensure_breaker_state"} & _sched_calls),
      "scheduler moves breakers")

# A18g: the scheduler's only breaker contact is the read-only
# preflight (breaker_allows) and the resilient claim path.
check("A18g scheduler breaker contact is read-only preflight + "
      "resilient claim",
      "breaker_allows" in _sched_src
      and "claim_job_resilient" in _sched_src,
      "scheduler breaker contact wrong")

# A18h: R11 preserved — the reconciler has no breaker authority.
_rec_calls = _calls(ast.parse(_rec_src))
check("A18h R11 preserved: reconciler has no breaker authority",
      not ({"transition_breaker", "record_breaker_signal",
            "ensure_breaker_state", "breaker_allows",
            "claim_job_resilient", "get_breaker_state"}
           & _rec_calls),
      "reconciler has breaker authority")

# A18i: human gates preserved — the controller never writes
# PAUSED_FOR_HUMAN and never mentions rung-5 escalation.
check("A18i human gates preserved: no PAUSED_FOR_HUMAN in "
      "resilience.py",
      "PAUSED_FOR_HUMAN" not in _res_src,
      "human gate violated")

# A18j: fail-closed — the transition graph has no OPEN->CLOSED edge
# and no self-loops; corrupt rows are denied (breaker_allows returns
# (False, "corrupt")).
check("A18j fail-closed graph: no OPEN->CLOSED, no self-loops",
      '"OPEN": ("HALF_OPEN",)' in _gate_src
      and '"CLOSED": ("OPEN",)' in _gate_src,
      "transition graph not fail-closed")
check("A18j corrupt rows fail closed: breaker_allows returns "
      "(False, 'corrupt')",
      'return (False, "corrupt")' in _gate_src,
      "corrupt not fail-closed")

# A18k: no SQL in exec/resilience.py (AST: no SQL string constants
# outside docstrings, no .execute() calls).
_docstrings = set()
for _node in ast.walk(_res_tree):
    if isinstance(_node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                          ast.AsyncFunctionDef)):
        _body = getattr(_node, "body", [])
        if (_body and isinstance(_body[0], ast.Expr)
                and isinstance(_body[0].value, ast.Constant)
                and isinstance(_body[0].value.value, str)):
            _docstrings.add(id(_body[0].value))
_sql_words = ("SELECT", "UPDATE", "INSERT", "DELETE", "CREATE", "DROP",
              "ALTER")
_sql_bad = []
for _node in ast.walk(_res_tree):
    if (isinstance(_node, ast.Constant)
            and isinstance(_node.value, str)
            and id(_node) not in _docstrings
            and any(w in _node.value.upper().split()
                    for w in _sql_words)):
        _sql_bad.append((_node.lineno, _node.value[:50]))
    if (isinstance(_node, ast.Call)
            and isinstance(_node.func, ast.Attribute)
            and _node.func.attr == "execute"):
        _sql_bad.append((_node.lineno, ".execute()"))
check("A18k no SQL in resilience.py (AST)",
      not _sql_bad,
      f"SQL found: {_sql_bad}")

# A18l: no duplicate breaker authority — only store/gate.py writes to
# breaker_state / breaker_signals.
_other_writers = []
for _p in py_files():
    if _p.endswith(os.path.join("store", "gate.py")):
        continue
    _src = read(_p)
    if "breaker_state" in _src or "breaker_signals" in _src:
        # Allow read-only mentions in tests/docs; flag writes.
        if re.search(r"(INSERT|UPDATE|DELETE)\s+(INTO\s+)?breaker_",
                     _src, re.IGNORECASE):
            _other_writers.append(os.path.relpath(_p, AXOS))
check("A18l no duplicate breaker authority: only the gate writes "
      "breaker_state/breaker_signals",
      not _other_writers,
      f"other writers: {_other_writers}")

# ------------------------------------------------------------------ A19
# R13 release-finalization authority envelope: the finalizer reads and
# evaluates; only the gate's R13 API writes finalization_runs, and it
# does so with CAS, pinned desired-state versions, and a VERIFIED R5
# release checkpoint with a release attestation.

from axos.store import FinalizationConflict as _FinConflict  # noqa: E402
from axos.store.gate import (canonical_release_generation,  # noqa: E402
                             canonical_finalization_id,
                             build_finalization_manifest,
                             finalization_manifest_hash,
                             _release_checkpoint_id,
                             _release_checkpoint_entries)
from axos.exec.finalizer import FinalizationConfig as _FinCfg  # noqa: E402

_fin_p = os.path.join(AXOS, "exec", "finalizer.py")
_fin_src = read(_fin_p)
_fin_tree = ast.parse(_fin_src)
_fin_calls = _calls(_fin_tree)
_pub_src = inspect.getsource(TransitionGate.publish_finalization)
_pub_tree = ast.parse(textwrap.dedent(_pub_src))
_begin_src = inspect.getsource(TransitionGate.begin_finalization_run)
_mig_src = read(os.path.join(AXOS, "store", "migrations.py"))

# A19a: one authoritative store path — exec/finalizer.py contains zero
# SQL (AST: no SQL string constants outside docstrings, no .execute()
# calls). Like A18k.
_fin_docstrings = set()
for _n in ast.walk(_fin_tree):
    if isinstance(_n, (ast.Module, ast.ClassDef, ast.FunctionDef,
                       ast.AsyncFunctionDef)):
        _b = getattr(_n, "body", [])
        if (_b and isinstance(_b[0], ast.Expr)
                and isinstance(_b[0].value, ast.Constant)
                and isinstance(_b[0].value.value, str)):
            _fin_docstrings.add(id(_b[0].value))
_fin_sql_bad = []
for _n in ast.walk(_fin_tree):
    if (isinstance(_n, ast.Constant)
            and isinstance(_n.value, str)
            and id(_n) not in _fin_docstrings
            and any(w in _n.value.upper().split()
                    for w in ("SELECT", "UPDATE", "INSERT", "DELETE",
                              "CREATE", "DROP", "ALTER"))):
        _fin_sql_bad.append((_n.lineno, _n.value[:50]))
    if (isinstance(_n, ast.Call)
            and isinstance(_n.func, ast.Attribute)
            and _n.func.attr == "execute"):
        _fin_sql_bad.append((_n.lineno, ".execute()"))
check("A19a zero SQL in exec/finalizer.py (AST)",
      not _fin_sql_bad,
      f"sql={_fin_sql_bad}")

# A19b: publication is CAS-protected — expected_version is on the write
# path, every UPDATE in publish_finalization is version-guarded, and
# the loser gets FinalizationConflict (structural: no write without
# version comparison).
_pub_updates = [n.value for n in ast.walk(_pub_tree)
                if isinstance(n, ast.Constant)
                and isinstance(n.value, str)
                and "UPDATE" in n.value.upper()]
_pub_conflict = [n for n in ast.walk(_pub_tree)
                 if isinstance(n, ast.Raise)
                 and "FinalizationConflict" in ast.dump(n)]
check("A19b CAS-protected publication: expected_version on the write "
      "path, version-guarded UPDATE, FinalizationConflict on the loser",
      "expected_version: int" in _pub_src
      and _pub_updates
      and all("version=?" in s for s in _pub_updates)
      and "AND version=?" in _pub_src
      and len(_pub_conflict) >= 1
      and issubclass(_FinConflict, Exception),
      f"updates={_pub_updates} conflict_raises={len(_pub_conflict)}")

# A19c: generation identity — the generation IS the desired-state
# version ("ds-v" + version); a mismatched pair is rejected before any
# write; an existing run pinning a different version is a contradiction
# and fails closed; worker actors are barred from finalization.
_r13_tmpdb = _tempfile.NamedTemporaryFile(suffix=".db", delete=False).name
_r13_mismatch = False
_r13_worker_barred = False
try:
    _r13_store = _open_store(_r13_tmpdb)
    _migrate(_r13_store)
    _r13_gate = TransitionGate(_r13_store)
    try:
        _r13_gate.begin_finalization_run("ds-v9", 3, "finalizer")
    except _TR:
        _r13_mismatch = True
    try:
        _r13_gate._check_finalizer_actor("worker:7", "publish_finalization")
    except _TR:
        _r13_worker_barred = True
    _r13_store.close()
finally:
    os.unlink(_r13_tmpdb)
_cfg_barred = False
try:
    _FinCfg(poll_interval_s=30.0, batch_size=10, actor="worker:9")
except ValueError:
    _cfg_barred = True
check("A19c generation identity enforced: canonical ds-vN; mismatch "
      "rejected; contradictory re-begin fails closed; worker actors "
      "barred",
      canonical_release_generation(3) == "ds-v3"
      and canonical_finalization_id("ds-v3") \
      == canonical_finalization_id("ds-v3")
      and "not the canonical identity" in _begin_src
      and "contradictory re-begin" in _begin_src
      and _r13_mismatch and _r13_worker_barred and _cfg_barred,
      f"mismatch={_r13_mismatch} worker_barred={_r13_worker_barred}"
      f" cfg_barred={_cfg_barred}")

# A19c2: no global boolean finalized flag — finalization is a
# per-generation state machine (CHECK constraint enumerates the states;
# release_generation is UNIQUE); no finalized= assignment anywhere in
# the gate.
check("A19c no global boolean: per-generation state machine with "
      "UNIQUE(release_generation)",
      "release_generation TEXT NOT NULL UNIQUE" in _mig_src
      and "CHECK(state IN" in _mig_src
      and "'FINALIZED'" in _mig_src
      and not any(re.search(r"\bfinalized\s*=\s*(True|False|1)\b", line)
                  for _, line in code_lines(os.path.join(AXOS, "store",
                                                         "gate.py"))),
      "global boolean or missing UNIQUE")

# A19d: desired-state version enforced — evaluate re-pins the head
# inside the authoritative txn; publish re-reads the head; a stale
# finalizer fails closed with STALE_GENERATION.
_rec_out_src = inspect.getsource(TransitionGate._record_evaluation_outcome)
check("A19d evaluate pins the desired head in-txn; a moved head "
      "refuses (STALE_GENERATION)",
      "_read_desired_head(conn)" in _rec_out_src
      and 'head["version"] != cur_row["desired_state_version"]'
      in _rec_out_src
      and "STALE_GENERATION" in _rec_out_src,
      "evaluate head pin missing")
check("A19d publish re-reads the desired head; a stale finalizer "
      "fails (STALE_GENERATION)",
      "_read_desired_head(conn)" in _pub_src
      and 'head["version"] != row["desired_state_version"]' in _pub_src
      and "STALE_GENERATION" in _pub_src,
      "publish head re-read missing")

# A19e: the release checkpoint is verified — publish demands a VERIFIED
# R5 release checkpoint with a release attestation receipt, revalidated
# inside the same write_txn; the idempotent FINALIZED path verifies the
# record too.
_reval_src = inspect.getsource(
    TransitionGate._revalidate_release_checkpoint_record)
_verify_fin_src = inspect.getsource(TransitionGate._verify_finalized_record)
check("A19e publish requires a VERIFIED release checkpoint with a "
      "release attestation receipt",
      "_revalidate_release_checkpoint_record(" in _pub_src
      and 'ck["verification_status"] != "VERIFIED"' in _reval_src
      and 'receipt.get("release") is not True' in _reval_src
      and 'receipt.get("checkpoint_id") != checkpoint_id' in _reval_src
      and 'ck["verification_status"] != "VERIFIED"' in _verify_fin_src,
      "checkpoint verification requirement missing")

# A19f: the manifest is deterministic — no uuid/random/wall-clock/pid
# in the gate's manifest construction path; no uuid/random in
# finalizer.py; time.time() there is _loop telemetry only (the A18d
# rule).
_r13_builders = {
    "build_finalization_manifest": build_finalization_manifest,
    "finalization_manifest_hash": finalization_manifest_hash,
    "canonical_release_generation": canonical_release_generation,
    "canonical_finalization_id": canonical_finalization_id,
    "_release_checkpoint_id": _release_checkpoint_id,
    "_release_checkpoint_entries": _release_checkpoint_entries,
}
_r13_nondet = []
for _bname, _bfn in _r13_builders.items():
    _bt = ast.parse(textwrap.dedent(inspect.getsource(_bfn)))
    _bnames = ({n.id for n in ast.walk(_bt) if isinstance(n, ast.Name)}
               | {n.attr for n in ast.walk(_bt)
                  if isinstance(n, ast.Attribute)})
    _bad = _bnames & {"uuid", "random", "time", "getpid", "monotonic",
                      "perf_counter"}
    if _bad:
        _r13_nondet.append((_bname, sorted(_bad)))
check("A19f deterministic manifest: no uuid/random/wall-clock in the "
      "gate manifest builders",
      not _r13_nondet, f"hits={_r13_nondet}")
_fin_names = ({n.id for n in ast.walk(_fin_tree)
               if isinstance(n, ast.Name)}
              | {n.attr for n in ast.walk(_fin_tree)
                 if isinstance(n, ast.Attribute)})
_fin_parent = {}
for _node in ast.walk(_fin_tree):
    for _child in ast.iter_child_nodes(_node):
        _fin_parent[_child] = _node
_fin_wall_bad = []
for _node in ast.walk(_fin_tree):
    if (isinstance(_node, ast.Call)
            and isinstance(_node.func, ast.Attribute)
            and _node.func.attr == "time"
            and isinstance(_node.func.value, ast.Name)
            and _node.func.value.id == "time"):
        _f, _fname = _node, "<module>"
        while _f in _fin_parent:
            _f = _fin_parent[_f]
            if isinstance(_f, (ast.FunctionDef, ast.AsyncFunctionDef)):
                _fname = _f.name
                break
        if _fname != "_loop":
            _fin_wall_bad.append((_fname, _node.lineno))
check("A19f no uuid/random in finalizer.py; time.time() only in _loop",
      not ({"uuid", "random"} & _fin_names)
      and "getpid" not in _fin_names
      and not _fin_wall_bad,
      f"wall={_fin_wall_bad}")

# A19g: FINALIZED cannot exist without release-checkpoint evidence —
# exactly one UPDATE writes state='FINALIZED', inside
# publish_finalization, after the checkpoint revalidation; a READY run
# with no checkpoint is a contradiction.
check("A19g FINALIZED written by exactly one UPDATE, in "
      "publish_finalization, after checkpoint revalidation",
      _gate_src.count("SET state='FINALIZED'") == 1
      and "SET state='FINALIZED'" in _pub_src
      and "_revalidate_release_checkpoint_record(" in _pub_src
      and "is READY with no release checkpoint" in _reval_src,
      "FINALIZED write path broken")

# A19h: the finalizer's capability envelope — read/evaluate only. Any
# call outside the gate's R13 read/evaluate API is an authority breach:
# no job creation/claiming, no worker dispatch, no process kill, no
# fence, no lease reclaim, no desired-state mutation, no R8/R9 policy,
# no R12 breaker moves, no R5 artifact protocol, no human-gate writes.
_r13_forbidden = {"create_job", "ensure_job", "ensure_job_for_desired_state",
                  "claim_job", "claim_job_bounded", "claim_job_resilient",
                  "claim_recovery_attempt", "start_worker", "dispatch",
                  "dispatch_restart", "terminate", "kill", "killpg",
                  "fence", "fence_sweep", "reclaim_lease", "release_lease",
                  "set_desired_item", "retire_desired_item",
                  "consume_policy", "consume_policy_attempts",
                  "cas_update_recovery_policy", "select_rung",
                  "transition_breaker", "record_breaker_signal",
                  "ensure_breaker_state", "breaker_allows",
                  "stage_artifact", "verify_artifact", "commit_artifact",
                  "begin_commit", "transition_job", "fail_job_execution",
                  "ingest_heartbeat", "create_task",
                  "complete_recovery_attempt", "create_recovery_attempt",
                  "run_recovery", "signal", "update_job_progress"}
check("A19h finalizer capability envelope: no job/lease/fence/"
      "dispatch/desired-state/policy/breaker/artifact/human-gate "
      "authority",
      not (_r13_forbidden & _fin_calls)
      and "PAUSED_FOR_HUMAN" not in _fin_src,
      f"breaches={sorted(_r13_forbidden & _fin_calls)}")

# A19i: the F1 boundary holds for the finalizer — Stores are opened
# through open_store only; no sqlite3.connect, no write_txn(), no raw
# connection executes or cursors.
_fin_f1_hits = [f"finalizer.py:{i}"
                for i, line in code_lines(_fin_p)
                if ("write_txn(" in line or "sqlite3.connect" in line
                    or ".conn.execute(" in line or ".cursor()" in line)]
check("A19i F1 boundary: finalizer.py opens no sqlite3 connections "
      "and calls no write_txn()",
      not _fin_f1_hits, f"hits={_fin_f1_hits}")

# A19j: published evidence is never overwritten — every FINALIZED
# branch in publish_finalization verifies the record (zero writes, no
# duplicate ledger event); begin is INSERT ... ON CONFLICT DO NOTHING
# (idempotent, no duplicate ledger event).
_fin_ifs = []
for _n in ast.walk(_pub_tree):
    if (isinstance(_n, ast.If)
            and "FINALIZED" in ast.dump(_n.test)
            and "state" in ast.dump(_n.test)):
        _body_calls = {c.func.attr for c in ast.walk(_n)
                       if isinstance(c, ast.Call)
                       and isinstance(c.func, ast.Attribute)}
        _fin_ifs.append((_n.lineno, _body_calls))
check("A19j no overwrite of published evidence: every FINALIZED "
      "branch verifies with zero writes; begin is conflict-DO-NOTHING",
      _fin_ifs
      and all("_verify_finalized_record" in c and "execute" not in c
              for _, c in _fin_ifs)
      and "ON CONFLICT(finalization_id) DO NOTHING" in _begin_src,
      f"branches={_fin_ifs}")

# A19k: no direct SQL authority bypass — only store/gate.py holds
# finalization_runs DML (INSERT/UPDATE/DELETE). Production tree only:
# tests/ and audit/ are not authority paths (like A18l's exec/ scope).
_r13_dml = re.compile(
    r"\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+finalization_runs\b",
    re.IGNORECASE)
_r13_other_writers = []
for _root, _dirs, _files in os.walk(AXOS):
    _dirs[:] = [d for d in _dirs
                if d not in ("tests", "audit", "__pycache__")]
    for _f in _files:
        if not _f.endswith(".py"):
            continue
        _p = os.path.join(_root, _f)
        if _p == os.path.join(AXOS, "store", "gate.py"):
            continue
        if _r13_dml.search(read(_p)):
            _r13_other_writers.append(os.path.relpath(_p, AXOS))
check("A19k no SQL authority bypass: only the gate writes "
      "finalization_runs",
      not _r13_other_writers,
      f"other writers: {_r13_other_writers}")

# A19l: no duplicate finalization authority — no second module defines
# the R13 gate ops, and the finalizer's only finalization-named calls
# are the authorized R13 read/evaluate API plus the re-exported pure
# helpers (never DML — see A19k).
_r13_dupes = []
for _root, _dirs, _files in os.walk(AXOS):
    _dirs[:] = [d for d in _dirs if d != "__pycache__"]
    for _f in _files:
        if not _f.endswith(".py"):
            continue
        _p = os.path.join(_root, _f)
        if _p == os.path.join(AXOS, "store", "gate.py"):
            continue
        if re.search(r"def (begin|evaluate|publish)_finalization\b",
                     read(_p)):
            _r13_dupes.append(os.path.relpath(_p, AXOS))
_r13_allowed_fin = {"begin_finalization_run", "get_finalization_run",
                    "list_finalization_runs", "evaluate_finalization",
                    "publish_finalization", "canonical_finalization_id",
                    "build_finalization_manifest",
                    "finalization_manifest_hash"}
_r13_fin_calls = {c for c in _fin_calls if "finalization" in c.lower()}
check("A19l no duplicate finalization authority: single R13 op "
      "definition; finalizer touches only the authorized R13 "
      "read/evaluate API",
      not _r13_dupes
      and _r13_fin_calls <= _r13_allowed_fin,
      f"dupes={_r13_dupes} fin_calls={sorted(_r13_fin_calls)}")

# A20 (R14): no private connection-capability access in exec/. A1-A3
# block write_txn(), sqlite3.connect, and SQL writes, but the store's
# raw writable connection (Store._conn) is a separate capability from
# the PRAGMA query_only=ON Store.conn that A4 permits for read-only
# reconstruct() SELECTs. Direct access to Store._conn (or the
# read-only Store._ro_conn, which must stay behind the store's own
# accessors) from exec/ would bypass the F1 read-only guarantee, so
# it is rejected explicitly. Verified with the ast module, not regex:
# any Attribute access named _conn or _ro_conn in exec/ fails.
_a20_hits = []
for _p in py_files():
    try:
        _tree = ast.parse(read(_p))
    except SyntaxError as e:
        _a20_hits.append(f"{os.path.basename(_p)}:unparseable:{e}")
        continue
    for _node in ast.walk(_tree):
        if isinstance(_node, ast.Attribute) and _node.attr in (
                "_conn", "_ro_conn"):
            _a20_hits.append(
                f"{os.path.basename(_p)}:{_node.lineno}:"
                f".{_node.attr}")
check("A20 no private connection-capability access in exec/: "
      "Store._conn/_ro_conn unreachable outside the store",
      not _a20_hits, f"hits: {_a20_hits}")


print()
if FAILURES:
    print(f"AUTHORITY AUDIT: {len(FAILURES)} FAILING CHECK(S): {FAILURES}")
    sys.exit(1)
print("AUTHORITY AUDIT: all checks passed — every authoritative mutation in"
      " the execution layer goes through TransitionGate.")
