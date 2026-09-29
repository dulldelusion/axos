"""Phase 1C R5 — verified artifacts & artifact integrity tests.

STANDARD: real bytes, real sha256, real fsync'd files, real SQLite
transactions. Corruption tests modify actual bytes on disk. No mocked
hashes, no `artifact_exists = True` fakes.

Test IDs:
  R5-01  deterministic artifact hash (content addressing)
  R5-02  different bytes => different identity
  R5-03  worker-claimed checksum mismatch is rejected
  R5-04  real artifact staging (bytes on disk, gate-computed hash)
  R5-05  validator provenance (receipt identity + hash binding)
  R5-06  validation hash mismatch (real corruption => quarantine, durable
          FAIL receipt)
  R5-07  VERIFIED requires validators (no setter, no bypass)
  R5-08  stale worker cannot mutate artifacts
  R5-09  atomic completion (single txn: COMPLETE + artifact + hash +
          ledger)
  R5-10  missing artifact => completion rejected
  R5-11  corrupted artifact => completion rejected
  R5-12  missing validation => completion rejected
  R5-13  crash before completion => not COMPLETE, resumable
  R5-14  crash after committed completion => COMPLETE intact
  R5-15  uncertain completion inspection (COMMITTED / UNCERTAIN /
          NOT_COMMITTED + resolver adoption)
  R5-16  COMPLETE/artifact contradiction is detected, never manufactured
  R5-17  deterministic checkpoint ID
  R5-18  canonical manifest equivalence
  R5-19  checkpoint artifact validation
  R5-20  checkpoint receipt identity binding
  R5-21  latest-known-good ordering
  R5-22  failed checkpoint cannot replace latest-known-good
  R5-23  release revalidates 100% (no sampling)
  R5-24  release corruption is rejected
  R5-25  stale worker cannot mutate checkpoints
  R5-26  repeated artifact registration is idempotent
  R5-27  repeated completion is idempotent
  R5-28  transaction rollback under injected failure
  R5-28b mid-transaction failure inside the authoritative completion write
  R5-28c bytes changing between verify_artifact's two transactions
  R5-26c crash between byte fsync and row registration (orphan bytes)
  R5-33  v4 migration leaves historical COMPLETE rows untouched
  R5-29  real subprocess worker completes through the R5 protocol
  R5-30  R1/R2/R3/R4 composition with R5 completion
  R5-31  authority audit (A11) is green
  R5-32  explicit I-4 regression: no artifact-free path to COMPLETE
"""
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

WORKSPACE_ROOT = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))  # ~/workspace
sys.path.insert(0, WORKSPACE_ROOT)

from axos.store import (TransitionGate, TransitionRejected, LeaseError,
                        open_store)  # noqa: E402
from axos.exec.supervisor import Supervisor  # noqa: E402

AXOS_DIR = os.path.join(WORKSPACE_ROOT, "axos")


def _event_count(gate, event_type):
    return gate.store.conn.execute(
        "SELECT COUNT(*) FROM ledger WHERE event_type=?",
        (event_type,)).fetchone()[0]


class R5Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="axos-r5-")
        self.db = os.path.join(self.tmp, "t.db")
        self.sup = Supervisor(self.db, actor="test-r5")
        self.gate = self.sup.gate
        self._sups = [self.sup]

    def tearDown(self):
        for s in self._sups:
            try:
                for wid in list(s._procs):
                    try:
                        s.kill_worker(wid)
                    except Exception:
                        pass
                s.close()
            except Exception:
                pass
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- fixtures ----------------------------------------------------
    def make_task(self, tid="t"):
        try:
            self.gate.create_task(tid, {"objective": "x"}, {"usd": 1},
                                  "scheduler")
        except Exception:
            pass
        return tid

    def make_job(self, jid, task="t"):
        self.make_task(task)
        self.gate.create_job(jid, task, "s", "scheduler")
        return jid

    def running(self, jid, wid="w", ttl=60.0, task="t"):
        self.make_job(jid, task)
        self.gate.claim_job(jid, wid, ttl, "scheduler")
        self.gate.transition_job(jid, "RUNNING", f"worker:{wid}")
        return self.gate.get_job(jid)["fencing_token"]

    def stage(self, jid, wid, tok, data, task="t", actor=None):
        return self.gate.stage_artifact(
            job_id=jid, worker_id=wid, fencing_token=tok, task_id=task,
            kind="result", data=data,
            actor=actor or f"worker:{wid}")

    def full_protocol(self, jid, wid, tok, data, task="t", evidence=None):
        """RUNNING -> stage -> begin -> verify -> atomic commit."""
        art = self.stage(jid, wid, tok, data, task)
        aid = art["artifact_id"]
        self.gate.begin_commit(jid, wid, tok, artifact_id=aid,
                               actor=f"worker:{wid}")
        self.gate.verify_artifact(aid, actor=f"worker:{wid}",
                                  worker_id=wid, fencing_token=tok)
        row = self.gate.commit_artifact(
            jid, wid, tok, artifact_id=aid, actor=f"worker:{wid}",
            evidence=evidence or {})
        return row, self.gate.get_artifact(aid)

    def verified_artifact(self, jid, wid, tok, data, task="t"):
        """Stage + gate-verify (authority actor) for checkpoint fixtures."""
        art = self.stage(jid, wid, tok, data, task)
        self.gate.verify_artifact(art["artifact_id"], actor="test")
        return self.gate.get_artifact(art["artifact_id"])

    def wait_for(self, pred, timeout=20.0, interval=0.1):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return True
            time.sleep(interval)
        raise AssertionError("wait_for timed out")

    def wait_reaped(self, sup, wid, timeout=30.0):
        def _poll():
            sup.observe()
            return wid in sup._reaped
        self.wait_for(_poll, timeout)
        return sup._reaped[wid]


# ------------------------------------------------- R5-01 .. R5-08: staging
class ArtifactStagingTest(R5Base):
    def test_R5_01_deterministic_artifact_hash(self):
        tok = self.running("j1")
        data = b"deterministic-bytes-001"
        a1 = self.stage("j1", "w", tok, data)
        a2 = self.stage("j1", "w", tok, data)  # re-stage identical bytes
        expected = hashlib.sha256(data).hexdigest()
        self.assertEqual(a1["artifact_id"], expected)
        self.assertEqual(a2["artifact_id"], expected)
        self.assertEqual(a1["content_hash"], expected)

    def test_R5_02_different_bytes_different_identity(self):
        tok = self.running("j1")
        a1 = self.stage("j1", "w", tok, b"bytes-A")
        a2 = self.stage("j1", "w", tok, b"bytes-B")
        self.assertNotEqual(a1["artifact_id"], a2["artifact_id"])
        self.assertNotEqual(a1["content_hash"], a2["content_hash"])

    def test_R5_03_worker_checksum_mismatch_rejected(self):
        tok = self.running("j1")
        with self.assertRaises(TransitionRejected):
            self.gate.stage_artifact(
                job_id="j1", worker_id="w", fencing_token=tok, task_id="t",
                kind="result", data=b"real-bytes",
                claimed_hash="0" * 64, actor="worker:w")
        # Nothing was registered.
        self.assertEqual(
            self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM artifacts").fetchone()[0], 0)

    def test_R5_04_real_artifact_staging(self):
        tok = self.running("j1")
        data = b"real-bytes-on-real-disk"
        art = self.stage("j1", "w", tok, data)
        self.assertEqual(art["status"], "STAGING")
        with open(art["uri"], "rb") as fh:
            on_disk = fh.read()
        self.assertEqual(on_disk, data)
        self.assertEqual(hashlib.sha256(on_disk).hexdigest(),
                         art["content_hash"])
        # A staged artifact alone never completes the job.
        self.assertEqual(self.gate.get_job("j1")["status"], "RUNNING")

    def test_R5_05_validator_provenance(self):
        tok = self.running("j1")
        art = self.stage("j1", "w", tok, b"provenance-bytes")
        self.gate.verify_artifact(art["artifact_id"], actor="test")
        row = self.gate.store.conn.execute(
            "SELECT validator_id, validator_version, result, content_hash,"
            " method FROM validations WHERE artifact_id=?",
            (art["artifact_id"],)).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual((row["validator_id"], row["validator_version"]),
                         ("axos-structural", "1"))
        self.assertEqual(row["result"], "PASS")
        self.assertEqual(row["content_hash"], art["content_hash"])
        self.assertEqual(self.gate.get_artifact(
            art["artifact_id"])["status"], "VALIDATED")

    def test_R5_06_validation_hash_mismatch_quarantines_durably(self):
        tok = self.running("j1")
        art = self.stage("j1", "w", tok, b"pristine-bytes")
        aid = art["artifact_id"]
        # Corrupt the ACTUAL bytes on disk.
        with open(art["uri"], "wb") as fh:
            fh.write(b"tampered-bytes!!")
        with self.assertRaises(TransitionRejected):
            self.gate.verify_artifact(aid, actor="worker:w",
                                      worker_id="w", fencing_token=tok)
        # The quarantine + FAIL receipt are DURABLE despite the raise
        # (two-transaction forensic discipline — the old rollback bug).
        self.assertEqual(self.gate.get_artifact(aid)["status"], "QUARANTINED")
        row = self.gate.store.conn.execute(
            "SELECT result, content_hash FROM validations"
            " WHERE artifact_id=? AND validator_id='axos-structural'",
            (aid,)).fetchone()
        self.assertEqual(row["result"], "FAIL")
        self.assertEqual(row["content_hash"], aid)
        # A quarantined artifact can never complete the job.
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j1", "w", tok, artifact_id=aid,
                                      actor="worker:w")

    def test_R5_07_verified_requires_validators(self):
        tok = self.running("j1")
        art = self.stage("j1", "w", tok, b"needs-receipts")
        aid = art["artifact_id"]
        # An extra required validator with no receipt blocks verification.
        with self.assertRaises(TransitionRejected):
            self.gate.verify_artifact(
                aid, actor="test",
                required_validators=(("axos-structural", "1"),
                                     ("axos-extra", "9")))
        # And without verification the artifact cannot complete the job:
        # there is no setter that can mark it VERIFIED by assertion.
        self.assertNotIn("set_artifact_verified",
                         dir(self.gate))
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j1", "w", tok, artifact_id=aid,
                                      actor="worker:w")

    def test_R5_08_stale_worker_cannot_mutate_artifacts(self):
        tok = self.running("j1")
        art = self.stage("j1", "w", tok, b"epoch-one")
        aid = art["artifact_id"]
        # R1 reclaim supersedes the old epoch: token bumps, owner cleared.
        self.gate.reclaim_lease("j1", actor="reconciler", reason="test",
                                expected_owner="w", expected_token=tok,
                                force=True, verdict="DEAD",
                                incident_id="i1")
        # Stale worker cannot stage...
        with self.assertRaises(LeaseError):
            self.stage("j1", "w", tok, b"stale-bytes")
        # ...nor verify (fencing is checked before state).
        with self.assertRaises(LeaseError):
            self.gate.verify_artifact(aid, actor="worker:w",
                                      worker_id="w", fencing_token=tok)


# ------------------------------------------- R5-09 .. R5-16: completion
class AtomicCompletionTest(R5Base):
    def test_R5_09_atomic_completion(self):
        tok = self.running("j1")
        data = b"atomic-result-bytes"
        n_before = _event_count(self.gate, "job.committed")
        row, art = self.full_protocol("j1", "w", tok, data,
                                      evidence={"n": 1})
        self.assertEqual(row["status"], "COMPLETE")
        self.assertEqual(row["commit_outcome"], "SUCCESS")
        self.assertEqual(row["result_artifact_id"], art["artifact_id"])
        self.assertEqual(row["content_hash"],
                         hashlib.sha256(data).hexdigest())
        self.assertEqual(art["status"], "VALIDATED")
        # One transaction wrote job + artifact linkage + the ledger event.
        self.assertEqual(_event_count(self.gate, "job.committed"),
                         n_before + 1)
        ev = self.gate.store.conn.execute(
            "SELECT payload FROM ledger WHERE event_type='job.committed'"
            " ORDER BY seq DESC LIMIT 1").fetchone()
        payload = json.loads(ev["payload"])
        self.assertEqual(payload["artifact_id"], art["artifact_id"])
        self.assertEqual(payload["content_hash"], row["content_hash"])

    def test_R5_10_missing_artifact_rejected(self):
        tok = self.running("j1")
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j1", "w", tok,
                                      artifact_id="0" * 64,
                                      actor="worker:w")
        self.assertEqual(self.gate.get_job("j1")["status"], "RUNNING")

    def test_R5_11_corrupted_artifact_rejected_at_commit(self):
        # The commit re-reads the bytes inside its own transaction: bytes
        # corrupted after verification still cannot complete the job.
        tok = self.running("j1")
        art = self.stage("j1", "w", tok, b"will-be-corrupted")
        aid = art["artifact_id"]
        self.gate.begin_commit("j1", "w", tok, artifact_id=aid,
                               actor="worker:w")
        self.gate.verify_artifact(aid, actor="worker:w", worker_id="w",
                                  fencing_token=tok)
        with open(art["uri"], "wb") as fh:
            fh.write(b"corrupted-after-verify")
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j1", "w", tok, artifact_id=aid,
                                      actor="worker:w")
        job = self.gate.get_job("j1")
        self.assertEqual(job["status"], "COMMITTING")
        self.assertIsNone(job["result_artifact_id"])

    def test_R5_12_missing_validation_rejected(self):
        tok = self.running("j1")
        art = self.stage("j1", "w", tok, b"never-verified")
        aid = art["artifact_id"]
        self.gate.begin_commit("j1", "w", tok, artifact_id=aid,
                               actor="worker:w")
        # No verify_artifact call: the commit must refuse.
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j1", "w", tok, artifact_id=aid,
                                      actor="worker:w")
        self.assertEqual(self.gate.get_job("j1")["status"], "COMMITTING")

    def test_R5_13_crash_before_completion_is_resumable(self):
        tok = self.running("j1")
        art = self.stage("j1", "w", tok, b"crash-me")
        aid = art["artifact_id"]
        self.gate.begin_commit("j1", "w", tok, artifact_id=aid,
                               actor="worker:w")
        self.gate.verify_artifact(aid, actor="worker:w", worker_id="w",
                                  fencing_token=tok)
        # "Crash": drop every connection; reopen over the same files.
        self.sup.close()
        g2 = TransitionGate(open_store(self.db))
        job = g2.get_job("j1")
        self.assertEqual(job["status"], "COMMITTING")
        self.assertIsNone(job["result_artifact_id"])
        # Nothing claims this was a completion.
        insp = g2.inspect_uncertain_completion("j1")
        self.assertNotEqual(insp["disposition"], "COMMITTED")
        # The protocol resumes and completes exactly once.
        row = g2.commit_artifact("j1", "w", tok, artifact_id=aid,
                                 actor="worker:w", evidence={})
        self.assertEqual(row["status"], "COMPLETE")
        self.assertEqual(row["result_artifact_id"], aid)
        g2.store.close()

    def test_R5_14_crash_after_commit_is_intact(self):
        tok = self.running("j1")
        data = b"durable-completion"
        row, art = self.full_protocol("j1", "w", tok, data)
        aid = art["artifact_id"]
        self.sup.close()
        g2 = TransitionGate(open_store(self.db))
        job = g2.get_job("j1")
        self.assertEqual(job["status"], "COMPLETE")
        self.assertEqual(job["result_artifact_id"], aid)
        self.assertEqual(job["content_hash"],
                         hashlib.sha256(data).hexdigest())
        insp = g2.inspect_uncertain_completion("j1")
        self.assertEqual(insp["disposition"], "COMMITTED")
        self.assertEqual(insp["integrity"], "consistent")
        g2.store.close()

    def test_R5_15_uncertain_completion_inspection(self):
        # NOT_COMMITTED: UNCERTAIN job, no adoptable artifact.
        tok = self.running("j1")
        self.gate.reclaim_lease("j1", actor="reconciler", reason="test",
                                expected_owner="w", expected_token=tok,
                                force=True, verdict="DEAD",
                                incident_id="i1")
        insp = self.gate.inspect_uncertain_completion("j1")
        self.assertEqual(insp["disposition"], "NOT_COMMITTED")

        # UNCERTAIN with an adoptable verified artifact (staged under the
        # superseded epoch) -> the recovery authority adopts it.
        tok2 = self.running("j2", "w2")
        art = self.verified_artifact("j2", "w2", tok2, b"adoptable")
        aid = art["artifact_id"]
        self.gate.reclaim_lease("j2", actor="reconciler", reason="test",
                                expected_owner="w2", expected_token=tok2,
                                force=True, verdict="DEAD",
                                incident_id="i2")
        insp = self.gate.inspect_uncertain_completion("j2")
        self.assertEqual(insp["disposition"], "UNCERTAIN")
        self.assertIn(aid, insp["adoptable_artifacts"])
        # The artifact alone is not completion: the job is still UNCERTAIN.
        self.assertEqual(self.gate.get_job("j2")["status"], "UNCERTAIN")
        # Resolver adoption: token lineage proves the bytes predate the
        # reclaim (artifact token < job's bumped token).
        row = self.gate.commit_artifact("j2", None, None, artifact_id=aid,
                                        actor="reconciler", evidence={})
        self.assertEqual(row["status"], "COMPLETE")
        self.assertEqual(row["result_artifact_id"], aid)
        # A non-authority actor cannot adopt.
        tok3 = self.running("j3", "w3")
        art3 = self.verified_artifact("j3", "w3", tok3, b"no-adopt")
        self.gate.reclaim_lease("j3", actor="reconciler", reason="test",
                                expected_owner="w3", expected_token=tok3,
                                force=True, verdict="DEAD",
                                incident_id="i3")
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j3", None, None,
                                      artifact_id=art3["artifact_id"],
                                      actor="worker:w3", evidence={})

    def test_R5_16_complete_artifact_contradiction_detected(self):
        tok = self.running("j1")
        data = b"contradiction-target"
        row, art = self.full_protocol("j1", "w", tok, data)
        # Corrupt the bytes behind the COMPLETE job's back.
        with open(art["uri"], "wb") as fh:
            fh.write(b"contradicted!!")
        insp = self.gate.inspect_uncertain_completion("j1")
        self.assertEqual(insp["disposition"], "UNCERTAIN")
        self.assertEqual(insp["integrity"], "contradiction")
        self.assertTrue(insp["problems"])
        # The inspector never manufactures a clean bill: the job still
        # says COMPLETE, the problem list says why that is now suspect.
        self.assertEqual(self.gate.get_job("j1")["status"], "COMPLETE")


# ------------------------------------------- R5-17 .. R5-25: checkpoints
class CheckpointTest(R5Base):
    def _manifest(self, *arts):
        return [{"artifact_id": a["artifact_id"],
                 "content_hash": a["content_hash"]} for a in arts]

    def _verified(self, jid, data, task="t"):
        tok = self.running(jid, f"w-{jid}", task=task)
        return self.verified_artifact(jid, f"w-{jid}", tok, data, task)

    def test_R5_17_deterministic_checkpoint_id(self):
        a = self._verified("j1", b"ckpt-a")
        m = self._manifest(a)
        c1 = self.gate.stage_checkpoint("t", actor="test", manifest=m,
                                        trigger="policy")
        c2 = self.gate.stage_checkpoint("t", actor="test", manifest=m,
                                        trigger="policy")
        self.assertEqual(c1["checkpoint_id"], c2["checkpoint_id"])
        self.assertEqual(c1["checkpoint_id"],
                         hashlib.sha256(
                             c1["canonical_manifest"].encode()).hexdigest())

    def test_R5_18_canonical_manifest_equivalence(self):
        a = self._verified("j1", b"ckpt-a")
        b = self._verified("j2", b"ckpt-b")
        m1 = self._manifest(a, b)                      # order: a, b
        m2 = self._manifest(b, a)                      # order: b, a
        # Same logical content, shuffled key order + an unknown key:
        # canonical form drops unknown keys, so the identity matches.
        m3 = [{"content_hash": a["content_hash"],
               "artifact_id": a["artifact_id"], "note": "dropped"},
              {"artifact_id": b["artifact_id"],
               "content_hash": b["content_hash"], "note": "dropped"}]
        c1 = self.gate.stage_checkpoint("t", actor="test", manifest=m1,
                                        trigger="policy")
        c2 = self.gate.stage_checkpoint("t", actor="test", manifest=m2,
                                        trigger="policy")
        c3 = self.gate.stage_checkpoint("t", actor="test", manifest=m3,
                                        trigger="policy")
        self.assertEqual(c1["checkpoint_id"], c2["checkpoint_id"])
        self.assertEqual(c1["checkpoint_id"], c3["checkpoint_id"])
        self.assertEqual(c1["canonical_manifest"],
                         c2["canonical_manifest"])
        # A known optional key (kind) participates in the identity:
        # different canonical bytes => different checkpoint.
        m4 = [{"artifact_id": a["artifact_id"],
               "content_hash": a["content_hash"], "kind": "result"},
              {"artifact_id": b["artifact_id"],
               "content_hash": b["content_hash"]}]
        c4 = self.gate.stage_checkpoint("t", actor="test", manifest=m4,
                                        trigger="policy")
        self.assertNotEqual(c1["checkpoint_id"], c4["checkpoint_id"])
        # Different manifest => different identity. No random IDs.
        other = self._verified("j3", b"ckpt-c")
        c5 = self.gate.stage_checkpoint("t", actor="test",
                                        manifest=self._manifest(other),
                                        trigger="policy")
        self.assertNotEqual(c1["checkpoint_id"], c5["checkpoint_id"])

    def test_R5_19_checkpoint_artifact_validation(self):
        a = self._verified("j1", b"ckpt-a")
        cp = self.gate.stage_checkpoint("t", actor="test",
                                        manifest=self._manifest(a),
                                        trigger="pre_risky_operation")
        self.assertEqual(cp["verification_status"], "UNVERIFIED")
        # An unverified candidate is never latest-known-good.
        self.assertIsNone(self.gate.latest_known_good("t"))
        out = self.gate.verify_checkpoint(cp["checkpoint_id"], actor="test")
        self.assertEqual(out["verification_status"], "VERIFIED")
        lkg = self.gate.latest_known_good("t")
        self.assertEqual(lkg["checkpoint_id"], cp["checkpoint_id"])

    def test_R5_20_checkpoint_receipt_identity_binding(self):
        a = self._verified("j1", b"ckpt-a")
        cp = self.gate.stage_checkpoint("t", actor="test",
                                        manifest=self._manifest(a),
                                        trigger="policy")
        out = self.gate.verify_checkpoint(cp["checkpoint_id"], actor="test")
        receipt = json.loads(out["verification_receipt"])
        # The receipt binds the exact checkpoint identity...
        self.assertEqual(receipt["checkpoint_id"], cp["checkpoint_id"])
        self.assertEqual(receipt["manifest_hash"], cp["checkpoint_id"])
        # ...records the full-revalidation method, and names every
        # artifact with its verdict.
        self.assertEqual(receipt["method"], "full-revalidation")
        self.assertTrue(receipt["manifest_full"])
        self.assertEqual(len(receipt["artifacts"]), 1)
        self.assertTrue(receipt["artifacts"][0]["ok"])
        self.assertEqual(receipt["artifacts"][0]["artifact_id"],
                         a["artifact_id"])

    def test_R5_21_latest_known_good_ordering(self):
        a = self._verified("j1", b"ckpt-a")
        b = self._verified("j2", b"ckpt-b")
        cp1 = self.gate.stage_checkpoint("t", actor="test",
                                         manifest=self._manifest(a),
                                         trigger="policy")
        cp2 = self.gate.stage_checkpoint("t", actor="test",
                                         manifest=self._manifest(b),
                                         trigger="policy")
        self.gate.verify_checkpoint(cp1["checkpoint_id"], actor="test")
        self.assertEqual(self.gate.latest_known_good("t")["checkpoint_id"],
                         cp1["checkpoint_id"])
        self.gate.verify_checkpoint(cp2["checkpoint_id"], actor="test")
        # Greatest created_at among VERIFIED wins.
        self.assertEqual(self.gate.latest_known_good("t")["checkpoint_id"],
                         cp2["checkpoint_id"])

    def test_R5_22_failed_checkpoint_cannot_replace_lkg(self):
        good = self._verified("j1", b"ckpt-good")
        cp1 = self.gate.stage_checkpoint("t", actor="test",
                                         manifest=self._manifest(good),
                                         trigger="policy")
        self.gate.verify_checkpoint(cp1["checkpoint_id"], actor="test")
        # A candidate whose bytes are corrupted behind its back.
        bad_tok_art = self._verified("j2", b"ckpt-bad")
        with open(bad_tok_art["uri"], "wb") as fh:
            fh.write(b"corrupted!")
        cp2 = self.gate.stage_checkpoint(
            "t", actor="test", manifest=self._manifest(bad_tok_art),
            trigger="policy")
        out = self.gate.verify_checkpoint(cp2["checkpoint_id"], actor="test")
        self.assertEqual(out["verification_status"], "CORRUPT")
        # Latest-known-good never moved.
        self.assertEqual(self.gate.latest_known_good("t")["checkpoint_id"],
                         cp1["checkpoint_id"])

    def test_R5_23_release_revalidates_everything(self):
        arts = [self._verified(f"j{i}", f"ckpt-{i}".encode())
                for i in range(1, 4)]
        cp = self.gate.stage_checkpoint("t", actor="test",
                                        manifest=self._manifest(*arts),
                                        trigger="policy")
        out = self.gate.verify_checkpoint(cp["checkpoint_id"], actor="test",
                                          release=True)
        self.assertEqual(out["verification_status"], "VERIFIED")
        receipt = json.loads(out["verification_receipt"])
        # No sampling: every manifest entry was revalidated.
        self.assertTrue(receipt["release"])
        self.assertTrue(receipt["manifest_full"])
        self.assertEqual(len(receipt["artifacts"]), 3)
        self.assertTrue(all(e["ok"] for e in receipt["artifacts"]))

    def test_R5_24_release_corruption_rejected(self):
        arts = [self._verified(f"j{i}", f"ckpt-{i}".encode())
                for i in range(1, 4)]
        # Corrupt exactly one entry's bytes.
        with open(arts[1]["uri"], "wb") as fh:
            fh.write(b"one-bad-apple")
        cp = self.gate.stage_checkpoint("t", actor="test",
                                        manifest=self._manifest(*arts),
                                        trigger="policy")
        out = self.gate.verify_checkpoint(cp["checkpoint_id"], actor="test",
                                          release=True)
        self.assertEqual(out["verification_status"], "CORRUPT")
        receipt = json.loads(out["verification_receipt"])
        bad = [e for e in receipt["artifacts"]
               if e["artifact_id"] == arts[1]["artifact_id"]]
        self.assertEqual(len(bad), 1)
        self.assertFalse(bad[0]["ok"])
        self.assertTrue(bad[0]["problems"])
        self.assertIsNone(self.gate.latest_known_good("t"))

    def test_R5_25_stale_worker_cannot_mutate_checkpoints(self):
        a = self._verified("j1", b"ckpt-a")
        m = self._manifest(a)
        with self.assertRaises(TransitionRejected):
            self.gate.stage_checkpoint("t", actor="worker:w",
                                       manifest=m, trigger="policy")
        cp = self.gate.stage_checkpoint("t", actor="test", manifest=m,
                                        trigger="policy")
        with self.assertRaises(TransitionRejected):
            self.gate.verify_checkpoint(cp["checkpoint_id"],
                                        actor="worker:w")
        # The candidate is untouched by the stale attempt.
        self.assertEqual(
            self.gate.store.conn.execute(
                "SELECT verification_status FROM checkpoints"
                " WHERE checkpoint_id=?", (cp["checkpoint_id"],)
            ).fetchone()["verification_status"], "UNVERIFIED")


# --------------------------------- R5-26 .. R5-32: hardening & contract
class R5HardeningTest(R5Base):
    def test_R5_26_repeated_artifact_registration(self):
        tok = self.running("j1")
        data = b"register-me-twice"
        a1 = self.stage("j1", "w", tok, data)
        n1 = _event_count(self.gate, "artifact.staged")
        a2 = self.stage("j1", "w", tok, data)
        # Same identity, one row, no duplicate ledger event.
        self.assertEqual(a1["artifact_id"], a2["artifact_id"])
        self.assertEqual(
            self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM artifacts WHERE artifact_id=?",
                (a1["artifact_id"],)).fetchone()[0], 1)
        self.assertEqual(_event_count(self.gate, "artifact.staged"), n1)

    def test_R5_26b_cross_job_identical_bytes(self):
        # R5-26 extension (R14 contract fix for the R14-125 defect):
        # identical bytes staged by a DIFFERENT job. Content identity is
        # global (same bytes => same artifact_id); each job's staging is
        # recorded in artifact_stagings under its own live lease, and
        # EITHER job can complete through the full protocol against the
        # shared row. A job that never staged the bytes still cannot
        # adopt the row, and a COMMITTING job cannot swap to an artifact
        # it never staged.
        tok_a = self.running("jA", "wA")
        data = b"shared-bytes-across-jobs"
        a = self.stage("jA", "wA", tok_a, data)
        tok_b = self.running("jB", "wB")
        b = self.stage("jB", "wB", tok_b, data)
        self.assertEqual(a["artifact_id"], b["artifact_id"])
        self.assertEqual(b["job_id"], "jA")  # canonical registration
        # B staged the same bytes under its own lease, so B can open a
        # commit with the shared artifact and complete it.
        self.gate.begin_commit("jB", "wB", tok_b,
                               artifact_id=a["artifact_id"],
                               actor="worker:wB")
        self.gate.verify_artifact(a["artifact_id"], actor="worker:wB",
                                  worker_id="wB", fencing_token=tok_b,
                                  job_id="jB")
        row_b = self.gate.commit_artifact("jB", "wB", tok_b,
                                          artifact_id=a["artifact_id"],
                                          actor="worker:wB", evidence={})
        self.assertEqual(row_b["status"], "COMPLETE")
        self.assertEqual(row_b["result_artifact_id"], a["artifact_id"])
        # A completes against the same (now VALIDATED) row - completion
        # never depends on another job's progress.
        self.gate.begin_commit("jA", "wA", tok_a,
                               artifact_id=a["artifact_id"],
                               actor="worker:wA")
        self.gate.verify_artifact(a["artifact_id"], actor="worker:wA",
                                  worker_id="wA", fencing_token=tok_a,
                                  job_id="jA")
        row = self.gate.commit_artifact("jA", "wA", tok_a,
                                        artifact_id=a["artifact_id"],
                                        actor="worker:wA", evidence={})
        self.assertEqual(row["status"], "COMPLETE")
        self.assertEqual(row["result_artifact_id"], a["artifact_id"])
        # A job that never staged the bytes cannot adopt the shared row.
        tok_c = self.running("jC", "wC")
        with self.assertRaises(TransitionRejected):
            self.gate.begin_commit("jC", "wC", tok_c,
                                   artifact_id=a["artifact_id"],
                                   actor="worker:wC")
        self.assertEqual(self.gate.get_job("jC")["status"], "RUNNING")
        # ...nor substitute it at commit time: jD is COMMITTING on its
        # own verified artifact but never staged the shared bytes.
        tok_d = self.running("jD", "wD")
        own = self.stage("jD", "wD", tok_d, b"d-own-bytes")
        self.gate.begin_commit("jD", "wD", tok_d,
                               artifact_id=own["artifact_id"],
                               actor="worker:wD")
        self.gate.verify_artifact(own["artifact_id"], actor="worker:wD",
                                  worker_id="wD", fencing_token=tok_d,
                                  job_id="jD")
        with self.assertRaises(TransitionRejected) as ctx:
            self.gate.commit_artifact("jD", "wD", tok_d,
                                      artifact_id=a["artifact_id"],
                                      actor="worker:wD", evidence={})
        self.assertIn("not 'jD'", str(ctx.exception))
        row_d = self.gate.commit_artifact("jD", "wD", tok_d,
                                          artifact_id=own["artifact_id"],
                                          actor="worker:wD", evidence={})
        self.assertEqual(row_d["status"], "COMPLETE")

    def test_R5_27_repeated_completion(self):
        tok = self.running("j1")
        row, art = self.full_protocol("j1", "w", tok, b"once-only",
                                      evidence={"n": 1})
        n1 = _event_count(self.gate, "job.committed")
        # Same artifact repeated: idempotent no-op, same row.
        row2 = self.gate.commit_artifact("j1", "w", tok,
                                         artifact_id=art["artifact_id"],
                                         actor="worker:w",
                                         evidence={"n": 2})
        self.assertEqual(row2["status"], "COMPLETE")
        self.assertEqual(json.loads(row2["commit_evidence"]), {"n": 1})
        self.assertEqual(_event_count(self.gate, "job.committed"), n1)
        # Contradictory duplicate: a different artifact is rejected, and
        # the recorded completion is untouched.
        tok2 = self.running("j2", "w2")
        other = self.stage("j2", "w2", tok2, b"other-bytes")
        self.gate.begin_commit("j2", "w2", tok2,
                               artifact_id=other["artifact_id"],
                               actor="worker:w2")
        self.gate.verify_artifact(other["artifact_id"], actor="worker:w2",
                                  worker_id="w2", fencing_token=tok2)
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j1", "w", tok,
                                      artifact_id=other["artifact_id"],
                                      actor="worker:w")
        self.assertEqual(
            self.gate.get_job("j1")["result_artifact_id"],
            art["artifact_id"])

    def test_R5_28_transaction_rollback_under_failure(self):
        # Injected failure at begin_commit: missing bytes. The rejection
        # leaves the job exactly where it was — RUNNING, no partial
        # COMMITTING, no completion.
        tok = self.running("j1")
        art = self.stage("j1", "w", tok, b"rollback-me")
        os.remove(art["uri"])
        before = dict(self.gate.get_job("j1"))
        with self.assertRaises(TransitionRejected):
            self.gate.begin_commit("j1", "w", tok,
                                   artifact_id=art["artifact_id"],
                                   actor="worker:w")
        after = dict(self.gate.get_job("j1"))
        self.assertEqual(after["status"], "RUNNING")
        for k in ("owner_worker_id", "fencing_token", "result_artifact_id",
                  "content_hash", "commit_outcome"):
            self.assertEqual(after[k], before[k])
        # Injected failure at commit: bytes vanish after verification.
        tok2 = self.running("j2", "w2")
        art2 = self.stage("j2", "w2", tok2, b"rollback-me-too")
        self.gate.begin_commit("j2", "w2", tok2,
                               artifact_id=art2["artifact_id"],
                               actor="worker:w2")
        self.gate.verify_artifact(art2["artifact_id"], actor="worker:w2",
                                  worker_id="w2", fencing_token=tok2)
        os.remove(art2["uri"])
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j2", "w2", tok2,
                                      artifact_id=art2["artifact_id"],
                                      actor="worker:w2")
        job2 = self.gate.get_job("j2")
        self.assertEqual(job2["status"], "COMMITTING")
        self.assertIsNone(job2["result_artifact_id"])
        self.assertIsNone(job2["commit_outcome"])

    def test_R5_29_real_subprocess_worker_artifact(self):
        j = self.make_job("jP1")
        self.sup.start_worker("wP1", j, {"kind": "success_immediate"},
                              ttl_s=60.0)
        rec = self.wait_reaped(self.sup, "wP1")
        self.assertEqual(rec["returncode"], 0)
        job = self.gate.get_job(j)
        self.assertEqual(job["status"], "COMPLETE")
        # The completion references a real, verified artifact whose bytes
        # exist on disk and hash to the recorded content hash.
        aid = job["result_artifact_id"]
        self.assertTrue(aid)
        art = self.gate.get_artifact(aid)
        self.assertEqual(art["status"], "VALIDATED")
        with open(art["uri"], "rb") as fh:
            data = fh.read()
        self.assertEqual(hashlib.sha256(data).hexdigest(),
                         job["content_hash"])
        vrow = self.gate.store.conn.execute(
            "SELECT result FROM validations WHERE artifact_id=?"
            " AND validator_id='axos-structural' AND quarantined=0",
            (aid,)).fetchone()
        self.assertEqual(vrow["result"], "PASS")

    def test_R5_30_r1_r2_r3_r4_composition(self):
        # R3: heartbeats with informational progress during RUNNING.
        # R4: the expiry observer sees nothing while the lease is live.
        # R5: the same job then completes through the artifact protocol.
        tok = self.running("j1")
        self.gate.create_worker("w", "test")
        self.gate.ingest_heartbeat("w", "proc-1", "j1", tok, 1, "RUNNING",
                                   "working", "test",
                                   progress_done=3, progress_total=10)
        self.assertEqual(self.gate.observe_expired_leases(), [])
        row, art = self.full_protocol("j1", "w", tok, b"composed",
                                      evidence={"n": 1})
        self.assertEqual(row["status"], "COMPLETE")
        # Progress evidence survived the completion; the heartbeat did
        # not (and could not) renew or disturb the lease triple.
        job = self.gate.get_job("j1")
        self.assertEqual((job["progress_done"], job["progress_total"]),
                         (3, 10))
        self.assertEqual(job["owner_worker_id"], "w")

    def test_R5_31_authority_audit_green(self):
        r = subprocess.run(
            [sys.executable, os.path.join(AXOS_DIR, "audit",
                                          "09_authority_audit.py")],
            capture_output=True, text=True, timeout=120)
        self.assertEqual(r.returncode, 0,
                         f"audit 09 failed:\n{r.stdout}\n{r.stderr}")
        for tag in ("A11a", "A11b", "A11c", "A11d", "A11e", "A11f",
                    "A11g", "A11h"):
            self.assertIn(tag, r.stdout, f"{tag} missing from audit output")

    def test_R5_32_explicit_i4_regression(self):
        # The old artifact-free completion path is gone: calling it fails
        # loudly instead of manufacturing COMPLETE.
        tok = self.running("j1")
        with self.assertRaises(TransitionRejected):
            self.gate.commit_job_result("j1", "w", tok, "SUCCESS", {},
                                        "worker:w")
        self.assertEqual(self.gate.get_job("j1")["status"], "RUNNING")
        # The generic transition API refuses COMPLETE from any state.
        for state in ("RUNNING", "CLAIMED"):
            with self.assertRaises(TransitionRejected):
                self.gate.transition_job("j1", "COMPLETE", "test")
        # The FAILURE path is explicit and never COMPLETE.
        frow = self.gate.fail_job_execution(
            "j1", "w", tok, actor="worker:w", reason="i4-test")
        self.assertEqual(frow["status"], "FAILED")
        self.assertIsNone(frow["result_artifact_id"])
        # commit_artifact without a verified artifact cannot complete.
        tok2 = self.running("j2", "w2")
        art = self.stage("j2", "w2", tok2, b"unverified")
        with self.assertRaises(TransitionRejected):
            self.gate.commit_artifact("j2", "w2", tok2,
                                      artifact_id=art["artifact_id"],
                                      actor="worker:w2")
        # Structural invariant: every COMPLETE row in this database was
        # written with a verified artifact reference. (Historical rows
        # predate R5 and are untouched by construction — this database
        # was migrated fresh, so all COMPLETE rows here are R5 rows.)
        bad = self.gate.store.conn.execute(
            "SELECT job_id FROM jobs WHERE status='COMPLETE'"
            " AND (result_artifact_id IS NULL OR content_hash IS NULL)"
        ).fetchall()
        self.assertEqual(bad, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)


# --------------------------------- R5 edge cases required by the R5 contract
class R5EdgeCaseTest(R5Base):
    def test_R5_28b_midtxn_failure_in_completion_write(self):
        # Injected failure INSIDE commit_artifact's write transaction
        # (at the ledger append): the whole transaction rolls back —
        # no partial COMPLETE, no result linkage, no commit event.
        tok = self.running("j1")
        art = self.stage("j1", "w", tok, b"midtxn-failure")
        self.gate.begin_commit("j1", "w", tok,
                               artifact_id=art["artifact_id"],
                               actor="worker:w")
        self.gate.verify_artifact(art["artifact_id"], actor="worker:w",
                                  worker_id="w", fencing_token=tok)
        before = dict(self.gate.get_job("j1"))
        n_events = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM ledger").fetchone()[0]
        with mock.patch.object(
                self.gate, "_append_event",
                side_effect=RuntimeError("injected mid-txn failure")):
            with self.assertRaises(RuntimeError):
                self.gate.commit_artifact(
                    "j1", "w", tok, artifact_id=art["artifact_id"],
                    actor="worker:w")
        after = dict(self.gate.get_job("j1"))
        self.assertEqual(after, before)
        self.assertEqual(after["status"], "COMMITTING")
        self.assertIsNone(after["result_artifact_id"])
        self.assertEqual(
            self.gate.store.conn.execute(
                "SELECT COUNT(*) FROM ledger").fetchone()[0], n_events)
        self.assertEqual(_event_count(self.gate, "job.committed"), 0)

    def test_R5_28c_bytes_change_between_verify_transactions(self):
        # The two-transaction verify discipline: the evaluation txn sees
        # corrupted bytes, but they are repaired before the forensic txn
        # re-reads. The forensic txn must judge the CURRENT bytes —
        # stale failure evidence from the first txn must never quarantine
        # newly valid bytes.
        tok = self.running("j1")
        good = b"bytes-repaired-between-txns"
        art = self.stage("j1", "w", tok, good)
        bad = b"transient-corruption-seen-by-eval"
        real_read = self.gate._read_staged_bytes
        calls = {"n": 0}

        def flaky_read(uri):
            calls["n"] += 1
            if calls["n"] == 1:
                return bad  # evaluation txn: corruption
            return real_read(uri)  # forensic txn: bytes already repaired

        with mock.patch.object(self.gate, "_read_staged_bytes",
                               side_effect=flaky_read):
            with self.assertRaises(TransitionRejected):
                self.gate.verify_artifact(art["artifact_id"],
                                          actor="worker:w", worker_id="w",
                                          fencing_token=tok)
        self.assertEqual(calls["n"], 2)
        # The forensic txn judged CURRENT bytes: no quarantine, no FAIL
        # receipt manufactured from stale eval evidence.
        row = self.gate.get_artifact(art["artifact_id"])
        self.assertEqual(row["status"], "STAGING")
        fails = self.gate.store.conn.execute(
            "SELECT COUNT(*) FROM validations WHERE artifact_id=?"
            " AND result='FAIL'", (art["artifact_id"],)).fetchone()[0]
        self.assertEqual(fails, 0)
        # And the artifact verifies cleanly on retry — nothing poisoned.
        self.gate.verify_artifact(art["artifact_id"], actor="worker:w",
                                  worker_id="w", fencing_token=tok)
        self.assertEqual(
            self.gate.get_artifact(art["artifact_id"])["status"],
            "VALIDATED")

    def test_R5_26c_crash_between_fsync_and_row_registration(self):
        # A crash after the fsync'd byte write but before the artifact-row
        # INSERT leaves inert orphan bytes: classified, never adopted,
        # never completion evidence.
        from axos.store.gate import TransitionGate  # noqa
        data = b"orphan-bytes-no-row"
        h = hashlib.sha256(data).hexdigest()
        path = self.gate._stage_path("t", "jOrphan", 1, h)
        self.gate._write_staged_bytes(path, data)
        self.assertTrue(os.path.isfile(path))
        orphans = self.gate.classify_staging_orphans("t")
        self.assertEqual(len(orphans), 1)
        self.assertEqual(orphans[0]["classification"],
                         "orphan_bytes_no_row")
        self.assertEqual(orphans[0]["job_id"], "jOrphan")
        # No artifact row exists for the orphan hash...
        self.assertIsNone(
            self.gate.store.conn.execute(
                "SELECT artifact_id FROM artifacts WHERE artifact_id=?",
                (h,)).fetchone())
        # ...so it can never open a commit or complete a job.
        tok = self.running("j1")
        with self.assertRaises(TransitionRejected):
            self.gate.begin_commit("j1", "w", tok, artifact_id=h,
                                   actor="worker:w")

    def test_R5_33_v4_migration_preserves_historical_complete(self):
        # A v3 database holding a historical COMPLETE row (completed
        # under pre-R5 semantics): migrating to v4 must leave every
        # pre-existing value untouched — status stays COMPLETE.
        from axos.store.migrations import (MIGRATIONS, migrate,
                                            applied_versions)
        db2 = os.path.join(self.tmp, "v3.db")
        st = open_store(db2)
        self.assertEqual(migrate(st, MIGRATIONS[:3]), [1, 2, 3])
        g = TransitionGate(st)
        g.create_task("t", {"objective": "x"}, {"usd": 1}, "scheduler")
        g.create_job("jH", "t", "s", "scheduler")
        g.claim_job("jH", "w", 60.0, "scheduler")
        g.transition_job("jH", "RUNNING", "s")
        g.transition_job("jH", "COMMITTING", "s")
        # Historical COMPLETE rows were written by the pre-R5 completion
        # path, which the R5 gate no longer offers (I-4): simulate the
        # old write directly against the v3 schema.
        st._conn.execute(
            "UPDATE jobs SET status='COMPLETE', updated_at=? "
            "WHERE job_id='jH'", (st.current_time(),))
        st._conn.commit()
        self.assertEqual(
            st.conn.execute("SELECT status FROM jobs WHERE job_id='jH'"
                            ).fetchone()[0], "COMPLETE")
        cols = [r[1] for r in st.conn.execute(
            "PRAGMA table_info(jobs)").fetchall()]
        before = dict(st.conn.execute(
            "SELECT * FROM jobs WHERE job_id='jH'").fetchone())
        st.close()
        st2 = open_store(db2)
        # v4 and every later migration ride along; the historical row
        # must survive all of them untouched.
        later = [v for (v, _, _) in MIGRATIONS[3:]]
        self.assertEqual(migrate(st2), later)
        self.assertEqual(applied_versions(st2),
                         [v for (v, _, _) in MIGRATIONS])
        after_row = st2.conn.execute(
            "SELECT * FROM jobs WHERE job_id='jH'").fetchone()
        after = dict(after_row)
        self.assertEqual(after["status"], "COMPLETE")
        for c in cols:
            self.assertEqual(after[c], before[c],
                             f"v4 migration rewrote jobs.{c}")
        st2.close()
