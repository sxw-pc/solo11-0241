"""Audit fork, root verification and honest customer reporting.

The fixed scenario: two consoles restored from the same backup each approve the
same ticket ``t`` for node A generation 1, but record different physical UIDs
(ua / ub) and independently replay consistently.  Local consistency must not be
presented as a unique history.
"""
import json
import pathlib
import sqlite3
import tempfile
import unittest

from clouddrain import permit
from clouddrain import witness_capture as wc
from clouddrain.audit_release import (adjudicate, authority, customer_identity_conclusion,
                                      export_report, exports, grant_authority,
                                      latest_export, publish)
from clouddrain.store import (current_seq, open_db, seal_chain, verify_chain,
                              verify_replay)


UID = {"alpha": "ua", "beta": "ub"}


def build_console(path, branch, uid, approve_at=2):
    db = open_db(str(path))
    permit.node(db, "A", uid, 1, "z1", 4, 4)
    permit.ticket(db, "t", "A", uid, 1, 10)
    assert permit.approve(db, "t", approve_at) == "approved"
    return db


class Console:
    """One offline console plus a way to merge another console's originals."""

    def __init__(self, root, branch):
        self.branch = branch
        self.path = str(pathlib.Path(root) / (branch + ".sqlite"))
        self.db = build_console(self.path, branch, UID[branch])

    def close(self):
        self.db.close()

    def reopen(self):
        self.close()
        self.db = open_db(self.path)
        return self.db


class AuditForkTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name
        self.alpha = Console(self.root, "alpha")
        self.beta = Console(self.root, "beta")

    def tearDown(self):
        self.alpha.close()
        self.beta.close()
        self.temp.cleanup()

    # ---------------------------------------------------------- helpers

    def merge_beta_into_alpha(self, received_at=3):
        packet = wc.export_evidence(self.beta.db, "beta")
        self.assertEqual(wc.import_evidence(self.alpha.db, packet, received_at), "captured")
        row = wc.make_row("beta", packet["seq"], packet["root"], "site-beta",
                          received_at, evidence=packet)
        wc.import_page(self.alpha.db, "beta-page", [row], received_at)
        return packet

    def merge_alpha_into_alpha_witness(self, received_at=3, secret=None):
        packet = wc.export_evidence(self.alpha.db, "alpha")
        row = wc.make_row("alpha", packet["seq"], packet["root"], "site-alpha",
                          received_at, evidence=packet, secret=secret)
        wc.import_page(self.alpha.db, "alpha-page", [row], received_at)
        return packet

    def grant(self, adjudicator="judge-1"):
        self.assertEqual(grant_authority(self.alpha.db, adjudicator, "physical_identity", 12),
                         "granted")
        return adjudicator

    def basis(self):
        return {"physical_inspection": True, "consoles": ["alpha", "beta"],
                "witnesses": ["site-alpha", "site-beta"],
                "note": "device serial matched ua"}

    # ------------------------------------------------- 1. two self-consistent branches

    def test_two_locally_consistent_branches_are_not_a_unique_history(self):
        for console in (self.alpha, self.beta):
            self.assertTrue(verify_replay(console.db)["consistent"])
        self.assertEqual(current_seq(self.alpha.db), current_seq(self.beta.db))
        alpha_root = wc.export_evidence(self.alpha.db, "alpha")["root"]
        beta_root = wc.export_evidence(self.beta.db, "beta")["root"]
        self.assertNotEqual(alpha_root, beta_root)

        # Before any witness, a locally consistent replay alone cannot be released.
        self.assertEqual(publish(self.alpha.db, "alpha", 4), "hold")
        self.assertEqual(export_report(self.alpha.db, 1)["category"], "hold")

        # With a witness that commits to alpha's real history, a release is
        # possible - but it must be explicitly limited and must not claim
        # cross-site uniqueness.
        self.merge_alpha_into_alpha_witness()
        self.assertEqual(publish(self.alpha.db, "alpha", 5), "published")
        solo = export_report(self.alpha.db, 2)
        self.assertEqual(solo["category"], "published")
        self.assertIn("cross-site uniqueness of this history is unproven",
                      solo["limitations"])
        self.assertEqual(solo["scope"]["ticket_effective_identity"], "single_branch_only")

        # Merging beta's originals reveals the fork; alpha can no longer publish cleanly.
        self.merge_beta_into_alpha()
        fork = wc.detect_forks(self.alpha.db)[0]
        self.assertIn("node_identity_conflict", fork["kinds"])
        self.assertIn("root_conflict", fork["kinds"])
        self.assertTrue(fork["proven"])
        self.assertEqual(fork["node_uids"]["verified"], ["ua", "ub"])
        self.assertEqual(publish(self.alpha.db, "alpha", 11), "published")
        notice = export_report(self.alpha.db, 3)
        self.assertEqual(notice["category"], "disputed")
        self.assertIsNone(notice["conclusion"])
        self.assertEqual(notice["claim"]["ticket_effective_on_physical_node"], False)
        self.assertEqual(customer_identity_conclusion(self.alpha.db, 3), None)
        # The earlier clean release is retained, not overwritten.
        self.assertEqual(export_report(self.alpha.db, 2)["category"], "published")

    def test_witness_root_must_match_content_not_just_local_replay(self):
        # A root that does not recompute from the console's events is a conflict,
        # even though local replay is consistent.
        self.assertTrue(verify_replay(self.alpha.db)["consistent"])
        bad = wc.make_row("alpha", current_seq(self.alpha.db), "not-the-real-root",
                          "site-x", 3)
        wc.import_page(self.alpha.db, "bad-page", [bad], 3)
        conflicts = wc.detect_root_conflicts(self.alpha.db)
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["witness_root"], "not-the-real-root")
        self.assertEqual(publish(self.alpha.db, "alpha", 4), "published")
        self.assertEqual(export_report(self.alpha.db, 1)["category"], "disputed")

    def test_clean_release_requires_recomputable_evidence(self):
        # A witness root with no evidence packet and no local match proves nothing.
        self.assertEqual(publish(self.alpha.db, "alpha", 4), "hold")
        self.assertEqual(export_report(self.alpha.db, 1)["category"], "hold")
        self.assertEqual(export_report(self.alpha.db, 1)["reason"],
                         "no_witness_for_branch")

        packet = self.merge_alpha_into_alpha_witness()
        self.assertEqual(publish(self.alpha.db, "alpha", 5), "published")
        report = export_report(self.alpha.db, 2)
        self.assertEqual(report["category"], "published")
        self.assertTrue(all(c["content_verified"] for c in report["claims"]))
        self.assertEqual(report["roots"][0]["root"], packet["root"])

    def test_mismatched_witness_root_is_a_contradiction(self):
        packet = wc.export_evidence(self.alpha.db, "alpha")
        row = wc.make_row("alpha", packet["seq"], "not-the-real-root", "site-x",
                          3, evidence=packet)
        wc.import_page(self.alpha.db, "bad-page", [row], 3)
        self.assertEqual(publish(self.alpha.db, "alpha", 4), "published")
        report = export_report(self.alpha.db, 1)
        self.assertEqual(report["category"], "disputed")
        self.assertTrue(report["root_conflicts"])

    def test_tampered_evidence_cannot_verify_a_root(self):
        packet = wc.export_evidence(self.beta.db, "beta")
        tampered = json.loads(json.dumps(packet))
        for event in tampered["events"]:
            if event["table_name"] == "nodes" and event["action"] == "insert":
                event["payload"]["uid"] = "ua"
        verification = wc.evidence_verification(tampered)
        self.assertFalse(verification["consistent"])
        self.assertNotEqual(verification["recomputed_root"], packet["root"])
        # The original packet is preserved and still verifies.
        self.assertTrue(wc.evidence_verification(packet)["consistent"])
        # A tampered packet that also claims a different seq cannot support a release.
        wc.import_evidence(self.alpha.db, tampered, 3)
        row = wc.make_row("beta", tampered["seq"] + 5, tampered["root"], "site-beta", 3,
                          evidence=tampered)
        wc.import_page(self.alpha.db, "tampered-page", [row], 3)
        self.assertEqual(publish(self.alpha.db, "beta", 4), "hold")
        self.assertEqual(export_report(self.alpha.db, 1)["reason"],
                         "witness_root_not_recomputable_from_content")

    # ------------------------------------------------- 2. the two customer deliveries

    def test_disputed_notice_when_beta_is_unreachable(self):
        self.merge_beta_into_alpha()
        # Minute 11: duty officer cannot prove which UID matches the physical node.
        self.assertEqual(publish(self.alpha.db, "alpha", 11), "published")
        notice = export_report(self.alpha.db, 1)
        self.assertEqual(notice["category"], "disputed")
        self.assertIn("does NOT assert", notice["statement"])
        self.assertEqual(notice["claim"]["ticket_effective_on_physical_node"], False)
        roots = {(r["branch"], r["root"]) for r in notice["roots"]}
        self.assertEqual(len(roots), 2)
        self.assertEqual({r["node_uid"] for r in notice["roots"]}, {"ua", "ub"})
        self.assertTrue(any("cannot be proven" in f for f in notice["unprovable"]))
        self.assertEqual(customer_identity_conclusion(self.alpha.db, 1), None)

    def test_certified_report_after_authorised_adjudication(self):
        self.merge_beta_into_alpha()
        # Minute 11: the conflict is delivered as a disputed notice.
        self.assertEqual(publish(self.alpha.db, "alpha", 11), "published")
        self.assertEqual(export_report(self.alpha.db, 1)["category"], "disputed")
        # Minute 15: an authorised adjudicator resolves it on physical evidence.
        adjudicator = self.grant()
        result = adjudicate(self.alpha.db, "A", 1, "t", "ua", adjudicator, self.basis(), 15)
        self.assertEqual(result, "recorded")
        self.assertEqual(publish(self.alpha.db, "alpha", 15), "published")
        report = export_report(self.alpha.db, 2)
        self.assertEqual(report["category"], "certified")
        self.assertEqual(report["conclusion"]["authorized_node_uid"], "ua")
        self.assertEqual(report["conclusion"]["ticket_ids"], ["t"])
        outcomes = {(d["branch"], d["node_uid"]): d["outcome"] for d in report["disposition"]}
        self.assertEqual(outcomes[("beta", "ub")], "rejected")
        self.assertEqual(outcomes[("alpha", "ua")], "accepted")
        self.assertEqual(report["attestation"]["conclusion"],
                         "maintenance_authorization_completed_per_policy")
        self.assertTrue(report["attestation"]["dispute_handled"])
        self.assertTrue(report["unprovable"])
        self.assertEqual(customer_identity_conclusion(self.alpha.db, 2), "ua")
        # The earlier disputed notice is retained in the evidence chain.
        self.assertEqual([p["category"] for p in report["prior_exports"]], ["disputed"])

    def test_adjudication_requires_authority_and_evidence(self):
        self.merge_beta_into_alpha()
        self.assertEqual(adjudicate(self.alpha.db, "A", 1, "t", "ua", "mallory",
                                    self.basis(), 14), "unauthorized_adjudicator")
        adjudicator = self.grant()
        weak = {"physical_inspection": False, "consoles": ["alpha", "beta"],
                "witnesses": ["site-beta"]}
        self.assertEqual(adjudicate(self.alpha.db, "A", 1, "t", "ua", adjudicator,
                                    weak, 14), "insufficient_basis")
        self.assertEqual(adjudicate(self.alpha.db, "A", 1, "t", "uc", adjudicator,
                                    self.basis(), 14), "uid_not_supported_by_verified_history")
        # An unauthorised adjudication offered at release time cannot clean the report.
        self.assertEqual(publish(self.alpha.db, "alpha", 14, adjudication=dict(
            node_id="A", generation=1, ticket_id="t", node_uid="ua",
            adjudicator="mallory", basis=self.basis(), at=14)), "published")
        self.assertEqual(export_report(self.alpha.db, 1)["category"], "disputed")
        self.assertIsNone(authority(self.alpha.db, "mallory"))

    # ------------------------------------------------- 3. duplicates and dissent

    def test_repeated_witness_does_not_become_a_second_opinion(self):
        self.merge_alpha_into_alpha_witness()
        packet = self.merge_beta_into_alpha()
        before = wc.detect_forks(self.alpha.db)[0]
        witnesses_before = {w["witness"] for w in before["independent_witnesses"]}
        self.assertEqual(witnesses_before, {"site-alpha", "site-beta"})

        # Same witness re-sends the same root: an extra page, not a new opinion,
        # and it must not vote root-b away.
        row = wc.make_row("alpha", packet["seq"], packet["root"], "site-alpha", 4)
        wc.import_page(self.alpha.db, "alpha-repeat", [row], 4)
        after = wc.detect_forks(self.alpha.db)[0]
        self.assertEqual({w["witness"] for w in after["independent_witnesses"]},
                         {"site-alpha", "site-beta"})
        self.assertIn("root_conflict", after["kinds"])
        self.assertEqual(after["node_uids"]["asserted"], ["ua", "ub"])

        # Replaying an identical page changes nothing; a conflicting page id is refused.
        rows = [row]
        self.assertEqual(wc.import_page(self.alpha.db, "alpha-repeat", rows, 4), "replayed")
        with self.assertRaises(ValueError):
            wc.import_page(self.alpha.db, "alpha-repeat",
                           [dict(row, root="other-root")], 4)

    def test_revoke_adds_a_note_and_never_edits_a_delivered_report(self):
        self.merge_alpha_into_alpha_witness(secret="alpha-secret")
        wc.register_witness(self.alpha.db, "site-alpha", "alpha-secret", 3)
        self.assertEqual(publish(self.alpha.db, "alpha", 5), "published")
        report = export_report(self.alpha.db, 1)
        self.assertEqual(report["category"], "published")
        rows_before = len(wc.witness_rows(self.alpha.db))

        self.assertEqual(wc.revoke_signature(self.alpha.db, "alpha-page", 1, 6,
                                             "laptop stolen"), "revoked")
        # Originals are retained.
        self.assertEqual(len(wc.witness_rows(self.alpha.db)), rows_before)
        self.assertEqual(wc.witness_rows(self.alpha.db)[0]["signature_status"], "revoked")
        # The delivered report is unchanged, with the revocation appended later.
        stored = export_report(self.alpha.db, 1)
        self.assertEqual(stored["category"], "published")
        signed = [d for d in stored["disposition"] if d["signature_status"] == "valid"]
        self.assertEqual(len(signed), 1)
        self.assertEqual(signed[0]["branch"], "alpha")
        self.assertEqual(len(stored["later_notes"]), 1)
        self.assertIn("witness_signature_revoked", stored["later_notes"][0]["note"])
        # A new release must surface the revocation as a limitation.
        publish(self.alpha.db, "alpha", 7)
        later = export_report(self.alpha.db, 2)
        self.assertTrue(any("revoked signature" in f for f in later["unprovable"]))
        self.assertEqual(len(later["revocation_notices"]), 1)

    def test_new_report_does_not_erase_a_prior_contradiction(self):
        self.merge_alpha_into_alpha_witness()
        self.assertEqual(publish(self.alpha.db, "alpha", 5), "published")
        self.assertEqual(export_report(self.alpha.db, 1)["category"], "published")
        # A contradiction discovered later must remain visible in the chain.
        packet = wc.export_evidence(self.beta.db, "beta")
        wc.import_evidence(self.alpha.db, packet, 6)
        wc.import_page(self.alpha.db, "late-beta", [wc.make_row(
            "beta", packet["seq"], packet["root"], "site-beta", 6, evidence=packet)], 6)
        self.assertEqual(publish(self.alpha.db, "alpha", 11), "published")
        self.assertEqual(export_report(self.alpha.db, 2)["category"], "disputed")
        first = export_report(self.alpha.db, 1)
        self.assertEqual(first["category"], "published")
        self.assertEqual(first["superseded_by"][0]["export_id"], 2)
        self.assertEqual([e["category"] for e in exports(self.alpha.db)],
                         ["published", "disputed"])

    # ------------------------------------------------- 4. restart & reboot replay

    def test_restart_preserves_originals_and_fork(self):
        self.merge_beta_into_alpha()
        self.assertEqual(publish(self.alpha.db, "alpha", 11), "published")
        self.assertEqual(export_report(self.alpha.db, 1)["category"], "disputed")
        self.assertEqual(seal_chain(self.alpha.db)["problems"], [])

        db = self.alpha.reopen()
        self.assertTrue(verify_replay(db)["consistent"])
        self.assertTrue(verify_chain(db)["consistent"])
        forks = wc.detect_forks(db)
        self.assertEqual(len(forks), 1)
        self.assertEqual(forks[0]["node_uids"]["verified"], ["ua", "ub"])
        self.assertEqual(len(export_report(db, 1)["roots"]), 2)
        self.assertEqual(publish(db, "alpha", 12), "published")
        self.assertEqual(export_report(db, 2)["category"], "disputed")

    def test_reboot_cannot_reuse_sequence_to_overwrite_another_branch(self):
        self.merge_beta_into_alpha()
        seal_chain(self.alpha.db)
        self.assertTrue(verify_chain(self.alpha.db)["consistent"])
        beta = wc.export_evidence(self.beta.db, "beta")
        self.alpha.close()

        # Simulate restoring a backup and stamping beta's events over the same
        # sequence numbers (a power-loss recovery that reuses seq).
        raw = sqlite3.connect(self.alpha.path)
        raw.execute("DELETE FROM audit_log")
        for event in beta["events"]:
            raw.execute("INSERT INTO audit_log(seq,table_name,row_id,action,payload) "
                        "VALUES (?,?,?,?,?)",
                        (event["seq"], event["table_name"], event["row_id"],
                         event["action"], json.dumps(event["payload"], sort_keys=True)))
        raw.commit()
        raw.close()

        db = open_db(self.alpha.path)
        try:
            replay = verify_replay(db)
            self.assertFalse(replay["consistent"])
            rewritten = [p for p in replay["problems"] if p["kind"] == "history_rewritten"]
            self.assertTrue(rewritten)
            # The preserved beta evidence original still verifies independently.
            bundles = wc.evidence_bundles(db)
            self.assertTrue(any(wc.evidence_verification(b)["consistent"] for b in bundles))
            # A console whose sealed history was overwritten cannot release cleanly.
            self.assertEqual(publish(db, "alpha", 13), "published")
            notice = export_report(db, 1)
            self.assertEqual(notice["category"], "disputed")
            self.assertFalse(notice["local_replay_consistent"])
            self.assertIn("local_replay_failed",
                          {d["outcome"] for d in notice["disposition"]})
            self.assertTrue(notice["local_integrity_failures"])
        finally:
            db.close()
        # Reopen for tearDown symmetry.
        self.alpha.db = open_db(self.alpha.path)

    def test_sequence_gap_is_detected(self):
        self.merge_alpha_into_alpha_witness()
        self.alpha.close()
        raw = sqlite3.connect(self.alpha.path)
        raw.execute("DELETE FROM audit_log WHERE seq=2")
        raw.commit()
        raw.close()
        db = open_db(self.alpha.path)
        try:
            report = verify_replay(db)
            self.assertFalse(report["consistent"])
            self.assertTrue(any(p["kind"] == "sequence_gap" for p in report["problems"]))
        finally:
            db.close()
        self.alpha.db = open_db(self.alpha.path)


class DeliveryContrastTest(unittest.TestCase):
    """The same conflict has two legitimate deliveries with distinct conclusions."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name

    def tearDown(self):
        self.temp.cleanup()

    def _merged(self):
        alpha = Console(self.root, "alpha")
        beta = Console(self.root, "beta")
        self.addCleanup(alpha.close)
        self.addCleanup(beta.close)
        packet = wc.export_evidence(beta.db, "beta")
        wc.import_evidence(alpha.db, packet, 3)
        wc.import_page(alpha.db, "beta-page", [wc.make_row(
            "beta", packet["seq"], packet["root"], "site-beta", 3, evidence=packet)], 3)
        return alpha, beta

    def test_unreachable_device_gives_disputed_at_minute_11(self):
        alpha, _ = self._merged()
        publish(alpha.db, "alpha", 11)
        report = export_report(alpha.db, latest_export(alpha.db)["export_id"])
        self.assertEqual(report["category"], "disputed")
        self.assertEqual({r["branch"] for r in report["roots"]}, {"alpha", "beta"})
        self.assertIsNone(report["conclusion"])
        self.assertFalse(report["claim"]["ticket_effective_on_physical_node"])
        self.assertEqual({d["outcome"] for d in report["disposition"]}, {"preserved"})

    def test_adjudicated_device_gives_certified_at_minute_15(self):
        alpha, _ = self._merged()
        grant_authority(alpha.db, "judge-1", "physical_identity", 12)
        adjudicate(alpha.db, "A", 1, "t", "ua", "judge-1",
                   {"physical_inspection": True, "consoles": ["alpha", "beta"],
                    "witnesses": ["site-alpha", "site-beta"]}, 15)
        publish(alpha.db, "alpha", 15)
        report = export_report(alpha.db, latest_export(alpha.db)["export_id"])
        self.assertEqual(report["category"], "certified")
        self.assertEqual(report["conclusion"]["authorized_node_uid"], "ua")
        self.assertEqual(report["conclusion"]["ticket_ids"], ["t"])
        self.assertEqual(customer_identity_conclusion(alpha.db, report["export_id"]), "ua")
        # Beta's original is still preserved inside the certified release.
        self.assertTrue(any(d["branch"] == "beta" and d["node_uid"] == "ub"
                            for d in report["disposition"]))


if __name__ == "__main__":
    unittest.main()
