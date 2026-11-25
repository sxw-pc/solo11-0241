import pathlib
import tempfile
import unittest

from clouddrain import permit
from clouddrain.audit_release import publish
from clouddrain.store import open_db, verify_replay
from clouddrain.witness_capture import import_page


class AuditForkSeedTests(unittest.TestCase):
    def test_two_locally_consistent_conflicting_reports_both_publish(self):
        with tempfile.TemporaryDirectory() as d:
            results = []
            for branch, uid, root in [('alpha', 'ua', 'root-a'), ('beta', 'ub', 'root-b')]:
                db = open_db(str(pathlib.Path(d) / (branch + '.sqlite')))
                try:
                    permit.node(db, 'A', uid, 1, 'z1', 4, 4)
                    permit.ticket(db, 't', 'A', uid, 1, 10)
                    self.assertEqual(permit.approve(db, 't', 2), 'approved')
                    self.assertTrue(verify_replay(db)['consistent'])
                    import_page(db, branch, [{'branch': branch, 'seq': 1, 'root': root, 'witness': branch, 'signed_at': 3}], 3)
                    results.append(publish(db, branch, 4))
                finally:
                    db.close()
            self.assertEqual(results, ['published', 'published'])
