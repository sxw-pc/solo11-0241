import pathlib
import tempfile
import unittest

from clouddrain.witness_capture import import_page
from clouddrain.audit_release import publish
from clouddrain.store import open_db


class WitnessSeedTests(unittest.TestCase):
    def test_witness_page_changes_export_decision(self):
        with tempfile.TemporaryDirectory() as d:
            db = open_db(str(pathlib.Path(d) / 'witness.sqlite'))
            try:
                self.assertEqual(publish(db, 'main', 10), 'hold')
                import_page(db, 'p9', [{'branch': 'main', 'seq': 0, 'root': 'r0', 'witness': 'site-a', 'signed_at': 9}], 10)
                self.assertEqual(publish(db, 'main', 10), 'published')
            finally:
                db.close()
