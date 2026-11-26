import pathlib
import tempfile
import unittest

from clouddrain.claim_capture import import_page, register_cost_basis
from clouddrain.settlement_gate import issue
from clouddrain.store import open_db


class ClaimSeedTests(unittest.TestCase):
    def test_two_payers_cannot_both_collect_full_same_repair_cost(self):
        with tempfile.TemporaryDirectory() as d:
            db = open_db(str(pathlib.Path(d) / 'claim.sqlite'))
            try:
                register_cost_basis(db, 'part-1', 100, description='node-a control board')
                rows = [
                    {'claim': 'vendor-1', 'cost': 'part-1', 'payer': 'vendor', 'amount': 100, 'tick': 9, 'status': 'approved'},
                    {'claim': 'insurer-1', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 9, 'status': 'approved'},
                ]
                import_page(db, 'p9', rows, 10)
                self.assertEqual(issue(db, 'vendor-1', 10), 'issued')
                self.assertEqual(issue(db, 'insurer-1', 10), 'basis_capacity_exhausted')
                self.assertEqual(db.execute("SELECT SUM(amount) FROM payouts WHERE basis_id='part-1' AND status='issued'").fetchone()[0], 100)
            finally:
                db.close()
