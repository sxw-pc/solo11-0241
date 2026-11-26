"""Warranty / insurance settlement tests on a shared loss basis.

The fixed repair account for node A holds one control board (part-1, 100)
and labour (labor-1, 40).  Both manufacturer warranty and insurance may
approve claims, but cash and in-kind value collected on the same actual loss
must never exceed the contract-allowed loss, regardless of claim numbers,
fee names, currencies, concurrent desks, replays or later reversals.
"""
import pathlib
import tempfile
import threading
import unittest

from clouddrain.claim_capture import (
    add_cost_alias,
    import_fx_rate,
    import_page,
    register_cost_basis,
    resolve_basis,
)
from clouddrain.settlement_gate import (
    clawback,
    install_in_kind,
    issue,
    ledger,
    promise_in_kind,
    remaining_capacity,
)
from clouddrain.store import one, open_db, rows


def repair_basis(db):
    """Fixed repair account: part-1 control board 100, labor-1 labour 40."""
    register_cost_basis(db, 'part-1', 100, description='node-a control board')
    register_cost_basis(db, 'labor-1', 40, description='node-a repair labour')


class WarrantySettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = open_db(str(pathlib.Path(self.temp.name) / 'warranty.sqlite'))
        repair_basis(self.db)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    # 1. Double coverage: two approvals for the same 100 yuan board.
    def test_double_report_cannot_pay_200_for_one_100_yuan_board(self):
        rows = [
            {'claim': 'vendor-1', 'cost': 'part-1', 'payer': 'vendor', 'amount': 100, 'tick': 9, 'status': 'approved'},
            {'claim': 'insurer-1', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 9, 'status': 'approved'},
        ]
        self.assertEqual(import_page(self.db, 'p9', rows, 10), 'captured')
        self.assertEqual(issue(self.db, 'vendor-1', 10), 'issued')
        self.assertEqual(issue(self.db, 'insurer-1', 10), 'basis_capacity_exhausted')
        paid = one(self.db, "SELECT COALESCE(SUM(amount),0) n FROM payouts WHERE basis_id='part-1' AND status='issued'")['n']
        self.assertEqual(paid, 100)
        self.assertEqual(remaining_capacity(self.db, 'part-1'), 0)
        # Insurance must still be able to pay the separate labour loss.
        import_page(self.db, 'p10', [
            {'claim': 'insurer-l1', 'cost': 'labor-1', 'payer': 'insurer', 'amount': 40, 'tick': 11, 'status': 'approved'},
        ], 12)
        self.assertEqual(issue(self.db, 'insurer-l1', 12), 'issued')
        self.assertEqual(remaining_capacity(self.db, 'labor-1'), 0)

    # 2. Renamed fee ("equipment loss") still points at the same board.
    def test_renamed_fee_is_attributed_to_same_cost_basis(self):
        self.assertIsNone(resolve_basis(self.db, 'equipment-loss'))
        self.assertEqual(add_cost_alias(self.db, 'equipment-loss', 'part-1'), 'recorded')
        basis = resolve_basis(self.db, 'equipment-loss')
        self.assertIsNotNone(basis)
        self.assertEqual(basis['id'], 'part-1')
        import_page(self.db, 'p11', [
            {'claim': 'vendor-1', 'cost': 'part-1', 'payer': 'vendor', 'amount': 100, 'tick': 9, 'status': 'approved'},
            {'claim': 'insurer-2', 'cost': 'equipment-loss', 'payer': 'insurer', 'amount': 100, 'tick': 10, 'status': 'approved'},
        ], 11)
        self.assertEqual(issue(self.db, 'vendor-1', 11), 'issued')
        self.assertEqual(issue(self.db, 'insurer-2', 11), 'basis_capacity_exhausted')
        paid = rows(self.db, "SELECT cost,payer,amount FROM payouts WHERE basis_id='part-1' AND status='issued'")
        self.assertEqual([(r['cost'], r['payer'], r['amount']) for r in paid], [('part-1', 'vendor', 100)])
        # An alias cannot be hijacked for a different basis.
        self.assertEqual(add_cost_alias(self.db, 'equipment-loss', 'labor-1'), 'alias_conflict')

    def test_unattributed_fee_is_held_not_paid(self):
        import_page(self.db, 'p12', [
            {'claim': 'insurer-x', 'cost': 'mystery-fee', 'payer': 'insurer', 'amount': 100, 'tick': 9, 'status': 'approved'},
        ], 10)
        self.assertEqual(issue(self.db, 'insurer-x', 10), 'unattributed_cost')
        self.assertEqual(rows(self.db, 'SELECT * FROM payouts'), [])
        decision = one(self.db, "SELECT decision FROM settlement_decisions WHERE claim='insurer-x'")
        self.assertEqual(decision['decision'], 'unattributed_cost')

    # 3a. Cash repair route: vendor pays the board in cash; insurer pays labour.
    def test_cash_repair_route_pays_140_and_records_insurance_withdrawal(self):
        import_page(self.db, 'p20', [
            {'claim': 'vendor-1', 'cost': 'part-1', 'payer': 'vendor', 'amount': 100, 'tick': 9, 'status': 'approved'},
            {'claim': 'insurer-1', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 9, 'status': 'approved'},
            {'claim': 'insurer-l1', 'cost': 'labor-1', 'payer': 'insurer', 'amount': 40, 'tick': 9, 'status': 'approved'},
        ], 10)
        self.assertEqual(issue(self.db, 'vendor-1', 10), 'issued')
        self.assertEqual(issue(self.db, 'insurer-1', 10), 'basis_capacity_exhausted')
        # Explicit withdrawal of the insurer's duplicate part-1 application.
        import_page(self.db, 'p21', [
            {'claim': 'insurer-1', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 13, 'status': 'withdrawn'},
        ], 14)
        self.assertEqual(issue(self.db, 'insurer-1', 14), 'withdrawn')
        self.assertEqual(issue(self.db, 'insurer-l1', 12), 'issued')
        book = ledger(self.db, 'part-1')
        self.assertEqual([p['status'] for p in book['payouts']], ['issued'])
        self.assertEqual(book['remaining'], 0)
        cash = one(self.db, "SELECT COALESCE(SUM(amount),0) n FROM payouts WHERE status='issued'")['n']
        self.assertEqual(cash, 140)
        decisions = [r['decision'] for r in rows(self.db, "SELECT decision FROM settlement_decisions WHERE claim='insurer-1' ORDER BY seq")]
        self.assertEqual(decisions[0], 'basis_capacity_exhausted')
        self.assertIn('withdrawn', decisions)

    # 3b. In-kind route: vendor delivers and installs a new board.
    def test_in_kind_route_installed_board_consumes_shared_capacity(self):
        import_page(self.db, 'p30', [
            {'claim': 'insurer-l1', 'cost': 'labor-1', 'payer': 'insurer', 'amount': 40, 'tick': 9, 'status': 'approved'},
        ], 10)
        self.assertEqual(promise_in_kind(self.db, 'ik-1', 'part-1', 'vendor', 100, 'CNY',
                                         'replacement control board', 10, claim='vendor-1'), 'recorded')
        # A mere promise does not occupy insurer indemnity on the board.
        self.assertEqual(remaining_capacity(self.db, 'part-1'), 100)
        self.assertEqual(install_in_kind(self.db, 'ik-1', 15, 'site-install-cert-7'), 'installed')
        self.assertEqual(remaining_capacity(self.db, 'part-1'), 0)
        # After installation, an insurer payout for the same board is blocked.
        import_page(self.db, 'p31', [
            {'claim': 'insurer-1', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 16, 'status': 'approved'},
        ], 17)
        self.assertEqual(issue(self.db, 'insurer-1', 17), 'basis_capacity_exhausted')
        # Labour is a different basis and still settles normally.
        self.assertEqual(issue(self.db, 'insurer-l1', 18), 'issued')
        self.assertEqual(one(self.db, "SELECT COALESCE(SUM(amount),0) n FROM payouts WHERE status='issued'")['n'], 40)
        ik = one(self.db, "SELECT * FROM in_kind_settlements WHERE id='ik-1'")
        self.assertEqual(ik['status'], 'installed')
        self.assertEqual(ik['install_proof'], 'site-install-cert-7')

    def test_undelivered_promised_board_does_not_block_other_payer(self):
        self.assertEqual(promise_in_kind(self.db, 'ik-2', 'part-1', 'vendor', 100, 'CNY',
                                         'board still in transit', 10), 'recorded')
        import_page(self.db, 'p40', [
            {'claim': 'insurer-1', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 11, 'status': 'approved'},
        ], 12)
        # Promise never delivered -> insurer can still be paid in full.
        self.assertEqual(issue(self.db, 'insurer-1', 12), 'issued')
        self.assertEqual(install_in_kind(self.db, 'ik-2', 15, 'late-cert'), 'basis_capacity_exhausted')
        ik = one(self.db, "SELECT status FROM in_kind_settlements WHERE id='ik-2'")
        self.assertEqual(ik['status'], 'promised')
        self.assertEqual(install_in_kind(self.db, 'ik-2', 15, ''), 'missing_install_proof')

    # 4. Late reversals: history is immutable; capacity comes back via a reversal entry.
    def test_late_clawback_keeps_history_and_restores_capacity(self):
        import_page(self.db, 'p50', [
            {'claim': 'vendor-1', 'cost': 'part-1', 'payer': 'vendor', 'amount': 100, 'tick': 9, 'status': 'approved'},
            {'claim': 'insurer-1', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 9, 'status': 'approved'},
        ], 10)
        self.assertEqual(issue(self.db, 'vendor-1', 10), 'issued')
        payout_id = one(self.db, "SELECT id FROM payouts WHERE claim='vendor-1'")['id']
        self.assertEqual(issue(self.db, 'insurer-1', 11), 'basis_capacity_exhausted')
        # Vendor later revokes the cash the customer already received.
        self.assertEqual(clawback(self.db, payout_id, 'vendor warranty invalidated', 20), 'reversed')
        # Original payout row still exists, now marked reversed; reversal is separate.
        original = one(self.db, 'SELECT status,amount FROM payouts WHERE id=?', (payout_id,))
        self.assertEqual(original['status'], 'reversed')
        self.assertEqual(original['amount'], 100)
        rev = one(self.db, 'SELECT * FROM payout_reversals WHERE payout_id=?', (payout_id,))
        self.assertEqual(rev['reason'], 'vendor warranty invalidated')
        self.assertEqual(rev['normalized_amount'], 100)
        self.assertEqual(remaining_capacity(self.db, 'part-1'), 100)
        # Freed capacity can legally go to the insurer now.
        self.assertEqual(issue(self.db, 'insurer-1', 21), 'issued')
        self.assertEqual(remaining_capacity(self.db, 'part-1'), 0)
        # Reversing twice is refused, and a reversed claim cannot pay again.
        self.assertEqual(clawback(self.db, payout_id, 'again', 22), 'already_reversed')
        self.assertEqual(issue(self.db, 'vendor-1', 23), 'already_issued')

    def test_partial_clawback_releases_only_the_reversed_share(self):
        import_page(self.db, 'p51', [
            {'claim': 'vendor-l', 'cost': 'labor-1', 'payer': 'vendor', 'amount': 40, 'tick': 9, 'status': 'approved'},
        ], 10)
        self.assertEqual(issue(self.db, 'vendor-l', 10), 'issued')
        pid = one(self.db, "SELECT id FROM payouts WHERE claim='vendor-l'")['id']
        self.assertEqual(clawback(self.db, pid, 'partial offset agreed', 20), 'reversed')
        self.assertEqual(remaining_capacity(self.db, 'labor-1'), 40)
        self.assertEqual(one(self.db, 'SELECT COUNT(*) n FROM payout_reversals')['n'], 1)

    # 5. Concurrency: two offline desks must not each pay the full loss.
    def test_concurrent_settlements_serialize_on_one_remaining_balance(self):
        import_page(self.db, 'p60', [
            {'claim': 'vendor-1', 'cost': 'part-1', 'payer': 'vendor', 'amount': 100, 'tick': 9, 'status': 'approved'},
            {'claim': 'insurer-1', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 9, 'status': 'approved'},
        ], 10)
        path = one(self.db, 'PRAGMA database_list')['file']
        barrier = threading.Barrier(2)
        results = []

        def settle(claim):
            desk = open_db(path)
            barrier.wait()
            try:
                results.append(issue(desk, claim, 12))
            finally:
                desk.close()

        threads = [threading.Thread(target=settle, args=(c,)) for c in ('vendor-1', 'insurer-1')]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertCountEqual(results, ['issued', 'basis_capacity_exhausted'])
        paid = one(self.db, "SELECT COALESCE(SUM(normalized_amount),0) n FROM payouts WHERE basis_id='part-1' AND status='issued'")['n']
        self.assertEqual(paid, 100)

    # 6. Replay: same request repeated settles at most once.
    def test_replayed_request_is_idempotent(self):
        import_page(self.db, 'p70', [
            {'claim': 'vendor-1', 'cost': 'part-1', 'payer': 'vendor', 'amount': 100, 'tick': 9, 'status': 'approved'},
        ], 10)
        self.assertEqual(issue(self.db, 'vendor-1', 11, idem_key='req-001'), 'issued')
        self.assertEqual(issue(self.db, 'vendor-1', 12, idem_key='req-001'), 'replayed')
        self.assertEqual(issue(self.db, 'vendor-1', 13, idem_key='req-002'), 'already_issued')
        self.assertEqual(one(self.db, 'SELECT COUNT(*) n FROM payouts')['n'], 1)
        path = one(self.db, 'PRAGMA database_list')['file']
        other = open_db(path)
        try:
            self.assertEqual(issue(other, 'vendor-1', 14, idem_key='req-001'), 'replayed')
        finally:
            other.close()
        self.assertEqual(one(self.db, 'SELECT COUNT(*) n FROM payouts')['n'], 1)

    def test_page_replay_is_idempotent_and_conflicts_are_rejected(self):
        page = [
            {'claim': 'vendor-1', 'cost': 'part-1', 'payer': 'vendor', 'amount': 100, 'tick': 9, 'status': 'approved'},
        ]
        self.assertEqual(import_page(self.db, 'p71', page, 10), 'captured')
        self.assertEqual(import_page(self.db, 'p71', page, 10), 'replayed')
        with self.assertRaises(ValueError):
            import_page(self.db, 'p71', [
                {'claim': 'vendor-1', 'cost': 'part-1', 'payer': 'vendor', 'amount': 90, 'tick': 9, 'status': 'approved'},
            ], 10)

    # Currency: confirmed settlements freeze the rate known at valuation time.
    def test_foreign_currency_uses_recorded_rate_and_never_revalues(self):
        register_cost_basis(self.db, 'part-eur', 100, description='board billed in euros')
        import_fx_rate(self.db, 'EUR', 8, 500_000)      # 1 EUR = 0.5 CNY
        import_page(self.db, 'p80', [
            {'claim': 'vendor-eur', 'cost': 'part-eur', 'payer': 'vendor', 'amount': 160, 'tick': 9, 'status': 'approved', 'currency': 'EUR'},
        ], 10)
        # 160 EUR at 0.5 = 80 CNY against a 100 CNY allowed loss.
        self.assertEqual(issue(self.db, 'vendor-eur', 11), 'issued')
        p = one(self.db, "SELECT * FROM payouts WHERE claim='vendor-eur'")
        self.assertEqual(p['amount'], 160)
        self.assertEqual(p['currency'], 'EUR')
        self.assertEqual(p['normalized_amount'], 80)
        self.assertEqual(remaining_capacity(self.db, 'part-eur'), 20)
        # A later, different rate cannot alter the confirmed settlement.
        import_fx_rate(self.db, 'EUR', 20, 1_000_000)
        self.assertEqual(p['normalized_amount'], 80)
        self.assertEqual(remaining_capacity(self.db, 'part-eur'), 20)
        # A second payer using the new rate is capped by the same frozen basis.
        import_page(self.db, 'p81', [
            {'claim': 'insurer-eur', 'cost': 'part-eur', 'payer': 'insurer', 'amount': 50, 'tick': 21, 'status': 'approved', 'currency': 'EUR'},
        ], 22)
        self.assertEqual(issue(self.db, 'insurer-eur', 23), 'basis_capacity_exhausted')

    def test_foreign_currency_without_rate_is_held(self):
        register_cost_basis(self.db, 'part-usd', 100, description='board billed in dollars')
        import_page(self.db, 'p82', [
            {'claim': 'vendor-usd', 'cost': 'part-usd', 'payer': 'vendor', 'amount': 100, 'tick': 9, 'status': 'approved', 'currency': 'USD'},
        ], 10)
        self.assertEqual(issue(self.db, 'vendor-usd', 11), 'missing_fx_rate')
        self.assertEqual(rows(self.db, 'SELECT * FROM payouts'), [])

    def test_historical_rate_is_immutable(self):
        import_fx_rate(self.db, 'JPY', 5, 6)
        self.assertEqual(import_fx_rate(self.db, 'JPY', 5, 6), 'replayed')
        with self.assertRaises(ValueError):
            import_fx_rate(self.db, 'JPY', 5, 7)

    # Claim lifecycle: non-approved lines never pay, every decision is logged.
    def test_pending_denied_and_withdrawn_claims_are_held_or_refused(self):
        import_page(self.db, 'p90', [
            {'claim': 'c-pending', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 9, 'status': 'pending'},
            {'claim': 'c-denied', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 9, 'status': 'denied'},
            {'claim': 'c-unknown', 'cost': 'part-1', 'payer': 'insurer', 'amount': 100, 'tick': 9, 'status': 'approved'},
        ], 10)
        self.assertEqual(issue(self.db, 'c-pending', 11), 'hold')
        self.assertEqual(issue(self.db, 'c-denied', 11), 'denied')
        self.assertEqual(issue(self.db, 'never-captured', 11), 'hold')
        # The approved line still settles once status is reached.
        self.assertEqual(issue(self.db, 'c-unknown', 11), 'issued')
        logged = {r['decision'] for r in rows(self.db, 'SELECT DISTINCT decision FROM settlement_decisions')}
        self.assertIn('hold', logged)
        self.assertIn('denied', logged)
        self.assertIn('issued', logged)

    def test_split_invoices_share_one_basis_and_one_cap(self):
        # Same physical board split into two invoices by the same payer.
        add_cost_alias(self.db, 'part-1-split-a', 'part-1')
        add_cost_alias(self.db, 'part-1-split-b', 'part-1')
        import_page(self.db, 'p91', [
            {'claim': 'v-a', 'cost': 'part-1-split-a', 'payer': 'vendor', 'amount': 60, 'tick': 9, 'status': 'approved'},
            {'claim': 'v-b', 'cost': 'part-1-split-b', 'payer': 'vendor', 'amount': 60, 'tick': 9, 'status': 'approved'},
        ], 10)
        self.assertEqual(issue(self.db, 'v-a', 11), 'issued')
        self.assertEqual(issue(self.db, 'v-b', 11), 'basis_capacity_exhausted')
        self.assertEqual(remaining_capacity(self.db, 'part-1'), 40)


if __name__ == '__main__':
    unittest.main()
