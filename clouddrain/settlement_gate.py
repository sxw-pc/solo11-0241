"""Settlement gate paying repair reimbursements against the real loss.

Every cash payout, reversal and in-kind delivery is booked on a canonical
cost basis, never on a payer-specific claim number or fee label.  The
remaining indemnity of a basis is one explainable number shared by every
payer:

    remaining = contract-allowed actual loss
                - net cash already paid out (payouts minus reversals)
                - value of parts physically delivered and installed

All mutations run under a single immediate write transaction so two offline
settlement desks settling concurrently or replaying the same request cannot
each pay the full loss.  Reversals never delete the original payout: money
the customer once received stays in history and the claw-back is recorded as
its own ledger entry.
"""
import sqlite3
import uuid

from .store import one, rows, tx
from .claim_capture import (
    MissingFxRate,
    _ensure_schema,
    latest_row,
    normalize_amount,
    resolve_basis,
)


def _schema(db):
    _ensure_schema(db)
    db.execute('CREATE TABLE IF NOT EXISTS payouts (id TEXT PRIMARY KEY,claim TEXT NOT NULL,cost TEXT NOT NULL,basis_id TEXT NOT NULL,payer TEXT NOT NULL,amount INTEGER NOT NULL,currency TEXT NOT NULL,normalized_amount INTEGER NOT NULL,at INTEGER NOT NULL,status TEXT NOT NULL DEFAULT \'issued\',idem_key TEXT)')
    db.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_payouts_active_claim ON payouts(claim) WHERE status=\'issued\'')
    db.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_payouts_idem ON payouts(idem_key) WHERE idem_key IS NOT NULL')
    db.execute('CREATE TABLE IF NOT EXISTS payout_reversals (id TEXT PRIMARY KEY,payout_id TEXT NOT NULL,claim TEXT NOT NULL,basis_id TEXT NOT NULL,amount INTEGER NOT NULL,currency TEXT NOT NULL,normalized_amount INTEGER NOT NULL,reason TEXT NOT NULL,at INTEGER NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS in_kind_settlements (id TEXT PRIMARY KEY,basis_id TEXT NOT NULL,payer TEXT NOT NULL,claim TEXT,description TEXT NOT NULL,value INTEGER NOT NULL,currency TEXT NOT NULL,normalized_value INTEGER NOT NULL,promised_at INTEGER,installed_at INTEGER,status TEXT NOT NULL,install_proof TEXT)')
    db.execute('CREATE TABLE IF NOT EXISTS settlement_decisions (seq INTEGER PRIMARY KEY AUTOINCREMENT,claim TEXT,cost TEXT,basis_id TEXT,payer TEXT,decision TEXT NOT NULL,detail TEXT,at INTEGER NOT NULL)')


def _log(db, at, decision, claim=None, cost=None, basis_id=None, payer=None, detail=None):
    db.execute('INSERT INTO settlement_decisions(claim,cost,basis_id,payer,decision,detail,at) VALUES (?,?,?,?,?,?,?)',
               (claim, cost, basis_id, payer, decision, detail, at))


def _cash_paid(db, basis_id):
    # Payout rows are never deleted: a reversed payout still counts in gross
    # paid and its own reversal row cancels it, keeping history intact.
    paid = one(db, 'SELECT COALESCE(SUM(normalized_amount),0) n FROM payouts WHERE basis_id=?', (basis_id,))['n']
    reversed_ = one(db, 'SELECT COALESCE(SUM(normalized_amount),0) n FROM payout_reversals WHERE basis_id=?', (basis_id,))['n']
    return paid - reversed_


def _installed_value(db, basis_id):
    return one(db, 'SELECT COALESCE(SUM(normalized_value),0) n FROM in_kind_settlements WHERE basis_id=? AND status=\'installed\'', (basis_id,))['n']


def remaining_capacity(db, basis_id):
    """Unique remaining indemnity on one basis, shared across all payers."""
    _schema(db)
    basis = one(db, 'SELECT * FROM cost_basis WHERE id=?', (basis_id,))
    if not basis:
        return None
    return basis['allowed_loss'] - _cash_paid(db, basis_id) - _installed_value(db, basis_id)


def issue(db, claim, at, idem_key=None):
    """Settle the latest approved line of ``claim`` against shared capacity."""
    _schema(db)
    row = latest_row(db, claim)
    if not row:
        with tx(db):
            _log(db, at, 'hold', claim=claim, detail='no_captured_claim')
        return 'hold'
    if row['status'] != 'approved':
        mapped = 'hold' if row['status'] == 'pending' else row['status']
        with tx(db):
            _log(db, at, mapped, claim=row['claim'], cost=row['cost'], payer=row['payer'],
                 detail='claim_status:' + row['status'])
        return mapped
    basis = resolve_basis(db, row['cost'])
    if not basis:
        with tx(db):
            _log(db, at, 'unattributed_cost', claim=row['claim'], cost=row['cost'],
                 payer=row['payer'], detail='no_cost_basis')
        return 'unattributed_cost'
    try:
        normalized = normalize_amount(db, row['amount'], row['currency'], basis['currency'], row['tick'])
    except MissingFxRate:
        with tx(db):
            _log(db, at, 'missing_fx_rate', claim=row['claim'], cost=row['cost'],
                 basis_id=basis['id'], payer=row['payer'])
        return 'missing_fx_rate'
    payout_id = 'pay-' + uuid.uuid4().hex[:16]
    with tx(db):
        if idem_key is not None:
            dup = one(db, 'SELECT id FROM payouts WHERE idem_key=?', (idem_key,))
            if dup:
                _log(db, at, 'replayed', claim=row['claim'], cost=row['cost'],
                     basis_id=basis['id'], payer=row['payer'], detail='idem_key:' + idem_key)
                return 'replayed'
        # A claim settles at most once: clawing back its payout never
        # makes the same claim re-payable.
        active = one(db, 'SELECT id,status FROM payouts WHERE claim=? ORDER BY rowid LIMIT 1', (claim,))
        if active:
            _log(db, at, 'already_issued', claim=row['claim'], cost=row['cost'],
                 basis_id=basis['id'], payer=row['payer'], detail='payout:' + active['id'])
            return 'already_issued'
        remaining = basis['allowed_loss'] - _cash_paid(db, basis['id']) - _installed_value(db, basis['id'])
        if normalized > remaining:
            _log(db, at, 'basis_capacity_exhausted', claim=row['claim'], cost=row['cost'],
                 basis_id=basis['id'], payer=row['payer'],
                 detail='requested:%d remaining:%d' % (normalized, remaining))
            return 'basis_capacity_exhausted'
        try:
            db.execute('INSERT INTO payouts(id,claim,cost,basis_id,payer,amount,currency,normalized_amount,at,status,idem_key) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                       (payout_id, row['claim'], row['cost'], basis['id'], row['payer'],
                        row['amount'], row['currency'], normalized, at, 'issued', idem_key))
        except sqlite3.IntegrityError:
            # Lost a race on the idempotency or active-claim unique index.
            _log(db, at, 'replayed', claim=row['claim'], cost=row['cost'],
                 basis_id=basis['id'], payer=row['payer'])
            return 'replayed'
        _log(db, at, 'issued', claim=row['claim'], cost=row['cost'],
             basis_id=basis['id'], payer=row['payer'], detail='payout:' + payout_id)
    return 'issued'


def clawback(db, payout_id, reason, at, reversal_id=None):
    """Reverse an issued payout without erasing the fact it was paid."""
    _schema(db)
    rid = reversal_id or ('rev-' + uuid.uuid4().hex[:16])
    with tx(db):
        payout = one(db, 'SELECT * FROM payouts WHERE id=?', (payout_id,))
        if not payout:
            _log(db, at, 'unknown_payout', detail='payout:' + payout_id)
            return 'unknown_payout'
        if payout['status'] != 'issued':
            _log(db, at, 'already_reversed', claim=payout['claim'], basis_id=payout['basis_id'],
                 payer=payout['payer'], detail='payout:' + payout_id)
            return 'already_reversed'
        db.execute('UPDATE payouts SET status=\'reversed\' WHERE id=?', (payout_id,))
        db.execute('INSERT INTO payout_reversals VALUES (?,?,?,?,?,?,?,?,?)',
                   (rid, payout_id, payout['claim'], payout['basis_id'], payout['amount'],
                    payout['currency'], payout['normalized_amount'], reason, at))
        _log(db, at, 'reversed', claim=payout['claim'], basis_id=payout['basis_id'],
             payer=payout['payer'], detail='payout:' + payout_id + ' reason:' + reason)
    return 'reversed'


def promise_in_kind(db, settlement_id, basis_id, payer, value, currency, description, at, claim=None):
    """Record a promised replacement part.  Promises consume no capacity."""
    _schema(db)
    if not isinstance(value, int) or value <= 0:
        raise ValueError('invalid_in_kind_value')
    basis = one(db, 'SELECT * FROM cost_basis WHERE id=?', (basis_id,))
    if not basis:
        return 'unknown_cost_basis'
    try:
        normalized = normalize_amount(db, value, currency, basis['currency'], at)
    except MissingFxRate:
        return 'missing_fx_rate'
    with tx(db):
        old = one(db, 'SELECT * FROM in_kind_settlements WHERE id=?', (settlement_id,))
        if old:
            return 'replayed' if (old['basis_id'], old['payer'], old['value'], old['currency']) == (basis_id, payer, value, currency) else 'in_kind_conflict'
        db.execute('INSERT INTO in_kind_settlements(id,basis_id,payer,claim,description,value,currency,normalized_value,promised_at,installed_at,status,install_proof) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                   (settlement_id, basis_id, payer, claim, description, value, currency,
                    normalized, at, None, 'promised', None))
        _log(db, at, 'in_kind_promised', claim=claim, basis_id=basis_id, payer=payer,
             detail='settlement:' + settlement_id)
    return 'recorded'


def install_in_kind(db, settlement_id, at, install_proof):
    """Confirm a promised part is physically delivered and installed.

    Only this step consumes shared basis capacity; a promise that never
    arrives cannot occupy another payer's indemnity.
    """
    _schema(db)
    if not install_proof:
        return 'missing_install_proof'
    with tx(db):
        s = one(db, 'SELECT * FROM in_kind_settlements WHERE id=?', (settlement_id,))
        if not s:
            _log(db, at, 'unknown_settlement', detail='settlement:' + settlement_id)
            return 'unknown_settlement'
        if s['status'] == 'installed':
            return 'replayed'
        basis = one(db, 'SELECT * FROM cost_basis WHERE id=?', (s['basis_id'],))
        remaining = basis['allowed_loss'] - _cash_paid(db, basis['id']) - _installed_value(db, basis['id'])
        if s['normalized_value'] > remaining:
            _log(db, at, 'basis_capacity_exhausted', claim=s['claim'], basis_id=basis['id'],
                 payer=s['payer'], detail='settlement:%s remaining:%d' % (settlement_id, remaining))
            return 'basis_capacity_exhausted'
        db.execute('UPDATE in_kind_settlements SET status=\'installed\',installed_at=?,install_proof=? WHERE id=?',
                   (at, install_proof, settlement_id))
        _log(db, at, 'in_kind_installed', claim=s['claim'], basis_id=basis['id'],
             payer=s['payer'], detail='settlement:' + settlement_id)
    return 'installed'


def ledger(db, basis_id):
    """Read view of every cash, reversal and in-kind entry on one basis."""
    _schema(db)
    return {
        'payouts': rows(db, 'SELECT * FROM payouts WHERE basis_id=? ORDER BY at,id', (basis_id,)),
        'reversals': rows(db, 'SELECT * FROM payout_reversals WHERE basis_id=? ORDER BY at,id', (basis_id,)),
        'in_kind': rows(db, 'SELECT * FROM in_kind_settlements WHERE basis_id=? ORDER BY promised_at,id', (basis_id,)),
        'remaining': remaining_capacity(db, basis_id),
    }
