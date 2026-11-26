"""Capture repair reimbursement decisions from outside payers.

A captured claim line never stands on its own: it is attributed to a
canonical *cost basis*, i.e. the verifiable actual loss of one repair item
(one control board, one hour of labour).  Two payers may call the same loss
by different names ("spare replacement" versus "equipment loss") or split it
across several invoices, so duplicate coverage must never be inferred from
claim numbers or fee text.  Every payer-specific label has to be registered
as an alias of the basis it really points at before settlement is possible.

Historical currency rates are append-only: a rate recorded for a valuation
moment can never be overwritten, and amounts converted for a confirmed
settlement stay frozen at that rate forever.
"""
import hashlib
import json

from .store import one, tx

BASE_CURRENCY = 'CNY'
FX_SCALE = 1_000_000
CLAIM_STATUSES = ('approved', 'pending', 'denied', 'withdrawn')
REQUIRED_KEYS = {'claim', 'cost', 'payer', 'amount', 'tick', 'status'}
OPTIONAL_KEYS = {'currency'}


class MissingFxRate(Exception):
    """Raised when no recorded rate can value a non-basis currency at tick."""


def _ensure_schema(db):
    db.execute('CREATE TABLE IF NOT EXISTS claim_pages (id TEXT PRIMARY KEY,digest TEXT NOT NULL,received_at INTEGER NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS claim_rows (page_id TEXT,line_no INTEGER,claim TEXT,cost TEXT,payer TEXT,amount INTEGER,tick INTEGER,status TEXT,currency TEXT NOT NULL DEFAULT \'CNY\',PRIMARY KEY(page_id,line_no))')
    db.execute('CREATE TABLE IF NOT EXISTS cost_basis (id TEXT PRIMARY KEY,description TEXT NOT NULL,allowed_loss INTEGER NOT NULL,currency TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS cost_aliases (alias TEXT PRIMARY KEY,basis_id TEXT NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS fx_rates (currency TEXT NOT NULL,at INTEGER NOT NULL,rate_per_unit INTEGER NOT NULL,PRIMARY KEY(currency,at))')


def _valid_row(r):
    if not isinstance(r, dict) or not REQUIRED_KEYS <= set(r):
        return False
    if set(r) - REQUIRED_KEYS - OPTIONAL_KEYS:
        return False
    if not isinstance(r['tick'], int) or not isinstance(r['amount'], int) or r['amount'] < 0:
        return False
    if r['status'] not in CLAIM_STATUSES:
        return False
    currency = r.get('currency', BASE_CURRENCY)
    return isinstance(currency, str) and bool(currency)


def import_page(db, page_id, rows, received_at):
    _ensure_schema(db)
    if not isinstance(rows, list) or not rows or any(not _valid_row(r) for r in rows):
        raise ValueError('invalid_claim_page')
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    old = db.execute('SELECT digest FROM claim_pages WHERE id=?', (page_id,)).fetchone()
    if old:
        if old[0] != digest:
            raise ValueError('claim_page_conflict')
        return 'replayed'
    with tx(db):
        db.execute('INSERT INTO claim_pages VALUES (?,?,?)', (page_id, digest, received_at))
        for i, r in enumerate(rows, 1):
            db.execute('INSERT INTO claim_rows VALUES (?,?,?,?,?,?,?,?,?)',
                       (page_id, i, r['claim'], r['cost'], r['payer'], r['amount'],
                        r['tick'], r['status'], r.get('currency', BASE_CURRENCY)))
    return 'captured'


def latest_row(db, claim):
    """Newest captured line for a claim; deterministic on tick ties."""
    _ensure_schema(db)
    return one(db, ('SELECT * FROM claim_rows WHERE claim=? '
                'ORDER BY tick DESC,page_id DESC,line_no DESC LIMIT 1'), (claim,))


def register_cost_basis(db, basis_id, allowed_loss, currency=BASE_CURRENCY, description='', aliases=()):
    """Register the verifiable actual loss of one repair item.

    ``allowed_loss`` is the contract-permitted actual loss in ``currency``.
    The basis id itself and every entry in ``aliases`` become names that map
    back to this basis, so renamed or split payer fees stay attributable.
    """
    _ensure_schema(db)
    if not isinstance(basis_id, str) or not basis_id or not isinstance(allowed_loss, int) or allowed_loss <= 0:
        raise ValueError('invalid_cost_basis')
    if not isinstance(currency, str) or not currency:
        raise ValueError('invalid_cost_basis')
    with tx(db):
        old = one(db, 'SELECT * FROM cost_basis WHERE id=?', (basis_id,))
        if old:
            if old['allowed_loss'] != allowed_loss or old['currency'] != currency or old['description'] != description:
                return 'cost_basis_conflict'
            result = 'replayed'
        else:
            db.execute('INSERT INTO cost_basis VALUES (?,?,?,?)', (basis_id, description, allowed_loss, currency))
            result = 'recorded'
        for alias in [basis_id, *aliases]:
            conflict = one(db, 'SELECT basis_id FROM cost_aliases WHERE alias=?', (alias,))
            if conflict and conflict['basis_id'] != basis_id:
                raise ValueError('alias_conflict')
            db.execute('INSERT OR IGNORE INTO cost_aliases VALUES (?,?)', (alias, basis_id))
    return result


def add_cost_alias(db, alias, basis_id):
    """Attach a payer-specific fee name (e.g. a renamed fee) to a basis."""
    _ensure_schema(db)
    with tx(db):
        if not one(db, 'SELECT 1 FROM cost_basis WHERE id=?', (basis_id,)):
            return 'unknown_cost_basis'
        conflict = one(db, 'SELECT basis_id FROM cost_aliases WHERE alias=?', (alias,))
        if conflict:
            return 'replayed' if conflict['basis_id'] == basis_id else 'alias_conflict'
        db.execute('INSERT INTO cost_aliases VALUES (?,?)', (alias, basis_id))
    return 'recorded'


def resolve_basis(db, cost):
    """Return the canonical cost basis a fee label points at, if any."""
    _ensure_schema(db)
    return one(db, ('SELECT b.* FROM cost_aliases a JOIN cost_basis b ON b.id=a.basis_id '
                'WHERE a.alias=?'), (cost,))


def import_fx_rate(db, currency, at, rate_per_unit):
    """Record an immutable historical rate: units of basis currency per 1e6."""
    _ensure_schema(db)
    if not isinstance(currency, str) or not currency or not isinstance(at, int) or not isinstance(rate_per_unit, int) or rate_per_unit <= 0:
        raise ValueError('invalid_fx_rate')
    with tx(db):
        old = one(db, 'SELECT rate_per_unit FROM fx_rates WHERE currency=? AND at=?', (currency, at))
        if old:
            if old['rate_per_unit'] != rate_per_unit:
                raise ValueError('fx_rate_conflict')
            return 'replayed'
        db.execute('INSERT INTO fx_rates VALUES (?,?,?)', (currency, at, rate_per_unit))
    return 'recorded'


def normalize_amount(db, amount, currency, basis_currency, tick):
    """Convert ``amount`` to basis currency using the rate known at ``tick``.

    Only rates recorded at or before the valuation moment are considered, so
    a later rate can never quietly revalue an earlier settlement.
    """
    if currency == basis_currency:
        return amount
    rate = one(db, ('SELECT rate_per_unit FROM fx_rates WHERE currency=? AND at<=? '
                'ORDER BY at DESC LIMIT 1'), (currency, tick))
    if not rate:
        raise MissingFxRate(currency)
    return amount * rate['rate_per_unit'] // FX_SCALE
