"""Capture repair reimbursement decisions from outside payers."""
import hashlib
import json


def import_page(db, page_id, rows, received_at):
    db.execute('CREATE TABLE IF NOT EXISTS claim_pages (id TEXT PRIMARY KEY,digest TEXT NOT NULL,received_at INTEGER NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS claim_rows (page_id TEXT,line_no INTEGER,claim TEXT,cost TEXT,payer TEXT,amount INTEGER,tick INTEGER,status TEXT,PRIMARY KEY(page_id,line_no))')
    keys = {'claim', 'cost', 'payer', 'amount', 'tick', 'status'}
    if not isinstance(rows, list) or any(not isinstance(r, dict) or set(r) != keys or not isinstance(r['tick'], int) or not isinstance(r['amount'], int) or r['amount'] < 0 for r in rows):
        raise ValueError('invalid_claim_page')
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    old = db.execute('SELECT digest FROM claim_pages WHERE id=?', (page_id,)).fetchone()
    if old:
        if old[0] != digest:
            raise ValueError('claim_page_conflict')
        return 'replayed'
    with db:
        db.execute('INSERT INTO claim_pages VALUES (?,?,?)', (page_id, digest, received_at))
        for i, r in enumerate(rows, 1):
            db.execute('INSERT INTO claim_rows VALUES (?,?,?,?,?,?,?,?)', (page_id, i, r['claim'], r['cost'], r['payer'], r['amount'], r['tick'], r['status']))
    return 'captured'
