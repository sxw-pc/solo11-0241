"""Capture offline audit checkpoints from external witnesses."""
import hashlib
import json


def import_page(db, page_id, rows, received_at):
    db.execute('CREATE TABLE IF NOT EXISTS witness_pages (id TEXT PRIMARY KEY,digest TEXT NOT NULL,received_at INTEGER NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS witness_rows (page_id TEXT,line_no INTEGER,branch TEXT,seq INTEGER,root TEXT,witness TEXT,signed_at INTEGER,PRIMARY KEY(page_id,line_no))')
    keys = {'branch', 'seq', 'root', 'witness', 'signed_at'}
    if not isinstance(rows, list) or any(not isinstance(r, dict) or set(r) != keys or not isinstance(r['seq'], int) or not isinstance(r['signed_at'], int) for r in rows):
        raise ValueError('invalid_witness_page')
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    old = db.execute('SELECT digest FROM witness_pages WHERE id=?', (page_id,)).fetchone()
    if old:
        if old[0] != digest:
            raise ValueError('witness_page_conflict')
        return 'replayed'
    with db:
        db.execute('INSERT INTO witness_pages VALUES (?,?,?)', (page_id, digest, received_at))
        for i, r in enumerate(rows, 1):
            db.execute('INSERT INTO witness_rows VALUES (?,?,?,?,?,?,?)', (page_id, i, r['branch'], r['seq'], r['root'], r['witness'], r['signed_at']))
    return 'captured'
