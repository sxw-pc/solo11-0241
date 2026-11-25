"""Provisional customer audit release from local replay and a witness row."""
from .store import verify_replay


def publish(db, branch, at):
    db.execute('CREATE TABLE IF NOT EXISTS audit_exports (branch TEXT,at INTEGER,decision TEXT)')
    try:
        row = db.execute('SELECT root FROM witness_rows WHERE branch=? ORDER BY seq DESC LIMIT 1', (branch,)).fetchone()
    except __import__('sqlite3').OperationalError:
        row = None
    decision = 'published' if row and verify_replay(db)['consistent'] else 'hold'
    with db:
        db.execute('INSERT INTO audit_exports VALUES (?,?,?)', (branch, at, decision))
    return decision
