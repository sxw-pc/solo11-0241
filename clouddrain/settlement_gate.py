"""Provisional payout of individually approved claims."""


def issue(db, claim, at):
    db.execute('CREATE TABLE IF NOT EXISTS payouts (claim TEXT,cost TEXT,payer TEXT,amount INTEGER,at INTEGER)')
    try:
        row = db.execute('SELECT cost,payer,amount,status FROM claim_rows WHERE claim=? ORDER BY tick DESC LIMIT 1', (claim,)).fetchone()
    except __import__('sqlite3').OperationalError:
        row = None
    if not row or row[3] != 'approved':
        return 'hold'
    with db:
        db.execute('INSERT INTO payouts VALUES (?,?,?,?,?)', (claim, row[0], row[1], row[2], at))
    return 'issued'
