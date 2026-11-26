import json
import sqlite3
from contextlib import contextmanager

SCHEMA="""
CREATE TABLE IF NOT EXISTS nodes(id TEXT PRIMARY KEY,uid TEXT NOT NULL,generation INTEGER NOT NULL,zone TEXT NOT NULL,cpu INTEGER NOT NULL,mem INTEGER NOT NULL, schedulable INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS guard(service TEXT PRIMARY KEY,max_down INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS tickets(id TEXT PRIMARY KEY,node_id TEXT NOT NULL,node_uid TEXT NOT NULL,start_ts INTEGER NOT NULL,end_ts INTEGER NOT NULL,urgent INTEGER NOT NULL,status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS approval(id TEXT PRIMARY KEY,ticket_id TEXT NOT NULL,role TEXT NOT NULL,actor TEXT NOT NULL,until_ts INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS data_loss_waiver(ticket_id TEXT NOT NULL,workload_id TEXT NOT NULL,approver TEXT NOT NULL,reason TEXT NOT NULL,PRIMARY KEY(ticket_id,workload_id));
CREATE TABLE IF NOT EXISTS workloads(id TEXT PRIMARY KEY,service TEXT NOT NULL,node_id TEXT NOT NULL,zone TEXT NOT NULL,cpu INTEGER NOT NULL,mem INTEGER NOT NULL,kind TEXT NOT NULL,volume_id TEXT,version TEXT NOT NULL,generation INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS eviction(id TEXT PRIMARY KEY,ticket_id TEXT NOT NULL,workload_id TEXT NOT NULL,target_id TEXT,status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS target_receipt(id TEXT PRIMARY KEY,eviction_id TEXT NOT NULL,target_uid TEXT NOT NULL,workload_generation INTEGER NOT NULL,version TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS volume(id TEXT PRIMARY KEY,owner_node TEXT NOT NULL,target_node TEXT,generation INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS volume_receipt(id TEXT PRIMARY KEY,volume_id TEXT NOT NULL,kind TEXT NOT NULL,node_uid TEXT NOT NULL,volume_generation INTEGER NOT NULL,workload_generation INTEGER NOT NULL,at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS probes(id TEXT PRIMARY KEY,node_id TEXT NOT NULL,generation INTEGER NOT NULL,kind TEXT NOT NULL,at INTEGER NOT NULL,healthy INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS snapshot(ticket_id TEXT NOT NULL,workload_id TEXT NOT NULL,PRIMARY KEY(ticket_id,workload_id));
CREATE TABLE IF NOT EXISTS stranded(ticket_id TEXT NOT NULL,workload_id TEXT NOT NULL,PRIMARY KEY(ticket_id,workload_id));
CREATE TABLE IF NOT EXISTS orphan(id TEXT PRIMARY KEY,ticket_id TEXT NOT NULL,workload_id TEXT NOT NULL,tenant TEXT,status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ownership(workload_id TEXT PRIMARY KEY,tenant TEXT NOT NULL,generation INTEGER NOT NULL,valid_until INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS closure(ticket_id TEXT PRIMARY KEY,status TEXT NOT NULL,reason TEXT NOT NULL);
-- Source identity captured when a maintenance ticket is approved.  A receipt is
-- only useful while the workload still matches this binding.
CREATE TABLE IF NOT EXISTS source_binding(ticket_id TEXT NOT NULL,workload_id TEXT NOT NULL,node_id TEXT NOT NULL,node_uid TEXT NOT NULL,workload_generation INTEGER NOT NULL,version TEXT NOT NULL,PRIMARY KEY(ticket_id,workload_id));
-- A target promise is the persistent reservation created by a plan.  Reserving
-- and moving are two different decisions; the promise survives retries so a
-- source workload never loses its service identity while a receipt is pending.
CREATE TABLE IF NOT EXISTS target_promise(id TEXT PRIMARY KEY,owner_kind TEXT NOT NULL,owner_id TEXT NOT NULL,workload_id TEXT NOT NULL,target_id TEXT NOT NULL,target_uid TEXT NOT NULL,status TEXT NOT NULL,created_at INTEGER NOT NULL,ended_at INTEGER,reason TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_promise_per_owner ON target_promise(owner_kind,owner_id) WHERE status='active';
CREATE UNIQUE INDEX IF NOT EXISTS one_active_promise_per_workload ON target_promise(workload_id) WHERE status='active';
-- Every target confirmation attempt, including rejected ones, is retained.
CREATE TABLE IF NOT EXISTS placement_attempt(id TEXT PRIMARY KEY,receipt_id TEXT,owner_kind TEXT NOT NULL,owner_id TEXT NOT NULL,target_id TEXT,target_uid TEXT,workload_generation INTEGER NOT NULL,version TEXT NOT NULL,at INTEGER NOT NULL,status TEXT NOT NULL,reason TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS one_attempt_per_receipt ON placement_attempt(receipt_id,owner_kind,owner_id) WHERE receipt_id IS NOT NULL;
-- A business-owned relocation of an already serving workload.  It is separate
-- from maintenance evictions: it needs an ownership grant and never moves the
-- workload before the new target returns a readiness receipt.
CREATE TABLE IF NOT EXISTS relocation(id TEXT PRIMARY KEY,workload_id TEXT NOT NULL,source_id TEXT NOT NULL,source_uid TEXT NOT NULL,target_id TEXT NOT NULL,target_uid TEXT NOT NULL,owner_generation INTEGER NOT NULL,owner_valid_until INTEGER NOT NULL,status TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS relocation_receipt(id TEXT PRIMARY KEY,relocation_id TEXT NOT NULL,target_uid TEXT NOT NULL,workload_generation INTEGER NOT NULL,version TEXT NOT NULL,at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS relocation_attempt(id TEXT PRIMARY KEY,relocation_id TEXT NOT NULL,workload_id TEXT NOT NULL,target_id TEXT,result TEXT NOT NULL,at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS audit_log(seq INTEGER PRIMARY KEY AUTOINCREMENT,table_name TEXT NOT NULL,row_id INTEGER NOT NULL,action TEXT NOT NULL,payload TEXT);
"""
BUSINESS_TABLES=["nodes","guard","tickets","approval","data_loss_waiver","workloads","eviction","target_receipt","volume","volume_receipt","probes","snapshot","stranded","orphan","ownership","closure","source_binding","target_promise","placement_attempt","relocation","relocation_receipt","relocation_attempt"]

def open_db(path):
    db=sqlite3.connect(path,timeout=10)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    for table in BUSINESS_TABLES:
        cols=[r["name"] for r in db.execute(f"PRAGMA table_info({table})")]
        for action,ref in (("insert","NEW"),("update","NEW"),("delete","OLD")):
            fields=",".join(f"'{c}',{ref}.\"{c}\"" for c in cols)
            db.execute(f"""CREATE TRIGGER IF NOT EXISTS audit_{table}_{action} AFTER {action.upper()} ON {table}
              BEGIN INSERT INTO audit_log(table_name,row_id,action,payload)
              VALUES('{table}',{ref}.rowid,'{action}',json_object({fields})); END""")
    return db

@contextmanager
def tx(db):
    db.execute("BEGIN IMMEDIATE")
    try:yield
    except Exception:
        db.rollback();raise
    else:db.commit()

def one(db,sql,args=()):
    x=db.execute(sql,args).fetchone()
    return dict(x) if x else None

def rows(db,sql,args=()):
    return [dict(x) for x in db.execute(sql,args)]

def report(db):
    state={n:rows(db,"SELECT * FROM "+n) for n in BUSINESS_TABLES}
    state["audit_replay"]=verify_replay(db)
    return state

def verify_replay(db):
    reconstructed={name:{} for name in BUSINESS_TABLES}
    for record in rows(db,"SELECT * FROM audit_log ORDER BY seq"):
        table=reconstructed[record["table_name"]]
        if record["action"]=="delete":table.pop(record["row_id"],None)
        else:table[record["row_id"]]=json.loads(record["payload"])
    mismatches=[]
    for name in BUSINESS_TABLES:
        actual={r["rowid"]:{k:v for k,v in r.items() if k!="rowid"} for r in rows(db,f"SELECT rowid,* FROM {name}")}
        if actual!=reconstructed[name]:mismatches.append(name)
    return {"consistent":not mismatches,"mismatches":mismatches,"events":one(db,"SELECT COUNT(*) n FROM audit_log")["n"]}
