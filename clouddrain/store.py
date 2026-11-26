import hashlib
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
CREATE TABLE IF NOT EXISTS audit_log(seq INTEGER PRIMARY KEY AUTOINCREMENT,table_name TEXT NOT NULL,row_id INTEGER NOT NULL,action TEXT NOT NULL,payload TEXT);
"""
BUSINESS_TABLES=["nodes","guard","tickets","approval","data_loss_waiver","workloads","eviction","target_receipt","volume","volume_receipt","probes","snapshot","stranded","orphan","ownership","closure"]

# The hash chain lives outside audit_log.  audit_log is appended by SQLite
# triggers and cannot compute a digest itself; audit_chain is appended by
# seal_chain and is append-only.  Every sealed value commits to the complete
# event content and to the business table state rebuilt from that prefix, so a
# restored backup that reuses a sequence number for other content no longer
# matches the sealed history.
AUDIT_SCHEMA="""
CREATE TABLE IF NOT EXISTS audit_chain(seq INTEGER PRIMARY KEY,prev_hash TEXT NOT NULL,event_hash TEXT NOT NULL,event_digest TEXT NOT NULL,node_id TEXT,ticket_id TEXT);
"""
AUDIT_GENESIS="clouddrain-audit-genesis-v1"
APPEND_ONLY_TABLES=["audit_chain"]

def _append_only(db,tables):
    for table in tables:
        for op in ("UPDATE","DELETE"):
            db.execute(f"""CREATE TRIGGER IF NOT EXISTS {table}_no_{op.lower()} BEFORE {op} ON {table}
              BEGIN SELECT RAISE(ABORT,'{table}_append_only'); END""")

def open_db(path):
    db=sqlite3.connect(path,timeout=10)
    db.row_factory=sqlite3.Row
    db.executescript(SCHEMA)
    db.executescript(AUDIT_SCHEMA)
    for table in BUSINESS_TABLES:
        cols=[r["name"] for r in db.execute(f"PRAGMA table_info({table})")]
        for action,ref in (("insert","NEW"),("update","NEW"),("delete","OLD")):
            fields=",".join(f"'{c}',{ref}.\"{c}\"" for c in cols)
            db.execute(f"""CREATE TRIGGER IF NOT EXISTS audit_{table}_{action} AFTER {action.upper()} ON {table}
              BEGIN INSERT INTO audit_log(table_name,row_id,action,payload)
              VALUES('{table}',{ref}.rowid,'{action}',json_object({fields})); END""")
    _append_only(db,APPEND_ONLY_TABLES)
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

# ---------------------------------------------------------------- audit chain

def _canonical(value):
    return json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False)

def _sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def _normalize_event(record):
    payload=record["payload"]
    if isinstance(payload,str):
        payload=json.loads(payload) if payload else {}
    return {"seq":int(record["seq"]),"table_name":record["table_name"],
            "row_id":int(record["row_id"]),"action":record["action"],
            "payload":payload}

def audit_events(db):
    return [_normalize_event(r) for r in rows(db,"SELECT seq,table_name,row_id,action,payload FROM audit_log ORDER BY seq")]

def snapshot_tables(db):
    return {name:{str(r["rowid"]):{k:v for k,v in r.items() if k!="rowid"}
                  for r in rows(db,f"SELECT rowid,* FROM {name}")} for name in BUSINESS_TABLES}

def _event_digest(record):
    return _sha(_canonical({"seq":record["seq"],"table":record["table_name"],
                            "row":record["row_id"],"action":record["action"],
                            "payload":record["payload"]}))

def chain_for_events(events):
    """Build the hash chain, tracing each event to its node and ticket."""
    prev=AUDIT_GENESIS
    chain=[]
    ticket_node={}
    workload_node={}
    for raw in sorted(events,key=lambda r:int(r["seq"])):
        record=_normalize_event(raw)
        p=record["payload"] or {}
        table=record["table_name"]
        node=ticket=None
        if table=="nodes":
            node=p.get("id")
        elif table=="tickets":
            node=p.get("node_id");ticket=p.get("id")
            if record["action"]!="delete" and ticket is not None:
                ticket_node[ticket]=node
        elif table in ("approval","data_loss_waiver","eviction","snapshot","stranded","orphan","closure"):
            ticket=p.get("ticket_id")
        elif table=="workloads":
            node=p.get("node_id")
            if record["action"]!="delete" and p.get("id") is not None:
                workload_node[p.get("id")]=node
        elif table in ("ownership","target_receipt","volume_receipt"):
            wid=p.get("workload_id")
            node=workload_node.get(wid)
        if node is None and ticket is not None:
            node=ticket_node.get(ticket)
        digest=_event_digest(record)
        event_hash=_sha(prev+"|"+digest)
        chain.append({**record,"prev_hash":prev,"event_digest":digest,
                      "event_hash":event_hash,"node_id":node,"ticket_id":ticket})
        prev=event_hash
    return chain

def replay_events(events):
    tables={name:{} for name in BUSINESS_TABLES}
    for record in sorted(events,key=lambda r:int(r["seq"])):
        record=_normalize_event(record)
        target=tables[record["table_name"]]
        if record["action"]=="delete":
            target.pop(record["row_id"],None)
        else:
            target[record["row_id"]]=record["payload"]
    return tables

def _tables_canonical(tables):
    return {name:{str(rid):tables[name][rid] for rid in sorted(tables[name])} for name in BUSINESS_TABLES}

def state_digest(tables):
    return _sha(_canonical(_tables_canonical(tables)))

def root_from_events(events,seq):
    """Witnessed root for a point in history.

    The root commits to the full preceding event chain *and* to the business
    table state reconstructed from exactly those events.  Two consoles that
    reuse a sequence number with different node identities therefore cannot
    produce the same root.
    """
    seq=int(seq)
    if seq<0:return None
    normalized=sorted((_normalize_event(e) for e in events),key=lambda r:r["seq"])
    seqs=[e["seq"] for e in normalized]
    if len(seqs)!=len(set(seqs)):return None
    if seq>0 and seq not in seqs:return None
    prefix=[e for e in normalized if e["seq"]<=seq]
    chain=chain_for_events(prefix)
    head=chain[-1]["event_hash"] if chain else AUDIT_GENESIS
    return _sha(_canonical({"v":1,"seq":seq,"head":head,"state":state_digest(replay_events(prefix))}))

def root_at(db,seq):
    return root_from_events(audit_events(db),seq)

def current_seq(db):
    return one(db,"SELECT COALESCE(MAX(seq),0) seq FROM audit_log")["seq"]

def current_root(db):
    return root_at(db,current_seq(db))

def _linkage_problems(chain):
    problems=[]
    nodes={};tickets={};workloads={};evictions={};volumes={}
    def flag(seq,table,kind,ref):
        problems.append({"kind":kind,"seq":seq,"table":table,"ref":ref})
    for item in chain:
        seq=item["seq"];table=item["table_name"];action=item["action"];p=item["payload"] or {}
        if table=="nodes":
            if action=="delete":nodes.pop(p.get("id"),None)
            else:nodes[p.get("id")]=p
        elif table=="tickets":
            if action=="delete":
                tickets.pop(p.get("id"),None)
            else:
                node=nodes.get(p.get("node_id"))
                if not node:flag(seq,table,"ticket_without_node",p.get("node_id"))
                elif node.get("uid")!=p.get("node_uid"):flag(seq,table,"ticket_node_uid_mismatch",p.get("node_id"))
                tickets[p.get("id")]=p
        elif table=="approval":
            if p.get("ticket_id") not in tickets:flag(seq,table,"approval_without_ticket",p.get("ticket_id"))
        elif table=="data_loss_waiver":
            if p.get("ticket_id") not in tickets:flag(seq,table,"waiver_without_ticket",p.get("ticket_id"))
        elif table=="workloads":
            if action=="delete":
                workloads.pop(p.get("id"),None)
            else:
                if p.get("node_id") not in nodes:flag(seq,table,"workload_without_node",p.get("node_id"))
                workloads[p.get("id")]=p
        elif table=="eviction":
            if action=="delete":
                evictions.pop(p.get("id"),None)
            else:
                if p.get("ticket_id") not in tickets:flag(seq,table,"eviction_without_ticket",p.get("ticket_id"))
                if p.get("workload_id") not in workloads:flag(seq,table,"eviction_without_workload",p.get("workload_id"))
                if p.get("target_id") and p.get("target_id") not in nodes:flag(seq,table,"eviction_without_target",p.get("target_id"))
                evictions[p.get("id")]=p
        elif table=="target_receipt":
            if p.get("eviction_id") not in evictions:flag(seq,table,"receipt_without_eviction",p.get("eviction_id"))
        elif table=="volume":
            if action=="delete":
                volumes.pop(p.get("id"),None)
            else:
                if p.get("owner_node") not in nodes:flag(seq,table,"volume_without_owner",p.get("owner_node"))
                if p.get("target_node") and p.get("target_node") not in nodes:flag(seq,table,"volume_without_target",p.get("target_node"))
                volumes[p.get("id")]=p
        elif table=="volume_receipt":
            if p.get("volume_id") not in volumes:flag(seq,table,"volume_receipt_without_volume",p.get("volume_id"))
        elif table=="probes":
            if p.get("node_id") not in nodes:flag(seq,table,"probe_without_node",p.get("node_id"))
        elif table in ("snapshot","stranded","orphan","closure"):
            if p.get("ticket_id") not in tickets:flag(seq,table,table+"_without_ticket",p.get("ticket_id"))
            if table in ("stranded","orphan") and p.get("workload_id") not in workloads:
                flag(seq,table,table+"_without_workload",p.get("workload_id"))
        elif table=="ownership":
            if p.get("workload_id") not in workloads:flag(seq,table,"ownership_without_workload",p.get("workload_id"))
    return problems

def _history_problems(chain):
    problems=[]
    previous=None
    seen=set()
    for item in chain:
        seq=item["seq"]
        if seq in seen:problems.append({"kind":"sequence_reuse","seq":seq})
        seen.add(seq)
        if previous is not None and seq!=previous+1:
            problems.append({"kind":"sequence_gap","seq":seq,"expected":previous+1})
        previous=seq
    return problems

def verify_event_history(events,tables=None):
    """Verify an event list independently of the console that supplied it.

    Checks sequence continuity, per-event linkage to node/ticket, and, when the
    claimed tables are supplied, that replaying the events rebuilds exactly
    those tables.
    """
    normalized=sorted((_normalize_event(e) for e in events),key=lambda r:r["seq"])
    chain=chain_for_events(normalized)
    problems=_history_problems(chain)+_linkage_problems(chain)
    if tables is not None:
        claimed={name:{int(rid):row for rid,row in (tables.get(name) or {}).items()} for name in BUSINESS_TABLES}
        if claimed!=replay_events(normalized):problems.append({"kind":"table_state_mismatch"})
    return {"consistent":not problems,"problems":problems,"events":len(chain),
            "head":chain[-1]["event_hash"] if chain else AUDIT_GENESIS}

def seal_chain(db):
    """Append new events to the sealed chain; never rewrite sealed history."""
    events=audit_events(db)
    chain=chain_for_events(events)
    sealed={r["seq"]:r for r in rows(db,"SELECT * FROM audit_chain")}
    problems=[]
    to_insert=[]
    for item in chain:
        old=sealed.get(item["seq"])
        if old:
            if old["event_hash"]!=item["event_hash"] or old["prev_hash"]!=item["prev_hash"]:
                problems.append({"kind":"history_rewritten","seq":item["seq"]})
            continue
        to_insert.append(item)
    seen={item["seq"] for item in chain}
    for seq in sorted(sealed):
        if seq not in seen:problems.append({"kind":"history_missing","seq":seq})
    if problems:
        return {"sealed":0,"checked":len(chain),"problems":problems}
    if to_insert:
        with tx(db):
            for item in to_insert:
                db.execute("INSERT INTO audit_chain VALUES(?,?,?,?,?,?)",
                           (item["seq"],item["prev_hash"],item["event_hash"],
                            item["event_digest"],item["node_id"],item["ticket_id"]))
    return {"sealed":len(to_insert),"checked":len(chain),"problems":[]}

def verify_chain(db):
    chain=chain_for_events(audit_events(db))
    problems=_history_problems(chain)+_linkage_problems(chain)
    sealed={r["seq"]:r for r in rows(db,"SELECT * FROM audit_chain")}
    seen={item["seq"] for item in chain}
    for item in chain:
        old=sealed.get(item["seq"])
        if old and (old["event_hash"]!=item["event_hash"] or old["prev_hash"]!=item["prev_hash"]):
            problems.append({"kind":"history_rewritten","seq":item["seq"]})
    for seq in sorted(sealed):
        if seq not in seen:problems.append({"kind":"history_missing","seq":seq})
    head=chain[-1]["event_hash"] if chain else AUDIT_GENESIS
    return {"consistent":not problems,"problems":problems,"events":len(chain),
            "head":head,"sealed":len(sealed)}

def verify_replay(db):
    events=audit_events(db)
    reconstructed=replay_events(events)
    mismatches=[]
    for name in BUSINESS_TABLES:
        actual={r["rowid"]:{k:v for k,v in r.items() if k!="rowid"} for r in rows(db,f"SELECT rowid,* FROM {name}")}
        if actual!=reconstructed[name]:mismatches.append(name)
    chain=verify_chain(db)
    return {"consistent":not mismatches and chain["consistent"],
            "mismatches":mismatches,"events":len(events),
            "chain":chain,"problems":chain["problems"]}
