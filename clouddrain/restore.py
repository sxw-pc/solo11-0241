from .store import one,rows,tx

def probe(db,probe_id,node_id,generation,kind,at,healthy):
    if kind not in ("disk","network","runtime"):return "unknown_probe_kind"
    with tx(db):
        db.execute("INSERT INTO probes VALUES(?,?,?,?,?,?)",(probe_id,node_id,generation,kind,at,int(healthy)))
        return "recorded"

def ownership(db,workload_id,tenant,generation,valid_until):
    with tx(db):
        current=one(db,"SELECT * FROM ownership WHERE workload_id=?",(workload_id,))
        if current and generation<=current["generation"]:
            return "stale_ownership"
        db.execute("INSERT INTO ownership VALUES(?,?,?,?) ON CONFLICT(workload_id) DO UPDATE SET tenant=excluded.tenant,generation=excluded.generation,valid_until=excluded.valid_until",(workload_id,tenant,generation,valid_until))
        return "recorded"

def inspect(db,ticket_id):
    with tx(db):
        t=one(db,"SELECT * FROM tickets WHERE id=?",(ticket_id,))
        if not t:return "unknown_ticket"
        db.execute("DELETE FROM stranded WHERE ticket_id=?",(ticket_id,))
        for w in rows(db,"""SELECT w.id FROM snapshot s JOIN workloads w ON w.id=s.workload_id
            WHERE s.ticket_id=? AND w.node_id=?""",(ticket_id,t["node_id"])):
            db.execute("INSERT INTO stranded VALUES(?,?)",(ticket_id,w["id"]))
        for w in rows(db,"""SELECT w.id FROM workloads w LEFT JOIN snapshot s
            ON s.workload_id=w.id AND s.ticket_id=? WHERE w.node_id=? AND s.workload_id IS NULL""",(ticket_id,t["node_id"])):
            db.execute("INSERT OR IGNORE INTO orphan VALUES(?,?,?,?,?)",("auto-"+ticket_id+"-"+w["id"],ticket_id,w["id"],None,"quarantined"))
        count=one(db,"SELECT COUNT(*) n FROM stranded WHERE ticket_id=?",(ticket_id,))["n"]
        unknown=one(db,"SELECT COUNT(*) n FROM orphan WHERE ticket_id=? AND status='quarantined'",(ticket_id,))["n"]
        return "stranded:"+str(count) if count else "orphan:"+str(unknown) if unknown else "empty"

def orphan(db,orphan_id,ticket_id,workload_id,tenant=None,proof=False):
    with tx(db):
        t=one(db,"SELECT * FROM tickets WHERE id=?",(ticket_id,))
        w=one(db,"SELECT * FROM workloads WHERE id=?",(workload_id,))
        if not t or not w or w["node_id"]!=t["node_id"]:return "not_on_ticket_node"
        if one(db,"SELECT * FROM snapshot WHERE ticket_id=? AND workload_id=?",(ticket_id,workload_id)):
            return "already_registered"
        registry=one(db,"SELECT * FROM ownership WHERE workload_id=?",(workload_id,))
        valid=registry and registry["tenant"]==tenant and registry["generation"]==w["generation"] and registry["valid_until"]>=t["end_ts"]
        status="claimed" if tenant and proof and valid else "quarantined"
        old=one(db,"SELECT * FROM orphan WHERE ticket_id=? AND workload_id=?",(ticket_id,workload_id))
        if old:
            db.execute("UPDATE orphan SET tenant=?,status=? WHERE id=?",(tenant,status,old["id"]))
        else:
            db.execute("INSERT INTO orphan VALUES(?,?,?,?,?)",(orphan_id,ticket_id,workload_id,tenant,status))
        return status

def certify(db,ticket_id,at):
    with tx(db):
        t=one(db,"SELECT * FROM tickets WHERE id=?",(ticket_id,))
        if not t or t["status"]!="approved":return "ticket_not_active"
        n=one(db,"SELECT * FROM nodes WHERE id=?",(t["node_id"],))
        inspect_result=inspect_status(db,ticket_id)
        if inspect_result=="stranded":return "stranded_workload"
        if inspect_result=="unregistered":return "orphan_unregistered"
        if inspect_result=="claimed_pending":return "orphan_claimed_pending"
        if one(db,"SELECT id FROM orphan WHERE ticket_id=? AND status='quarantined'",(ticket_id,)):
            return "orphan_quarantined"
        if one(db,"SELECT id FROM volume WHERE owner_node=? AND target_node IS NOT NULL",(n["id"],)):
            return "volume_not_handed_off"
        for kind in ("disk","network","runtime"):
            p=one(db,"""SELECT * FROM probes WHERE node_id=? AND generation=? AND kind=?
               AND at>=? AND at<=? ORDER BY at DESC LIMIT 1""",(n["id"],n["generation"],kind,t["start_ts"],at))
            if not p or not p["healthy"]:return "health_incomplete:"+kind
        db.execute("UPDATE nodes SET schedulable=1 WHERE id=?",(n["id"],))
        db.execute("UPDATE tickets SET status='closed' WHERE id=?",(ticket_id,))
        db.execute("INSERT INTO closure VALUES(?,?,?) ON CONFLICT(ticket_id) DO UPDATE SET status=excluded.status,reason=excluded.reason",(ticket_id,"closed","all_evidence_passed"))
        return "closed"

def inspect_status(db,ticket_id):
    t=one(db,"SELECT * FROM tickets WHERE id=?",(ticket_id,))
    if not t:return "unknown_ticket"
    live=one(db,"""SELECT w.id FROM snapshot s JOIN workloads w ON w.id=s.workload_id
       WHERE s.ticket_id=? AND w.node_id=? LIMIT 1""",(ticket_id,t["node_id"]))
    if live:return "stranded"
    claimed=one(db,"""SELECT w.id FROM workloads w JOIN orphan o ON o.workload_id=w.id
       WHERE o.ticket_id=? AND o.status='claimed' AND w.node_id=? LIMIT 1""",(ticket_id,t["node_id"]))
    if claimed:return "claimed_pending"
    unknown=one(db,"""SELECT w.id FROM workloads w LEFT JOIN snapshot s ON s.ticket_id=? AND s.workload_id=w.id
       WHERE w.node_id=? AND s.workload_id IS NULL AND NOT EXISTS
       (SELECT 1 FROM orphan o WHERE o.ticket_id=? AND o.workload_id=w.id) LIMIT 1""",(ticket_id,t["node_id"],ticket_id))
    return "unregistered" if unknown else "empty"
