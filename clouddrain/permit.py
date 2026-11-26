from .store import one,rows,tx

def node(db,node_id,uid,generation,zone,cpu,mem):
    with tx(db):
        old=one(db,"SELECT * FROM nodes WHERE id=?",(node_id,))
        if old and generation<=old["generation"]:return "stale_node_generation"
        db.execute("INSERT INTO nodes(id,uid,generation,zone,cpu,mem) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET uid=excluded.uid,generation=excluded.generation,zone=excluded.zone,cpu=excluded.cpu,mem=excluded.mem,schedulable=1",(node_id,uid,generation,zone,cpu,mem))
        return "accepted"

def guard(db,service,max_down):
    with tx(db):
        db.execute("INSERT INTO guard VALUES(?,?) ON CONFLICT(service) DO UPDATE SET max_down=excluded.max_down",(service,max_down))
        return "accepted"

def approval(db,approval_id,ticket_id,role,actor,until_ts):
    with tx(db):
        db.execute("INSERT INTO approval VALUES(?,?,?,?,?)",(approval_id,ticket_id,role,actor,until_ts))
        return "recorded"

def ticket(db,ticket_id,node_id,node_uid,start_ts,end_ts,urgent=False):
    with tx(db):
        db.execute("INSERT INTO tickets VALUES(?,?,?,?,?,?,?)",(ticket_id,node_id,node_uid,start_ts,end_ts,int(urgent),"draft"))
        return "draft"

def waiver(db,ticket_id,workload_id,approver,reason):
    with tx(db):
        t=one(db,"SELECT * FROM tickets WHERE id=?",(ticket_id,))
        if not t or not t["urgent"] or not approver or len(reason)<8:
            return "invalid_data_loss_waiver"
        db.execute("INSERT INTO data_loss_waiver VALUES(?,?,?,?)",(ticket_id,workload_id,approver,reason))
        return "recorded"

def approve(db,ticket_id,at):
    with tx(db):
        t=one(db,"SELECT * FROM tickets WHERE id=?",(ticket_id,))
        if not t:return "unknown_ticket"
        n=one(db,"SELECT * FROM nodes WHERE id=?",(t["node_id"],))
        if not n or n["uid"]!=t["node_uid"]:return "stale_node_identity"
        if not t["start_ts"]<=at<=t["end_ts"]:return "outside_window"
        if t["status"]=="approved":return "approved"
        if t["urgent"]:
            approvals=rows(db,"SELECT * FROM approval WHERE ticket_id=? AND until_ts>=?",(ticket_id,at))
            roles={x["role"] for x in approvals}
            actors={x["actor"] for x in approvals}
            if len(roles)<2 or len(actors)<2:return "missing_two_roles"
        else:
            for workload in rows(db,"SELECT DISTINCT service FROM workloads WHERE node_id=?",(t["node_id"],)):
                g=one(db,"SELECT * FROM guard WHERE service=?",(workload["service"],))
                if not g:return "missing_service_guard"
                active=one(db,"""SELECT COUNT(DISTINCT t.id) n FROM tickets t JOIN workloads w ON w.node_id=t.node_id
                  WHERE t.status='approved' AND w.service=?""",(workload["service"],))["n"]
                if active>=g["max_down"]:return "protection_budget_exhausted"
        db.execute("UPDATE tickets SET status='approved' WHERE id=?",(ticket_id,))
        db.execute("UPDATE nodes SET schedulable=0 WHERE id=?",(t["node_id"],))
        db.execute("INSERT OR IGNORE INTO snapshot SELECT ?,id FROM workloads WHERE node_id=?",(ticket_id,t["node_id"]))
        # Freeze the source identity that each snapshot workload may hand off.
        # A later receipt is rejected unless the workload still matches this.
        for w in rows(db,"SELECT * FROM workloads WHERE node_id=?",(n["id"],)):
            db.execute("""INSERT OR IGNORE INTO source_binding
                (ticket_id,workload_id,node_id,node_uid,workload_generation,version)
                VALUES(?,?,?,?,?,?)""",
                (ticket_id,w["id"],n["id"],n["uid"],w["generation"],w["version"]))
        return "approved"
